import logging
from pathlib import Path

import pandas as pd
from fagfunksjoner.log.statlogger import StatLogger
from pandera.errors import SchemaError
from pandera.errors import SchemaErrors

from config.config import settings
from functions.file_abstraction import create_dir_if_not_exist
from functions.file_abstraction import get_dir_files
from functions.file_abstraction import read_parquet_file
from functions.file_abstraction import replace_directory
from functions.file_abstraction import write_parquet_file
from functions.kartverket import administrative_units_from_position
from schemas.weather_station_schemas import WeatherStationKlargjortSchema

logger = logging.getLogger(__name__)


def process_observation_file(filepath: Path | str, target_dir: Path | str) -> None:
    """Automatic edits of an observation file."""
    logger.info("Processing observation file %s", filepath)
    observations = read_parquet_file(filepath)

    # No automatic edits for now

    target_path = replace_directory(filepath, target_dir)
    write_parquet_file(target_path, observations)
    logger.info("Saving file %s", target_dir)


def _get_failed_indices(errors: SchemaErrors | SchemaError) -> pd.Index:
    """Extract the DataFrame indices of rows that failed pandera validation."""
    failure_cases = getattr(errors, "failure_cases", None)
    if not isinstance(failure_cases, pd.DataFrame):
        logger.warning(
            "No failure_cases DataFrame available on %s; cannot identify failing rows.",
            type(errors).__name__,
        )
        return pd.Index([])
    if "index" not in failure_cases.columns:
        logger.warning(
            "failure_cases has no 'index' column; cannot identify failing rows."
        )
        return pd.Index([])
    idx = pd.Index(failure_cases["index"].dropna().unique())
    if idx.empty and not failure_cases.empty:
        logger.warning(
            "All 'index' values in failure_cases are None (likely a default RangeIndex "
            "DataFrame); cannot identify which rows failed."
        )
    return idx


def _log_failed_rows(weather_stations: pd.DataFrame, failed_idx: pd.Index) -> None:
    """Log the rows from weather_stations that failed validation."""
    if failed_idx.empty:
        return
    try:
        failed_rows = weather_stations.loc[failed_idx]
    except KeyError:
        failed_rows = weather_stations.iloc[failed_idx.astype(int)]
    logger.warning("Rows that failed validation:")
    logger.info("\n%s", failed_rows.to_string())


def _fix_komm_fylke_from_geometry(
    weather_stations: pd.DataFrame, errors: SchemaErrors | SchemaError
) -> pd.DataFrame:
    """Fix komm_nr and fylke_nr using geometry_coordinates for rows that fail those checks."""
    failure_cases = getattr(errors, "failure_cases", None)
    if not isinstance(failure_cases, pd.DataFrame):
        logger.warning(
            "No failure_cases DataFrame on %s; skipping komm_nr/fylke_nr fix.",
            type(errors).__name__,
        )
        return weather_stations
    if "column" not in failure_cases.columns:
        logger.warning(
            "failure_cases has no 'column' column (SchemaError without lazy=True?); "
            "skipping komm_nr/fylke_nr fix."
        )
        return weather_stations

    komm_fylke_cols = {"komm_nr", "fylke_nr"}
    rows_to_fix_idx = (
        failure_cases[failure_cases["column"].isin(komm_fylke_cols)]["index"]
        .dropna()
        .unique()
    )

    ws = weather_stations.copy()
    for idx in rows_to_fix_idx:
        try:
            row = ws.loc[idx]
        except KeyError:
            row = ws.iloc[int(idx)]

        coords = row.get("geometry_coordinates")
        if not isinstance(coords, str) or not coords.strip():
            logger.warning("No geometry_coordinates for index %s, skipping fix", idx)
            continue

        coords_clean = coords.strip().strip("[]")
        lon_str, lat_str = coords_clean.split(",")
        lon, lat = float(lon_str.strip()), float(lat_str.strip())
        units = administrative_units_from_position(lat, lon)
        logger.info(
            "Fixing index %s: komm_nr=%s, fylke_nr=%s",
            idx,
            units.kommunenummer,
            units.fylkesnummer,
        )
        ws.loc[idx, "komm_nr"] = units.kommunenummer
        ws.loc[idx, "fylke_nr"] = units.fylkesnummer

    return ws


def process_weather_station_file(filepath: Path | str, target_dir: Path | str) -> None:
    """Automatic edits of a weather stations file."""
    logger.info("Processing weather station file %s", filepath)
    weather_stations = read_parquet_file(filepath)

    # Step 1: Validate and fix komm_nr / fylke_nr errors using geometry
    try:
        WeatherStationKlargjortSchema.validate(weather_stations, lazy=True)
    except (SchemaErrors, SchemaError) as e:
        failed_idx = _get_failed_indices(e)
        logger.warning("Initial validation failed for %d row(s)", len(failed_idx))
        _log_failed_rows(weather_stations, failed_idx)
        weather_stations = _fix_komm_fylke_from_geometry(weather_stations, e)

    # Step 2: Re-validate; drop rows that still fail, then store
    failed_idx = pd.Index([])
    try:
        WeatherStationKlargjortSchema.validate(weather_stations, lazy=True)
    except (SchemaErrors, SchemaError) as e:
        failed_idx = _get_failed_indices(e)
        logger.warning(
            "Re-validation failed for %d row(s) after correction", len(failed_idx)
        )
        _log_failed_rows(weather_stations, failed_idx)

    validated = weather_stations.drop(index=failed_idx, errors="ignore")

    target_path = replace_directory(filepath, target_dir)
    write_parquet_file(target_path, validated)
    logger.info("Saving file %s", target_dir)


def run_all() -> None:
    """Run the code in this module."""
    logger.info("Running %s", Path(__file__).name)
    logger.info("Using environment: %s", settings.env_for_dynaconf)
    source_dir = settings.inndata_dir
    target_dir = settings.pre_edit_dir
    create_dir_if_not_exist(target_dir)

    observation_files = get_dir_files(source_dir, settings.observations_file_prefix)
    for file in observation_files:
        process_observation_file(file, target_dir)

    ws_files = get_dir_files(source_dir, settings.weather_stations_file_prefix)
    for file in ws_files:
        process_weather_station_file(file, target_dir)


if __name__ == "__main__":
    root_logger = StatLogger()
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    run_all()
