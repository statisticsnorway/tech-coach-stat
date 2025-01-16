from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast

import pandas as pd
import pandera as pa
from isodate import parse_duration
from pandera.typing import DataFrame

from functions.config import settings
from functions.file_abstraction import add_filename_to_path
from functions.file_abstraction import create_dir_if_not_exist
from functions.file_abstraction import directory_diff
from functions.file_abstraction import read_parquet_file
from functions.file_abstraction import replace_directory
from functions.file_abstraction import write_parquet_file
from functions.versions import get_latest_file_version
from schemas.observation_schemas import ObservationInndataSchema
from schemas.weather_station_schemas import WeatherStationInndataSchema


def get_latest_weather_stations() -> pd.DataFrame:
    """Fetches the latest weather stations data from a specified parquet file.

    Raises:
        FileNotFoundError: If the file is not found or is not readable.

    Returns:
        A dataframe containing the data from the latest weather stations parquet file.
    """
    base_file = add_filename_to_path(
        settings.pre_inndata_dir, "weather_stations.parquet"
    )
    if latest_file := get_latest_file_version(base_file):
        return read_parquet_file(latest_file)
    else:
        raise FileNotFoundError(f"File {latest_file!s} not found or not readable")


@pa.check_types(lazy=True)
def transform_ws_to_inndata(df: pd.DataFrame) -> DataFrame[WeatherStationInndataSchema]:
    """Transforms a weather stations dataframe to inndata and validates the data.

    Convert municipalityId and countyId to column names defined by Standardutvalget.
    Also convert type to from int to str with leading zeros, as required by Klass.
    See https://ssbno.sharepoint.com/sites/Avdelingerutvalgograd/SitePages/Vedtak-fra-Standardutvalget.aspx#standardnavn-for-enhetstypeidentifikatorer

    The transformed dataframe is validated according to the WeatherStationInndataSchema.

    Args:
        df: A DataFrame containing weather station records.
            Assumes columns `municipalityId` and `countyId` are present and need
            to be transformed.

    Returns:
        Transformed DataFrame adhering to the WeatherStationInndataSchema.
    """
    df["komm_nr"] = df["municipalityId"].astype(str).str.zfill(4).astype("string")
    df["fylke_nr"] = df["countyId"].astype(str).str.zfill(2).astype("string")

    # The pipe is used to cast the data type so that mypy understands the type
    return df.pipe(DataFrame[WeatherStationInndataSchema])


@pa.check_types(lazy=True)
def transform_obs_to_inndata(df: pd.DataFrame) -> DataFrame[ObservationInndataSchema]:
    """Transforms an observations dataframe to inndata and validates the data.

    The transformed dataframe is validated according to the ObservationInndataSchema.

    Args:
        df: A DataFrame containing observation records.

    Returns:
        Transformed DataFrame adhering to the ObservationInndataSchema.
    """
    # Remove everything after the first ':' in sourceId
    # The weather station is the part before the ':'
    df["sourceId"] = df["sourceId"].apply(lambda s: s.split(":", 1)[0])

    df["observationTime"] = df.apply(_calc_observation_time, axis=1)

    # Remove unneeded columns, reorder and sort
    column_order = ["sourceId", "elementId", "observationTime", "value", "unit"]
    df = df[column_order].sort_values(by=["sourceId", "elementId", "observationTime"])
    df.reset_index(drop=True, inplace=True)

    return df.pipe(DataFrame[ObservationInndataSchema])


def _calc_observation_time(row: pd.Series) -> datetime:
    """Calculate observation time based on referenceTime and timeOffset columns."""
    reference_time = cast(datetime, row["referenceTime"])
    time_offset = cast(timedelta, parse_duration(row["timeOffset"]))
    return reference_time + time_offset


def handle_validation_errors(df: pd.DataFrame, errors: pa.errors.SchemaErrors) -> None:
    """Handle and log validation errors from the schema validation process.

    Display the relevant rows from the provided DataFrame that failed validation,
    along with associated failure details.

    Args:
        df: The DataFrame that was validated.
        errors: The schema validation errors encountered during the validation
            process, including details about the failure cases.
    """
    print(errors)
    failure_cases = errors.failure_cases
    failed_indices = failure_cases["index"].unique()
    columns_to_print = ["id", "name", "municipalityId", "countyId"]
    failed_rows = df.loc[failed_indices, columns_to_print]
    print("Failed Rows:")
    print(failed_rows)


def process_weather_station_file(filepath: Path | str, target_dir: Path | str) -> None:
    """Process a weather station file."""
    print(f"Processing file {filepath}")
    weather_stations = read_parquet_file(filepath)
    try:
        inndata_ws_df = transform_ws_to_inndata(weather_stations)
        target_path = replace_directory(filepath, target_dir)
        write_parquet_file(target_path, inndata_ws_df)
        print(f"Saving file {target_path}")
    except pa.errors.SchemaErrors as errors:
        handle_validation_errors(weather_stations, errors)


def process_observation_file(filepath: Path | str, target_dir: Path | str) -> None:
    """Process a weather observations file."""
    print(f"Processing file {filepath}")
    observations = read_parquet_file(filepath)
    try:
        inndata_obs_df = transform_obs_to_inndata(observations)
        target_path = replace_directory(filepath, target_dir)
        write_parquet_file(target_path, inndata_obs_df)
        print(f"Saving file {target_path}")
    except pa.errors.SchemaErrors as errors:
        handle_validation_errors(observations, errors)


def run_all() -> None:
    """Run the code in this module."""
    print(f"Running {Path(__file__).name}")
    source_dir = settings.pre_inndata_dir
    target_dir = settings.inndata_dir
    create_dir_if_not_exist(target_dir)

    new_weather_station_files = directory_diff(source_dir, target_dir, prefix="weather")
    for file in new_weather_station_files:
        process_weather_station_file(file, target_dir)

    new_observations_files = directory_diff(
        source_dir, target_dir, prefix="observations"
    )
    for file in new_observations_files:
        process_observation_file(file, target_dir)


if __name__ == "__main__":
    run_all()
