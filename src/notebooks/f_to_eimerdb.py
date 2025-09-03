import logging
from pathlib import Path
import pandas as pd

from fagfunksjoner.log.statlogger import StatLogger

from config.config import settings
from functions.file_abstraction import get_dir_files
from functions.file_abstraction import read_parquet_file
import eimerdb as db


logger = logging.getLogger(__name__)


def process_observation_file(filepath: Path | str) -> None:
    """Load an observation file into eimerdb."""
    logger.info("Processing observation file %s", filepath)
    observations = read_parquet_file(filepath)
    logger.info("Shape of observations: %s", observations.shape)


def process_weather_station_file(
    filepath: Path | str, bucket: str, db_name: str
) -> None:
    """Load a weather station file into eimerdb."""
    logger.info("Processing weather station file %s", filepath)
    weather_stations = read_parquet_file(filepath)
    logger.info("Shape of weather_stations: %s", weather_stations.shape)

    frostdb = db.EimerDBInstance(bucket, db_name)

    db_weather_stations = get_db_table(frostdb, "weather_stations")

    # Handle new weather stations
    new_weather_stations = weather_stations[
        ~weather_stations["id"].isin(db_weather_stations["id"])
    ].copy()

    if len(new_weather_stations) > 0:
        if "validTo" not in new_weather_stations.columns:
            new_weather_stations["validTo"] = pd.Series(
                pd.NaT, dtype=pd.DatetimeTZDtype(tz="UTC")
            )
        logger.info("Shape of new weather stations: %s", new_weather_stations.shape)

        frostdb.insert("weather_stations", new_weather_stations)
        get_db_table(frostdb, "weather_stations", extra_text="after insert")
    else:
        logger.info("No new weather stations in %s", filepath)

    # Handle existing weather stations
    existing_weather_stations = weather_stations[
        weather_stations["id"].isin(db_weather_stations["id"])
    ].copy()

    # Find changed, non-edited weather stations and update them
    none_edited_ws = get_db_table(frostdb, "weather_stations", edited=False)
    db_only_columns = set(none_edited_ws.columns) - set(
        existing_weather_stations.columns
    )
    cleaned_none_edited_ws = none_edited_ws.drop(columns=db_only_columns)

    # Merge existing and non-edited weather stations to find differences
    merged_ws = existing_weather_stations.merge(
        cleaned_none_edited_ws, on="id", how="inner", suffixes=("_new", "_old")
    )

    # Find rows where at least one column differs (excluding "id")
    mask = (
        merged_ws.filter(like="_new").values != merged_ws.filter(like="_old").values
    ).any(axis=1)

    # Get the differing rows from df1
    diff = existing_weather_stations[
        existing_weather_stations["id"].isin(merged_ws.loc[mask, "id"])
    ]

    # Get common columns except 'id' to compare
    compare_cols = [col for col in existing_weather_stations.columns if col != "id"]

    # Filter rows where any column values differ
    diff_mask = False
    for col in compare_cols:
        diff_mask = diff_mask | (merged_ws[f"{col}_new"] != merged_ws[f"{col}_old"])

    # Create update dataframe with rows that need updating
    update_ws = existing_weather_stations[
        existing_weather_stations["id"].isin(merged_ws.loc[diff_mask, "id"])
    ].copy()

    logger.info("Shape of weather stations to update: %s", update_ws.shape)


def get_db_table(
    conn: db.EimerDBInstance,
    table_name: str,
    edited: bool | None = None,
    extra_text: str | None = None,
) -> pd.DataFrame:
    """Get a database table as a pandas Dataframe.

    Args:
        conn: The database connection instance to execute the query.
        table_name: The name of the table to query.
        edited: Optional flag for returning edited or unedited rows. When None, all rows are returned.
        extra_text: Optional additional text to include in the log message.

    Returns:
        A DataFrame containing all rows from the specified table.
    """
    if edited is None:
        db_table_df = conn.query(f"SELECT * FROM {table_name}")
        logger.info("Shape of db_%s %s: %s", table_name, extra_text, db_table_df.shape)
    else:
        db_table_df = conn.query(f"SELECT * FROM {table_name}", unedited=not edited)
        logger.info(
            "Shape of db_%s, edited=%s %s: %s",
            table_name,
            edited,
            extra_text,
            db_table_df.shape,
        )
    return db_table_df


def run_all() -> None:
    """Run the code in this module."""
    logger.info("Running %s", Path(__file__).name)
    logger.info("Using environment: %s", settings.env_for_dynaconf)
    obs_dir = settings.pre_edit_dir
    ws_dir = settings.inndata_dir

    ws_files = get_dir_files(ws_dir, prefix=settings.weather_stations_file_prefix)
    for file in ws_files:
        process_weather_station_file(file, "arneso-test-bucket", "frost-db")

    obs_files = get_dir_files(obs_dir, prefix=settings.observations_file_prefix)
    for file in obs_files:
        process_observation_file(file)


if __name__ == "__main__":
    root_logger = StatLogger(logging.INFO)
    run_all()
