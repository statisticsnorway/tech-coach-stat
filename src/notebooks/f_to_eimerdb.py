import logging
from pathlib import Path

import eimerdb as db
import pandas as pd
from fagfunksjoner.log.statlogger import StatLogger

from config.config import settings
from functions.file_abstraction import get_dir_files
from functions.file_abstraction import read_parquet_file
from functions.query import get_updated_rows


logger = logging.getLogger(__name__)


def process_observation_file(filepath: Path | str, bucket: str, db_name: str) -> None:
    """Load an observation file into eimerdb."""
    logger.info("Processing observation file %s", filepath)
    observations = read_parquet_file(filepath)
    logger.info("Shape of observations: %s", observations.shape)

    frostdb = db.EimerDBInstance(bucket, db_name)

    db_observations = get_db_table(frostdb, "observations")
    non_edited_obs = get_db_table(frostdb, "observations", edited=False)

    # Handle new observations, rows with a different composite key (sourceId, observationDate)
    key_cols = ["sourceId", "observationDate"]
    for df in (
        observations,
        db_observations,
    ):  # Normalize observationDate to UTC to ensure consistent comparisons
        df["observationDate"] = pd.to_datetime(
            df["observationDate"], utc=True, errors="coerce"
        )
    merged = observations.merge(
        db_observations[key_cols].drop_duplicates(),
        on=key_cols,
        how="left",
        indicator=True,
    )
    new_observations = (
        merged.loc[merged["_merge"] == "left_only"].drop(columns="_merge").copy()
    )

    if len(new_observations) > 0:
        logger.info("Shape of new observations: %s", new_observations.shape)
        frostdb.insert("observations", new_observations)
        get_db_table(frostdb, "observations", extra_text="after insert")
    else:
        logger.info("No new observations in %s", filepath)

    # Handle existing observations
    existing_obs = merged.loc[merged["_merge"] == "both"].drop(columns="_merge").copy()

    # Find changed, non-edited weather stations and update them
    db_only_columns = set(non_edited_obs.columns) - set(existing_obs.columns)
    cleaned_non_edited_obs = non_edited_obs.drop(columns=db_only_columns)

    rows_to_update = get_updated_rows(existing_obs, cleaned_non_edited_obs, key_cols)

    if rows_to_update is not None:
        logger.info("Shape of observations to update: %s", rows_to_update.shape)

        # Generate and execute UPDATE statements for each row
        for _, row in rows_to_update.iterrows():
            set_clauses = []
            for column in row.index:
                if column not in key_cols:  # Skip the primary key
                    value = row[column]
                    if pd.isna(value):
                        set_clauses.append(f"{column} = NULL")
                    elif isinstance(value, pd.Timestamp | pd.DatetimeTZDtype):
                        set_clauses.append(f"{column} = '{value}'")
                    elif isinstance(value, str):
                        set_clauses.append(f"{column} = '{value}'")
                    else:
                        set_clauses.append(f"{column} = {value}")

            update_query = f"""
                UPDATE observations
                SET {', '.join(set_clauses)}
                WHERE sourceId = '{row['sourceId']}' AND observationDate = '{row['observationDate']}'
            """
            frostdb.query(update_query)

        logger.info("Updated %d observations", len(rows_to_update))


def process_weather_station_file(
    filepath: Path | str, bucket: str, db_name: str
) -> None:
    """Load a weather station file into eimerdb."""
    logger.info("Processing weather station file %s", filepath)
    weather_stations = read_parquet_file(filepath)
    logger.info("Shape of weather_stations: %s", weather_stations.shape)

    frostdb = db.EimerDBInstance(bucket, db_name)

    db_weather_stations = get_db_table(frostdb, "weather_stations")
    non_edited_ws = get_db_table(frostdb, "weather_stations", edited=False)

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
    db_only_columns = set(non_edited_ws.columns) - set(
        existing_weather_stations.columns
    )
    cleaned_non_edited_ws = non_edited_ws.drop(columns=db_only_columns)

    rows_to_update = get_updated_rows(
        existing_weather_stations, cleaned_non_edited_ws, ["id"]
    )
    if rows_to_update is not None:
        logger.info("Shape of weather stations to update: %s", rows_to_update.shape)

        # Generate and execute UPDATE statements for each row
        for _, row in rows_to_update.iterrows():
            set_clauses = []
            for column in row.index:
                if column != "id":  # Skip the primary key
                    value = row[column]
                    if pd.isna(value):
                        set_clauses.append(f"{column} = NULL")
                    elif isinstance(value, pd.Timestamp | pd.DatetimeTZDtype):
                        set_clauses.append(f"{column} = '{value}'")
                    elif isinstance(value, str):
                        set_clauses.append(f"{column} = '{value}'")
                    else:
                        set_clauses.append(f"{column} = {value}")

            update_query = f"""
                UPDATE weather_stations
                SET {', '.join(set_clauses)}
                WHERE id = '{row['id']}'
            """
            frostdb.query(update_query)

        logger.info("Updated %d weather stations", len(rows_to_update))


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
    # bucket = "arneso-test-bucket"
    bucket = "ssb-tip-tutorials-data-produkt-prod"
    db_name = "frost-db"

    ws_files = get_dir_files(ws_dir, prefix=settings.weather_stations_file_prefix)
    for file in ws_files:
        process_weather_station_file(file, bucket, db_name)

    obs_files = get_dir_files(obs_dir, prefix=settings.observations_file_prefix)
    for file in obs_files:
        process_observation_file(file, bucket, db_name)


if __name__ == "__main__":
    root_logger = StatLogger(logging.INFO)
    run_all()
