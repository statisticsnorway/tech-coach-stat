import logging
from pathlib import Path
import pandas as pd

from fagfunksjoner.log.statlogger import StatLogger

from config.config import settings
from functions.file_abstraction import get_dir_files
from functions.file_abstraction import read_parquet_file
import eimerdb as db
from pandas.api.types import is_datetime64_any_dtype


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


def get_updated_rows(
    new_df: pd.DataFrame, old_df: pd.DataFrame, primary_key: list[str]
) -> pd.DataFrame | None:
    """Find rows where there are differences in at least one column.

    Identifies rows in the new DataFrame that are updated compared to the old
    DataFrame. Only differences in values from corresponding columns are considered.
    Returns the updated rows from the new DataFrame if differences are found; otherwise, returns None.

    Args:
        new_df (pd.DataFrame): The DataFrame containing the new data.
        old_df (pd.DataFrame): The DataFrame containing the old data to compare against.
        primary_key (list[str]): The list of column names used as the (composite) primary key
            to match rows between the two DataFrames. Provide one or more column names.

    Returns:
        A DataFrame containing the rows from `new_df` that have changed, or None if no changes are detected.

    Raises:
        ValueError: If the primary key columns contain duplicate combinations in either
            `new_df` or `old_df`.
        AssertionError: If any primary key column does not exist in either `new_df` or `old_df`.
    """
    # Normalize primary_key to list and validate
    if not isinstance(primary_key, list) or len(primary_key) == 0:
        raise AssertionError("primary_key must be a non-empty list of column names")
    for key in primary_key:
        assert (key in new_df.columns) and (
            key in old_df.columns
        ), f"primary key column {key} not in new_df or old_df"

    # Check uniqueness of the composite key
    if new_df.duplicated(subset=primary_key).any():
        raise ValueError(f"Duplicate composite key {primary_key} in new_df")
    if old_df.duplicated(subset=primary_key).any():
        raise ValueError(f"Duplicate composite key {primary_key} in old_df")

    # Align to common keys and same index
    new_df_idx = new_df.set_index(primary_key, drop=False)
    old_df_idx = old_df.set_index(primary_key, drop=False)
    # Ensure index names are set to the key columns to aid later extraction
    new_df_idx.index.names = primary_key
    old_df_idx.index.names = primary_key
    common_keys = new_df_idx.index.intersection(old_df_idx.index)
    if len(common_keys) == 0:
        return None

    # Align to common columns (exclude primary key columns from value comparisons)
    value_cols = [
        c
        for c in new_df_idx.columns
        if c not in primary_key and c in old_df_idx.columns
    ]
    if not value_cols:
        return None

    # Subset and order identically
    new_aligned = new_df_idx.reindex(common_keys).loc[:, value_cols]
    old_aligned = old_df_idx.reindex(common_keys).loc[:, value_cols]

    # Normalize datetime columns: convert both sides to UTC and drop tz to ignore tz differences
    for col in value_cols:
        if is_datetime64_any_dtype(new_aligned[col]) or is_datetime64_any_dtype(
            old_aligned[col]
        ):
            new_aligned[col] = pd.to_datetime(
                new_aligned[col], utc=True, errors="coerce"
            ).dt.tz_localize(None)
            old_aligned[col] = pd.to_datetime(
                old_aligned[col], utc=True, errors="coerce"
            ).dt.tz_localize(None)

    # Ensure labels are identical before compare (required by pandas)
    assert new_aligned.columns.equals(old_aligned.columns), "Columns are not aligned"
    assert new_aligned.index.equals(old_aligned.index), "Index is not aligned"

    diffs = new_aligned.compare(old_aligned, keep_equal=False, align_axis=0)
    if diffs.empty:
        return None

    # Extract changed rows, for multi-key case
    if isinstance(diffs.index, pd.MultiIndex):
        keys_for_rows_to_update = diffs.index.unique()
        keys_df = keys_for_rows_to_update.to_frame(index=False)
        keys_df.columns = primary_key
        updated_rows = new_df.merge(keys_df, on=primary_key, how="inner")
    else:
        # Single-key case represented as Index
        ids_to_update = diffs.index.unique()
        updated_rows = new_df[new_df[primary_key[0]].isin(ids_to_update)].copy()

    return None if updated_rows.empty else updated_rows


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
    bucket = "arneso-test-bucket"
    db_name = "frost-db"

    # ws_files = get_dir_files(ws_dir, prefix=settings.weather_stations_file_prefix)
    # for file in ws_files:
    #     process_weather_station_file(file, bucket, db_name)

    obs_files = get_dir_files(obs_dir, prefix=settings.observations_file_prefix)
    for file in obs_files:
        process_observation_file(file, bucket, db_name)


if __name__ == "__main__":
    root_logger = StatLogger(logging.INFO)
    run_all()
