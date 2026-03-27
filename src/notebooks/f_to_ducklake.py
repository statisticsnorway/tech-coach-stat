"""Module inserting data into ssb-parquedit (DuckLake) tables for metstat."""

import logging
from pathlib import Path

from fagfunksjoner.log.statlogger import StatLogger
from ssb_parquedit import ParquEdit

from config.config import settings
from functions.file_abstraction import get_dir_files
from functions.file_abstraction import read_parquet_file

logger = logging.getLogger(__name__)


def insert_data(table_name: str, filepath: Path | str, keys: list[str]) -> None:
    """Insert data into existing DuckLake table.

    Insert only new rows.
    """
    con = ParquEdit()
    if not con.exists(table_name):
        raise ValueError(f"Table {table_name} does not exist.")

    logger.info("Reading file file %s", filepath)
    df = read_parquet_file(filepath)
    logger.info("Shape of new dataframe: %s", df.shape)

    old_df = con.view(table_name)
    missing_keys = set(keys) - set(old_df.columns)
    if missing_keys:
        raise ValueError(f"Missing columns in dataframe: {missing_keys}")

    # --- Find new rows (anti-join on keys) ---
    new_df = (
        df.merge(old_df[keys], on=keys, how="left", indicator=True)
        .loc[lambda x: x["_merge"] == "left_only"]
        .drop(columns="_merge")
    )

    if new_df.empty:
        logger.info("No new rows to insert.")
        return

    logger.info("Inserting %d new rows into table %s", len(new_df), table_name)
    con.insert_data(table_name, new_df)
    logger.info("Insert done.")


def run_all() -> None:
    """Run the code in this module."""
    logger.info("Running %s", Path(__file__).name)
    logger.info("Using environment: %s", settings.env_for_dynaconf)
    obs_dir = settings.pre_edit_dir
    ws_dir = settings.inndata_dir

    ws_files = get_dir_files(ws_dir, prefix=settings.weather_stations_file_prefix)
    for file in ws_files:
        insert_data("weather_stations", file, ["id"])

    obs_files = get_dir_files(obs_dir, prefix=settings.observations_file_prefix)
    for file in obs_files:
        insert_data("observations", file, ["sourceId", "observationDate"])


if __name__ == "__main__":
    root_logger = StatLogger(logging.INFO)
    run_all()
