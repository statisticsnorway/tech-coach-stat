"""Module creating a ssb-parquedit (DuckLake) tables for metstat.

The easiest way to create the ssb-parqedit tables is based on
dataframes or parquet files. This module creates the dataframes and
create the tables based on them.
"""

import logging
from pathlib import Path

from fagfunksjoner.log.statlogger import StatLogger
from ssb_parquedit import ParquEdit

from config.config import settings
from functions.file_abstraction import get_dir_files
from functions.file_abstraction import read_parquet_file

logger = logging.getLogger(__name__)


def create_table(table_name: str, filepath: Path | str) -> None:
    """Create DuckLake table."""
    logger.info("Reading file file %s", filepath)
    df = read_parquet_file(filepath)
    logger.info("Shape of dataframe: %s", df.shape)

    con = ParquEdit()
    tables = con.list_tables()
    logger.info("Number of tables before creating %s: %d", table_name, len(tables))
    if table_name not in tables:
        con.create_table(table_name, df, settings.short_name)
        logger.info("Creating new table: %s", table_name)
    else:
        logger.info("Table: %s already exist", table_name)

    tables = con.list_tables()
    logger.info("Tables after: %s", tables)


def run_all() -> None:
    """Run the code in this module."""
    logger.info("Running %s", Path(__file__).name)
    logger.info("Using environment: %s", settings.env_for_dynaconf)
    obs_dir = settings.pre_edit_dir
    ws_dir = settings.inndata_dir

    ws_files = get_dir_files(ws_dir, prefix=settings.weather_stations_file_prefix)
    if ws_files:
        create_table("weather_stations", ws_files[0])

    obs_files = get_dir_files(obs_dir, prefix=settings.observations_file_prefix)
    if obs_files:
        create_table("observations", obs_files[0])


if __name__ == "__main__":
    root_logger = StatLogger(logging.INFO)
    run_all()
