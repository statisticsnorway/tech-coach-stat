import logging
from pathlib import Path

import pandas as pd
from fagfunksjoner.log.statlogger import StatLogger

from config.config import settings
from functions.file_abstraction import create_dir_if_not_exist
from functions.file_abstraction import get_dir_files
from functions.file_abstraction import read_parquet_file
from functions.file_abstraction import replace_directory
from functions.file_abstraction import write_parquet_file

logger = logging.getLogger(__name__)


def process_observation_file(filepath: Path | str, target_dir: Path | str) -> None:
    """Automatic edits of an observation file."""
    logger.info("Processing observation file %s", filepath)
    observations = read_parquet_file(filepath)

    # No automatic edits for now

    target_path = replace_directory(filepath, target_dir)
    write_parquet_file(target_path, observations)
    logger.info("Saving file %s", target_dir)


def process_weather_station_file(filepath: Path | str, target_dir: Path | str) -> None:
    """Automatic edits of a weather stations file."""
    logger.info("Processing weather station file %s", filepath)
    weather_stations = read_parquet_file(filepath)

    # No automatic edits for now

    target_path = replace_directory(filepath, target_dir)
    write_parquet_file(target_path, weather_stations)
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
    run_all()
