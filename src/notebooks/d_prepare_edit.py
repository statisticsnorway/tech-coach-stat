import logging
from pathlib import Path

import pandas as pd
from fagfunksjoner.log.statlogger import StatLogger

from config.config import settings
from functions.file_abstraction import create_dir_if_not_exist
from functions.file_abstraction import get_dir_files_bucket
from functions.file_abstraction import get_dir_files_filesystem
from functions.file_abstraction import read_parquet_file
from functions.file_abstraction import replace_directory
from functions.file_abstraction import write_parquet_file


logger = logging.getLogger(__name__)


def process_observation_file(filepath: Path | str, target_dir: Path | str) -> None:
    """Prepare an observation file for editing.

    Transform observations to a wide format:
    - Add observationDate (date part of observationTime)
    - One row per sourceId and observationDate
    - One column per elementId containing its value.
    """
    logger.info("Processing observation file %s", filepath)
    observations = read_parquet_file(filepath)

    # Ensure observationTime is datetime with timezone (UTC) and derive observationDate
    if "observationTime" not in observations.columns:
        logger.error("Column 'observationTime' not found in observations")
        print(observations.head())
        return

    obs_time = pd.to_datetime(
        observations["observationTime"], utc=True, errors="coerce"
    )
    observations["observationDate"] = obs_time.dt.date

    # Pivot to wide format: index=[sourceId, observationDate], columns=elementId, values=value
    required_cols = {"sourceId", "elementId", "value", "observationDate"}
    missing = required_cols - set(observations.columns)
    if missing:
        logger.error("Missing required columns for pivot: %s", missing)
        print(observations.head())
        return

    wide = observations.pivot_table(
        index=["sourceId", "observationDate"],
        columns="elementId",
        values="value",
        aggfunc="mean",
    ).sort_index()

    # Flatten columns and bring sourceId/observationDate back as columns
    wide.columns.name = None
    wide = wide.reset_index()

    logger.info("Transformed to wide format with shape %s", wide.shape)
    target_path = replace_directory(filepath, target_dir)
    write_parquet_file(target_path, wide)
    logger.info("Saving file %s", target_dir)


def get_directory_files(directory: Path | str, prefix: str) -> list[Path] | list[str]:
    """Get the list of files with a given prefix in a directory."""
    if isinstance(directory, Path):
        return get_dir_files_filesystem(directory, prefix)
    elif isinstance(directory, str):
        return get_dir_files_bucket(directory, prefix)
    else:
        raise TypeError("Type must be Path or string.")


def run_all() -> None:
    """Run the code in this module."""
    logger.info("Running %s", Path(__file__).name)
    logger.info("Using environment: %s", settings.env_for_dynaconf)
    source_dir = settings.inndata_dir
    target_dir = settings.pre_edit_dir
    create_dir_if_not_exist(target_dir)

    observation_files = get_directory_files(
        source_dir, settings.observations_file_prefix
    )
    if len(observation_files) > 0:
        process_observation_file(
            observation_files[-1], target_dir
        )  # Process the latest observation file


if __name__ == "__main__":
    root_logger = StatLogger()
    run_all()
