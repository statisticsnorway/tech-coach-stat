import logging
from pathlib import Path

from fagfunksjoner.log.statlogger import StatLogger

from config.config import settings
from functions.file_abstraction import get_dir_files_bucket
from functions.file_abstraction import get_dir_files_filesystem
from functions.file_abstraction import read_parquet_file


logger = logging.getLogger(__name__)


def process_observation_file(filepath: Path | str) -> None:
    """Process an observation file."""
    logger.info("Processing observation file %s", filepath)
    observations = read_parquet_file(filepath)
    print(observations.info())


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

    observation_files = get_directory_files(
        source_dir, settings.observations_file_prefix
    )
    if len(observation_files) > 0:
        process_observation_file(
            observation_files[-1]
        )  # Process the latest observation file


if __name__ == "__main__":
    root_logger = StatLogger()
    run_all()
