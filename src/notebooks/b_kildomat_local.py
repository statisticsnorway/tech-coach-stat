"""Kildomat-file for running locally.

This file does the same as `kildomat.py`, but is used when is running locally.
It requires that the environment `local_files` in `config.py` is used.
"""

import logging
from pathlib import Path

from b_kildomat import main
from fagfunksjoner.log.statlogger import StatLogger

from config.config import settings
from functions.file_abstraction import create_dir_if_not_exist


logger = logging.getLogger(__name__)


def run_all() -> None:
    """Run the code in this module.

    Scan the kildedata directory for files and feed each of them to the kildomat.
    """
    logger.info("Running %s", Path(__file__).name)
    if settings.env_for_dynaconf != "local_files":
        raise RuntimeError("Kildomat_local only works when environment is local_files")
    logger.info("Using environment: %s", settings.env_for_dynaconf)

    source_dir = settings.kildedata_root_dir
    logger.info("Scanning files in directory: %s", source_dir)
    target_dir = settings.pre_inndata_dir
    create_dir_if_not_exist(target_dir)

    for filepath in source_dir.iterdir():
        if filepath.is_file():
            main(filepath, target_dir)


if __name__ == "__main__":
    root_logger = StatLogger()
    run_all()
