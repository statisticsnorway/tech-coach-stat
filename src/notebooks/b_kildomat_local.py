"""Kildomat-file for running locally.

This file does the same as `kildomat.py`, but is used when is running locally.
It requires that the environment `local_files` in `config.py` is used.
"""

import logging

from kildomat import main

from functions.config import settings
from functions.file_abstraction import create_dir_if_not_exist


def run_all() -> None:
    """Run the code in this module.

    Scan the kildedata directory for files and feed each of them to the kildomat.
    """
    if settings.env_for_dynaconf != "local_files":
        raise RuntimeError("Kildomat_local only works when environment is local_files")

    print("Running kildomat_local.py")
    source_dir = settings.kildedata_root_dir
    logging.info(f"Scanning files in directory: {source_dir}")
    target_dir = settings.pre_inndata_dir
    create_dir_if_not_exist(target_dir)

    for filepath in source_dir.iterdir():
        if filepath.is_file():
            main(filepath, target_dir)


if __name__ == "__main__":
    run_all()
