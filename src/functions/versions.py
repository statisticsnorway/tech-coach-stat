"""This module contains abstractions for working with versions in buckets and files.

The datatype `str` is used to represent files and directories in buckets.
The datatype `pathlib.Path` is used represent files and directories in a file system.
"""

from pathlib import Path

from ._versions_bucket import get_latest_file_version as get_latest_file_version_bucket
from ._versions_bucket import get_next_file_version as get_next_file_version_bucket
from ._versions_pathlib import (
    get_latest_file_version as get_latest_file_version_pathlib,
)
from ._versions_pathlib import get_next_file_version as get_next_file_version_pathlib


def get_latest_file_version(filepath: Path | str) -> Path | str | None:
    """Returns the latest version of a file based on the version number in the filename.

    This function searches for files in the same directory as the given filename that
    start with the same base filename and contain a version number denoted by '_v'
    followed by digits. It then returns the file with the highest version number.

    Args:
        filepath: The path to the file whose latest version is to be found.

    Returns:
        The latest version of the file, or None if no such files are found.

    Raises:
        TypeError: If filepath is not of type pathlib.Path or str.
    """
    if isinstance(filepath, Path):
        return get_latest_file_version_pathlib(filepath)
    elif isinstance(filepath, str):
        return get_latest_file_version_bucket(filepath)
    else:
        raise TypeError("Filepath is not of type pathlib.Path or str.")


def get_next_file_version(filepath: Path | str) -> Path | str:
    """Generate the next version filename based on the provided filename.

    This function takes a filename that includes a version number and creates a new
    filename by incrementing that version number. It ensures that the input filename
    is valid and contains a version indicator before generating the new filename.

    Args:
        filepath: The path of the file for which to generate the next version.

    Returns:
        Path: The path of the new filename with the incremented version number.

    Raises:
        AssertionError: If the provided filename is not a file or does not contain
        a version number.
    """
    if isinstance(filepath, Path):
        return get_next_file_version_pathlib(filepath)
    elif isinstance(filepath, str):
        return get_next_file_version_bucket(filepath)
    else:
        raise TypeError("Filepath is not of type pathlib.Path or str.")
