"""This module contains abstractions for working with versions in buckets and files.

The datatype `str` is used to represent files and directories in buckets.
The datatype `pathlib.Path` is used to represent files and directories in a file system.
"""

import re
from pathlib import Path

from .file_abstraction import get_dir_files


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
    _validate_filepath(filepath)
    base_filename = _get_base_filename(filepath)
    suffix = _get_suffix(filepath)
    directory = _get_directory_path(filepath)

    files = get_dir_files(directory)
    pattern = re.compile(rf"^{re.escape(base_filename)}_v(\d+)")

    matching_files = []
    for f in files:
        if _get_suffix(f) == suffix:
            if match := pattern.match(_get_filename(f)):
                # Store tuple of (file, version_number) for easy sorting
                matching_files.append((f, int(match[1])))

    if not matching_files:
        return None

    # Sort by version number and return the path of the last one
    matching_files.sort(key=lambda x: x[1])
    return matching_files[-1][0]


def get_latest_file_date(filepath: Path | str) -> Path | str | None:
    """Returns the latest version of a file based on the date in the filename.

    This function searches for files in the same directory as the given filename that
    start with the same base filename and contain a date denoted by '_p'
    followed by YYYY-MM-DD. It then returns the file with the latest date.

    Args:
        filepath: The path to the file whose latest date version is to be found.

    Returns:
        The latest date version of the file, or None if no such files are found.

    Raises:
        TypeError: If filepath is not of type pathlib.Path or str.
    """
    _validate_filepath(filepath)
    base_filename = _get_base_filename(filepath, strip_date=True)
    suffix = _get_suffix(filepath)
    directory = _get_directory_path(filepath)

    files = get_dir_files(directory)

    # Match the pattern <base_filename>_pYYYY-MM-DD,
    # optionally followed by a version number denoted by '_v' followed by digits.
    pattern = re.compile(
        rf"^{re.escape(base_filename)}_p(\d{{4}}-\d{{2}}-\d{{2}})(?:_v\d+)?"
    )

    matching_files = []
    for f in files:
        if _get_suffix(f) == suffix:
            if match := pattern.match(_get_filename(f)):
                date_str = match[1]
                matching_files.append((f, date_str))

    if not matching_files:
        return None

    # Sort by date string, then by filename to handle multiple versions of same date,
    # and return the last one.
    matching_files.sort(key=lambda x: (x[1], _get_filename(x[0])))
    return matching_files[-1][0]


def get_next_file_version(filepath: Path | str) -> Path | str:
    """Generate the next version filename based on the provided filename.

    This function takes a filename that includes a version number and creates a new
    filename by incrementing that version number. It ensures that the input filename
    is valid and contains a version indicator before generating the new filename.

    Args:
        filepath: The path of the file for which to generate the next version.

    Returns:
        The path of the new filename with the incremented version number.

    Raises:
        AssertionError: If the provided filename does not contain a version number.
        TypeError: If filepath is not of type pathlib.Path or str.
    """
    _validate_filepath(filepath)
    filename = _get_filename(filepath)

    if not re.search(r"_v\d+", filename):
        raise AssertionError(f"Filename {filename} does not contain a version number.")

    # Extract the version number, increment it, and create a new filename
    new_filename = re.sub(
        r"_v(\d+)", lambda match: f"_v{int(match.group(1)) + 1}", filename
    )

    directory = _get_directory_path(filepath)
    if isinstance(filepath, Path):
        return directory / new_filename
    return f"{directory}{new_filename}"


# --- Private Helper Functions ---


def _get_filename(path: Path | str) -> str:
    """Return the filename part of the path."""
    return path.name if isinstance(path, Path) else path.split("/")[-1]


def _get_suffix(path: Path | str) -> str:
    """Return the suffix of the path, including the leading dot."""
    if isinstance(path, Path):
        return path.suffix
    return "." + path.rsplit(".", 1)[-1] if "." in path else ""


def _get_base_filename(path: Path | str, strip_date: bool = False) -> str:
    """Return the filename part of the path, with suffix, version and date removed."""
    filename = _get_filename(path)
    # Remove extension
    base = filename.rsplit(".", 1)[0] if "." in filename else filename
    # Remove version number if the base ends with one
    base = re.sub(r"_v\d+$", "", base)
    # Remove date if the base ends with one and strip date is true
    return re.sub(r"_p\d{4}-\d{2}-\d{2}$", "", base) if strip_date else base


def _get_directory_path(path: Path | str) -> Path | str:
    """Return the directory part of the path. Ends with / for strings (GCS)."""
    if isinstance(path, Path):
        return path.parent
    if "/" not in path:
        return "./"
    directory = path.rsplit("/", 1)[0]
    return directory if directory.endswith("/") else f"{directory}/"


def _validate_filepath(filepath: Path | str) -> None:
    """Ensure the filepath is a Path or str."""
    if not isinstance(filepath, (Path, str)):
        raise TypeError("Expected filepath to be of type Path or str.")
