import re
from pathlib import Path

from dapla import FileClient


def get_latest_file_version(filepath: str) -> str | None:
    """Returns the latest version of a file based on the version number in the filename.

    This function searches for files in the same directory as the given filename that
    start with the same base filename and contain a version number denoted by '_v'
    followed by digits. It then returns the file with the highest version number.

    Args:
        filepath: The path to the file whose latest version is to be found.

    Returns:
        The latest version of the file, or None if no such files are found.
    """
    base_filename = _get_base_filename(filepath)
    matching_files = _get_matching_files(filepath, base_filename)
    if not matching_files:
        return None

    # Sort files by the number following '_v' in the filename
    pattern = re.compile(rf"^{re.escape(base_filename)}_v(\d+)")
    sorted_files = sorted(
        matching_files,
        key=lambda file: int(pattern.match(get_filename(file))[1]),  # type: ignore[index]
    )
    return sorted_files[-1]  # Return the last one


def get_next_file_version(filepath: str) -> str:
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
    return filepath
    # assert filepath.is_file()
    # assert re.search(r"_v\d+", filepath.name)
    #
    # # Extract the version number, increment it, and create a new filename
    # new_filename = re.sub(
    #     r"_v(\d+)", lambda match: f"_v{int(match.group(1)) + 1}", filepath.name
    # )
    # return filepath.parent / new_filename


def get_filename(filepath: str) -> str:
    """Return the filename part of the filepath."""
    return filepath.split("/")[-1]


def _get_base_filename(filepath: str) -> str:
    """Return the filename part of the path, with the suffix and version removed."""
    filename = get_filename(filepath)

    # Split on the last dot and return the part before it
    base_filename = filename.rsplit(".", 1)[0] if "." in filename else filename

    # Remove the version number if the base_filename ends with one
    return re.sub(r"_v\d+$", "", base_filename)


def _get_matching_files(filepath: str, base_filename: str) -> list[str]:
    """Get files that start with base_filename followed by '_v' and a number."""
    directory_files = _get_directory_files(filepath)
    pattern = re.compile(rf"^{re.escape(base_filename)}_v\d+")
    return [
        file
        for file in directory_files
        if pattern.match(get_filename(file))
        and _get_suffix(file) == _get_suffix(filepath)
    ]


def _get_directory(filepath: str) -> str:
    """Return the directory part of the filepath."""
    return filepath.rsplit("/", 1)[0]


def _get_directory_files(filepath: str) -> list[str]:
    """Return all files in the directory specified by the filepath."""
    fs = FileClient.get_gcs_file_system()
    glob_pattern = f"{_get_directory(filepath)}/*"
    return fs.glob(glob_pattern)  # type: ignore[no-any-return]


def _get_suffix(filepath: str) -> str:
    """Return the suffix, the part after the last `.`, of the filepath."""
    return filepath.split(".")[-1] if "." in filepath else ""
