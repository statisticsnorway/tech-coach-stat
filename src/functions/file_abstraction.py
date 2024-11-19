import json
from pathlib import Path
from typing import Any
from typing import cast

from dapla import FileClient


def write_json_file(filepath: Path | str, data: list[dict[str, Any]]) -> None:
    """Writes dictionaries to a JSON file stored in a GCS bucket or in a local file system.

    Args:
        filepath: The path to the file where the data should be written.
            Use the `pathlib.Path` type if it is a file on a file system.
            Use the `str` type if it is a file stored in a GCS bucket.
        data: The data to be written to the file.

    Raises:
        TypeError: If the `filepath` is not of type `Path` or `str`.
    """
    _validate_filepath(filepath)
    if isinstance(filepath, Path):
        with filepath.open(mode="w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
    elif isinstance(filepath, str):
        with FileClient.gcs_open(filepath, mode="w") as file:
            json.dump(data, file, indent=4)


def read_json_file(filepath: Path | str) -> list[dict[str, Any]]:
    """Reads a JSON file stored in a GCS bucket or in a local file system.

    Args:
        filepath: The path to the file which should be read.
            Use the `pathlib.Path` type if it is a file on a file system.
            Use the `str` type if it is a file stored in a GCS bucket.

    Returns:
        The content of the JSON file.

    Raises:
        TypeError: If the `filepath` is not of type `Path` or `str`.
    """
    _validate_filepath(filepath)
    if isinstance(filepath, Path):
        with filepath.open(encoding="utf-8") as file:
            return cast(list[dict[str, Any]], json.load(file))
    elif isinstance(filepath, str):
        with FileClient.gcs_open(filepath) as file:
            return cast(list[dict[str, Any]], json.load(file))


def add_filename_to_path(filepath: Path | str, filename: str) -> Path | str:
    """Add a filename to a filepath, handling both filepath as Path and str.

    Args:
        filepath: The path to which the filename should be added.
            Use the `pathlib.Path` type if it is a file on a file system.
            Use the `str` type if it is a file stored in a GCS bucket.
        filename: The filename to add to the filepath.

    Returns:
        A filepath with the filename added.

    Raises:
        TypeError: If the `filepath` is not of type `Path` or `str`.
    """
    _validate_filepath(filepath)
    if isinstance(filepath, Path):
        return filepath / filename
    elif isinstance(filepath, str):
        return f"{filepath}/{filename}"


def create_dir_if_not_exist(directory: Path | str) -> None:
    """Create directory if it does not exist, handling both directory as Path and str.

    The function handles the case on DaplaLab where the first two levels of the
    directory path are read-only, for example `/bucket/produkt`.

    If the type is `str`, that means representing a path in a GCS bucket, there is
    no need to do anything. Since directories does not exist in a bucket.

    Args:
        directory: The directory to check or create.
            Use the `pathlib.Path` type if it is a file on a file system.
            Use the `str` type if it is a file stored in a GCS bucket.
    """
    if isinstance(directory, Path):
        if str(directory).startswith("/buckets"):
            parts = directory.parts
            if len(parts) < 3:
                raise ValueError("The provided path must have at least three levels.")

            # Construct the writable path starting from the third level
            writable_path = Path(*parts[:2]) / Path(*parts[2:])
            if not writable_path.exists():
                writable_path.mkdir(parents=True, exist_ok=True)
        else:
            directory.mkdir(parents=True, exist_ok=True)


def _validate_filepath(filepath: Path | str) -> None:
    if not isinstance(filepath, Path | str):
        raise TypeError("Expected filepath to be of type Path or str.")
