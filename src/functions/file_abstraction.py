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
    if isinstance(filepath, Path):
        with filepath.open(mode="w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
    elif isinstance(filepath, str):
        with FileClient.gcs_open(filepath, mode="w") as file:
            json.dump(data, file, indent=4)
    else:
        raise TypeError("Expected filepath to be of type Path or str.")


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
    if isinstance(filepath, Path):
        with filepath.open(encoding="utf-8") as file:
            return cast(list[dict[str, Any]], json.load(file))
    elif isinstance(filepath, str):
        with FileClient.gcs_open(filepath) as file:
            return cast(list[dict[str, Any]], json.load(file))
    else:
        raise TypeError("Expected filepath to be of type Path or str.")


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
    if isinstance(filepath, Path):
        return filepath / filename
    elif isinstance(filepath, str):
        return f"{filepath}/{filename}"
    else:
        raise TypeError("Expected filepath to be of type Path or str.")
