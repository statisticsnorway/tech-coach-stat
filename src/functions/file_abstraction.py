"""This module contains file abstractions for working with buckets and files.

The datatype `str` is used to represent files and directories in buckets.
The datatype `pathlib.Path` is used represent files and directories in a file system.
"""

import json
from pathlib import Path
from typing import Any
from typing import cast

import dapla as dp
import gcsfs
import pandas as pd
from dapla import FileClient


GS_URI_PREFIX = "gs://"


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


def write_parquet_file(filepath: Path | str, df: pd.DataFrame) -> None:
    """Writes a dataframe to a parquet file stored in a GCS bucket or in a local file system.

    Args:
        filepath: The path to the file where the data should be written.
            Use the `pathlib.Path` type if it is a file on a file system.
            Use the `str` type if it is a file stored in a GCS bucket.
        df: The dataframe to be written to the file.

    Raises:
        TypeError: If the `filepath` is not of type `Path` or `str`.
    """
    _validate_filepath(filepath)
    if isinstance(filepath, Path):
        df.to_parquet(filepath)
    elif isinstance(filepath, str):
        dp.write_pandas(df=df, gcs_path=filepath)


def read_parquet_file(filepath: Path | str) -> pd.DataFrame:
    """Read a parquet file stored in a GCS bucket or in a local file system to a dataframe.

    Args:
        filepath: The path to the file which should be read.
            Use the `pathlib.Path` type if it is a file on a file system.
            Use the `str` type if it is a file stored in a GCS bucket.

    Returns:
        The content of the parquet file.

    Raises:
        TypeError: If the `filepath` is not of type `Path` or `str`.
    """
    _validate_filepath(filepath)
    if isinstance(filepath, Path):
        return pd.read_parquet(filepath)
    elif isinstance(filepath, str):
        result = dp.read_pandas(gcs_path=filepath)
        if not isinstance(result, pd.DataFrame):
            raise TypeError("Expected a pandas DataFrame but got a different type")
        return result


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
        filepath = filepath.rstrip("/")  # Remove trailing slash id there is one
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


def directory_diff(
    source_dir: Path | str,
    target_dir: Path | str,
    prefix: str | None = None,
) -> set[Path] | set[str]:
    """Compares the contents of two directories and identifies files that exist in the source directory but not in the target directory.

    The function supports comparison both for files in a local file system
    (Paths) and for files in cloud storage buckets (strings).

    The datatype `str` is used to represent directories in buckets.
    The datatype `pathlib.Path` is used represent directories in a file system.
    The `source_dir` and `target_dir` must have the same type.

    Args:
        source_dir: The source directory to compare.
        target_dir: The target directory to compare.
        prefix: An optional string to filter filenames that start with this prefix.

    Returns:
        A set of files that are present in the source directory but not in the target directory.

    Raises:
        ValueError: If either of the provided arguments is not of type Path or str.
    """
    if isinstance(source_dir, Path) and isinstance(target_dir, Path):
        source_file_paths = get_dir_files_filesystem(source_dir, prefix)
        target_file_paths = get_dir_files_filesystem(target_dir, prefix)
        return set(source_file_paths) - set(target_file_paths)

    elif isinstance(source_dir, str) and isinstance(target_dir, str):
        source_file_strings = get_dir_files_bucket(source_dir, prefix)
        target_file_strings = get_dir_files_bucket(target_dir, prefix)
        return set(source_file_strings) - set(target_file_strings)
    else:
        raise ValueError("Both source_dir and target_dir must be of type Path or str.")


def get_dir_files_bucket(directory: str, prefix: str | None = None) -> list[str]:
    """Get a list of files within the specified directory in a GCS bucket.

    This function retrieves all files in the provided GCS directory path. It ensures
    the path ends with a forward slash and that the directory exists before processing.
    Only files at the given directory level (not within subdirectories) are included
    in the returned list.

    If the optional `prefix` is defined, only filenames starting with this prefix is
    returned.

    Args:
        directory: The GCS bucket directory path. Must end with a forward slash (/).
        prefix: An optional string to filter filenames that start with this prefix.

    Returns:
        A list of file names within the specified directory.

    Raises:
        ValueError: If the provided `directory` is not a gcs directory or does not exist.
    """
    if not (directory.startswith(GS_URI_PREFIX) and directory.endswith("/")):
        raise ValueError(
            f"{directory} is not a gcs directory. It must start with `gs://` and end with `/`"
        )
    fs = gcsfs.GCSFileSystem()
    if not fs.exists(directory):
        raise ValueError(f"{directory} does not exist.")

    all_files = fs.ls(directory)
    all_files_with_gcs_uri = [_ensure_gcs_uri_prefix(uri) for uri in all_files]
    return [
        file
        for file in all_files_with_gcs_uri
        if fs.isfile(file)
        and "/" not in file[len(directory) :]
        and (prefix is None or file[len(directory) :].startswith(prefix))
    ]


def get_dir_files_filesystem(directory: Path, prefix: str | None = None) -> list[Path]:
    """Get a list of files within the specified directory in a local file system.

    This function retrieves all files in the provided directory.
    Directories or files in subdirectories are excluded from the result.
    If the optional `prefix` is defined, only filenames starting with this prefix is
    returned.

    Args:
        directory: The directory to search for files. It must
            be a valid existing directory.
        prefix: An optional string to filter filenames that start with this prefix.

    Returns:
        A list of Paths, where each path represents a file in the specified directory.

    Raises:
        ValueError: If the provided `directory` is not a directory.
    """
    if not directory.is_dir():
        raise ValueError(f"{directory} is not a directory.")
    return [
        file
        for file in directory.iterdir()
        if file.is_file() and (prefix is None or file.name.startswith(prefix))
    ]


def replace_directory(filepath: Path | str, target_dir: Path | str) -> Path | str:
    """Keep the filename and replace the directory part with a new target_dir path.

    This function takes a file path and a target directory path and replaces the
    current directory in the file path with the specified target directory.

    The datatype `str` is used to represent directories in buckets.
    The datatype `pathlib.Path` is used represent directories in a file system.

    Args:
        filepath: The original file path whose directory will be replaced.
        target_dir: The target directory path to replace the current directory of
            the file path.

    Returns:
        The updated file path with the directory replaced by the target directory.

    Raises:
        ValueError: If the type of `filepath` and `target_dir` is not as expected.
    """
    if isinstance(filepath, Path) and isinstance(target_dir, Path):
        return target_dir / filepath.name
    elif isinstance(filepath, str) and isinstance(target_dir, str):
        return f"{target_dir}/{filepath.split('/')[-1]}"
    else:
        raise ValueError("Both filepath and target_dir must be of type Path or str.")


def _validate_filepath(filepath: Path | str) -> None:
    if not isinstance(filepath, Path | str):
        raise TypeError("Expected filepath to be of type Path or str.")


def _ensure_gcs_uri_prefix(gcs_path: str) -> str:
    """Ensure that a GCS uri has the 'gs://' prefix."""
    if not gcs_path.startswith(GS_URI_PREFIX):
        gcs_path = f"{GS_URI_PREFIX}{gcs_path}"
    return gcs_path
