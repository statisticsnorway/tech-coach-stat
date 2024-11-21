"""Kildomat-file.

This is the `kildomat`-file used for automatic processing of data from kildedata
to pre-inndata in Statistics Norway. Copy this file to `process_source_data.py` in
the iac-project for the Dapola team.

When running locally or for testing, use the kildomat_local.py file instead.
"""

import json
import logging
from pathlib import Path
from typing import Any
from typing import cast

import dapla as dp
import pandas as pd
from dapla import FileClient


def main(source_file: Path | str, target_dir: Path | None = None) -> None:
    """Orchestrates the processing of the given source file.

    Args:
        source_file: The path to the source file to be processed.
        target_dir: Optional target directory. Used when running locally.
    """
    logging.info(f"Start processing {source_file}")
    if "weather_station" in str(source_file):
        process_weather_stations(source_file, target_dir)
    else:
        process_observations(source_file, target_dir)


def process_weather_stations(source_file: Path | str, target_dir: Path | None) -> None:
    """Process weather station data from the kildedata to pre-inndata data state.

    Args:
        source_file: Path to the source JSON file containing weather station data.
        target_dir: Target directory. Used when running locally, otherwise `None`.
    """
    logging.info("Start processing weather stations file")
    data = read_json_file(source_file)
    df = pd.json_normalize(data)

    # Data minimization
    df = df.drop(
        columns=[
            "ontologyId",
            "externalIds",
            "wigosId",
            "geometry.@type",
            "geometry.nearest",
            "wmoId",
            "icaoCodes",
            "shipCodes",
        ],
        errors="ignore",
    )

    # Convert datatypes
    columns_to_convert = ["masl", "countyId", "municipalityId"]
    for col in columns_to_convert:
        df[col] = df[col].astype("Int64")
    df["validFrom"] = pd.to_datetime(df["validFrom"])
    df = df.astype(
        {col: "string" for col in df.select_dtypes(include="object").columns}
    )

    target_filepath = get_target_filepath(source_file, target_dir)
    write_parquet_file(target_filepath, df)
    logging.info(f"Wrote file: {target_filepath}")


def process_observations(source_file: Path | str, target_dir: Path | None) -> None:
    """Process weather observations from the kildedata to pre-inndata data state.

    Args:
        source_file: The path to the source JSON file containing observations data.
        target_dir: Target directory. Used when running locally, otherwise `None`.
    """
    logging.info("Start processing observations file")
    data = read_json_file(source_file)
    df = pd.json_normalize(
        data, record_path=["observations"], meta=["sourceId", "referenceTime"]
    )

    # Data minimization
    df = df.drop(
        columns=[
            "timeSeriesId",
            "performanceCategory",
            "exposureCategory",
            "qualityCode",
            "level.levelType",
            "level.unit",
            "level.value",
        ],
        errors="ignore",
    )

    # Convert datatypes
    df["referenceTime"] = pd.to_datetime(df["referenceTime"])
    df = df.astype(
        {col: "string" for col in df.select_dtypes(include="object").columns}
    )

    target_filepath = get_target_filepath(source_file, target_dir)
    write_parquet_file(target_filepath, df)
    logging.info(f"Wrote file: {target_filepath}")


def get_target_filepath(source_file: Path | str, target_dir: Path | None) -> Path | str:
    """Calculate a target filepath based on the source_file and some constants.

    Args:
        source_file: The path to the source file which is to be converted.
        target_dir: Target directory. Used when running locally, otherwise `None`.

    Returns:
        The target file path with the converted file extension and in the correct location.
    """
    if target_dir:
        target_filepath = target_dir / source_file.with_suffix(".parquet").name
    else:
        target_bucket = "gs://ssb-tip-tutorials-data-produkt-prod"
        folder = "tip-tutorials/inndata/temp/pre-inndata"
        target_filename = source_file.split("/")[-1].replace("json", "parquet")
        target_filepath = f"{target_bucket}/{folder}/{target_filename}"

    logging.info(f"Target file: {target_filepath}")
    return target_filepath


# Copy of code from src/functions/file_abstraction.py
# Need to replicate it since the code in not available for the kildomat otherwise.
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


def _validate_filepath(filepath: Path | str) -> None:
    if not isinstance(filepath, Path | str):
        raise TypeError("Expected filepath to be of type Path or str.")
