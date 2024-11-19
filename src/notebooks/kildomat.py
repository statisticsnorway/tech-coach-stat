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


def main(source_file: str) -> None:
    """Orchestrates the processing of the given source file.

    Args:
        source_file: The path to the source file to be processed.
    """
    logging.info(f"Start processing {source_file}")
    if "weather_station" in source_file:
        process_weather_stations(source_file)
    else:
        process_observations(source_file)


def process_weather_stations(source_file: str) -> None:
    """Process weather station data from the kildedata to pre-inndata data state.

    Args:
        source_file: Path to the source JSON file containing weather station data.
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
        ]
    )

    # Convert datatypes
    columns_to_convert = ["masl", "countyId", "municipalityId"]
    for col in columns_to_convert:
        df[col] = df[col].astype("Int64")
    df["validFrom"] = pd.to_datetime(df["validFrom"])
    df = df.astype(
        {col: "string" for col in df.select_dtypes(include="object").columns}
    )

    target_filepath = get_target_filepath(source_file)
    dp.write_pandas(df=df, gcs_path=target_filepath)
    logging.info(f"Wrote file: {target_filepath}")


def process_observations(source_file: str) -> None:
    """Process weather observations from the kildedata to pre-inndata data state.

    Args:
        source_file: The path to the source JSON file containing observations data.
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
        ]
    )

    # Convert datatypes
    df["referenceTime"] = pd.to_datetime(df["referenceTime"])
    df = df.astype(
        {col: "string" for col in df.select_dtypes(include="object").columns}
    )

    target_filepath = get_target_filepath(source_file)
    dp.write_pandas(df=df, gcs_path=target_filepath)
    logging.info(f"Wrote file: {target_filepath}")


def get_target_filepath(source_file: str) -> str:
    """Calculate a target filepath based on the source_file and some constants.

    Args:
        source_file: The path to the source file which is to be converted.

    Returns:
        The target file path with the converted file extension and in the correct location.
    """
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
    if not isinstance(filepath, Path | str):
        raise TypeError("Expected filepath to be of type Path or str.")
    if isinstance(filepath, Path):
        with filepath.open(encoding="utf-8") as file:
            return cast(list[dict[str, Any]], json.load(file))
    elif isinstance(filepath, str):
        with FileClient.gcs_open(filepath) as file:
            return cast(list[dict[str, Any]], json.load(file))
