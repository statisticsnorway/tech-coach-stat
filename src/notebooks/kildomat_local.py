"""Kildomat-file for running locally.

This file does the same as `kildomat.py`, but is used when is running locally.
It requires that the environment `local_files` in `config.py` is used.
"""

import logging
from pathlib import Path

import pandas as pd

from functions.config import settings
from functions.file_abstraction import create_dir_if_not_exist
from functions.file_abstraction import read_json_file
from functions.file_abstraction import write_parquet_file


def main(source_file: Path | str) -> None:
    """Orchestrates the processing of the given source file.

    Args:
        source_file: The path to the source file to be processed.
    """
    logging.info(f"Start processing {source_file}")
    if "weather_station" in str(source_file):
        process_weather_stations(source_file)
    else:
        process_observations(source_file)


def process_weather_stations(source_file: Path | str) -> None:
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
    write_parquet_file(target_filepath, df)
    logging.info(f"Wrote file: {target_filepath}")


def process_observations(source_file: Path | str) -> None:
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
    write_parquet_file(target_filepath, df)
    logging.info(f"Wrote file: {target_filepath}")


def get_target_filepath(source_file: Path) -> Path:
    """Calculate a target filepath based on the source_file and some constants.

    Args:
        source_file: The path to the source file which is to be converted.

    Returns:
        The target file path with the converted file extension and in the correct location.
    """
    target_dir = settings.pre_inndata_dir
    if settings.env_for_dynaconf != "local_files":
        raise RuntimeError("Kildomat_local only works when environment is local_files")

    target_filepath = target_dir / source_file.with_suffix(".parquet").name
    logging.info(f"Target file: {target_filepath}")
    return target_filepath  # type: ignore[no-any-return]


def run_all() -> None:
    """Run the code in this module."""
    source_dir = settings.kildedata_root_dir
    logging.info(f"Scanning files in directory: {source_dir}")

    target_dir = settings.pre_inndata_dir
    if settings.env_for_dynaconf != "local_files":
        raise RuntimeError("Kildomat_local only works when environment is local_files")
    create_dir_if_not_exist(target_dir)

    for filepath in source_dir.iterdir():
        if filepath.is_file():
            main(filepath)


if __name__ == "__main__":
    run_all()
