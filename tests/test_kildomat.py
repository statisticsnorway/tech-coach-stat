from pathlib import Path

import pandas as pd
from pytest_mock import MockerFixture
from src.notebooks.b_kildomat import process_observations
from src.notebooks.b_kildomat import process_weather_stations


def test_process_weather_stations(mocker: MockerFixture) -> None:
    source_file = Path(__file__).parent / "testdata" / "weather_stations_v1.json"

    # Mock the function write_parquet_file. Then only a dummy target_dir is needed.
    mock_write_parquet_file = mocker.patch(
        "src.notebooks.b_kildomat.write_parquet_file"
    )
    dummy_target_dir = Path(__file__).parent

    process_weather_stations(source_file, target_dir=dummy_target_dir)
    mock_write_parquet_file.assert_called_once()

    # Capture the dataframe, the second argument to write_parquet_file function
    captured_df = mock_write_parquet_file.call_args[0][1]
    assert isinstance(captured_df, pd.DataFrame)
    assert not captured_df.empty

    required_columns = ("id", "name", "countyId", "municipalityId")
    assert all(
        col in captured_df.columns for col in required_columns
    ), "Not all required columns are present in the DataFrame."


def test_process_observations(mocker: MockerFixture) -> None:
    source_file = Path(__file__).parent / "testdata" / "observations_p2011_v1.json"

    # Mock the function write_parquet_file. Then only a dummy target_dir is needed.
    mock_write_parquet_file = mocker.patch(
        "src.notebooks.b_kildomat.write_parquet_file"
    )
    dummy_target_dir = Path(__file__).parent

    process_observations(source_file, target_dir=dummy_target_dir)
    mock_write_parquet_file.assert_called_once()

    # Capture the dataframe, the second argument to write_parquet_file function
    captured_df = mock_write_parquet_file.call_args[0][1]
    assert isinstance(captured_df, pd.DataFrame)
    assert not captured_df.empty

    required_columns = ("sourceId", "elementId", "value", "referenceTime")
    assert all(
        col in captured_df.columns for col in required_columns
    ), "Not all required columns are present in the DataFrame."
