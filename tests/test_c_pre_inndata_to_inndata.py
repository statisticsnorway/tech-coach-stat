from pathlib import Path

import pandas as pd
from pytest_mock import MockerFixture

from notebooks.c_pre_inndata_to_inndata import process_observation_file
from notebooks.c_pre_inndata_to_inndata import process_weather_station_file


class TestProcessWeatherStationFile:

    # Integration test comparing with manually checked facit file
    def test_pathlib_integration(self, mocker: MockerFixture, tmp_path: Path) -> None:
        pre_inndata_filename = "weather_stations_pre_inndata_v1.parquet"
        pre_inndata_file = Path(__file__).parent / "testdata" / pre_inndata_filename

        process_weather_station_file(pre_inndata_file, tmp_path)
        result_df = pd.read_parquet(tmp_path / pre_inndata_filename)

        inndata_filename = "weather_stations_inndata_v1.parquet"
        facit_file = Path(__file__).parent / "testdata" / inndata_filename
        facit_df = pd.read_parquet(facit_file)
        pd.testing.assert_frame_equal(result_df, facit_df)


class TestProcessObservationFile:

    def test_pathlib_integration(self, mocker: MockerFixture, tmp_path: Path) -> None:
        pre_inndata_filename = "observations_pre_inndata.parquet"
        pre_inndata_file = Path(__file__).parent / "testdata" / pre_inndata_filename

        process_observation_file(pre_inndata_file, tmp_path)
        result_df = pd.read_parquet(tmp_path / pre_inndata_filename)

        inndata_filename = "observations_inndata.parquet"
        facit_file = Path(__file__).parent / "testdata" / inndata_filename
        facit_df = pd.read_parquet(facit_file)
        pd.testing.assert_frame_equal(result_df, facit_df)