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

    def test_handles_validation_errors_with_autocorrection(
        self, mocker: MockerFixture, tmp_path: Path, ws_autocorrect: pd.DataFrame
    ) -> None:
        mock_read_parquet = mocker.patch(
            "notebooks.c_pre_inndata_to_inndata.read_parquet_file"
        )
        mock_read_parquet.return_value = ws_autocorrect

        pre_inndata_filename = "weather_stations_pre_inndata_v1.parquet"
        pre_inndata_file = Path(__file__).parent / "testdata" / pre_inndata_filename

        process_weather_station_file(pre_inndata_file, tmp_path)
        result_df = pd.read_parquet(tmp_path / pre_inndata_filename)

        assert mock_read_parquet.call_count == 1
        assert (
            "SN499999010" not in result_df["id"].values
        )  # Assert no rows with id 'SN499999010'


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
