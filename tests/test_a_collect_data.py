from datetime import date
from datetime import timedelta

import pytest
from google.auth.exceptions import DefaultCredentialsError
from pytest_mock import MockerFixture

from notebooks.a_collect_data import extract_latest_date_from_filename
from notebooks.a_collect_data import frost_client_id
from notebooks.a_collect_data import get_observations


class TestExtractLatestDateFromFilename:

    # Extract the latest date from a filename with the pattern "_pYYYY-MM-DD_pYYYY-MM-DD"
    def test_extract_latest_date_from_valid_filename(self) -> None:
        filename = "some_file_p2023-01-15_p2023-02-20.json"
        expected_date = date(2023, 2, 20)
        result = extract_latest_date_from_filename(filename)
        assert result == expected_date

    # Return None when no date pattern is found in the filename
    @pytest.mark.parametrize(
        "filename",
        [
            "file_without_pattern.json",
            "file_p2023-01-15.json",  # Only one date
            "file_p2023-01-15_2023-02-20.json",  # Missing 'p' prefix in second date
            "file_2023-01-15_p2023-02-20.json",  # Missing 'p' prefix in first date
        ],
    )
    def test_return_none_for_invalid_filename_pattern(self, filename: str) -> None:
        result = extract_latest_date_from_filename(filename)
        assert result is None


class TestFrostClientId:

    # Successfully retrieves client_id from Google Secret Manager
    def test_retrieves_client_id_from_gsm(self, mocker: MockerFixture) -> None:
        mock_get_secret = mocker.patch(
            "notebooks.a_collect_data.get_secret_version", return_value="test-client-id"
        )
        result = frost_client_id()
        mock_get_secret.assert_called_once_with("tip-tutorials-p-mb", "FROST_CLIENT_ID")
        assert result == "test-client-id"

    # Handles DefaultCredentialsError when GSM credentials are not available
    def test_gsm_secret_not_available(self, mocker: MockerFixture) -> None:
        mock_get_secret = mocker.patch(
            "notebooks.a_collect_data.get_secret_version",
            side_effect=DefaultCredentialsError(),
        )
        mock_getenv = mocker.patch("os.getenv", return_value="fallback-client-id")

        result = frost_client_id()

        mock_get_secret.assert_called_once()
        mock_getenv.assert_called_once_with("FROST_CLIENT_ID")
        assert result == "fallback-client-id"


class TestGetObservations:

    # Successfully retrieves observations when source_ids_ is provided and there are new observations
    def test_retrieves_observations_when_new_data_available(
        self, mocker: MockerFixture
    ) -> None:
        # Arrange
        source_ids = ["SN18700", "SN18701"]
        mock_today = date(year=2025, month=4, day=2)
        mock_latest_date = mock_today - timedelta(days=5)

        # Mock dependencies
        mocker.patch(
            "notebooks.a_collect_data.get_latest_observation_date",
            return_value=mock_latest_date,
        )
        mocker.patch("notebooks.a_collect_data.date").today.return_value = mock_today
        mock_fetch_data = mocker.patch("notebooks.a_collect_data.fetch_data")
        mock_observations = [
            {"referenceTime": f"{mock_latest_date + timedelta(days=1)}T00:00:00.000Z"},
            {"referenceTime": f"{mock_today}T00:00:00.000Z"},
        ]
        mock_fetch_data.return_value = mock_observations

        mocker.patch(
            "notebooks.a_collect_data.extract_timespan",
            return_value="2025-03-29_p2025-04-02",
        )
        mock_write_json = mocker.patch("notebooks.a_collect_data.write_json_file")

        # Act
        result = get_observations(source_ids)

        # Assert
        expected_parameters = {
            "sources": "SN18700,SN18701",
            "elements": (
                "min(air_temperature P1D),"
                "mean(air_temperature P1D),"
                "max(air_temperature P1D),"
                "sum(precipitation_amount P1D),"
                "max(wind_speed P1D)"
            ),
            "referencetime": f"{(mock_latest_date + timedelta(days=1)).isoformat()}/{mock_today.isoformat()}",
            "levels": "default",
            "timeoffsets": "default",
        }
        mock_fetch_data.assert_called_once_with(
            "https://frost.met.no/observations/v0.jsonld", expected_parameters
        )
        mock_write_json.assert_called_once()
        assert result == mock_observations
