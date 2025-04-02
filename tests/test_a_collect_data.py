from datetime import date

import pytest
from google.auth.exceptions import DefaultCredentialsError

from notebooks.a_collect_data import extract_latest_date_from_filename
from notebooks.a_collect_data import frost_client_id


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
    def test_retrieves_client_id_from_gsm(self, mocker):
        mock_get_secret = mocker.patch(
            "notebooks.a_collect_data.get_secret_version", return_value="test-client-id"
        )
        result = frost_client_id()
        mock_get_secret.assert_called_once_with("tip-tutorials-p-mb", "FROST_CLIENT_ID")
        assert result == "test-client-id"

    # Handles DefaultCredentialsError when GSM credentials are not available
    def test_gsm_secret_not_available(self, mocker):
        mock_get_secret = mocker.patch(
            "notebooks.a_collect_data.get_secret_version",
            side_effect=DefaultCredentialsError(),
        )
        mock_getenv = mocker.patch("os.getenv", return_value="fallback-client-id")

        result = frost_client_id()

        mock_get_secret.assert_called_once()
        mock_getenv.assert_called_once_with("FROST_CLIENT_ID")
        assert result == "fallback-client-id"
