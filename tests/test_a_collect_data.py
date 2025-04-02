from datetime import date

import pytest

from notebooks.a_collect_data import extract_latest_date_from_filename


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
