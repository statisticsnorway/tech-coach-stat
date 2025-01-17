import pytest

from functions._versions_bucket import _get_directory_files
from functions._versions_bucket import get_filename
from config.config import settings
from functions.ssbplatforms import is_dapla
from functions.versions import get_latest_file_version


if is_dapla():
    root_dir = settings.product_root_dir.removesuffix(f"/{settings.short_name}/")
    PREFIX = f"{root_dir}/temp/testcase/versiontest"


@pytest.mark.skipif(
    not is_dapla("tip-tutorials-developers"), reason="Bucket tests only runs on Dapla"
)
def test_get_directory_files() -> None:
    file = f"{PREFIX}/tc1/file_v1.txt"
    result = _get_directory_files(file)
    assert len(result) == 3


# Returns the latest file version when multiple versions exist
@pytest.mark.skipif(
    not is_dapla("tip-tutorials-developers"), reason="Bucket tests only runs on Dapla"
)
def test_returns_latest_version() -> None:
    file = f"{PREFIX}/tc1/file_v1.txt"

    latest_file = get_latest_file_version(file)
    assert get_filename(latest_file) == "file_v10.txt"


# Handles filenames with no version numbers correctly
@pytest.mark.skipif(
    not is_dapla("tip-tutorials-developers"), reason="Bucket tests only runs on Dapla"
)
def test_handles_no_version_numbers() -> None:
    file = f"{PREFIX}/tc2/file.txt"

    latest_file = get_latest_file_version(file)
    assert latest_file is None


# Deals with files having similar names but different extensions
@pytest.mark.skipif(
    not is_dapla("tip-tutorials-developers"), reason="Bucket tests only runs on Dapla"
)
def test_files_with_different_extensions() -> None:
    file = f"{PREFIX}/tc3/file_v1.txt"

    latest_file = get_latest_file_version(file)
    assert get_filename(latest_file) == "file_v3.txt"


# Handles filenames with special characters or spaces
@pytest.mark.skipif(
    not is_dapla("tip-tutorials-developers"), reason="Bucket tests only runs on Dapla"
)
def test_handles_special_characters_and_spaces() -> None:
    file = f"{PREFIX}/tc4/file @.txt"

    latest_file = get_latest_file_version(file)
    assert get_filename(latest_file) == "file @_v3.txt"


# Handles filenames with multiple '_v' patterns
@pytest.mark.skipif(
    not is_dapla("tip-tutorials-developers"), reason="Bucket tests only runs on Dapla"
)
def test_handles_multiple_v_patterns() -> None:
    file = f"{PREFIX}/tc5/file_v1_v2.txt"

    latest_file = get_latest_file_version(file)
    assert get_filename(latest_file) == "file_v1_v10.txt"
