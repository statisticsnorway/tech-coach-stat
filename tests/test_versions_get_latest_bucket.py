import pytest

from config.config import settings
from functions.file_abstraction import get_dir_files
from functions.ssbplatforms import is_dapla
from functions.versions import get_latest_file_version


def get_filename(filepath: str) -> str:
    """Return the filename part of the filepath."""
    return filepath.split("/")[-1]


def _is_non_bucket() -> bool:
    return settings.current_env != "default"


if is_dapla() and not _is_non_bucket():
    root_dir = settings.product_root_dir.removesuffix(f"/{settings.short_name}/")
    PREFIX = f"{root_dir}/temp/testcase/versiontest"


@pytest.mark.skipif(
    not is_dapla("tip-tutorials-developers") or _is_non_bucket(),
    reason="Bucket tests only runs on Dapla",
)
def test_get_directory_files() -> None:
    directory = f"{PREFIX}/tc1/"
    result = get_dir_files(directory)
    assert len(result) == 3


# Returns the latest file version when multiple versions exist
@pytest.mark.skipif(
    not is_dapla("tip-tutorials-developers") or _is_non_bucket(),
    reason="Bucket tests only runs on Dapla",
)
def test_returns_latest_version() -> None:
    file = f"{PREFIX}/tc1/file_v1.txt"

    latest_file = get_latest_file_version(file)
    assert isinstance(latest_file, str)
    assert get_filename(latest_file) == "file_v10.txt"


# Handles filenames with no version numbers correctly
@pytest.mark.skipif(
    not is_dapla("tip-tutorials-developers") or _is_non_bucket(),
    reason="Bucket tests only runs on Dapla",
)
def test_handles_no_version_numbers() -> None:
    file = f"{PREFIX}/tc2/file.txt"

    latest_file = get_latest_file_version(file)
    assert latest_file is None


# Deals with files having similar names but different extensions
@pytest.mark.skipif(
    not is_dapla("tip-tutorials-developers") or _is_non_bucket(),
    reason="Bucket tests only runs on Dapla",
)
def test_files_with_different_extensions() -> None:
    file = f"{PREFIX}/tc3/file_v1.txt"

    latest_file = get_latest_file_version(file)
    assert isinstance(latest_file, str)
    assert get_filename(latest_file) == "file_v3.txt"


# Handles filenames with special characters or spaces
@pytest.mark.skipif(
    not is_dapla("tip-tutorials-developers") or _is_non_bucket(),
    reason="Bucket tests only runs on Dapla",
)
def test_handles_special_characters_and_spaces() -> None:
    file = f"{PREFIX}/tc4/file @.txt"

    latest_file = get_latest_file_version(file)
    assert isinstance(latest_file, str)
    assert get_filename(latest_file) == "file @_v3.txt"


# Handles filenames with multiple '_v' patterns
@pytest.mark.skipif(
    not is_dapla("tip-tutorials-developers") or _is_non_bucket(),
    reason="Bucket tests only runs on Dapla",
)
def test_handles_multiple_v_patterns() -> None:
    file = f"{PREFIX}/tc5/file_v1_v2.txt"

    latest_file = get_latest_file_version(file)
    assert isinstance(latest_file, str)
    assert get_filename(latest_file) == "file_v1_v10.txt"
