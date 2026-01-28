import tempfile
from pathlib import Path

import pytest

from functions.versions import get_latest_file_date
from functions.versions import get_latest_file_version
from functions.versions import get_next_file_version


# Returns the latest file version when multiple versions exist
def test_returns_latest_version() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        dir_path = Path(temp_dir)
        (dir_path / "file_v1.txt").touch()
        (dir_path / "file_v10.txt").touch()
        (dir_path / "file_v2.txt").touch()

        latest_file = get_latest_file_version(dir_path / "file_v1.txt")
        assert isinstance(latest_file, Path)
        assert latest_file.name == "file_v10.txt"


# Handles filenames with no version numbers correctly
def test_handles_no_version_numbers() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        dir_path = Path(temp_dir)
        (dir_path / "file.txt").touch()

        latest_file = get_latest_file_version(dir_path / "file.txt")
        assert latest_file is None


# Deals with files having similar names but different extensions
def test_files_with_different_extensions() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        dir_path = Path(temp_dir)
        (dir_path / "file_v1.txt").touch()
        (dir_path / "file_v2.doc").touch()
        (dir_path / "file_v3.txt").touch()
        (dir_path / "file_v4.doc").touch()

        latest_file = get_latest_file_version(dir_path / "file_v1.txt")
        assert isinstance(latest_file, Path)
        assert latest_file.name == "file_v3.txt"


# Handles filenames with special characters or spaces
def test_handles_special_characters_and_spaces() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        dir_path = Path(temp_dir)
        (dir_path / "file @_v1.txt").touch()
        (dir_path / "file @_v2.txt").touch()
        (dir_path / "file @_v3.txt").touch()

        latest_file = get_latest_file_version(dir_path / "file @.txt")
        assert isinstance(latest_file, Path)
        assert latest_file.name == "file @_v3.txt"


# Handles filenames with multiple '_v' patterns
def test_handles_multiple_v_patterns() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        dir_path = Path(temp_dir)
        (dir_path / "file_v1_v2.txt").touch()
        (dir_path / "file_v1_v3.txt").touch()
        (dir_path / "file_v1_v10.txt").touch()

        latest_file = get_latest_file_version(dir_path / "file_v1_v2.txt")
        assert isinstance(latest_file, Path)
        assert latest_file.name == "file_v1_v10.txt"


# --- get_latest_file_date tests ---


def test_get_latest_file_date_pathlib():
    with tempfile.TemporaryDirectory() as temp_dir:
        dir_path = Path(temp_dir)
        (dir_path / "obs_p2024-01-01.parquet").touch()
        (dir_path / "obs_p2024-01-02.parquet").touch()
        (dir_path / "obs_p2023-12-31.parquet").touch()

        latest = get_latest_file_date(dir_path / "obs.parquet")
        assert latest.name == "obs_p2024-01-02.parquet"


def test_get_latest_file_date_with_versions():
    with tempfile.TemporaryDirectory() as temp_dir:
        dir_path = Path(temp_dir)
        (dir_path / "obs_p2023-12-31_v1.parquet").touch()
        (dir_path / "obs_p2024-01-01_v1.parquet").touch()
        (dir_path / "obs_p2024-01-01_v2.parquet").touch()

        latest = get_latest_file_date(dir_path / "obs_p2023-12-31_v1.parquet")
        assert latest.name == "obs_p2024-01-01_v2.parquet"


def test_get_latest_file_date_different_extensions():
    with tempfile.TemporaryDirectory() as temp_dir:
        dir_path = Path(temp_dir)
        (dir_path / "obs_p2024-01-01.json").touch()
        (dir_path / "obs_p2024-01-02.parquet").touch()

        latest = get_latest_file_date(dir_path / "obs.parquet")
        assert latest.name == "obs_p2024-01-02.parquet"

        latest_json = get_latest_file_date(dir_path / "obs.json")
        assert latest_json.name == "obs_p2024-01-01.json"


# --- get_next_file_version tests ---


def test_get_next_file_version_pathlib():
    with tempfile.TemporaryDirectory() as temp_dir:
        dir_path = Path(temp_dir)
        file_path = dir_path / "file_v1.txt"
        file_path.touch()

        next_file = get_next_file_version(file_path)
        assert next_file.name == "file_v2.txt"
        assert next_file.parent == dir_path


def test_get_next_file_version_bucket():
    file_path = "gs://bucket/path/file_v1.txt"
    next_file = get_next_file_version(file_path)
    assert next_file == "gs://bucket/path/file_v2.txt"


def test_get_next_file_version_no_version():
    with tempfile.TemporaryDirectory() as temp_dir:
        dir_path = Path(temp_dir)
        file_path = dir_path / "file.txt"
        file_path.touch()

        with pytest.raises(AssertionError):
            get_next_file_version(file_path)
