import os
import tempfile
from pathlib import Path

from functions.versions import get_latest_file_version


# Returns the latest file version when multiple versions exist
def test_returns_latest_version() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create files with different versions
        Path(os.path.join(temp_dir, "file_v1.txt")).touch()
        Path(os.path.join(temp_dir, "file_v10.txt")).touch()
        Path(os.path.join(temp_dir, "file_v2.txt")).touch()

        # Test the function
        latest_file = get_latest_file_version(Path(temp_dir) / "file_v1.txt")
        assert isinstance(latest_file, Path)
        assert latest_file.name == "file_v10.txt"


# Handles filenames with no version numbers correctly
def test_handles_no_version_numbers() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a file without a version number
        Path(os.path.join(temp_dir, "file.txt")).touch()

        # Test the function
        latest_file = get_latest_file_version(Path(temp_dir) / "file.txt")
        assert latest_file is None


# Deals with files having similar names but different extensions
def test_files_with_different_extensions() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create files with different extensions
        Path(os.path.join(temp_dir, "file_v1.txt")).touch()
        Path(os.path.join(temp_dir, "file_v2.doc")).touch()
        Path(os.path.join(temp_dir, "file_v3.txt")).touch()
        Path(os.path.join(temp_dir, "file_v4.doc")).touch()

        # Test the function
        latest_file = get_latest_file_version(Path(temp_dir) / "file_v1.txt")
        assert isinstance(latest_file, Path)
        assert latest_file.name == "file_v3.txt"


# Handles filenames with special characters or spaces
def test_handles_special_characters_and_spaces() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create files with special characters and spaces in the name
        Path(os.path.join(temp_dir, "file @_v1.txt")).touch()
        Path(os.path.join(temp_dir, "file @_v2.txt")).touch()
        Path(os.path.join(temp_dir, "file @_v3.txt")).touch()

        # Test the function
        latest_file = get_latest_file_version(Path(temp_dir) / "file @.txt")
        assert isinstance(latest_file, Path)
        assert latest_file.name == "file @_v3.txt"


# Handles filenames with multiple '_v' patterns
def test_handles_multiple_v_patterns() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create files with multiple '_v' patterns
        Path(os.path.join(temp_dir, "file_v1_v2.txt")).touch()
        Path(os.path.join(temp_dir, "file_v1_v3.txt")).touch()
        Path(os.path.join(temp_dir, "file_v1_v10.txt")).touch()

        # Test the function
        latest_file = get_latest_file_version(Path(temp_dir) / "file_v1_v2.txt")
        assert isinstance(latest_file, Path)
        assert latest_file.name == "file_v1_v10.txt"
