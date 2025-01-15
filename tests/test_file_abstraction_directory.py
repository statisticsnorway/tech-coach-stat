import tempfile
from pathlib import Path

import pytest
from pytest_mock import MockerFixture

from functions.file_abstraction import directory_diff
from functions.file_abstraction import get_dir_files_bucket
from functions.file_abstraction import get_dir_files_filesystem


class TestGetDirFilesBucket:

    # Returns list of files from valid GCS directory path with trailing slash
    def test_returns_files_from_valid_directory(self, mocker: MockerFixture) -> None:
        # Arrange
        mock_fs = mocker.patch("gcsfs.GCSFileSystem")
        mock_fs.return_value.exists.return_value = True
        mock_fs.return_value.ls.return_value = [
            "bucket/dir/file1.txt",
            "bucket/dir/file2.txt",
        ]
        mock_fs.return_value.isfile.return_value = True

        directory = "bucket/dir/"

        # Act
        result = get_dir_files_bucket(directory)

        # Assert
        assert result == ["bucket/dir/file1.txt", "bucket/dir/file2.txt"]
        mock_fs.return_value.exists.assert_called_once_with(directory)
        mock_fs.return_value.ls.assert_called_once_with(directory)

    # Correctly filters out subdirectories and nested files
    def test_filters_out_subdirectories_and_nested_files(
        self, mocker: MockerFixture
    ) -> None:
        # Arrange
        mock_fs = mocker.patch("gcsfs.GCSFileSystem")
        mock_fs.return_value.exists.return_value = True
        mock_fs.return_value.ls.return_value = [
            "bucket/dir/file1.txt",
            "bucket/dir/file2.txt",
            "bucket/dir/subdir/",
            "bucket/dir/subdir/file3.txt",
        ]
        mock_fs.return_value.isfile.side_effect = lambda x: not x.endswith("/")

        directory = "bucket/dir/"

        # Act
        result = get_dir_files_bucket(directory)

        # Assert
        assert result == ["bucket/dir/file1.txt", "bucket/dir/file2.txt"]
        mock_fs.return_value.exists.assert_called_once_with(directory)
        mock_fs.return_value.ls.assert_called_once_with(directory)

    # Returns empty list for directory with no files
    def test_returns_empty_list_for_empty_directory(
        self, mocker: MockerFixture
    ) -> None:
        # Arrange
        mock_fs = mocker.patch("gcsfs.GCSFileSystem")
        mock_fs.return_value.exists.return_value = True
        mock_fs.return_value.ls.return_value = []
        mock_fs.return_value.isfile.return_value = False

        directory = "bucket/empty_dir/"

        # Act
        result = get_dir_files_bucket(directory)

        # Assert
        assert result == []
        mock_fs.return_value.exists.assert_called_once_with(directory)
        mock_fs.return_value.ls.assert_called_once_with(directory)

    # Raises ValueError when directory path doesn't end with slash
    def test_raises_error_for_missing_trailing_slash(
        self, mocker: MockerFixture
    ) -> None:
        # Arrange
        directory = "bucket/dir"

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            get_dir_files_bucket(directory)

        assert (
            str(exc_info.value)
            == f"{directory} is not a directory. It must have a trailing `/`"
        )

    # Raises ValueError when directory doesn't exist in GCS
    def test_raises_value_error_for_nonexistent_directory(
        self, mocker: MockerFixture
    ) -> None:
        # Arrange
        mock_fs = mocker.patch("gcsfs.GCSFileSystem")
        mock_fs.return_value.exists.return_value = False

        directory = "bucket/nonexistent_dir/"

        # Act & Assert
        with pytest.raises(ValueError, match=f"{directory} does not exist."):
            get_dir_files_bucket(directory)

        mock_fs.return_value.exists.assert_called_once_with(directory)


class TestGetDirFilesFilesystem:
    # Returns list of file paths for all files in a valid directory
    def test_returns_file_paths_from_valid_directory(self):
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            test_dir = Path(temp_dir)
            (test_file1 := test_dir / "file1.txt").touch()
            (test_file2 := test_dir / "file2.txt").touch()

            # Act
            result = get_dir_files_filesystem(test_dir)

            # Assert
            assert len(result) == 2
            assert test_file1 in result
            assert test_file2 in result
            assert all(isinstance(f, Path) for f in result)

    # Correctly handles directory containing both files and subdirectories
    def test_handles_files_and_subdirectories(self):
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            test_dir = Path(temp_dir)
            (test_file1 := test_dir / "file1.txt").touch()
            (test_file2 := test_dir / "file2.txt").touch()
            subdirectory = test_dir / "subdir"
            subdirectory.mkdir()
            (subdirectory / "file3.txt").touch()

            # Act
            result = get_dir_files_filesystem(test_dir)

            # Assert
            assert len(result) == 2
            assert test_file1 in result
            assert test_file2 in result
            assert all(isinstance(f, Path) for f in result)

    # Raises ValueError when a file path is provided instead of a directory
    def test_raises_error_for_non_directory(self):
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            (test_file := Path(temp_dir) / "file1.txt").touch()

            with pytest.raises(ValueError) as exc_info:
                get_dir_files_filesystem(test_file)
            assert str(test_file) in str(exc_info.value)


class TestDirectoryDiff:
    # Compare directories with Path type and return files present in source but not in target
    def test_with_path_type(self, mocker: MockerFixture) -> None:
        # Arrange
        source_dir = Path("/test/source")
        target_dir = Path("/test/target")

        source_files = [Path("/test/source/file1.txt"), Path("/test/source/file2.txt")]
        target_files = [Path("/test/source/file2.txt")]

        mock_get_dir_files = mocker.patch(
            "functions.file_abstraction.get_dir_files_filesystem"
        )
        mock_get_dir_files.side_effect = [source_files, target_files]

        # Act
        result = directory_diff(source_dir, target_dir)

        # Assert
        assert result == {Path("/test/source/file1.txt")}
        mock_get_dir_files.assert_has_calls(
            [mocker.call(source_dir), mocker.call(target_dir)]
        )

    # Compare directories with str type (GCS buckets) and return files present in source but not in target
    def test_with_str_type(self, mocker: MockerFixture) -> None:
        # Arrange
        source_dir = "gs://test-bucket/source/"
        target_dir = "gs://test-bucket/target/"

        source_files = [
            "gs://test-bucket/source/file1.txt",
            "gs://test-bucket/source/file2.txt",
        ]
        target_files = ["gs://test-bucket/source/file2.txt"]

        mock_get_dir_files_bucket = mocker.patch(
            "functions.file_abstraction.get_dir_files_bucket"
        )
        mock_get_dir_files_bucket.side_effect = [source_files, target_files]

        # Act
        result = directory_diff(source_dir, target_dir)

        # Assert
        assert result == {"gs://test-bucket/source/file1.txt"}
        mock_get_dir_files_bucket.assert_has_calls(
            [mocker.call(source_dir), mocker.call(target_dir)]
        )

    # Mixed input types (Path and str) should raise ValueError
    def test_mixed_types_raises_error(self):
        # Arrange
        source_dir = Path("/test/source")
        target_dir = "gs://bucket/target/"

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            directory_diff(source_dir, target_dir)

        assert (
            str(exc_info.value)
            == "Both source_dir and target_dir must be of type Path or str."
        )
