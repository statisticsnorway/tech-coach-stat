import tempfile
from pathlib import Path

import pytest
from pytest_mock import MockerFixture

from functions.file_abstraction import directory_diff
from functions.file_abstraction import get_dir_files
from functions.file_abstraction import get_dir_files_bucket
from functions.file_abstraction import get_dir_files_filesystem
from functions.file_abstraction import replace_directory


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

        directory = "gs://bucket/dir/"

        # Act
        result = get_dir_files_bucket(directory)

        # Assert
        assert result == ["gs://bucket/dir/file1.txt", "gs://bucket/dir/file2.txt"]
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

        directory = "gs://bucket/dir/"

        # Act
        result = get_dir_files_bucket(directory)

        # Assert
        assert result == ["gs://bucket/dir/file1.txt", "gs://bucket/dir/file2.txt"]
        mock_fs.return_value.exists.assert_called_once_with(directory)
        mock_fs.return_value.ls.assert_called_once_with(directory)

    # Returns filtered list when prefix is provided
    def test_returns_filtered_files_with_prefix(self, mocker: MockerFixture) -> None:
        # Arrange
        mock_fs = mocker.patch("gcsfs.GCSFileSystem")
        mock_fs.return_value.exists.return_value = True
        mock_fs.return_value.ls.return_value = [
            "bucket/dir/file1.txt",
            "bucket/dir/file2.txt",
            "bucket/dir/prefix_file3.txt",
            "bucket/dir/prefix_file4.txt",
        ]
        mock_fs.return_value.isfile.side_effect = lambda x: x in [
            "gs://bucket/dir/file1.txt",
            "gs://bucket/dir/file2.txt",
            "gs://bucket/dir/prefix_file3.txt",
            "gs://bucket/dir/prefix_file4.txt",
        ]

        directory = "gs://bucket/dir/"
        prefix = "prefix_"

        # Act
        result = get_dir_files_bucket(directory, prefix)

        # Assert
        assert len(result) == 2
        assert "gs://bucket/dir/prefix_file3.txt" in result
        assert "gs://bucket/dir/prefix_file4.txt" in result

    # Returns empty list for directory with no files
    def test_returns_empty_list_for_empty_directory(
        self, mocker: MockerFixture
    ) -> None:
        # Arrange
        mock_fs = mocker.patch("gcsfs.GCSFileSystem")
        mock_fs.return_value.exists.return_value = True
        mock_fs.return_value.ls.return_value = []
        mock_fs.return_value.isfile.return_value = False

        directory = "gs://bucket/empty_dir/"

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
        directory = "gs://bucket/dir"

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            get_dir_files_bucket(directory)

        assert (
            str(exc_info.value)
            == f"{directory} is not a gcs directory. It must start with `gs://` and end with `/`"
        )

    # Raises ValueError when directory doesn't exist in GCS
    def test_raises_value_error_for_nonexistent_directory(
        self, mocker: MockerFixture
    ) -> None:
        # Arrange
        mock_fs = mocker.patch("gcsfs.GCSFileSystem")
        mock_fs.return_value.exists.return_value = False

        directory = "gs://bucket/nonexistent_dir/"

        # Act & Assert
        with pytest.raises(ValueError, match=f"{directory} does not exist."):
            get_dir_files_bucket(directory)

        mock_fs.return_value.exists.assert_called_once_with(directory)


class TestGetDirFilesFilesystem:
    # Returns list of file paths for all files in a valid directory
    def test_returns_file_paths_from_valid_directory(self) -> None:
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
    def test_handles_files_and_subdirectories(self) -> None:
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

    # Returns only files starting with given prefix when prefix is specified
    def test_returns_filtered_files_with_prefix(self) -> None:
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            test_dir = Path(temp_dir)
            test_files = ["prefix_file1.txt", "prefix_file2.txt", "other_file.txt"]
            for filename in test_files:
                (test_dir / filename).touch()

            # Act
            result = get_dir_files_filesystem(test_dir, prefix="prefix_")

            # Assert
            # Verify only files with the specified prefix are returned
            expected_files = ["prefix_file1.txt", "prefix_file2.txt"]
            assert len(result) == len(expected_files)
            assert all(f.name in expected_files for f in result)

    # Raises ValueError when a file path is provided instead of a directory
    def test_raises_error_for_non_directory(self) -> None:
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
        target_files = [Path("/test/source2/file2.txt")]

        mock_get_dir_files = mocker.patch(
            "functions.file_abstraction.get_dir_files_filesystem"
        )
        mock_get_dir_files.side_effect = [source_files, target_files]

        # Act
        result = directory_diff(source_dir, target_dir)

        # Assert
        assert result == [Path("/test/source/file1.txt")]
        mock_get_dir_files.assert_has_calls(
            [mocker.call(source_dir, None), mocker.call(target_dir, None)]  # type: ignore
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
        target_files = ["gs://test-bucket/source2/file2.txt"]

        mock_get_dir_files_bucket = mocker.patch(
            "functions.file_abstraction.get_dir_files_bucket"
        )
        mock_get_dir_files_bucket.side_effect = [source_files, target_files]

        # Act
        result = directory_diff(source_dir, target_dir)

        # Assert
        assert result == ["gs://test-bucket/source/file1.txt"]
        mock_get_dir_files_bucket.assert_has_calls(
            [mocker.call(source_dir, None), mocker.call(target_dir, None)]  # type: ignore
        )

    # Mixed input types (Path and str) should raise ValueError
    def test_mixed_types_raises_error(self) -> None:
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


class TestReplaceDirectory:
    # Replace directory in Path object with new Path directory while keeping filename
    def test_replace_directory_with_path_objects(self) -> None:
        # Arrange
        filepath = Path("/old/dir/file.txt")
        target_dir = Path("/new/dir")

        # Act
        result = replace_directory(filepath, target_dir)

        # Assert
        assert result == Path("/new/dir/file.txt")
        assert isinstance(result, Path)

    # Replace directory in string path with new string directory while keeping filename
    def test_replace_directory_with_string_paths(self) -> None:
        # Arrange
        filepath = "gs://old/dir/file.txt"
        target_dir = "gs://new/dir/"

        # Act
        result = replace_directory(filepath, target_dir)

        # Assert
        assert result == "gs://new/dir/file.txt"
        assert isinstance(result, str)

    # Both inputs are different types (Path vs str) triggering ValueError
    def test_replace_directory_with_mixed_types_raises_error(self) -> None:
        # Arrange
        filepath = Path("/old/dir/file.txt")
        target_dir = "gs://bucket/new/dir/"

        # Act & Assert
        with pytest.raises(TypeError) as exc_info:
            replace_directory(filepath, target_dir)
        assert (
            str(exc_info.value)
            == "Both filepath and target_dir must be of type Path or str."
        )

    # Check GCS directory format validation
    def test_replace_directory_with_invalid_gcs_directory(self) -> None:
        filepath = "gs://bucket/old_dir/file.txt"
        invalid_target_dir = "invalid_gcs_dir"
        with pytest.raises(ValueError, match="is not a gcs directory"):
            replace_directory(filepath, invalid_target_dir)


class TestGetDirFiles:
    # Delegates to filesystem implementation when directory is a Path and returns its result
    def test_path_delegates_and_returns(self, mocker: MockerFixture) -> None:
        # Arrange
        directory = Path("/tmp/dir")
        expected = [Path("/tmp/dir/a.txt"), Path("/tmp/dir/b.txt")]
        mock_get = mocker.patch(
            "functions.file_abstraction.get_dir_files_filesystem", return_value=expected
        )

        # Act
        result = get_dir_files(directory)

        # Assert
        assert result == expected
        mock_get.assert_called_once_with(directory, None)

    # Delegates to bucket implementation when directory is a str (GCS) and returns its result
    def test_str_delegates_and_returns(self, mocker: MockerFixture) -> None:
        # Arrange
        directory = "gs://bucket/dir/"
        expected = ["gs://bucket/dir/a.txt", "gs://bucket/dir/b.txt"]
        mock_get = mocker.patch(
            "functions.file_abstraction.get_dir_files_bucket", return_value=expected
        )

        # Act
        result = get_dir_files(directory)

        # Assert
        assert result == expected
        mock_get.assert_called_once_with(directory, None)

    # Raises TypeError when directory is neither Path nor str
    def test_invalid_type_raises_typeerror(self) -> None:
        with pytest.raises(TypeError) as exc_info:
            get_dir_files(123)  # type: ignore[arg-type]
        assert str(exc_info.value) == "Type must be Path or string."

    # Passes prefix through for filesystem case
    def test_passes_prefix_path(self, mocker: MockerFixture) -> None:
        # Arrange
        directory = Path("/tmp/dir")
        prefix = "pre_"
        expected = [Path("/tmp/dir/pre_a.txt")]
        mock_get = mocker.patch(
            "functions.file_abstraction.get_dir_files_filesystem", return_value=expected
        )

        # Act
        result = get_dir_files(directory, prefix=prefix)

        # Assert
        assert result == expected
        mock_get.assert_called_once_with(directory, prefix)

    # Passes prefix through for bucket case
    def test_passes_prefix_gcs(self, mocker: MockerFixture) -> None:
        # Arrange
        directory = "gs://bucket/dir/"
        prefix = "pre_"
        expected = ["gs://bucket/dir/pre_a.txt"]
        mock_get = mocker.patch(
            "functions.file_abstraction.get_dir_files_bucket", return_value=expected
        )

        # Act
        result = get_dir_files(directory, prefix=prefix)

        # Assert
        assert result == expected
        mock_get.assert_called_once_with(directory, prefix)
