from pathlib import Path

from functions.config import settings


def test_default_env() -> None:
    assert isinstance(settings.product_root_dir, str)


def test_daplalab_files_env() -> None:
    # In this environment the returned directory shall be of type pathlib.Path
    assert isinstance(settings.from_env("daplalab_files").product_root_dir, Path)


def test_local_files_env() -> None:
    # In this environment the returned directory shall be of type pathlib.Path
    assert isinstance(settings.from_env("local_files").product_root_dir, Path)
