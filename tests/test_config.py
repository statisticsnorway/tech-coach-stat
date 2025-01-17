from pathlib import Path

import pytest

from config.config import settings


@pytest.fixture(scope="session")
def default_env() -> None:
    settings.configure(FORCE_ENV_FOR_DYNACONF="default")


@pytest.fixture(scope="session")
def daplalab_files_env() -> None:
    settings.configure(FORCE_ENV_FOR_DYNACONF="daplalab_files")


@pytest.fixture(scope="session")
def local_files_env() -> None:
    settings.configure(FORCE_ENV_FOR_DYNACONF="local_files")


def test_daplalab_files_env(daplalab_files_env) -> None:
    # In this environment the returned directory shall be of type pathlib.Path
    assert isinstance(settings.product_root_dir, Path)

    # Check variable substitution
    assert settings.product_root_dir == Path(r"/buckets/produkt/metstat/")


def test_local_files_env(local_files_env) -> None:
    # In this environment the returned directory shall be of type pathlib.Path
    assert isinstance(settings.kildedata_root_dir, Path)
    assert isinstance(settings.product_root_dir, Path)
    assert isinstance(settings.pre_inndata_dir, Path)

    # Check variable substitution and converting of relative path
    lf_result = settings.product_root_dir
    lf_facit = (Path(__file__).parent / Path(r"../data/metstat/")).resolve()
    assert lf_result == lf_facit


def test_default_env(default_env) -> None:
    assert isinstance(settings.product_root_dir, str)

    # Check variable substitution
    assert (
        settings.product_root_dir == "gs://ssb-tip-tutorials-data-produkt-prod/metstat/"
    )
