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
    assert isinstance(settings.kildedata_root_dir, Path)
    assert isinstance(settings.product_root_dir, Path)
    assert isinstance(settings.pre_inndata_dir, Path)
    assert isinstance(settings.inndata_dir, Path)

    # Check variable substitution
    assert settings.product_root_dir == Path("/buckets/produkt/metstat/")
    assert settings.pre_inndata_dir == Path(
        "/buckets/produkt/metstat/inndata/temp/pre-inndata/frost/"
    )
    assert settings.inndata_dir == Path("/buckets/produkt/metstat/inndata/frost")


def test_local_files_env(local_files_env) -> None:
    # In this environment the returned directory shall be of type pathlib.Path
    assert isinstance(settings.kildedata_root_dir, Path)
    assert isinstance(settings.product_root_dir, Path)
    assert isinstance(settings.pre_inndata_dir, Path)
    assert isinstance(settings.inndata_dir, Path)

    # Check variable substitution and converting of relative path
    product_dir_result = settings.product_root_dir
    product_dir_facit = (Path(__file__).parent / Path("../data/metstat/")).resolve()
    assert product_dir_result == product_dir_facit

    pre_inndata_dir_result = settings.pre_inndata_dir
    pre_inndata_dir_facit = (
        Path(__file__).parent / Path("../data/metstat/inndata/temp/pre-inndata/frost/")
    ).resolve()
    assert pre_inndata_dir_result == pre_inndata_dir_facit

    inndata_dir_result = settings.inndata_dir
    inndata_dir_facit = (
        Path(__file__).parent / Path("../data/metstat/inndata/frost")
    ).resolve()
    assert inndata_dir_result == inndata_dir_facit


def test_default_env(default_env) -> None:
    assert isinstance(settings.kildedata_root_dir, str)
    assert isinstance(settings.product_root_dir, str)
    assert isinstance(settings.pre_inndata_dir, str)
    assert isinstance(settings.inndata_dir, str)

    # Check variable substitution
    assert (
        settings.product_root_dir == "gs://ssb-tip-tutorials-data-produkt-prod/metstat/"
    )
    assert (
        settings.pre_inndata_dir
        == "gs://ssb-tip-tutorials-data-produkt-prod/metstat/inndata/temp/pre-inndata/frost/"
    )
    assert (
        settings.inndata_dir
        == "gs://ssb-tip-tutorials-data-produkt-prod/metstat/inndata/frost/"
    )
