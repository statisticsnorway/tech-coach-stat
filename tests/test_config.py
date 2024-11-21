from pathlib import Path

import pytest

from functions.config import settings


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
    file = settings.weather_stations_kildedata_file
    assert file == Path(r"/buckets/kilde/metstat/frost/weather_stations_v1.json")


def test_local_files_env(local_files_env) -> None:
    # In this environment the returned directory shall be of type pathlib.Path
    assert isinstance(settings.kildedata_root_dir, Path)
    assert isinstance(settings.product_root_dir, Path)
    assert isinstance(settings.pre_inndata_dir, Path)

    # Check variable substitution and converting of relative path
    lf_result = settings.weather_stations_kildedata_file
    lf_facit = (
        Path(__file__).parent
        / Path(r"../data/metstat/kildedata/frost/weather_stations_v1.json")
    ).resolve()
    assert lf_result == lf_facit


def test_default_env(default_env) -> None:
    assert isinstance(settings.product_root_dir, str)

    # Check variable substitution
    assert (
        settings.weather_stations_kildedata_file
        == "gs://ssb-tip-tutorials-data-kilde-prod/metstat/frost/weather_stations_v1.json"
    )
