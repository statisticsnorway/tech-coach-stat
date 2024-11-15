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


def test_variable_expansion() -> None:
    assert (
        settings.weather_stations_kildedata_file
        == "gs://ssb-tip-tutorials-data-kilde-prod/weather_stations_v1.json"
    )

    dlf_result = settings.from_env("daplalab_files").weather_stations_kildedata_file
    assert dlf_result == Path(r"\bucket\kildedata\weather_stations_v1.json")

    lf_result = settings.from_env("local_files").weather_stations_kildedata_file
    lf_facit = (
        Path(__file__).parent / Path(r"..\data\kildedata\weather_stations_v1.json")
    ).resolve()
    assert lf_result == lf_facit
