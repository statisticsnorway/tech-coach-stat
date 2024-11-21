"""Load configurations files into a settings variable.

This module loads the configurations stored in the `config/settings.toml` file and make
them available in the settings variable.

The default config environment is working with buckets on Dapla. You can change the
environment by setting the env variable to `daplalab_files` or `local_files`.
See the file `config/settings.toml` for details.
"""

from pathlib import Path

from dynaconf import Dynaconf
from dynaconf import Validator


def absolute_path(relative_path: str) -> Path:
    """Converts a relative path based on the config directory into an absolute path.

    This function takes a relative path as input and appends it to the base config
    directory path. The resulting path is then resolved to an absolute path.
    The type in converted from `str` to `pathlib.Path`.

    Args:
        relative_path: The relative file path to be converted.

    Returns:
        The absolute path corresponding to the given relative path.
    """
    # Assuming this file is 3 levels below repository root
    base_dir = Path(__file__).parent.parent.parent / "config"
    return (base_dir / Path(relative_path)).resolve()


settings = Dynaconf(
    settings_files=["settings.toml"],
    envvar_prefix="DYNACONF",
    environments=True,
    env="default",  # Change this to switch environment: daplalab_files or local_files
    validators=[
        Validator(
            "kildedata_root_dir",
            "product_root_dir",
            "pre_inndata_dir",
            "weather_stations_kildedata_file",
            must_exist=True,
            cast=Path,
            env="daplalab_files",
        ),
        Validator(
            "kildedata_root_dir",
            "product_root_dir",
            "pre_inndata_dir",
            "weather_stations_kildedata_file",
            must_exist=True,
            cast=absolute_path,
            env="local_files",
        ),
        Validator(
            "dapla_team",
            "collect_from_date",
            "collect_to_date",
            "weather_station_names",
            must_exist=True,
        ),
    ],
)
