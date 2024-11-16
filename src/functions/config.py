"""Load configurations files into a settings variable.

This module loads the configurations stored in the `config/settings.toml` file and make
them available in the settings variable.

The default config environment is working with buckets on Dapla. You can change the
environment py setting the env variable to `daplalab_files` or `local_files`.
See the file `config/settings.toml` for details.
"""

from pathlib import Path

from dapla import repo_root_dir
from dynaconf import Dynaconf
from dynaconf import Validator


def absolute_path(relative_path: str) -> Path:
    """Convert a relative path as str to a pathlib.Path object with an absolute path."""
    base_dir = repo_root_dir(Path(__file__).parent) / "config"  # The paths are relative the config directory
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
            "weather_stations_kildedata_file",
            must_exist=True,
            cast=Path,
            env="daplalab_files",
        ),
        Validator(
            "kildedata_root_dir",
            "product_root_dir",
            "weather_stations_kildedata_file",
            must_exist=True,
            cast=absolute_path,
            env="local_files",
        ),
    ],
)
