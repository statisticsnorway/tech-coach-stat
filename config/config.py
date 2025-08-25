"""Load configurations files into a settings variable.

This module loads the configurations stored in the `config/settings.toml` file and make
them available in the settings variable.

The default config environment is working with buckets on DaplaLab. You can change the
environment by setting the env variable to `daplalab_files` or `local_files`.
See the file `config/settings.toml` for details.
"""

from pathlib import Path

from dynaconf import Dynaconf
from dynaconf import Validator


def _is_valid_gcs_directory(value: str) -> bool:
    """Check if a string is a valid Google Cloud Storage directory path."""
    return value.startswith("gs://") and value.endswith("/")


def _absolute_path(relative_path: str) -> Path:
    """Converts a relative path based on the config directory into an absolute path.

    This function is used to convert a relative path in `settings.toml` to an absolute
    path as a pathlib.Path object. The base directory for the relative paths is the
    config directory in the repo.

    Args:
        relative_path: The relative file path to be converted.

    Returns:
        The absolute path corresponding to the given relative path.
    """
    base_dir = Path(__file__).parent
    return (base_dir / Path(relative_path)).resolve()


settings = Dynaconf(
    settings_files=["settings.toml"],
    envvar_prefix="DYNACONF",
    environments=True,
    env="local_files",  # Change this to switch environment: default, default_test, daplalab_files or local_files
    validators=[
        Validator(
            "kildedata_root_dir",
            "product_root_dir",
            "pre_inndata_dir",
            "inndata_dir",
            "klargjort_dir",
            "pre_edit_dir",
            must_exist=True,
            condition=_is_valid_gcs_directory,
            env="default",
        ),
        Validator(
            "kildedata_root_dir",
            "product_root_dir",
            "pre_inndata_dir",
            "inndata_dir",
            "klargjort_dir",
            "pre_edit_dir",
            must_exist=True,
            cast=Path,
            env="daplalab_files",
        ),
        Validator(
            "kildedata_root_dir",
            "product_root_dir",
            "pre_inndata_dir",
            "inndata_dir",
            "klargjort_dir",
            "pre_edit_dir",
            must_exist=True,
            cast=_absolute_path,
            env="local_files",
        ),
        Validator(
            "dapla_team",
            "short_name",
            "gcp_project_id",
            "weather_stations_file_prefix",
            "observations_file_prefix",
            "collect_from_date",
            "weather_station_names",
            must_exist=True,
        ),
    ],
)
