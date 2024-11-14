"""Load configurations files into a settings variable.

This module loads the configurations stored in the `config/settings.toml` file and make
them available in the settings variable.

The default config environment is working with buckets on Dapla. You can change the
environment py setting the env variable to `daplalab_files` or `local_files`.
See the file `config/settings.toml` for details.
"""

from pathlib import Path

from dynaconf import Dynaconf
from dynaconf import Validator


settings = Dynaconf(
    settings_files=["settings.toml"],
    environments=True,
    env="default",
    validators=[
        Validator(
            "kildedata_root_dir",
            "product_root_dir",
            must_exist=True,
            cast=Path,
            env="daplalab_files",
        ),
        Validator(
            "kildedata_root_dir",
            "product_root_dir",
            must_exist=True,
            cast=Path,
            env="local_files",
        ),
    ],
)
