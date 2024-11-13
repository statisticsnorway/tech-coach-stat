"""Load configurations files into a settings variable.

This module loads the configurations stored in the config directory and make them
available in the settings variable.
"""

from dynaconf import Dynaconf


settings = Dynaconf(
    envvar_prefix="DYNACONF",
    settings_files=["settings.toml"],
)

# `envvar_prefix` = export envvars with `export DYNACONF_FOO=bar`.
# `settings_files` = Load these files in the order.
