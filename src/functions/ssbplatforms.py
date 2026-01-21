"""This module simplifies detecting the runtime environment in Statistics Norway (SSB).

The code is based on the environment variables described at:
https://manual.dapla.ssb.no/faq.html#hvilke-milj%C3%B8variabler-er-tilgjengelig-i-utviklingstjenestene
"""

import os


def is_dapla(group_context: str | None = None) -> bool:
    """True if running on DAPLA_LAB, false otherwise."""
    region = os.getenv("DAPLA_REGION")
    region_result = bool(region and region == "DAPLA_LAB")
    if group_context:
        group = os.getenv("DAPLA_GROUP_CONTEXT")
        return bool(region_result and group and group == group_context)
    return region_result


def is_on_prem() -> bool:
    """True if running on the on prem environment, false otherwise."""
    region = os.getenv("DAPLA_REGION")
    return bool(region and region == "ON_PREM")


def is_data_admin() -> bool:
    """True if running as a data admin, that is in the kildeproject, false otherwise."""
    group = os.getenv("DAPLA_GROUP_CONTEXT")
    return bool(group and "data-admin" in group)
