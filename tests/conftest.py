"""Pytest configuration and shared fixtures for the test suite.

Registers the ``--integration`` command-line flag and automatically skips any
test marked with ``@pytest.mark.integration`` unless that flag is supplied.
Also provides shared fixtures used across multiple test modules.
"""

import pandas as pd
import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="Run integration tests that make real network requests.",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    if config.getoption("--integration"):
        return
    skip = pytest.mark.skip(reason="Pass --integration to run integration tests.")
    for item in items:
        if item.get_closest_marker("integration"):
            item.add_marker(skip)


@pytest.fixture
def ws_autocorrect() -> pd.DataFrame:
    # pre-inndata with one OK (Kongsvinger) and one not OK (BLINDERN-KVT-IOT)
    data = {
        "id": ["SN5590", "SN499999010"],
        "name": ["KONGSVINGER", "BLINDERN-KVT-IOT"],
        "shortName": ["Kongsvinger", None],
        "municipalityId": [3401, 1111],
        "municipality": ["KONGSVINGER", None],
        "countyId": [34, 11],
        "county": ["INNLANDET", None],
        "countryCode": ["NO", "NO"],
        "masl": [148, 0],
        "geometry_coordinates": ["12.0067, 60.1903", "10.7167, 59.9333"],
        "validFrom": ["2006-07-01T00:00:00.000Z", None],
    }
    df = pd.DataFrame(data)
    # Ensure validFrom has dtype datetime64[ns, UTC]
    df["validFrom"] = pd.to_datetime(df["validFrom"], utc=True)
    return df
