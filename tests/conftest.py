import pandas as pd
import pytest


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
        "geometry.coordinates": ["12.0067, 60.1903", "10.7167, 59.9333"],
    }
    return pd.DataFrame(data)
