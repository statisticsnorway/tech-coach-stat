from pathlib import Path

import pytest

from functions.config import settings
from functions.file_abstraction import read_json_file
from functions.ssbplatforms import is_dapla


def test_read_json_pathlib() -> None:
    print(f"product_root_dir = {settings.product_root_dir}")
    jsonfile_pathlib = Path(__file__).parent / "testdata" / "weather_stations_v1.json"
    result = read_json_file(jsonfile_pathlib)
    assert len(result) == 1


@pytest.mark.skipif(not is_dapla(), reason="Bucket tests only runs on Dapla")
def test_read_json_bucket() -> None:
    jsonfile_bucket = (
        f"{settings.product_root_dir}/temp/versiontests/tc2/sources_v1.json"
    )
    result = read_json_file(jsonfile_bucket)
    assert len(result) == 1
