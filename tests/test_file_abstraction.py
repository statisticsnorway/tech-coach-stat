import tempfile
from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from functions.config import settings
from functions.file_abstraction import read_json_file
from functions.file_abstraction import read_parquet_file
from functions.file_abstraction import write_parquet_file
from functions.ssbplatforms import is_dapla


def test_read_json_pathlib() -> None:
    jsonfile_pathlib = Path(__file__).parent / "testdata" / "weather_stations_v1.json"
    result = read_json_file(jsonfile_pathlib)
    assert len(result) == 1


@pytest.mark.skipif(not is_dapla(), reason="Bucket tests only runs on Dapla")
def test_read_json_bucket() -> None:
    root_dir = settings.product_root_dir.removesuffix(f"/{settings.short_name}/")
    jsonfile_bucket = f"{root_dir}/temp/testcase/versiontest/tc2/sources_v1.json"
    result = read_json_file(jsonfile_bucket)
    assert len(result) == 1


def test_read_write_parquet_file_pathlib() -> None:
    data = {
        "Location": ["Kongsvinger", "Oslo"],
        "Population": [18058, 717710],
    }
    original_df = pd.DataFrame(data)

    with tempfile.TemporaryDirectory() as temp_dir:
        filepath = Path(temp_dir) / "test.parquet"
        write_parquet_file(filepath, original_df)
        result_df = read_parquet_file(filepath)
        assert_frame_equal(result_df, original_df)
