from pathlib import Path

import pytest

from functions.config import settings
from functions.ssbplatforms import is_dapla
from notebooks.collect_data import get_latest_jsonfile_content


def test_get_latest_jsonfile_content_pathlib() -> None:
    print(f"product_root_dir = {settings.product_root_dir}")
    jsonfile_pathlib = Path(__file__).parent / "testdata" / "sources_v1.json"
    result = get_latest_jsonfile_content(jsonfile_pathlib)
    assert len(result) == 1


@pytest.mark.skipif(not is_dapla(), reason="Bucket tests only runs on Dapla")
def test_get_latest_jsonfile_content_bucket() -> None:
    jsonfile_bucket = (
        f"{settings.product_root_dir}/temp/versiontests/tc2/sources_v1.json"
    )
    result = get_latest_jsonfile_content(jsonfile_bucket)
    assert len(result) == 1
