import pytest

from functions.ssbplatforms import is_dapla
from notebooks.collect_data import get_latest_jsonfile_content


@pytest.mark.skipif(not is_dapla(), reason="Bucket tests only runs on Dapla")
def test_get_latest_jsonfile_content_bucket() -> None:
    bucket_file = (
        "gs://ssb-tip-tutorials-data-produkt-prod/temp/versiontests/tc2/sources_v1.json"
    )
    result = get_latest_jsonfile_content(bucket_file)
    assert len(result) == 1
