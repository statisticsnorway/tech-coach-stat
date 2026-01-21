import pytest

from functions.ssbplatforms import is_dapla
from functions.ssbplatforms import is_data_admin
from functions.ssbplatforms import is_on_prem


@pytest.fixture(autouse=True)
def clear_env(monkeypatch):
    # Ensure a clean environment for each test
    for var in ("DAPLA_REGION", "DAPLA_GROUP_CONTEXT"):
        monkeypatch.delenv(var, raising=False)
    yield


def test_is_dapla_true_when_region_is_dapla_lab(monkeypatch):
    monkeypatch.setenv("DAPLA_REGION", "DAPLA_LAB")
    assert is_dapla() is True


def test_is_dapla_false_when_region_missing_or_other(monkeypatch):
    assert is_dapla() is False
    monkeypatch.setenv("DAPLA_REGION", "ON_PREM")
    assert is_dapla() is False


def test_is_dapla_with_group_context_matches(monkeypatch):
    monkeypatch.setenv("DAPLA_REGION", "DAPLA_LAB")
    monkeypatch.setenv("DAPLA_GROUP_CONTEXT", "proj-foo-data-admin")
    assert is_dapla("proj-foo-data-admin") is True


def test_is_dapla_with_group_context_requires_both(monkeypatch):
    # Correct group but wrong region -> False
    monkeypatch.setenv("DAPLA_GROUP_CONTEXT", "proj-foo-data-admin")
    monkeypatch.setenv("DAPLA_REGION", "ON_PREM")
    assert is_dapla("proj-foo-data-admin") is False
    # Correct region but missing group -> False
    monkeypatch.setenv("DAPLA_REGION", "DAPLA_LAB")
    monkeypatch.delenv("DAPLA_GROUP_CONTEXT", raising=False)
    assert is_dapla("proj-foo-data-admin") is False


def test_is_on_prem_true_when_region_is_on_prem(monkeypatch):
    monkeypatch.setenv("DAPLA_REGION", "ON_PREM")
    assert is_on_prem() is True


def test_is_on_prem_false_other_regions(monkeypatch):
    assert is_on_prem() is False
    monkeypatch.setenv("DAPLA_REGION", "DAPLA_LAB")
    assert is_on_prem() is False


def test_is_data_admin_true_when_group_contains_data_admin(monkeypatch):
    monkeypatch.setenv("DAPLA_GROUP_CONTEXT", "proj-foo-data-admin-bar")
    assert is_data_admin() is True


def test_is_data_admin_false_when_group_missing_or_not_match(monkeypatch):
    assert is_data_admin() is False
    monkeypatch.setenv("DAPLA_GROUP_CONTEXT", "proj-foo")
    assert is_data_admin() is False
