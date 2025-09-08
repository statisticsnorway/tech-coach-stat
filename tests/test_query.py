import pandas as pd
import pytest

from src.functions.query import get_updated_rows


def test_returns_only_changed_rows_single_key():
    new_df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "val": [10, 20, 30],
            "other": ["a", "b", "c"],
        }
    )
    old_df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "val": [10, 25, 30],  # id=2 changed val
            "other": ["a", "b", "X"],  # id=3 changed other
        }
    )

    result = get_updated_rows(new_df, old_df, primary_key=["id"])

    assert result is not None
    expected = (
        new_df[new_df["id"].isin([2, 3])].sort_values("id").reset_index(drop=True)
    )
    actual = result.sort_values("id").reset_index(drop=True)
    pd.testing.assert_frame_equal(actual, expected)


def test_returns_changed_rows_for_composite_primary_key():
    new_df = pd.DataFrame(
        {
            "id": [1, 1, 2],
            "code": ["A", "B", "A"],
            "val": [10, 20, 30],
        }
    )
    old_df = pd.DataFrame(
        {
            "id": [1, 1, 2],
            "code": ["A", "B", "A"],
            "val": [10, 22, 30],  # (1, B) changed
        }
    )

    result = get_updated_rows(new_df, old_df, primary_key=["id", "code"])

    assert result is not None
    expected = new_df[(new_df["id"] == 1) & (new_df["code"] == "B")].reset_index(
        drop=True
    )
    actual = result.sort_values(["id", "code"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(actual, expected)


def test_ignores_timezone_only_differences_in_datetime_columns():
    new_df = pd.DataFrame(
        {
            "id": [1],
            "ts": [pd.Timestamp("2020-01-01 00:00:00", tz="Europe/Oslo")],  # UTC+1
        }
    )
    old_df = pd.DataFrame(
        {
            "id": [1],
            "ts": [pd.Timestamp("2019-12-31 23:00:00", tz="UTC")],  # same instant
        }
    )

    result = get_updated_rows(new_df, old_df, primary_key=["id"])

    assert result is None


def test_raises_value_error_on_duplicate_composite_keys():
    new_df = pd.DataFrame(
        {
            "id": [1, 2],
            "code": ["A", "B"],
            "val": [10, 20],
        }
    )
    old_df = pd.DataFrame(
        {
            "id": [1, 1],  # duplicate composite key (1, A)
            "code": ["A", "A"],
            "val": [10, 10],
        }
    )

    with pytest.raises(ValueError):
        get_updated_rows(new_df, old_df, primary_key=["id", "code"])


def test_raises_assertion_error_on_invalid_primary_key_argument():
    new_df = pd.DataFrame({"id": [1], "val": [1]})
    old_df = pd.DataFrame({"id": [1], "val": [1]})

    for invalid_pk in ([], "id", None, 123):
        with pytest.raises(AssertionError):
            get_updated_rows(new_df, old_df, primary_key=invalid_pk)  # type: ignore[arg-type]


def test_raises_assertion_error_when_primary_key_column_missing():
    new_df = pd.DataFrame({"id": [1, 2], "val": [10, 20]})
    old_df = pd.DataFrame({"val": [10, 25]})  # missing 'id' column

    with pytest.raises(AssertionError):
        get_updated_rows(new_df, old_df, primary_key=["id"])


def test_returns_none_when_no_changes_in_shared_columns():
    new_df = pd.DataFrame({"id": [1, 2], "val": [10, 20]})
    old_df = pd.DataFrame({"id": [1, 2], "val": [10, 20]})

    result = get_updated_rows(new_df, old_df, primary_key=["id"])

    assert result is None


def test_returns_none_when_no_matching_primary_keys():
    new_df = pd.DataFrame({"id": [1, 2], "val": [10, 20]})
    old_df = pd.DataFrame({"id": [3, 4], "val": [10, 20]})

    result = get_updated_rows(new_df, old_df, primary_key=["id"])

    assert result is None
