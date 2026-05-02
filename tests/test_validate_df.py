import pandas as pd
import pandera as pa
import pytest
from pandera.pandas import DataFrameModel
from pandera.pandas import Field
from pandera.typing import Series

from functions.validate_df import validate_df


class SampleSchema(DataFrameModel):
    """Schema for validating and testing testcases."""

    id: Series[str] = Field(str_startswith="SN", nullable=False, unique=True)
    price: Series[int] = Field(gt=1)
    quantity: Series[int] = Field(in_range={"min_value": 1, "max_value": 100})
    value: Series[int]

    class Config:
        """Configurations for the schema."""

        strict = True  # No other columns are allowed

    @pa.dataframe_check
    def value_equals_price_times_quantity(cls, df: pd.DataFrame) -> Series[bool]:
        return df["value"] == df["price"] * df["quantity"]


@pytest.fixture
def valid_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": ["SN001", "SN002", "SN003"],
            "price": [10, 20, 5],
            "quantity": [3, 5, 10],
            "value": [30, 100, 50],
        }
    )


def test_valid_df_has_no_errors(valid_df):
    result = validate_df(valid_df, SampleSchema)
    assert result.has_errors is False


def test_valid_df_all_rows_are_valid(valid_df):
    result = validate_df(valid_df, SampleSchema)
    assert len(result.valid_rows) == len(valid_df)


def test_valid_df_non_valid_rows_is_empty(valid_df):
    result = validate_df(valid_df, SampleSchema)
    assert result.non_valid_rows.empty


def test_valid_df_non_row_errors_is_empty(valid_df):
    result = validate_df(valid_df, SampleSchema)
    assert result.non_row_errors.empty


def test_invalid_id_prefix_detected():
    df = pd.DataFrame(
        {
            "id": ["SN001", "XX002"],
            "price": [10, 20],
            "quantity": [3, 5],
            "value": [30, 100],
        }
    )
    result = validate_df(df, SampleSchema)
    assert result.has_errors is True


def test_invalid_id_prefix_row_is_non_valid(valid_df):
    df = pd.DataFrame(
        {
            "id": ["SN001", "XX002"],
            "price": [10, 20],
            "quantity": [3, 5],
            "value": [30, 100],
        }
    )
    result = validate_df(df, SampleSchema)
    assert len(result.non_valid_rows) == 1


def test_null_id_detected():
    df = pd.DataFrame(
        {
            "id": ["SN001", None],
            "price": [10, 20],
            "quantity": [3, 5],
            "value": [30, 100],
        }
    )
    result = validate_df(df, SampleSchema)
    assert result.has_errors is True


def test_duplicate_id_detected():
    df = pd.DataFrame(
        {
            "id": ["SN001", "SN001"],
            "price": [10, 20],
            "quantity": [3, 5],
            "value": [30, 100],
        }
    )
    result = validate_df(df, SampleSchema)
    assert result.has_errors is True


def test_price_not_greater_than_one_detected():
    df = pd.DataFrame(
        {
            "id": ["SN001", "SN002"],
            "price": [10, 1],
            "quantity": [3, 5],
            "value": [30, 5],
        }
    )
    result = validate_df(df, SampleSchema)
    assert result.has_errors is True


def test_price_not_greater_than_one_row_is_non_valid():
    df = pd.DataFrame(
        {
            "id": ["SN001", "SN002"],
            "price": [10, 1],
            "quantity": [3, 5],
            "value": [30, 5],
        }
    )
    result = validate_df(df, SampleSchema)
    assert len(result.non_valid_rows) == 1


def test_quantity_below_minimum_detected():
    df = pd.DataFrame(
        {
            "id": ["SN001"],
            "price": [10],
            "quantity": [0],
            "value": [0],
        }
    )
    result = validate_df(df, SampleSchema)
    assert result.has_errors is True


def test_quantity_above_maximum_detected():
    df = pd.DataFrame(
        {
            "id": ["SN001"],
            "price": [10],
            "quantity": [101],
            "value": [1010],
        }
    )
    result = validate_df(df, SampleSchema)
    assert result.has_errors is True


def test_value_not_equal_price_times_quantity_detected():
    df = pd.DataFrame(
        {
            "id": ["SN001", "SN002"],
            "price": [10, 20],
            "quantity": [3, 5],
            "value": [30, 99],  # 99 != 20 * 5
        }
    )
    result = validate_df(df, SampleSchema)
    assert result.has_errors is True


def test_extra_column_detected():
    df = pd.DataFrame(
        {
            "id": ["SN001"],
            "price": [10],
            "quantity": [3],
            "value": [30],
            "extra": ["not_allowed"],
        }
    )
    result = validate_df(df, SampleSchema)
    assert result.has_errors is True


def test_valid_rows_exclude_invalid_ones():
    df = pd.DataFrame(
        {
            "id": ["SN001", "SN002", "SN003"],
            "price": [10, 20, 5],
            "quantity": [3, 5, 10],
            "value": [30, 100, 999],  # row 2 invalid: 999 != 5 * 10
        }
    )
    result = validate_df(df, SampleSchema)
    assert "SN003" not in result.valid_rows["id"].values


def test_multiple_invalid_rows_all_captured():
    df = pd.DataFrame(
        {
            "id": ["SN001", "XX002", "YY003"],
            "price": [10, 20, 5],
            "quantity": [3, 5, 10],
            "value": [30, 100, 50],
        }
    )
    result = validate_df(df, SampleSchema)
    assert len(result.non_valid_rows) == 2


def test_single_valid_row(valid_df):
    single = valid_df.iloc[[0]].reset_index(drop=True)
    result = validate_df(single, SampleSchema)
    assert result.has_errors is False
    assert len(result.valid_rows) == 1


def test_missing_required_column_returns_non_row_errors():
    class MissingColumnSchema(DataFrameModel):
        required_col: Series[int]

    df = pd.DataFrame({"other_col": [1, 2, 3]})
    result = validate_df(df, MissingColumnSchema)

    assert result.has_errors is True
    assert result.valid_rows.empty
    assert result.non_valid_rows.empty
    assert not result.non_row_errors.empty
