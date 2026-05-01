from pandera.errors import SchemaErrors
from pandera.errors import SchemaError
from pandera.pandas import DataFrameModel
import pandas as pd
from typing import NamedTuple


class ValidationResult(NamedTuple):
    """Named tuple for storing validation results."""

    valid_rows: pd.DataFrame
    non_valid_rows: pd.DataFrame
    non_row_errors: pd.DataFrame
    has_errors: bool


def validate_df(df: pd.DataFrame, schema: DataFrameModel) -> ValidationResult:
    try:
        validated = schema.validate(df, lazy=True)
        return ValidationResult(validated, pd.DataFrame(), pd.DataFrame(), False)
    except SchemaErrors as exc:
        failure_cases = exc.failure_cases
        if "index" not in failure_cases.columns:
            return ValidationResult(pd.DataFrame(), pd.DataFrame(), failure_cases, True)

        row_level_errors = failure_cases[failure_cases["index"].notna()]
        failed_rows = exc.data.loc[row_level_errors["index"].unique()]
        schema_level_errors = failure_cases[failure_cases["index"].isna()]
        if schema_level_errors.empty:
            valid_df = exc.data.drop(index=failed_rows.index)
            return ValidationResult(valid_df, failed_rows, pd.DataFrame(), True)
        else:
            return ValidationResult(
                pd.DataFrame(), failed_rows, schema_level_errors, True
            )
    except SchemaError as exc:
        print(exc)
        return ValidationResult(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), True)
