import logging
from typing import NamedTuple

import pandas as pd
from pandera.errors import SchemaError
from pandera.errors import SchemaErrors
from pandera.pandas import DataFrameModel

logger = logging.getLogger(__name__)


class ValidationResult(NamedTuple):
    """Named tuple for storing validation results."""

    valid_rows: pd.DataFrame
    non_valid_rows: pd.DataFrame
    non_row_errors: pd.DataFrame
    has_errors: bool


def validate_df(df: pd.DataFrame, schema: DataFrameModel) -> ValidationResult:
    """Validates a given DataFrame against a specified schema.

    This function takes a pandas DataFrame and validates it using a
    provided Pandera DataFrameModel schema. If the DataFrame conforms to the
    rules defined in the schema, a successful validation result is
    returned. Otherwise, the function catches validation errors and
    constructs an appropriate ValidationResult object to indicate the
    failure.

    Args:
        df: The DataFrame to be validated.
        schema: The schema used for validation, which
            defines the rules the DataFrame must conform to.

    Returns:
        ValidationResult: An object representing the outcome of the validation
            process. It contains details on whether the validation was
            successful, as well as any errors if applicable.
    """
    try:
        validated = schema.validate(df, lazy=True)
        return ValidationResult(validated, pd.DataFrame(), pd.DataFrame(), False)
    except SchemaErrors as exc:
        return _validation_result_from_schema_errors(exc)
    except SchemaError as exc:
        return _validation_result_from_single_error(exc)


def _validation_result_from_schema_errors(exc: SchemaErrors) -> ValidationResult:
    failure_cases = exc.failure_cases
    if "index" not in failure_cases.columns:
        return ValidationResult(pd.DataFrame(), pd.DataFrame(), failure_cases, True)

    row_level_errors = failure_cases[failure_cases["index"].notna()]
    failed_rows = exc.data.loc[row_level_errors["index"].unique()]
    schema_level_errors = failure_cases[failure_cases["index"].isna()]
    if not schema_level_errors.empty:
        return ValidationResult(pd.DataFrame(), failed_rows, schema_level_errors, True)
    valid_df = exc.data.drop(index=failed_rows.index)
    return ValidationResult(valid_df, failed_rows, pd.DataFrame(), True)


def _validation_result_from_single_error(exc: SchemaError) -> ValidationResult:
    failure_cases = exc.failure_cases
    if "index" not in failure_cases.columns:
        return ValidationResult(pd.DataFrame(), pd.DataFrame(), failure_cases, True)

    row_level_errors = failure_cases[failure_cases["index"].notna()]
    failed_rows = exc.data.loc[row_level_errors["index"].unique()]
    if failed_rows.empty:  # No row-level errors, so it's a schema-level error
        return ValidationResult(pd.DataFrame(), pd.DataFrame(), failure_cases, True)

    valid_df = exc.data.drop(index=failed_rows.index)
    logger.warning("The validated dataframe might not be valid.")
    return ValidationResult(valid_df, failed_rows, pd.DataFrame(), True)
