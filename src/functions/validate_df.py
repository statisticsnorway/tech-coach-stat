import logging
from typing import NamedTuple

import pandas as pd
from pandera.errors import SchemaError
from pandera.errors import SchemaErrors
from pandera.pandas import DataFrameModel

logger = logging.getLogger(__name__)


def _flatten_error_messages(message: object) -> pd.DataFrame:
    """Flatten pandera error messages into a readable table."""
    if isinstance(message, dict):
        rows = []
        for _category, reasons in message.items():
            for _reason, errors in reasons.items():
                rows.extend(
                    {
                        "description": error.get("error"),
                    }
                    for error in errors
                )
        return pd.DataFrame(rows)
    return pd.DataFrame({"description": [message]}) if message else pd.DataFrame()


def _format_failure_cases(failure_cases: pd.DataFrame) -> pd.DataFrame:
    """Select and rename failure case fields for logging."""
    columns = [
        column
        for column in ["index", "column", "check", "failure_case"]
        if column in failure_cases.columns
    ]
    return failure_cases[columns].rename(
        columns={"check": "failed_check", "failure_case": "failed_value"}
    )


def _log_validation_errors(
    exc: SchemaErrors | SchemaError,
    schema_name: str,
    failure_cases: pd.DataFrame,
    failed_rows: pd.DataFrame | None = None,
) -> None:
    """Log validation errors with failure cases and optional failed rows."""
    error_messages = _flatten_error_messages(getattr(exc, "message", None))
    if error_messages.empty and exc.args:
        error_messages = _flatten_error_messages(exc.args[0])
    if not error_messages.empty:
        logger.info(
            "%s validation error descriptions:\n%s",
            schema_name,
            error_messages.to_string(index=False, header=False),
        )
    failure_details = _format_failure_cases(failure_cases)
    if not failure_details.empty:
        logger.info(
            "%s validation failed checks and values:\n%s",
            schema_name,
            failure_details.to_string(index=False, header=True),
        )
    if failed_rows is not None and not failed_rows.empty:
        logger.debug(
            "%i rows failed %s validation:\n%s",
            len(failed_rows),
            schema_name,
            failed_rows.to_string(),
        )


class ValidationResult(NamedTuple):
    """Named tuple for storing validation results."""

    valid_rows: pd.DataFrame
    non_valid_rows: pd.DataFrame
    failure_cases: pd.DataFrame
    has_errors: bool


def validate_df(
    df: pd.DataFrame, schema: type[DataFrameModel], lazy: bool = True
) -> ValidationResult:
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
        lazy: If True, all errors will be collected and returned in one go.
            If False, validation will stop at the first error encountered.
            Defaults to True.

    Returns:
        ValidationResult: An object representing the outcome of the validation
            process. It contains details on whether the validation was
            successful, as well as any errors if applicable.
    """
    try:
        validated = schema.validate(df, lazy=lazy)
        logger.info("%s validation successful", schema.__name__)
        return ValidationResult(validated, pd.DataFrame(), pd.DataFrame(), False)
    except (SchemaErrors, SchemaError) as exc:
        failure_cases = exc.failure_cases
        if not isinstance(failure_cases, pd.DataFrame):
            error_messages = pd.DataFrame(exc.args, columns=["error_message"])
            logger.warning(
                "%s validation failed with %i non-dataframe schema errors",
                schema.__name__,
                len(exc.args),
            )
            _log_validation_errors(exc, schema.__name__, error_messages)
            return ValidationResult(
                pd.DataFrame(), pd.DataFrame(), error_messages, True
            )
        if "index" not in failure_cases.columns:
            logger.warning(
                "%s validation failed with %i non-index schema errors",
                schema.__name__,
                len(failure_cases),
            )
            _log_validation_errors(exc, schema.__name__, failure_cases)
            return ValidationResult(pd.DataFrame(), pd.DataFrame(), failure_cases, True)

        row_level_errors = failure_cases[failure_cases["index"].notna()]
        failed_rows = exc.data.loc[row_level_errors["index"].unique()]
        schema_level_errors = failure_cases[failure_cases["index"].isna()]
        if not schema_level_errors.empty:
            logger.warning(
                "%s validation failed with %i schema errors and %i row errors",
                schema.__name__,
                len(schema_level_errors),
                len(failed_rows),
            )
            _log_validation_errors(exc, schema.__name__, failure_cases, failed_rows)
            return ValidationResult(pd.DataFrame(), failed_rows, failure_cases, True)
        valid_rows = exc.data.drop(index=failed_rows.index)
        logger.warning(
            "%s validation failed with %i row errors and %i valid rows",
            schema.__name__,
            len(failed_rows),
            len(valid_rows),
        )
        _log_validation_errors(exc, schema.__name__, failure_cases, failed_rows)
        return ValidationResult(valid_rows, failed_rows, failure_cases, True)
