import pandas as pd
from pandas.api.types import is_datetime64_any_dtype


def get_updated_rows(
    new_df: pd.DataFrame, old_df: pd.DataFrame, primary_key: list[str]
) -> pd.DataFrame | None:
    """Compare two dataframes and find rows where there are differences in at least one column.

    Identifies rows in the new DataFrame that are updated compared to the old
    DataFrame. Only differences in values from corresponding columns are considered.
    Returns the updated rows from the new DataFrame if differences are found; otherwise, returns None.

    Args:
        new_df: The DataFrame containing the new data.
        old_df: The DataFrame containing the old data to compare against.
        primary_key: The list of column names used as the (composite) primary key
            to match rows between the two DataFrames. Provide one or more column names.

    Returns:
        A DataFrame containing the rows from `new_df` that have changed, or None if no changes are detected.

    Raises:
        ValueError: If the primary key columns contain duplicate combinations in either
            `new_df` or `old_df`.
        AssertionError: If any primary key column does not exist in either `new_df` or `old_df`.
    """
    # Normalize primary_key to list and validate
    if not isinstance(primary_key, list) or len(primary_key) == 0:
        raise AssertionError("primary_key must be a non-empty list of column names")
    for key in primary_key:
        assert (key in new_df.columns) and (
            key in old_df.columns
        ), f"primary key column {key} not in new_df or old_df"

    # Check uniqueness of the composite key
    if new_df.duplicated(subset=primary_key).any():
        raise ValueError(f"Duplicate composite key {primary_key} in new_df")
    if old_df.duplicated(subset=primary_key).any():
        raise ValueError(f"Duplicate composite key {primary_key} in old_df")

    # Align to common keys and same index
    new_df_idx = new_df.set_index(primary_key, drop=False)
    old_df_idx = old_df.set_index(primary_key, drop=False)
    # Ensure index names are set to the key columns to aid later extraction
    new_df_idx.index.names = primary_key
    old_df_idx.index.names = primary_key
    common_keys = new_df_idx.index.intersection(old_df_idx.index)
    if len(common_keys) == 0:
        return None

    # Align to common columns (exclude primary key columns from value comparisons)
    value_cols = [
        c
        for c in new_df_idx.columns
        if c not in primary_key and c in old_df_idx.columns
    ]
    if not value_cols:
        return None

    # Subset and order identically
    new_aligned = new_df_idx.reindex(common_keys).loc[:, value_cols]
    old_aligned = old_df_idx.reindex(common_keys).loc[:, value_cols]

    # Normalize datetime columns: convert both sides to UTC and drop tz to ignore tz differences
    for col in value_cols:
        if is_datetime64_any_dtype(new_aligned[col]) or is_datetime64_any_dtype(
            old_aligned[col]
        ):
            new_aligned[col] = pd.to_datetime(
                new_aligned[col], utc=True, errors="coerce"
            ).dt.tz_localize(None)
            old_aligned[col] = pd.to_datetime(
                old_aligned[col], utc=True, errors="coerce"
            ).dt.tz_localize(None)

    # Ensure labels are identical before compare (required by pandas)
    assert new_aligned.columns.equals(old_aligned.columns), "Columns are not aligned"
    assert new_aligned.index.equals(old_aligned.index), "Index is not aligned"

    diffs = new_aligned.compare(old_aligned, keep_equal=False, align_axis=0)
    if diffs.empty:
        return None

    # Extract changed rows, for multi-key case
    if isinstance(diffs.index, pd.MultiIndex):
        keys_for_rows_to_update = diffs.index.unique()
        keys_df = keys_for_rows_to_update.to_frame(index=False)
        keys_df.columns = primary_key
        updated_rows = new_df.merge(keys_df, on=primary_key, how="inner")
    else:
        # Single-key case represented as Index
        ids_to_update = diffs.index.unique()
        updated_rows = new_df[new_df[primary_key[0]].isin(ids_to_update)].copy()

    return None if updated_rows.empty else updated_rows
