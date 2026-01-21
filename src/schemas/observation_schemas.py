import pandas as pd
from pandera.pandas import DataFrameModel
from pandera.pandas import Field
from pandera.typing import Series


class ObservationInndataSchema(DataFrameModel):
    """Schema for validating observations inndata dataframe."""

    sourceId: Series[str]
    elementId: Series[str]
    observationTime: Series[pd.DatetimeTZDtype] = Field(dtype_kwargs={"tz": "UTC"})
    value: Series[float]
    unit: Series[str]
