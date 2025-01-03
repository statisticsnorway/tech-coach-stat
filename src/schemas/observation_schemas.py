import pandas as pd
from pandera import DataFrameModel
from pandera import Field
from pandera.typing import Series


class ObservationInndataSchema(DataFrameModel):
    """Schema for validating observations inndata dataframe."""

    referenceTime: Series[pd.DatetimeTZDtype] = Field(dtype_kwargs={"tz": "UTC"})
    sourceId: Series[str]
    elementId: Series[str]
    timeOffset: Series[str]
    timeResolution: Series[str]
    value: Series[float]
    unit: Series[str]
