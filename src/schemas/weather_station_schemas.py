from functools import cache

import pandas as pd
import pandera.pandas as pa
from klass import KlassClassification
from pandera import DataFrameModel
from pandera import Field
from pandera.typing import Series


komm_nr_klass_id = "131"
fylke_nr_klass_id = "104"


class WeatherStationInndataSchema(DataFrameModel):
    """Schema for validating weather stations inndata dataframe."""

    id: Series[str] = Field(str_startswith="SN", nullable=False, unique=True)
    name: Series[str]
    shortName: Series[str]
    municipalityId: Series[int] = Field(ge=0, le=9999, nullable=True)
    municipality: Series[str] = Field(nullable=True)
    komm_nr: Series[str] = Field(nullable=True)
    countyId: Series[int] = Field(ge=0, le=99, nullable=True)
    county: Series[str] = Field(nullable=True)
    fylke_nr: Series[str] = Field(nullable=True)
    countryCode: Series[str] = Field(str_length={"min_value": 2, "max_value": 2})
    masl: Series[int] = Field(gt=-500, le=9999, nullable=True)
    coordinates: Series[str] = pa.Field(alias="geometry_coordinates", nullable=True)
    validFrom: Series[pd.DatetimeTZDtype] = Field(dtype_kwargs={"tz": "UTC"})
    validTo: Series[pd.DatetimeTZDtype] | None = Field(
        nullable=True, dtype_kwargs={"tz": "UTC"}
    )


class WeatherStationKlargjortSchema(DataFrameModel):
    """Schema for validating weather stations klargjort dataframe."""

    id: Series[str] = Field(str_startswith="SN", nullable=False, unique=True)
    name: Series[str]
    shortName: Series[str]
    komm_nr: Series[str]
    fylke_nr: Series[str]
    countryCode: Series[str] = Field(str_length={"min_value": 2, "max_value": 2})
    masl: Series[int] = Field(gt=-500, le=9999, nullable=True)

    @pa.check("komm_nr")
    def check_valid_komm_nr_ids(cls, ids: Series[str]) -> Series[bool]:
        """Custom check for valid komm_nr IDs."""
        return Series(ids.isin(get_valid_klass_ids(komm_nr_klass_id)))

    @pa.check("fylke_nr")
    def check_valid_fylke_nr_ids(cls, ids: Series[str]) -> Series[bool]:
        """Custom check for valid fylke_nr IDs."""
        return Series(ids.isin(get_valid_klass_ids(fylke_nr_klass_id)))


@cache  # Cache the result of the calls to this function
def get_valid_klass_ids(klass_id: str) -> list[str]:
    """Use Klass to check for valid keys like komm_nr and fylke_nr."""
    catalog = KlassClassification(klass_id)
    return list(catalog.get_codes().to_dict().keys())
