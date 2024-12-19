from functools import cache

import pandera as pa
from klass import KlassClassification
from pandera import DataFrameModel
from pandera import Field
from pandera.typing import Series


komm_nr_klass_id = "131"
fylke_nr_klass_id = "104"


class WeatherStationInputSchema(DataFrameModel):
    """Schema for validating weather stations pre-inndata dataframe."""

    id: Series[str] = Field(str_startswith="SN", nullable=False, unique=True)
    municipalityId: Series[int] = Field(in_range={"min_value": 0, "max_value": 9999})
    countyId: Series[int] = Field(in_range={"min_value": 0, "max_value": 99})
    komm_nr: Series[str]
    fylke_nr: Series[str]

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
