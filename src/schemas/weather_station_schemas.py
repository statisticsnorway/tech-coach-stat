from functools import lru_cache

import pandera as pa
from klass import KlassClassification
from pandera import DataFrameModel
from pandera import Field
from pandera.typing import Series


class WeatherStationInputSchema(DataFrameModel):
    """Schema for validating weather stations pre-inndata dataframe."""

    id: Series[str] = Field(str_startswith="SN", nullable=False, unique=True)
    municipalityId: Series[int] = Field(in_range={"min_value": 0, "max_value": 9999})
    countyId: Series[int] = Field(in_range={"min_value": 0, "max_value": 99})
    komm_nr: Series[str]
    fylke_nr: Series[str]

    @pa.check("komm_nr", element_wise=True)
    def check_valid_komm_nr_ids(cls, value: str) -> bool:
        """Custom check for valid komm_nr IDs."""
        return value in get_valid_klass_ids("131")

    @pa.check("fylke_nr", element_wise=True)
    def check_valid_fylke_nr_ids(cls, value: str) -> bool:
        """Custom check for valid fylke_nr IDs."""
        return value in get_valid_klass_ids("104")


@lru_cache(maxsize=1)  # Cache the result of the first call to this function
def get_valid_klass_ids(klass_id: str) -> list[str]:
    """Use Klass to check for valid keys like komm_nr and fylke_nr."""
    catalog = KlassClassification(klass_id)
    return list(catalog.get_codes().to_dict().keys())
