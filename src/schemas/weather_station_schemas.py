from functools import cache

import pandera as pa
from klass import KlassClassification
from pandera import DataFrameModel
from pandera import Field
from pandera.typing import Series


municipality_class_id = "131"
county_class_id = "104"


class WeatherStationInputSchema2(DataFrameModel):
    id: Series[str] = Field(str_startswith="SN", nullable=False, unique=True)
    municipality_id: Series[str] = Field(str_length={"min_value": 4, "max_value": 4})
    county_id: Series[str] = Field(str_length={"min_value": 2, "max_value": 2})

    @pa.check("municipality_id")
    def check_valid_municipality_id(cls, ids: Series[str]) -> Series[bool]:
        return Series(ids.isin(get_valid_class_ids(municipality_class_id)))

    @pa.check("county_id")
    def check_valid_county_ids(cls, ids: Series[str]) -> Series[bool]:
        return Series(ids.isin(get_valid_class_ids(county_class_id)))

    @pa.check("county_id", class_id="131")
    def check_valid_county_ids(cls, ids: Series[str]) -> Series[bool]:
        return Series(ids.isin(get_valid_class_ids(county_class_id)))

    @pa.check("county_id", class_id="104")
    def check_valid_county_ids(cls, ids: Series[str], **kwargs) -> Series[bool]:
        class_id = kwargs.get("class_id")
        print(f"{class_id=}")
        return Series(ids.isin(get_valid_class_ids(class_id)))


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
def get_valid_class_ids(class_id: str) -> list[str]:
    catalog = KlassClassification(class_id)
    return list(catalog.get_codes().to_dict().keys())
