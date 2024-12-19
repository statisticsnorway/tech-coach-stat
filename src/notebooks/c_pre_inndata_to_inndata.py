import pandas as pd
import pandera as pa

from functions.config import settings
from functions.file_abstraction import read_parquet_file
from functions.timeit import timeit
from functions.versions import get_latest_file_version
from schemas.weather_station_schemas import WeatherStationInputSchema


def get_latest_weather_stations() -> pd.DataFrame:
    """Fetches the latest weather stations data from a specified parquet file.

    Raises:
        FileNotFoundError: If the file is not found or is not readable.

    Returns:
        A dataframe containing the data from the latest weather stations parquet file.
    """
    # TODO: the joining below only works for Path objects, fix when str
    base_file = settings.pre_inndata_dir / "weather_stations.parquet"
    if latest_file := get_latest_file_version(base_file):
        return read_parquet_file(latest_file)
    else:
        raise FileNotFoundError(f"File {latest_file!s} not found or not readable")


@timeit
def validate_weather_stations_input(ws_df: pd.DataFrame) -> None:
    """Validate weather stations pre-inndata dataframe."""
    try:
        WeatherStationInputSchema.validate(ws_df, lazy=True)
    except pa.errors.SchemaErrors as errors:
        print(errors)

        # Print the failing rows
        failure_cases = errors.failure_cases
        failed_indices = failure_cases["index"].unique()
        columns_to_print = ["id", "name", "municipalityId", "countyId"]
        failed_rows = ws_df.loc[failed_indices, columns_to_print]
        print("Failed Rows:")
        print(failed_rows)


ws_df = get_latest_weather_stations()

# Convert municipalityId and countyId to column names defined by Standardutvalget.
# Also convert type to from int to str with leading zeros, as required by Klass.
# See https://ssbno.sharepoint.com/sites/Avdelingerutvalgograd/SitePages/Vedtak-fra-Standardutvalget.aspx#standardnavn-for-enhetstypeidentifikatorer

ws_df["komm_nr"] = ws_df["municipalityId"].astype(str).str.zfill(4)
ws_df["fylke_nr"] = ws_df["countyId"].astype(str).str.zfill(2)

validate_weather_stations_input(ws_df)
