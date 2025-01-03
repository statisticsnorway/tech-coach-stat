# To access the Frost API, you will need a client ID, which you can get by registering
# as a user at https://frost.met.no/howto.html. Place the client ID you receive
# in a .env file in the root directory of the repository. It should look like this:
#
# FROST_CLIENT_ID="5dc4-mange-nummer-e71cc"

import os
import re
from datetime import date
from datetime import timedelta
from pathlib import Path
from typing import Any
from typing import cast

import requests
from dapla import FileClient
from dapla.gsm import get_secret_version
from dotenv import load_dotenv
from google.auth.exceptions import DefaultCredentialsError

from functions.config import settings
from functions.file_abstraction import add_filename_to_path
from functions.file_abstraction import create_dir_if_not_exist
from functions.file_abstraction import read_json_file
from functions.file_abstraction import write_json_file
from functions.versions import get_latest_file_version
from functions.versions import get_next_file_version


def get_weather_stations() -> list[dict[str, Any]]:
    """Retrieve a list of weather stations from the FROST API and manage storage.

    Fetches the data and compares the fetched data to the most recent stored version of
    the data and, if the data has changed, saves the data as new version.

    Returns:
        The fetched weather stations and data about them.
    """
    endpoint = "https://frost.met.no/sources/v0.jsonld"
    parameters = {
        "country": "Norge",
    }
    data = fetch_data(endpoint, parameters)

    # Check if data is changed since last version and write new file if so
    base_file = settings.weather_stations_kildedata_file
    latest_file = get_latest_file_version(base_file)
    latest_data = read_json_file(latest_file) if latest_file is not None else None

    if (not latest_data) or (data != latest_data):
        if (latest_file_version := get_latest_file_version(base_file)) is not None:
            next_file = get_next_file_version(latest_file_version)
        else:
            next_file = settings.weather_stations_kildedata_file
        write_json_file(next_file, data)
        print(f"Storing to {next_file}")
    return data


def get_observations(source_ids_: list[str]) -> list[dict[str, Any]]:
    """Retrieve weather observations from the FROST API for specified sources/locations.

    This function constructs a request to the FROST API to fetch weather observations
    based on the provided source IDs. It processes the response data, saves it to
    a JSON file, and returns the data.

    Args:
        source_ids_: A list of source IDs for which to retrieve observations.
            A source ID is a unique key for an observation location. Example:
            The observation location Blindern in Oslo has source ID SN18700.

    Returns:
        A list of dictionaries containing the weather observation data.
    """
    if latest_date := get_latest_observation_date(settings.kildedata_root_dir):
        from_date_str = (latest_date + timedelta(days=1)).isoformat()
    else:
        from_date_str = settings.collect_from_date

    today_str = date.today().isoformat()
    if from_date_str == today_str:
        print("No new observations to collect.")
        return []

    endpoint = "https://frost.met.no/observations/v0.jsonld"
    parameters = {
        "sources": ",".join(source_ids_),
        "elements": (
            "min(air_temperature P1D),"
            "mean(air_temperature P1D),"
            "max(air_temperature P1D),"
            "sum(precipitation_amount P1D),"
            "max(wind_speed P1D)"
        ),
        "referencetime": f"{from_date_str}/{today_str}",
        "levels": "default",
        "timeoffsets": "default",
    }
    data = fetch_data(endpoint, parameters)
    print("Data retrieved from frost.met.no!")

    filename = f"observations_p{extract_timespan(data)}.json"
    observations_file = add_filename_to_path(settings.kildedata_root_dir, filename)
    print(f"Storing to {observations_file}")

    write_json_file(observations_file, data)
    return data


def frost_client_id() -> str:
    """Get the frost_client_id from Google Secret Manager or environment variable.

    Try to read the secret from Google Secret Manager first, and if it fails:
    fallback to read it from environment variable or .env file.

    Returns:
        The frost_client_id secret

    Raises:
        RuntimeError: If the environment variable is not defined.
    """
    secret_id = "FROST_CLIENT_ID"
    project_id = "tip-tutorials-p-mb"

    try:
        client_id = get_secret_version(project_id, secret_id)
    except DefaultCredentialsError as e:
        print(f"Error: Unable to find GSM credentials. {e} Fallback to use .env file.")

        load_dotenv()
        client_id = os.getenv(secret_id)
        if client_id is None:
            raise RuntimeError(
                f"{secret_id} environment variable is not defined"
            ) from e
    return client_id


def fetch_data(endpoint: str, parameters: dict[str, str]) -> list[dict[str, Any]]:
    """Request data from the FROST API and handle API-errors.

    This function constructs and sends a GET request to the specified API endpoint
    using the provided parameters. It handles authentication using the Frost client ID
    and raises an error if the response status code indicates a failure.

    Args:
        endpoint: The API endpoint to send the request to.
        parameters: A dictionary of parameters to include in the request.

    Returns:
        The data retrieved from the API.

    Raises:
        RuntimeError: If the response status code is not OK (200).
    """
    response = requests.get(endpoint, parameters, auth=(frost_client_id(), ""))
    response_data = response.json()
    if response.status_code != 200:
        raise RuntimeError(
            f"Error! Status code: {response.status_code}, "
            f"Message: {response_data['error']['message']}, "
            f"Reason: {response_data['error']['reason']}"
        )
    return cast(list[dict[str, Any]], response_data["data"])


def extract_timespan(observations: list[dict[str, Any]]) -> str:
    """Extract timespan for the downloaded observations.

    Args:
        observations: A list of observation dictionaries, each containing a
        `referenceTime`key.

    Returns:
        A string representing the timespan in the format "YYYY-MM-DD_pYYYY-MM-DD".
    """
    first_date = observations[0]["referenceTime"][:10]
    last_date = observations[-1]["referenceTime"][:10]
    return f"{first_date}_p{last_date}"


def get_weather_stations_ids(
    weather_stations_names: list[str], weather_stations: list[dict[str, Any]]
) -> list[str]:
    """Find source ids for the provided source names (observation locations).

    Args:
        weather_stations_names: List of observation locations to find ids for.
        weather_stations: List of dictionaries for looking up source ids.

    Returns:
        A list of ids corresponding to the provided weather stations names.
    """
    # Create a dictionary for quick lookup of names to ids
    name_to_id = {
        item["name"]: item["id"]
        for item in weather_stations
        if "name" in item and "id" in item
    }
    return [name_to_id[name] for name in weather_stations_names]


def get_latest_observation_date(directory: Path | str) -> date | None:
    """Find the latest observation date from filenames in the given directory.

    It is based on the file name pattern observations_pYYYY-MM-DD_pYYYY-MM-DD,
    where the last YYYY-MM-DD is the date of the latest observation.

    Args:
        directory: A path to the directory containing observation files.

    Returns:
        The latest date found in the filenames or `None` if no valid dates are found.
    """
    latest_date = None
    if isinstance(directory, Path) and directory.is_dir():
        for filepath in directory.glob("observations*"):
            if filepath.is_file():
                extracted_date = extract_latest_date_from_filename(filepath.name)
                if extracted_date and (
                    latest_date is None or extracted_date > latest_date
                ):
                    latest_date = extracted_date
    elif isinstance(directory, str):
        fs = FileClient.get_gcs_file_system()
        glob_pattern = f"{directory}/observations*"
        files = fs.glob(glob_pattern)
        for file in files:
            if isinstance(file, str):
                extracted_date = extract_latest_date_from_filename(file)
                if extracted_date and (
                    latest_date is None or extracted_date > latest_date
                ):
                    latest_date = extracted_date
    return latest_date


def extract_latest_date_from_filename(filename: str) -> date | None:
    """Extracts the latest date provided in the given filename.

    It is based on the file name pattern ending in `_pYYYY-MM-DD_pYYYY-MM-DD`,
    where the last `YYYY-MM-DD` is the date of the latest observation.

    Args:
        filename: The filename containing date information in the format
            `_pYYYY-MM-DD_pYYYY-MM-DD`.

    Returns:
        The latest date extracted from the filename or `None` if no match is found.
    """
    # Regular expression to find the second date in filename (format YYYY-MM-DD)
    match = re.search(r"_p\d{4}-\d{2}-\d{2}_p(\d{4}-\d{2}-\d{2})", filename)
    if match:
        latest_date_str = match.group(1)
        return date.fromisoformat(latest_date_str)
    else:
        return None


def run_all() -> None:
    """Run the code in this module."""
    print(f"Running {Path(__file__).name}")
    create_dir_if_not_exist(settings.kildedata_root_dir)

    print(f"Using environment: {settings.env_for_dynaconf}")
    print("Start collecting data.")
    weather_station_list = get_weather_stations()
    selected_weather_station_ids = get_weather_stations_ids(
        settings.weather_station_names, weather_station_list
    )
    get_observations(selected_weather_station_ids)


if __name__ == "__main__":
    run_all()
