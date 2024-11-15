# To access the Frost API, you will need a client ID, which you can get by registering
# as a user at https://frost.met.no/howto.html. Place the client ID you receive
# in a .env file in the root directory of the repository. It should look like this:
#
# FROST_CLIENT_ID="5dc4-mange-nummer-e71cc"

import json
import os
from pathlib import Path
from typing import Any
from typing import cast

import requests
from dapla import FileClient
from dotenv import load_dotenv

from functions.config import settings
from functions.versions import get_latest_file_version
from functions.versions import get_next_file_version


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


def frost_client_id() -> str:
    """Get the frost_client_id secret.

    Read the client id from environment variable or .env file.

    Returns:
        The frost_client_id secret

    Raises:
        RuntimeError: If the environment variable is not defined..
    """
    load_dotenv()
    client_id = os.getenv("FROST_CLIENT_ID")
    if client_id is None:
        raise RuntimeError("FROST_CLIENT_ID environment variable is not defined")
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
        "referencetime": f"{settings.collect_from_date}/{settings.collect_to_date}",
        "levels": "default",
        "timeoffsets": "default",
    }
    data = fetch_data(endpoint, parameters)
    filename = f"observations_p{extract_timespan(data)}.json"
    print("Data retrieved from frost.met.no!")
    print(f"Storing to {filename}")

    source_file = settings.kildedata_root_dir / filename
    with source_file.open(mode="w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)
    return data


def get_latest_jsonfile_content(filepath: Path | str) -> list[dict[str, Any]] | None:
    """Returns the content of the latest version of a file.

    Args:
        filepath: The path to the file whose latest version is to be found and get
            the content of.

    Returns:
        The content of the latest version of the file, or None if no such files are
        found.
    """
    latest_file = get_latest_file_version(filepath)
    if latest_file is None:
        return None
    return read_json_file(latest_file)


def get_weather_stations() -> list[dict[str, Any]]:
    """Retrieve a list of weather stations from the FROST API and manage storage.

    Fetches the data and compares the fetched data to the most recent local version of
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
    latest_data = get_latest_jsonfile_content(base_file)
    if data != latest_data:
        if (latest_file_version := get_latest_file_version(base_file)) is not None:
            next_file = get_next_file_version(latest_file_version)
        else:
            next_file = settings.weather_stations_kildedata_file
        write_json_file(next_file, data)
    return data


def get_weather_stations_ids(
    source_names: list[str], sources_: list[dict[str, Any]]
) -> list[str]:
    """Find source ids for the provided source names (observation locations).

    Args:
        source_names: List of observation locations to find ids for.
        sources_: List of dictionaries for looking up source ids.

    Returns:
        A list of ids corresponding to the provided observations locations.
    """
    # Create a dictionary for quick lookup of names to ids
    name_to_id = {
        item["name"]: item["id"] for item in sources_ if "name" in item and "id" in item
    }
    return [name_to_id[name] for name in source_names]


def write_json_file(filepath: Path | str, data: list[dict[str, Any]]) -> None:
    """Writes dictionaries to a JSON file stored in a GCS bucket or in a local file system.

    Args:
        filepath: The path to the file where the data should be written.
            Use the `pathlib.Path` type if it is a file on a file system.
            Use the `str` type if it is a file stored in a GCS bucket.
        data: The data to be written to the file.

    Raises:
        TypeError: If the `filepath` is not of type `Path` or `str`.
    """
    if isinstance(filepath, Path):
        with filepath.open(mode="w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
    elif isinstance(filepath, str):
        with FileClient.gcs_open(filepath, mode="w") as file:
            json.dump(data, file, indent=4)
    else:
        raise TypeError("Expected filepath to be of type Path or str.")


def read_json_file(filepath: Path | str) -> list[dict[str, Any]]:
    """Reads a JSON file stored in a GCS bucket or in a local file system.

    Args:
        filepath: The path to the file which should be read.
            Use the `pathlib.Path` type if it is a file on a file system.
            Use the `str` type if it is a file stored in a GCS bucket.

    Returns:
        The content of the JSON file.

    Raises:
        TypeError: If the `filepath` is not of type `Path` or `str`.
    """
    if isinstance(filepath, Path):
        with filepath.open(encoding="utf-8") as file:
            return cast(list[dict[str, Any]], json.load(file))
    elif isinstance(filepath, str):
        with FileClient.gcs_open(filepath) as file:
            return cast(list[dict[str, Any]], json.load(file))
    else:
        raise TypeError("Expected filepath to be of type Path or str.")


if __name__ == "__main__":
    # Create directories if they don't exist
    kildedata_dir = settings.kildedata_root_dir
    if isinstance(kildedata_dir, Path):
        kildedata_dir.mkdir(parents=True, exist_ok=True)

    sources = get_weather_stations()
    source_ids = get_weather_stations_ids(settings.observation_locations, sources)
    get_observations(source_ids)
