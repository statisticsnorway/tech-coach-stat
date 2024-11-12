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

from functions.versions import get_latest_file_version
from functions.versions import get_next_file_version


observation_locations = ["OSLO - BLINDERN", "KONGSVINGER"]
from_date = "2011-01-01"
to_date = "2012-01-01"
root_dir = Path(__file__).parent.parent.parent / "data"
kildedata_dir = root_dir / "kildedata"


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
        "referencetime": f"{from_date}/{to_date}",
        "levels": "default",
        "timeoffsets": "default",
    }
    data = fetch_data(endpoint, parameters)
    filename = f"observations_p{extract_timespan(data)}.json"
    print("Data retrieved from frost.met.no!")
    print(f"Storing to {filename}")

    source_file = kildedata_dir / filename
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
    if isinstance(latest_file, Path):
        with latest_file.open(encoding="utf-8") as file:
            return cast(list[dict[str, Any]], json.load(file))
    elif isinstance(latest_file, str):
        with FileClient.gcs_open(latest_file) as file:
            return cast(list[dict[str, Any]], json.load(file))


def get_sources() -> list[dict]:
    """Retrieve a list of observation locations from the FROST API and manage storage.

    Fetches the data and compares the fetched data to the most recent local version of
    the data and, if the data has changed, saves the data as new version.

    Returns:
        The fetched observation locations and data about them.
    """
    endpoint = "https://frost.met.no/sources/v0.jsonld"
    parameters = {
        "country": "Norge",
    }
    data = fetch_data(endpoint, parameters)
    source_file = kildedata_dir / "sources.json"

    # Check if data is changed since last version and write new file if so
    latest_data = get_latest_jsonfile_content(source_file)
    if data != latest_data:
        if (latest_file_version := get_latest_file_version(source_file)) is not None:
            next_file_version = get_next_file_version(latest_file_version)
        else:
            next_file_version = kildedata_dir / "sources_v1.json"
        with next_file_version.open(mode="w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
    return data


def find_source_ids(source_names: list[str], sources_: list[dict]) -> list[str]:
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


if __name__ == "__main__":
    # Create directories if they don't exist
    kildedata_dir.mkdir(parents=True, exist_ok=True)

    sources = get_sources()
    source_ids = find_source_ids(observation_locations, sources)
    get_observations(source_ids)
