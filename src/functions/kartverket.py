"""Kartverket API helpers.

Currently provides:
  - administrative_units_from_position: resolve kommunenummer / fylkesnummer
    from a WGS84 latitude/longitude coordinate using the Kartverket
    kommuneinfo REST API.

API reference:
  https://api.kartverket.no/kommuneinfo/v1/punkt
"""

from dataclasses import dataclass

import httpx

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_KOMMUNEINFO_URL = "https://api.kartverket.no/kommuneinfo/v1/punkt"

# Sentinel values returned when the API does not respond with HTTP 200.
_FALLBACK_KOMMUNENUMMER = "9999"
_FALLBACK_FYLKESNUMMER = "99"


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class AdministrativeUnits:
    """Administrative unit identifiers for a geographic position in Norway.

    Attributes:
        kommunenummer: Four-digit municipality code (e.g. ``"0301"`` for Oslo), or
            ``"9999"`` if the position could not be resolved.
        fylkesnummer: Two-digit county code (e.g. ``"03"`` for Oslo), or ``"99"`` if the
            position could not be resolved.
    """

    kommunenummer: str
    fylkesnummer: str


def administrative_units_from_position(lat: float, lon: float) -> AdministrativeUnits:
    """Return the Norwegian municipality and county codes for a WGS84 position.

    Queries the Kartverket kommuneinfo API:

        GET https://api.kartverket.no/kommuneinfo/v1/punkt
            ?nord=<lat>&ost=<lon>&koordsys=4326

    Args:
        lat: Latitude in decimal degrees (WGS84 / EPSG:4326).
        lon: Longitude in decimal degrees (WGS84 / EPSG:4326).

    Returns:
        AdministrativeUnits: Dataclass with ``kommunenummer`` and ``fylkesnummer``.
            Both fields are set to their fallback sentinels (``"9999"`` /
            ``"99"``) when the API returns a non-200 status code.

    Examples:
        >>> units = administrative_units_from_position(59.9139, 10.7522)
        >>> units.kommunenummer
        '0301'
        >>> units.fylkesnummer
        '03'
    """
    params = {"nord": lat, "ost": lon, "koordsys": 4326}

    timeout = 10.0  # seconds
    with httpx.Client(timeout=httpx.Timeout(timeout)) as client:
        response = client.get(_KOMMUNEINFO_URL, params=params)

    if response.status_code != 200:
        return AdministrativeUnits(
            kommunenummer=_FALLBACK_KOMMUNENUMMER,
            fylkesnummer=_FALLBACK_FYLKESNUMMER,
        )

    data: dict[str, str] = response.json()
    return AdministrativeUnits(
        kommunenummer=data["kommunenummer"],
        fylkesnummer=data["fylkesnummer"],
    )
