"""Tests for kartverket.administrative_units_from_position.

Tests are split into two groups:

* Unit tests - use ``respx`` to intercept ``httpx`` traffic so no real HTTP
  requests are made.  These run instantly and are always reliable.

* Integration tests - make real requests to the Kartverket API.  Skipped by
  default; opt in with the ``--integration`` flag.

Run only unit tests (default):
    pytest tests/test_kartverket.py -v

Run all tests including integration:
    pytest tests/test_kartverket.py -v --integration
"""

from collections.abc import Generator
from dataclasses import FrozenInstanceError

import httpx
import pytest
import respx

from functions.kartverket import AdministrativeUnits
from functions.kartverket import administrative_units_from_position

_KOMMUNEINFO_URL = "https://api.kartverket.no/kommuneinfo/v1/punkt"


class TestAdministrativeUnitsFromPositionUnit:
    """Fast, offline tests using mocked HTTP responses."""

    @pytest.fixture(autouse=True)
    def clear_cache(self) -> Generator[None, None, None]:
        administrative_units_from_position.cache_clear()  # Setup
        yield
        administrative_units_from_position.cache_clear()  # Teardown

    @respx.mock
    def test_success_returns_correct_codes(self) -> None:
        """A 200 response with valid JSON maps to the expected codes."""
        respx.get(_KOMMUNEINFO_URL).mock(
            return_value=httpx.Response(
                200,
                json={
                    "fylkesnavn": "Oslo",
                    "fylkesnummer": "03",
                    "kommunenavn": "Oslo",
                    "kommunenummer": "0301",
                },
            )
        )

        result = administrative_units_from_position(59.9139, 10.7522)

        assert result == AdministrativeUnits(kommunenummer="0301", fylkesnummer="03")

    @pytest.mark.parametrize(
        "status_code",
        [400, 404, 422, 500, 503],
        ids=["400", "404", "422", "500", "503"],
    )
    @respx.mock
    def test_non_200_returns_fallback(self, status_code: int) -> None:
        """Any non-200 status code must return the sentinel fallback values."""
        respx.get(_KOMMUNEINFO_URL).mock(
            return_value=httpx.Response(status_code, json={})
        )

        result = administrative_units_from_position(0.0, 0.0)

        assert result == AdministrativeUnits(kommunenummer="9999", fylkesnummer="99")

    @respx.mock
    def test_correct_query_parameters_sent(self) -> None:
        """The API call must use nord/ost/koordsys parameters."""
        route = respx.get(_KOMMUNEINFO_URL).mock(
            return_value=httpx.Response(
                200, json={"fylkesnummer": "03", "kommunenummer": "0301"}
            )
        )
        lat, lon = 59.9139, 10.7522

        administrative_units_from_position(lat, lon)

        assert route.called
        request = route.calls.last.request
        assert request.url.params["nord"] == str(lat)
        assert request.url.params["ost"] == str(lon)
        assert request.url.params["koordsys"] == "4326"

    @respx.mock
    def test_result_is_frozen_dataclass(self) -> None:
        """AdministrativeUnits must be immutable (frozen dataclass)."""
        respx.get(_KOMMUNEINFO_URL).mock(
            return_value=httpx.Response(
                200, json={"fylkesnummer": "03", "kommunenummer": "0301"}
            )
        )

        result = administrative_units_from_position(59.9139, 10.7522)

        with pytest.raises(FrozenInstanceError):
            result.kommunenummer = "XXXX"


# ---------------------------------------------------------------------------
# Integration test (hits the real API)
# ---------------------------------------------------------------------------
@pytest.mark.integration
class TestAdministrativeUnitsIntegration:
    """Live tests against the real Kartverket API."""

    def test_oslo_coordinates(self) -> None:
        """Oslo city hall (59.9073°N 10.7416°E) → Oslo kommune 0301, fylke 03."""
        result = administrative_units_from_position(59.9073, 10.7416)
        assert result.kommunenummer == "0301"
        assert result.fylkesnummer == "03"

    def test_bergen_coordinates(self) -> None:
        """Bryggen, Bergen (60.3975°N 5.3241°E) → Bergen 4601, fylke 46."""
        result = administrative_units_from_position(60.3975, 5.3241)
        assert result.kommunenummer == "4601"
        assert result.fylkesnummer == "46"

    def test_trondheim_coordinates(self) -> None:
        """Nidaros Cathedral, Trondheim → Trondheim 5001, fylke 50."""
        result = administrative_units_from_position(63.4269, 10.3969)
        assert result.kommunenummer == "5001"
        assert result.fylkesnummer == "50"
