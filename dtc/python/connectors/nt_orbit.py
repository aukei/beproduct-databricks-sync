"""
NT Orbit Duty Tools API connector (Phase 9b).

API docs: https://orbitduty.neotangent.com/API-DOCS/

Auth: Microsoft Entra ID delegated OAuth2 (see ``client.entra_auth``) — every
request carries ``Authorization: Bearer <access_token>``, NOT a static
``x-api-key`` (unlike the DTC connector). The access token is minted on demand
by an ``EntraTokenProvider`` and passed to ``RestClient`` as a
``bearer_token_provider`` callable, so it is transparently refreshed roughly
hourly without the caller having to think about it.

Endpoints used:
  GET  /api/v1/health            -> {"status": "healthy", ...}   (auth smoke test)
  POST /api/v1/calculate/single/  -> duty/tariff calculation for one shipment
       (the Phase 9b spec's prose used "/calcuate/single/" — a typo in the
       written spec, not in the live API; live-validated 2026-09-01 that the
       real endpoint is spelled "calculate". `/calcuate/single/` 404s.)
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional

from client.rest_client import RestClient

logger = logging.getLogger(__name__)

# Base URL is the same for all environments per the spec (no separate
# UAT/PROD host was given for NT Orbit, unlike DTC).
DEFAULT_BASE_URL = "https://orbitduty.neotangent.com"

CALCULATE_SINGLE_ENDPOINT = "/api/v1/calculate/single/"
HEALTH_ENDPOINT = "/api/v1/health"


class NTOrbitConnector:
    """Connector for the NT Orbit Duty Tools API."""

    def __init__(
        self,
        bearer_token_provider: Callable[[], str],
        base_url: str = DEFAULT_BASE_URL,
        timeout: int = 30,
    ):
        """
        Args:
            bearer_token_provider: zero-arg callable returning a fresh Entra
                access token (typically ``EntraTokenProvider(...).get_access_token``).
            base_url: NT Orbit API base URL.
            timeout: HTTP timeout (seconds).
        """
        self.client = RestClient(
            base_url=base_url,
            timeout=timeout,
            bearer_token_provider=bearer_token_provider,
        )
        logger.info(f"NTOrbitConnector initialized: base_url={base_url}")

    def health(self) -> Dict[str, Any]:
        """
        GET /api/v1/health — confirms the caller's Entra identity has been
        granted access to the Orbit API (a valid-but-unauthorized token still
        gets a non-200/expected-shape response here, per the spec's setup
        instructions: "need to grant right for the user that calls the Orbit
        API -> check this endpoint to see if the call succeeds").

        Returns the parsed body, e.g. {"status": "healthy", "timestamp": ..., "version": ...}.
        """
        return self.client.get(HEALTH_ENDPOINT)

    def is_healthy(self) -> bool:
        try:
            resp = self.health()
            return str(resp.get("status", "")).lower() == "healthy"
        except Exception as e:
            logger.warning(f"NT Orbit health check failed: {e}")
            return False

    def calculate_single(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /api/v1/calculate/single/ — calculate duty/tariff for one
        product/shipment.

        Args:
            payload: request body, e.g. (see dtc/python/sync/duty.py
                ``build_calc_request`` for the pure builder used by Phase 9b):
                {
                  "product_description": "...",
                  "origin_country_code": "BD",
                  "import_country_code": "US",
                  "export_country_code": "BD",
                  "de_minimis": false,
                  "mode_of_transport": "freight",
                }

        Returns:
            Parsed response dict, e.g. {"success": true, "data": {...}, ...}.
        """
        logger.info(
            "NT Orbit calculate_single: "
            f"{payload.get('origin_country_code')} -> {payload.get('import_country_code')} "
            f"({payload.get('product_description', '')[:60]!r})"
        )
        return self.client.post(CALCULATE_SINGLE_ENDPOINT, data=payload)

    def close(self):
        self.client.close()
