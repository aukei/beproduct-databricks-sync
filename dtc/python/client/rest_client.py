"""
Generic REST client wrapper for API calls with authentication, retry, and error handling.
"""

import logging
import time
from typing import Optional, Dict, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class RestClient:
    """Generic REST client with auth, retry logic, and error handling."""

    def __init__(
        self,
        base_url: str,
        api_key: str = None,
        timeout: int = 30,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
    ):
        """
        Initialize REST client.

        Args:
            base_url: Base URL for API (e.g., "https://dtc-api.lfuat.net/api")
            api_key: API key for authentication (if using x-api-key header)
            timeout: Request timeout in seconds
            max_retries: Number of retries for failed requests
            backoff_factor: Backoff factor for exponential retry
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

        # Create session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        logger.info(f"RestClient initialized for {base_url}")

    @staticmethod
    def _parse_body(resp) -> Dict[str, Any]:
        """
        Parse a response body, tolerating empty bodies (e.g. HTTP 204 No Content,
        which the DTC sheet write endpoint returns on success).

        Returns the parsed JSON dict, or {"status_code": <code>} when there is no
        body to decode.
        """
        if resp.status_code == 204 or not (resp.content and resp.content.strip()):
            return {"status_code": resp.status_code}
        try:
            return resp.json()
        except ValueError:
            return {"status_code": resp.status_code, "text": resp.text}

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers."""
        headers = {
            "Content-Type": "application/json",
        }
        if self.api_key:
            headers["x-api-key"] = self.api_key
        return headers

    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        GET request.

        Args:
            endpoint: API endpoint (e.g., "/v1/requests/123")
            params: Query parameters
            data: Optional JSON request body. Some DTC list endpoints expect a
                  body on GET (e.g. /v1/requests reads {"workspaceName", "filters"}
                  from the body, not query params).
            headers: Additional headers to merge with defaults

        Returns:
            Response JSON as dict
        """
        url = f"{self.base_url}{endpoint}"
        req_headers = self._get_headers()
        if headers:
            req_headers.update(headers)

        logger.debug(f"GET {url}")
        try:
            resp = self.session.request(
                "GET", url, params=params, json=data,
                headers=req_headers, timeout=self.timeout,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"GET {url} failed: {e}")
            raise

    def post(
        self,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        POST request.

        Args:
            endpoint: API endpoint
            data: Request body as dict (will be JSON encoded)
            headers: Additional headers

        Returns:
            Response JSON as dict
        """
        url = f"{self.base_url}{endpoint}"
        req_headers = self._get_headers()
        if headers:
            req_headers.update(headers)

        logger.debug(f"POST {url}")
        try:
            resp = self.session.post(
                url, json=data, headers=req_headers, timeout=self.timeout
            )
            resp.raise_for_status()
            return self._parse_body(resp)
        except requests.exceptions.RequestException as e:
            logger.error(f"POST {url} failed: {e}")
            raise

    def patch(
        self,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        PATCH request (partial update).

        Args:
            endpoint: API endpoint
            data: Request body as dict
            headers: Additional headers

        Returns:
            Response JSON as dict
        """
        url = f"{self.base_url}{endpoint}"
        req_headers = self._get_headers()
        if headers:
            req_headers.update(headers)

        logger.debug(f"PATCH {url}")
        try:
            resp = self.session.patch(
                url, json=data, headers=req_headers, timeout=self.timeout
            )
            resp.raise_for_status()
            return self._parse_body(resp)
        except requests.exceptions.RequestException as e:
            logger.error(f"PATCH {url} failed: {e}")
            raise

    def put(
        self,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        PUT request (full replacement).

        Args:
            endpoint: API endpoint
            data: Request body as dict
            headers: Additional headers

        Returns:
            Response JSON as dict
        """
        url = f"{self.base_url}{endpoint}"
        req_headers = self._get_headers()
        if headers:
            req_headers.update(headers)

        logger.debug(f"PUT {url}")
        try:
            resp = self.session.put(
                url, json=data, headers=req_headers, timeout=self.timeout
            )
            resp.raise_for_status()
            return self._parse_body(resp)
        except requests.exceptions.RequestException as e:
            logger.error(f"PUT {url} failed: {e}")
            raise

    def delete(
        self,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        DELETE request.

        Args:
            endpoint: API endpoint
            data: Optional JSON request body. Some DTC delete endpoints require a
                  body (e.g. removing sheet rows takes {"rowIndexes": [...]}).
            headers: Additional headers

        Returns:
            Response JSON as dict (or None if no content)
        """
        url = f"{self.base_url}{endpoint}"
        req_headers = self._get_headers()
        if headers:
            req_headers.update(headers)

        logger.debug(f"DELETE {url}")
        try:
            resp = self.session.delete(
                url, json=data, headers=req_headers, timeout=self.timeout
            )
            resp.raise_for_status()
            # DELETE may return 204 No Content
            if resp.status_code == 204:
                return None
            return resp.json() if resp.content else None
        except requests.exceptions.RequestException as e:
            logger.error(f"DELETE {url} failed: {e}")
            raise

    def close(self):
        """Close the session."""
        self.session.close()
        logger.info("RestClient session closed")
