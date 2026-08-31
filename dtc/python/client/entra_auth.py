"""
Microsoft Entra ID (Azure AD) delegated OAuth2 token helper — Phase 9b.

The NT Orbit Duty Tools API (https://orbitduty.neotangent.com/API-DOCS/) requires
a per-user, delegated Microsoft Entra ID **access token** in the ``Authorization:
Bearer <token>`` header. This is obtained via the standard OAuth2
authorization-code + refresh-token flow:

  1. ONE-TIME interactive setup (run locally, once, by the delegated user —
     "auchunkei@lifung.com" for this project): open the Entra authorize URL in
     a browser, sign in, and capture the redirected ``code`` via a short-lived
     local HTTP callback server. Exchange the code for an initial
     ``access_token`` + ``refresh_token`` pair. See
     ``scripts/nt_orbit_oauth_setup.py`` for the CLI that drives this module.
  2. ONGOING refresh (every run, on Databricks or locally): use the stored
     ``refresh_token`` to mint a new short-lived ``access_token`` (Entra access
     tokens are valid roughly ~1 hour) via the token endpoint's
     ``grant_type=refresh_token`` flow. ``EntraTokenProvider.get_access_token()``
     does this on demand and caches the token in memory until shortly before
     expiry.

Scope: per the Phase 9b spec, the app registration requests
``https://graph.microsoft.com/.default offline_access`` — ``offline_access`` is
what causes Entra to hand back a refresh token in the response; the returned
``access_token`` is what gets used as the Bearer token for the Orbit API calls
(the Orbit API validates the resulting token itself; it does not need to be a
Graph-scoped call).

IMPORTANT — refresh token rotation: Entra ID commonly returns a NEW
``refresh_token`` on every refresh (rotation / one-time-use refresh tokens).
Callers MUST persist the latest ``refresh_token`` returned by
``get_access_token()`` (via ``EntraTokenProvider.refresh_token`` after the
call, or the ``on_token_refreshed`` callback) or the credential will eventually
stop working. This module does not persist anything itself — see
``dtc/notebooks/p9b_fill_duty_rates.py`` for the Delta-table persistence
pattern used on Databricks (secrets are read-only from ``dbutils.secrets``, so
the rotated token is written to a small control table instead of back into the
secret scope).

No network calls happen at import time; everything here is a thin,
unit-testable wrapper around ``requests`` plus pure URL-building helpers.
"""

from __future__ import annotations

import logging
import secrets as _secrets
import time
import webbrowser
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Callable, Dict, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urlparse

import requests

logger = logging.getLogger(__name__)

AUTHORIZE_URL_TMPL = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/authorize"
TOKEN_URL_TMPL = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

# Per the Phase 9b spec: scope=https://graph.microsoft.com/.default, plus
# offline_access so the token endpoint issues a refresh_token.
DEFAULT_SCOPE = "https://graph.microsoft.com/.default offline_access"

# Refresh this many seconds before the access token's reported expiry, so a
# borderline-stale cached token is never handed to a caller mid-request.
DEFAULT_LEEWAY_SECONDS = 60


# ---------------------------------------------------------------------------
# Pure URL / payload builders (unit-testable, no network)
# ---------------------------------------------------------------------------

def build_authorize_url(
    tenant_id: str,
    client_id: str,
    redirect_uri: str,
    scope: str = DEFAULT_SCOPE,
    state: Optional[str] = None,
) -> Tuple[str, str]:
    """
    Build the Entra ID /authorize URL for the one-time interactive login.

    Returns (url, state) — state is generated (URL-safe random token) when not
    supplied, and must be verified against the callback's ``state`` query
    param to guard against CSRF.
    """
    state = state or _secrets.token_urlsafe(16)
    params = {
        "client_id": client_id,
        "response_type": "code",
        "redirect_uri": redirect_uri,
        "scope": scope,
        "state": state,
        "response_mode": "query",
    }
    url = AUTHORIZE_URL_TMPL.format(tenant_id=tenant_id) + "?" + urlencode(params)
    return url, state


def _token_request(
    tenant_id: str,
    data: Dict[str, Any],
    timeout: int = 30,
) -> Dict[str, Any]:
    url = TOKEN_URL_TMPL.format(tenant_id=tenant_id)
    resp = requests.post(url, data=data, timeout=timeout)
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError:
        # Entra's error body is genuinely useful ("AADSTS..." codes); surface it.
        try:
            logger.error(f"Entra token endpoint error body: {resp.json()}")
        except Exception:
            logger.error(f"Entra token endpoint error body: {resp.text}")
        raise
    return resp.json()


def exchange_code_for_tokens(
    tenant_id: str,
    client_id: str,
    redirect_uri: str,
    code: str,
    client_secret: Optional[str] = None,
    scope: str = DEFAULT_SCOPE,
) -> Dict[str, Any]:
    """
    Exchange an authorization ``code`` (from the one-time interactive login)
    for an initial ``access_token`` + ``refresh_token`` pair.

    ``client_secret`` is optional — omit it for a public client (SPA/native app
    registration using PKCE-less auth-code flow is not recommended, but Entra
    app registrations configured as "Mobile and desktop applications" work
    without a secret); pass it if the app registration is confidential.
    """
    data: Dict[str, Any] = {
        "client_id": client_id,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "scope": scope,
    }
    if client_secret:
        data["client_secret"] = client_secret
    return _token_request(tenant_id, data)


def refresh_access_token(
    tenant_id: str,
    client_id: str,
    refresh_token: str,
    client_secret: Optional[str] = None,
    scope: str = DEFAULT_SCOPE,
) -> Dict[str, Any]:
    """
    Use a ``refresh_token`` to mint a new ``access_token`` (and, typically, a
    newly-rotated ``refresh_token`` — see module docstring).
    """
    data: Dict[str, Any] = {
        "client_id": client_id,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "scope": scope,
    }
    if client_secret:
        data["client_secret"] = client_secret
    return _token_request(tenant_id, data)


# ---------------------------------------------------------------------------
# Local HTTP callback server for the one-time interactive login
# ---------------------------------------------------------------------------

class _CallbackResult:
    __slots__ = ("code", "state", "error", "error_description")

    def __init__(self):
        self.code: Optional[str] = None
        self.state: Optional[str] = None
        self.error: Optional[str] = None
        self.error_description: Optional[str] = None


def _make_callback_handler(result: _CallbackResult):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 (stdlib naming)
            qs = parse_qs(urlparse(self.path).query)
            result.code = (qs.get("code") or [None])[0]
            result.state = (qs.get("state") or [None])[0]
            result.error = (qs.get("error") or [None])[0]
            result.error_description = (qs.get("error_description") or [None])[0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            if result.error:
                body = (f"<h3>NT Orbit / Entra auth failed</h3>"
                        f"<p>{result.error}: {result.error_description}</p>"
                        f"<p>You can close this tab.</p>")
            else:
                body = "<h3>NT Orbit / Entra auth complete</h3><p>You can close this tab.</p>"
            self.wfile.write(body.encode("utf-8"))

        def log_message(self, fmt, *args):  # silence default stderr logging
            logger.debug("callback server: " + fmt, *args)

    return Handler


def run_local_callback_server(
    port: int,
    timeout: int = 300,
) -> _CallbackResult:
    """
    Serve one HTTP GET on ``http://localhost:<port>/...`` and return the parsed
    ``code``/``state``/``error`` query params (or all-None fields on timeout).

    Used only by the one-time interactive setup CLI
    (``scripts/nt_orbit_oauth_setup.py``); never called from a Databricks
    notebook (no browser/callback available there).
    """
    result = _CallbackResult()
    server = HTTPServer(("localhost", port), _make_callback_handler(result))
    server.timeout = timeout
    deadline = time.time() + timeout
    while result.code is None and result.error is None and time.time() < deadline:
        server.handle_request()
    server.server_close()
    return result


def open_browser_for_login(url: str) -> None:
    """Best-effort browser launch; safe to call headlessly (returns False)."""
    try:
        webbrowser.open(url)
    except Exception as e:  # pragma: no cover - environment dependent
        logger.warning(f"Could not open a browser automatically: {e}")


# ---------------------------------------------------------------------------
# Cached, auto-refreshing access-token provider
# ---------------------------------------------------------------------------

@dataclass
class _TokenCache:
    access_token: Optional[str] = None
    expires_at: float = 0.0


@dataclass
class EntraTokenProvider:
    """
    Delegated OAuth2 access-token provider for a single Entra user (e.g.
    "auchunkei@lifung.com"), backed by a long-lived refresh token.

    ``get_access_token()`` is safe to call before every NT Orbit request: it
    returns the cached access token unless it is missing/near-expiry, in which
    case it silently refreshes first. Pass ``get_access_token`` (bound method)
    as ``RestClient(bearer_token_provider=...)``.

    Attributes:
        tenant_id: Entra tenant (directory) ID.
        client_id: App registration (client) ID.
        refresh_token: Current refresh token. THIS FIELD IS MUTATED in place
            when Entra rotates the refresh token on a call to
            ``get_access_token()`` — read it back afterwards (or supply
            ``on_token_refreshed``) to persist the new value.
        client_secret: Optional; only needed for a confidential app
            registration.
        scope: OAuth2 scope string sent on every token request.
        on_token_refreshed: Optional callback ``(access_token, refresh_token,
            expires_in) -> None`` invoked after every successful refresh — use
            this to persist the (possibly rotated) refresh_token.
    """

    tenant_id: str
    client_id: str
    refresh_token: str
    client_secret: Optional[str] = None
    scope: str = DEFAULT_SCOPE
    leeway_seconds: int = DEFAULT_LEEWAY_SECONDS
    on_token_refreshed: Optional[Callable[[str, str, int], None]] = None
    _cache: _TokenCache = field(default_factory=_TokenCache, repr=False, compare=False)

    def get_access_token(self, force_refresh: bool = False) -> str:
        now = time.time()
        if (
            not force_refresh
            and self._cache.access_token
            and now < self._cache.expires_at - self.leeway_seconds
        ):
            return self._cache.access_token

        token_resp = refresh_access_token(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            refresh_token=self.refresh_token,
            client_secret=self.client_secret,
            scope=self.scope,
        )
        access_token = token_resp["access_token"]
        expires_in = int(token_resp.get("expires_in", 3600))
        self._cache.access_token = access_token
        self._cache.expires_at = now + expires_in

        new_refresh_token = token_resp.get("refresh_token")
        if new_refresh_token and new_refresh_token != self.refresh_token:
            logger.info("Entra refresh_token rotated; caller should persist the new value.")
            self.refresh_token = new_refresh_token

        if self.on_token_refreshed:
            try:
                self.on_token_refreshed(access_token, self.refresh_token, expires_in)
            except Exception as e:  # persistence failures must not break auth
                logger.warning(f"on_token_refreshed callback failed: {e}")

        return access_token
