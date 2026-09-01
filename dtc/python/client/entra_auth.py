"""
Microsoft Entra ID (Azure AD) delegated OAuth2 token helper — Phase 9b.

The NT Orbit Duty Tools API (https://orbitduty.neotangent.com/API-DOCS/) requires
a per-user, delegated Microsoft Entra ID **access token** in the ``Authorization:
Bearer <token>`` header, obtained by signing in as a specific person
("auchunkei@lifung.com" for this project). Phase 9b uses a DEDICATED
confidential app registration (has its own client_secret), but the Entra
sign-in itself is still a DELEGATED USER login, never client-credentials
(app-only) — NT Orbit authorizes the signed-in person, not the app.

``tenant_id`` / ``client_id`` / ``client_secret`` / ``redirect_uri`` are ALL
plain parameters (see ``scripts/nt_orbit_oauth_setup.py``'s CLI flags / env
vars), specifically so the same tooling works across environments (e.g.
dev/uat/prod each with their own app registration and their own registered
redirect URI) without any code change — just re-run the setup script with a
different ``--client-id``/``--redirect-uri`` pair.

  1. ONE-TIME interactive setup (run locally, once per environment, by the
     delegated user, e.g. "auchunkei@lifung.com"):
     ``build_authorize_url()`` + a short-lived local callback server
     (``run_local_callback_server()``) is the default, fully-scripted path
     (``--flow authcode`` — you register ``redirect_uri`` on the app once in
     the Entra portal, then everything else is automatic).
     ``exchange_code_for_tokens()`` does the ONE-TIME exchange of the
     resulting ``code`` (single-use, NOT the refresh_token itself) for an
     initial ``access_token`` + ``refresh_token`` pair. See
     ``scripts/nt_orbit_oauth_setup.py`` for the CLI that drives this. Two
     fallback flows are available when you can't register a redirect URI
     yourself (e.g. against someone else's pre-existing client_id):
     ``--flow manual`` (the officially-provided dev-test procedure — Entra
     ``/authorize`` with ``redirect_uri=DEFAULT_MANUAL_REDIRECT_URI``,
     Postman's public "display the code back to you" page, already
     registered on many shared client_ids; you paste the resulting ``code``
     or full redirected URL back — ``parse_callback_input()`` extracts the
     code either way) and ``--flow devicecode`` (OAuth2 Device Authorization
     Grant, RFC 8628 — same mechanism as ``az login --use-device-code``; no
     redirect URI at all, but requires the client_id to have public-client/
     device-code flows enabled).
  2. ONGOING refresh (every run, on Databricks or locally): use the stored
     ``refresh_token`` to mint a new short-lived ``access_token`` (Entra access
     tokens are valid roughly ~1 hour) via the token endpoint's
     ``grant_type=refresh_token`` flow. ``EntraTokenProvider.get_access_token()``
     does this on demand and caches the token in memory until shortly before
     expiry. This step is IDENTICAL regardless of which flow produced the
     initial refresh_token.

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
DEVICE_CODE_URL_TMPL = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/devicecode"
DEVICE_CODE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code"

# Per the Phase 9b spec: scope=https://graph.microsoft.com/.default, plus
# offline_access so the token endpoint issues a refresh_token.
DEFAULT_SCOPE = "https://graph.microsoft.com/.default offline_access"

# Postman's public OAuth "browser callback" display page. Used as the
# redirect_uri for the officially-provided manual dev-test procedure: it is
# already registered on the shared LiFung client_id, so no locally-run
# callback server or redirect-URI ownership is required — you just copy the
# ``code`` out of the browser's address bar after signing in.
DEFAULT_MANUAL_REDIRECT_URI = "https://oauth.pstmn.io/v1/browser-callback"

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


def parse_callback_input(text: str) -> Dict[str, Optional[str]]:
    """
    Parse whatever a human pastes back after the manual browser-callback
    login step — any of:

      * the full redirected URL, e.g.
        "https://oauth.pstmn.io/v1/browser-callback?code=1.XXX&state=123456&session_state=YYY"
      * just the query string, e.g. "code=1.XXX&state=123456"
      * just the bare authorization code itself, e.g. "1.XXX"

    Returns {"code": str|None, "state": str|None}. Never raises — a value
    that doesn't look like any of the above is treated as a bare code so a
    slightly-mangled paste still has a chance of working; the caller should
    still validate the result is non-empty before using it.
    """
    text = (text or "").strip()
    if not text:
        return {"code": None, "state": None}

    if text.startswith("http://") or text.startswith("https://"):
        qs = parse_qs(urlparse(text).query)
    elif "code=" in text or "state=" in text:
        # bare "code=...&state=..." (no scheme/host — e.g. copied without the
        # leading "https://host/path?")
        qs = parse_qs(text.lstrip("?"))
    else:
        return {"code": text, "state": None}

    return {
        "code": (qs.get("code") or [None])[0],
        "state": (qs.get("state") or [None])[0],
    }


# AADSTS700025: "Client is public so neither 'client_assertion' nor
# 'client_secret' should be presented." Entra decides "public vs confidential"
# for a given token request based on the PLATFORM TYPE the redirect_uri is
# registered under on the app (Authentication blade), NOT on whether the app
# has a client_secret configured elsewhere. A redirect URI added under
# "Mobile and desktop applications" (native/public platform) is ALWAYS
# treated as public and rejects a client_secret, even for an otherwise
# confidential app registration. Fix: register that redirect_uri under "Web"
# instead. Until/unless that's fixed, we retry once without the secret so a
# misconfigured platform type doesn't hard-fail the whole login.
_PUBLIC_CLIENT_SECRET_ERROR_CODE = 700025


def _is_public_client_secret_rejection(exc: "requests.exceptions.HTTPError") -> bool:
    resp = getattr(exc, "response", None)
    if resp is None:
        return False
    try:
        body = resp.json()
    except Exception:
        return False
    return _PUBLIC_CLIENT_SECRET_ERROR_CODE in (body.get("error_codes") or [])


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


def _token_request_with_secret_fallback(
    tenant_id: str,
    data: Dict[str, Any],
    client_secret: Optional[str],
) -> Dict[str, Any]:
    """
    POST to the token endpoint, including client_secret if given; if Entra
    rejects it specifically because the redirect_uri's platform type marks
    this as a public-client request (AADSTS700025), retry once WITHOUT the
    secret rather than hard-failing the whole login. See
    _PUBLIC_CLIENT_SECRET_ERROR_CODE docstring above.
    """
    if client_secret:
        data = {**data, "client_secret": client_secret}
    try:
        return _token_request(tenant_id, data)
    except requests.exceptions.HTTPError as e:
        if client_secret and _is_public_client_secret_rejection(e):
            logger.warning(
                "Entra rejected the client_secret for this redirect_uri "
                "(AADSTS700025 - the redirect URI is registered under a "
                "PUBLIC/native platform, e.g. 'Mobile and desktop "
                "applications', so Entra treats this request as a public "
                "client regardless of the app having a secret). Retrying "
                "WITHOUT client_secret. To use the secret going forward, "
                "move this redirect URI to the 'Web' platform in the Entra "
                "app registration instead."
            )
            data = dict(data)
            data.pop("client_secret", None)
            return _token_request(tenant_id, data)
        raise


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

    ``client_secret`` is optional — omit it for a public client. Pass it for a
    confidential app registration, but note the redirect_uri you're using with
    it must be registered under the "Web" platform (NOT "Mobile and desktop
    applications" — see ``_token_request_with_secret_fallback`` above for what
    happens if it's on the wrong platform).
    """
    data: Dict[str, Any] = {
        "client_id": client_id,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "scope": scope,
    }
    return _token_request_with_secret_fallback(tenant_id, data, client_secret)


class DeviceCodePending(Exception):
    """Raised internally while polling; callers should not need to catch this."""


class DeviceCodeExpired(Exception):
    """The user did not complete the device-code login before it expired."""


def start_device_code_flow(
    tenant_id: str,
    client_id: str,
    scope: str = DEFAULT_SCOPE,
    timeout: int = 30,
) -> Dict[str, Any]:
    """
    Step 1 of the Device Authorization Grant (RFC 8628) — no redirect URI
    needed, so this works with ANY existing client_id, not just one you
    control the app registration for.

    POST /oauth2/v2.0/devicecode -> {
        "device_code": "...",          # used internally by poll_device_code_token
        "user_code": "ABCD-1234",       # what the human types in
        "verification_uri": "https://microsoft.com/devicelogin",
        "expires_in": 900,
        "interval": 5,
        "message": "To sign in, use a web browser to open the page ...",
    }
    """
    url = DEVICE_CODE_URL_TMPL.format(tenant_id=tenant_id)
    resp = requests.post(
        url, data={"client_id": client_id, "scope": scope}, timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()


def poll_device_code_token(
    tenant_id: str,
    client_id: str,
    device_code: str,
    interval: int = 5,
    expires_in: int = 900,
    on_poll: Optional[Callable[[], None]] = None,
) -> Dict[str, Any]:
    """
    Step 2 — poll the token endpoint with the device_code from
    start_device_code_flow() until the user finishes signing in (or it
    expires). Honors Entra's ``slow_down``/``authorization_pending`` errors
    per RFC 8628 (backs off, keeps waiting) and raises DeviceCodeExpired /
    the underlying HTTPError for terminal failures (expired, declined, bad
    client, etc.).

    Args:
        on_poll: optional zero-arg callback invoked before every poll attempt
            (e.g. to print a "..." progress dot).
    """
    url = TOKEN_URL_TMPL.format(tenant_id=tenant_id)
    deadline = time.time() + expires_in
    wait = max(interval, 1)
    while time.time() < deadline:
        if on_poll:
            on_poll()
        resp = requests.post(url, data={
            "client_id": client_id,
            "grant_type": DEVICE_CODE_GRANT_TYPE,
            "device_code": device_code,
        }, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        try:
            body = resp.json()
        except Exception:
            resp.raise_for_status()
            raise  # pragma: no cover
        err = body.get("error")
        if err == "authorization_pending":
            time.sleep(wait)
            continue
        if err == "slow_down":
            wait += 5
            time.sleep(wait)
            continue
        if err == "expired_token":
            raise DeviceCodeExpired("Device code expired before sign-in completed.")
        # authorization_declined, bad_verification_code, invalid_client, ...
        logger.error(f"Entra device-code token error: {body}")
        resp.raise_for_status()
    raise DeviceCodeExpired("Device code expired before sign-in completed.")


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
    return _token_request_with_secret_fallback(tenant_id, data, client_secret)


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
