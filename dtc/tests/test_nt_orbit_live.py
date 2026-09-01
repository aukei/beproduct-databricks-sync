#!/usr/bin/env python3
"""
LIVE test against the REAL NT Orbit Duty Tools API + REAL Microsoft Entra ID
OAuth2 token endpoint (Phase 9b). Unlike dtc/tests/test_duty.py (pure-Python,
asserts against a canned/fixture response), this test:

  1. Does a real `grant_type=refresh_token` POST to Entra to mint a real
     access_token from your real refresh_token.
  2. Calls the real NT Orbit `/api/v1/health` endpoint with that token.
  3. Calls the real NT Orbit `/api/v1/calculate/single/` endpoint with the
     exact Cotton-t-shirt/BD->US example from the Phase 9b spec, and checks
     the shape of the actual response (not a fixture) via
     sync.duty.extract_duty_fields.
  4. Exercises token-refresh again (force_refresh=True) to prove a SECOND
     live refresh works, and reports whether Entra rotated the refresh_token
     (informational only - a live refresh_token is a moving target, so this
     is printed, never asserted to stay constant).

This makes real network calls (both to Microsoft Entra ID and to NT Orbit's
paid-per-call duty calculation endpoint), so it is opt-in and OFF by default -
it is intentionally NOT part of `dtc/tests/test_duty.py` and not run by CI.

Required env vars, read from .env AUTOMATICALLY (via python-dotenv, same as
scripts/upload_notebooks.py) if present there, or from your shell environment
if already exported — no manual `source .env` needed either way:

  NT_ORBIT_TENANT_ID        Entra tenant (directory) ID
  NT_ORBIT_CLIENT_ID        App registration (client) ID
  NT_ORBIT_REFRESH_TOKEN    A currently-valid refresh token (from
                             scripts/nt_orbit_oauth_setup.py, or the value
                             currently stored in the Databricks secret scope /
                             lft.beproduct.nt_orbit_oauth_state control table)
  NT_ORBIT_CLIENT_SECRET    Optional - only if the app registration is confidential

Opt-in flag (must ALSO be set, so accidentally-populated .env creds never
trigger a live/billable call by surprise):

  RUN_NT_ORBIT_LIVE_TEST=true

Usage (put the NT_ORBIT_* values in your local, untracked .env, then):
    RUN_NT_ORBIT_LIVE_TEST=true python3 dtc/tests/test_nt_orbit_live.py

    # or without touching .env:
    export NT_ORBIT_TENANT_ID=... NT_ORBIT_CLIENT_ID=... NT_ORBIT_REFRESH_TOKEN=...
    RUN_NT_ORBIT_LIVE_TEST=true python3 dtc/tests/test_nt_orbit_live.py

Any of these missing -> the test SKIPS cleanly (prints why, exits 0) rather
than failing, so it never blocks the pure-unit-test suite or CI.
"""

import os
import sys
from pathlib import Path

_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

try:
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env")
except ImportError:
    pass  # python-dotenv not installed; fall back to whatever's already exported

from client.entra_auth import EntraTokenProvider
from connectors.nt_orbit import NTOrbitConnector
from sync import duty

# ---------------------------------------------------------------------------
# Opt-in / credential gate
# ---------------------------------------------------------------------------

REQUIRED_ENV = ["NT_ORBIT_TENANT_ID", "NT_ORBIT_CLIENT_ID", "NT_ORBIT_REFRESH_TOKEN"]

if os.environ.get("RUN_NT_ORBIT_LIVE_TEST", "").strip().lower() != "true":
    print("SKIP: set RUN_NT_ORBIT_LIVE_TEST=true to run this live test "
          "(it makes real, potentially billable NT Orbit API calls).")
    sys.exit(0)

missing = [v for v in REQUIRED_ENV if not os.environ.get(v)]
if missing:
    print(f"SKIP: missing required env var(s): {', '.join(missing)}")
    sys.exit(0)

TENANT_ID = os.environ["NT_ORBIT_TENANT_ID"]
CLIENT_ID = os.environ["NT_ORBIT_CLIENT_ID"]
REFRESH_TOKEN = os.environ["NT_ORBIT_REFRESH_TOKEN"]
CLIENT_SECRET = os.environ.get("NT_ORBIT_CLIENT_SECRET") or None

failures = []


def check(cond, msg):
    print(("  ✅ " if cond else "  ❌ ") + msg)
    if not cond:
        failures.append(msg)


# ---------------------------------------------------------------------------
# 1. Real token refresh
# ---------------------------------------------------------------------------
print("[1] Refreshing a real Entra access_token from the real refresh_token …")

_rotated = {}


def _on_rotated(access_token, refresh_token, expires_in):
    _rotated["refresh_token"] = refresh_token
    _rotated["expires_in"] = expires_in


provider = EntraTokenProvider(
    tenant_id=TENANT_ID,
    client_id=CLIENT_ID,
    refresh_token=REFRESH_TOKEN,
    client_secret=CLIENT_SECRET,
    on_token_refreshed=_on_rotated,
)

access_token = provider.get_access_token()
check(bool(access_token) and isinstance(access_token, str), "got a non-empty access_token")
print(f"    access_token (truncated): {access_token[:24]}…")

if _rotated.get("refresh_token") and _rotated["refresh_token"] != REFRESH_TOKEN:
    print("    ⚠️  Entra ROTATED the refresh_token on this call.")
    print("        Update NT_ORBIT_REFRESH_TOKEN / the secret scope / the control")
    print(f"        table with: {_rotated['refresh_token']}")
else:
    print("    (refresh_token did not change on this call)")

# ---------------------------------------------------------------------------
# 2. Real health check
# ---------------------------------------------------------------------------
print("\n[2] Calling the real NT Orbit /api/v1/health …")
orbit = NTOrbitConnector(bearer_token_provider=provider.get_access_token)
health = orbit.health()
print(f"    response: {health}")
check(str(health.get("status", "")).lower() == "healthy",
      "NT Orbit reports status=healthy (this Entra identity IS granted API access)")

# ---------------------------------------------------------------------------
# 3. Real duty calculation call — spec example (Cotton t-shirt, BD -> US)
# ---------------------------------------------------------------------------
print("\n[3] Calling the real /api/v1/calculate/single/ (Cotton t-shirt, BD -> US) …")
sample_row = {
    "style_description": "Cotton t-shirt with printed design",
    "fabric_content": None, "gender": None, "class_name": None, "sub_class": None,
    "production_country": "BD",
}
payload = duty.build_calc_request(sample_row, "US")
print(f"    request payload: {payload}")

response = orbit.calculate_single(payload)
print(f"    raw response keys: {list(response.keys())}")
check(response.get("success") is True, "response.success is True")

try:
    result = duty.extract_duty_fields(response)
    check(bool(result.hts_code), f"got a real hs_code: {result.hts_code!r}")
    check(result.duty_rate is not None, f"got a real General Duty rate: {result.duty_rate!r}")
    print(f"    parsed: hts_code={result.hts_code!r} duty_rate={result.duty_rate!r} "
          f"tariff_rate={result.tariff_rate!r} classification={result.classification_name!r}")
except ValueError as e:
    check(False, f"extract_duty_fields() raised on a real response: {e}")

# ---------------------------------------------------------------------------
# 4. Second live refresh (proves the flow is repeatable, e.g. across a long
#    Databricks job run that spans the ~1hr access-token lifetime)
# ---------------------------------------------------------------------------
print("\n[4] Forcing a second live token refresh …")
access_token_2 = provider.get_access_token(force_refresh=True)
check(bool(access_token_2), "second refresh also returned a non-empty access_token")

orbit.close()

# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
if failures:
    print(f"❌ {len(failures)} live failure(s):")
    for f in failures:
        print("  -", f)
    sys.exit(1)
print("✅ LIVE NT ORBIT + ENTRA OAUTH2 TEST PASSED")
