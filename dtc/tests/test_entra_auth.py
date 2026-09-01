#!/usr/bin/env python3
"""
Unit tests for the pure/no-network helpers in dtc/python/client/entra_auth.py.

Pure-Python, no Spark, no network. Run:
    python3 dtc/tests/test_entra_auth.py
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from client import entra_auth
from client.entra_auth import build_authorize_url, parse_callback_input, DEFAULT_MANUAL_REDIRECT_URI

_failures = []


def check(cond, msg):
    if cond:
        print(f"  ✅ {msg}")
    else:
        print(f"  ❌ {msg}")
        _failures.append(msg)


print("\n[1] build_authorize_url()")
url, state = build_authorize_url(
    tenant_id="c4d8a220-a9ec-4572-8c77-ab36a3ecdbae",
    client_id="CLIENT123",
    redirect_uri=DEFAULT_MANUAL_REDIRECT_URI,
    state="123456",
)
check(url.startswith(
    "https://login.microsoftonline.com/c4d8a220-a9ec-4572-8c77-ab36a3ecdbae/oauth2/v2.0/authorize?"
), "targets the correct tenant's /authorize endpoint")
check("client_id=CLIENT123" in url, "carries client_id")
check("response_type=code" in url, "requests the authorization-code grant")
check("state=123456" in url, "carries the given state (matches the officially-provided example)")
check(state == "123456", "returns the state actually used")
check("oauth.pstmn.io" in url, "uses Postman's public browser-callback as redirect_uri when passed")

url2, state2 = build_authorize_url("t", "c", "http://localhost:9/callback")
check(state2 and len(state2) > 8, "a random state is generated when none is given")
check(state2 != state, "generated states differ across calls")

print("\n[2] parse_callback_input() — full Postman-style redirect URL")
full_url = ("https://oauth.pstmn.io/v1/browser-callback"
            "?code=1.XXXXXXXXXXXX&state=123456&session_state=YYYYYYYY")
parsed = parse_callback_input(full_url)
check(parsed["code"] == "1.XXXXXXXXXXXX", "extracts code from a full redirected URL")
check(parsed["state"] == "123456", "extracts state from a full redirected URL")

print("\n[3] parse_callback_input() — bare query string (no scheme/host)")
parsed2 = parse_callback_input("code=1.YYY&state=abc&session_state=zzz")
check(parsed2 == {"code": "1.YYY", "state": "abc"}, "extracts code+state from a bare query string")

parsed2b = parse_callback_input("?code=1.YYY&state=abc")
check(parsed2b["code"] == "1.YYY", "leading '?' on a bare query string is tolerated")

print("\n[4] parse_callback_input() — bare authorization code only")
parsed3 = parse_callback_input("1.ZZZZZZZZZZZZ")
check(parsed3 == {"code": "1.ZZZZZZZZZZZZ", "state": None},
      "a pasted value with no code=/state= markers is treated as the raw code")

print("\n[5] parse_callback_input() — edge cases")
check(parse_callback_input("") == {"code": None, "state": None}, "empty input -> no code/state")
check(parse_callback_input("   ") == {"code": None, "state": None}, "whitespace-only input -> no code/state")
check(parse_callback_input("  1.TRIMMED  ")["code"] == "1.TRIMMED", "surrounding whitespace is trimmed")


def _mock_response(status_code, json_body):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_body
    if status_code >= 400:
        import requests
        err = requests.exceptions.HTTPError(response=resp)
        resp.raise_for_status.side_effect = err
    else:
        resp.raise_for_status.side_effect = None
    return resp


print("\n[6] AADSTS700025 (public-client redirect_uri) — automatic secret fallback")
public_client_error_body = {
    "error": "invalid_client",
    "error_description": "AADSTS700025: Client is public so neither "
                         "'client_assertion' nor 'client_secret' should be presented.",
    "error_codes": [700025],
}
ok_body = {"access_token": "AT1", "refresh_token": "RT1", "expires_in": 3600}

with patch("client.entra_auth.requests.post") as mock_post:
    mock_post.side_effect = [
        _mock_response(401, public_client_error_body),  # 1st call: WITH secret -> rejected
        _mock_response(200, ok_body),                    # 2nd call: WITHOUT secret -> succeeds
    ]
    result = entra_auth.exchange_code_for_tokens(
        tenant_id="t", client_id="c", redirect_uri="http://localhost:8765/callback",
        code="CODE1", client_secret="SECRET1",
    )
    check(result == ok_body, "exchange_code_for_tokens retries without secret and returns the token on AADSTS700025")
    check(mock_post.call_count == 2, "exactly 2 HTTP calls made (1 with secret, 1 fallback without)")
    first_call_data = mock_post.call_args_list[0].kwargs.get("data", {})
    second_call_data = mock_post.call_args_list[1].kwargs.get("data", {})
    check(first_call_data.get("client_secret") == "SECRET1", "first attempt included client_secret")
    check("client_secret" not in second_call_data, "fallback attempt omitted client_secret")

with patch("client.entra_auth.requests.post") as mock_post:
    mock_post.side_effect = [_mock_response(200, ok_body)]
    result2 = entra_auth.refresh_access_token(
        tenant_id="t", client_id="c", refresh_token="RT_OLD", client_secret="SECRET1",
    )
    check(result2 == ok_body, "refresh_access_token succeeds normally when secret is accepted")
    check(mock_post.call_count == 1, "no unnecessary retry when the first attempt succeeds")

other_error_body = {"error": "invalid_grant", "error_codes": [70008]}  # expired/used code
with patch("client.entra_auth.requests.post") as mock_post:
    mock_post.side_effect = [_mock_response(400, other_error_body)]
    try:
        entra_auth.exchange_code_for_tokens(
            tenant_id="t", client_id="c", redirect_uri="http://x/callback",
            code="STALE", client_secret="SECRET1",
        )
        check(False, "a non-700025 error should propagate, not be swallowed")
    except Exception:
        check(mock_post.call_count == 1, "a different Entra error is NOT retried, propagates immediately")

# ---------------------------------------------------------------------------
print(f"\n{'='*60}")
if _failures:
    print(f"❌ {len(_failures)} check(s) failed:")
    for f in _failures:
        print(f"   - {f}")
    sys.exit(1)
else:
    print("✅ All checks passed")
