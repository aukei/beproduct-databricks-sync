#!/usr/bin/env python3
"""
One-time interactive Entra ID (Azure AD) authorization for the NT Orbit Duty
Tools API (Phase 9b).

Run this ONCE, locally, signed in as the delegated user for whom the Orbit API
access is granted (per the project spec: "auchunkei@lifung.com"). It:

  1. Opens the Microsoft Entra ID /authorize URL in your browser.
  2. Runs a short-lived local HTTP server to catch the OAuth2 redirect
     (?code=...&state=...).
  3. Exchanges the authorization code for an initial access_token +
     refresh_token pair.
  4. Prints the refresh_token (and confirms /api/v1/health succeeds with the
     resulting access_token) so you can store it in the Databricks secret
     scope `beproduct` as key `nt_orbit_refresh_token`.

You must also already have (from an Entra app registration, provided by your
tenant admin) and pass via env vars or flags:
  NT_ORBIT_TENANT_ID       Entra tenant (directory) ID
  NT_ORBIT_CLIENT_ID       App registration (client) ID
  NT_ORBIT_CLIENT_SECRET   Optional — only for a confidential app registration
  NT_ORBIT_REDIRECT_URI    Must exactly match a redirect URI registered on the
                            app (default: http://localhost:8765/callback)

Usage
-----
    python scripts/nt_orbit_oauth_setup.py
    python scripts/nt_orbit_oauth_setup.py --tenant-id ... --client-id ...

After a successful run, store the printed refresh_token as:
    databricks secrets put-secret beproduct nt_orbit_refresh_token
    databricks secrets put-secret beproduct nt_orbit_tenant_id
    databricks secrets put-secret beproduct nt_orbit_client_id
    databricks secrets put-secret beproduct nt_orbit_client_secret   # if used

Refresh tokens rotate on use — dtc/notebooks/p9b_fill_duty_rates.py persists
rotated tokens to a Delta control table automatically after the first run, so
you should only need to run this script again if the credential is revoked or
expires from prolonged disuse.
"""

import argparse
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).parent.parent / "dtc" / "python"))

from client import entra_auth  # noqa: E402
from connectors.nt_orbit import NTOrbitConnector  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tenant-id", default=os.environ.get("NT_ORBIT_TENANT_ID"))
    ap.add_argument("--client-id", default=os.environ.get("NT_ORBIT_CLIENT_ID"))
    ap.add_argument("--client-secret", default=os.environ.get("NT_ORBIT_CLIENT_SECRET"))
    ap.add_argument("--redirect-uri",
                    default=os.environ.get("NT_ORBIT_REDIRECT_URI", "http://localhost:8765/callback"))
    ap.add_argument("--scope", default=entra_auth.DEFAULT_SCOPE)
    ap.add_argument("--timeout", type=int, default=300, help="seconds to wait for the browser redirect")
    ap.add_argument("--no-browser", action="store_true", help="print the URL instead of opening it")
    args = ap.parse_args()

    if not args.tenant_id or not args.client_id:
        sys.exit("Set NT_ORBIT_TENANT_ID and NT_ORBIT_CLIENT_ID (env vars or --tenant-id/--client-id).")

    port = urlparse(args.redirect_uri).port or 8765

    url, state = entra_auth.build_authorize_url(
        tenant_id=args.tenant_id,
        client_id=args.client_id,
        redirect_uri=args.redirect_uri,
        scope=args.scope,
    )

    print("Sign in as the delegated user (e.g. auchunkei@lifung.com) at:\n")
    print(f"  {url}\n")
    if not args.no_browser:
        entra_auth.open_browser_for_login(url)

    print(f"Waiting up to {args.timeout}s for the redirect on {args.redirect_uri} …")
    result = entra_auth.run_local_callback_server(port=port, timeout=args.timeout)

    if result.error:
        sys.exit(f"Authorization failed: {result.error}: {result.error_description}")
    if not result.code:
        sys.exit("Timed out waiting for the browser redirect. Try again.")
    if result.state != state:
        sys.exit("state mismatch on callback — aborting (possible CSRF); try again.")

    print("✅ Got authorization code, exchanging for tokens …")
    tokens = entra_auth.exchange_code_for_tokens(
        tenant_id=args.tenant_id,
        client_id=args.client_id,
        redirect_uri=args.redirect_uri,
        code=result.code,
        client_secret=args.client_secret,
        scope=args.scope,
    )

    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")
    if not access_token or not refresh_token:
        sys.exit(f"Token response missing access_token/refresh_token: {tokens}")

    print("✅ Got access_token + refresh_token. Verifying against NT Orbit /api/v1/health …")
    orbit = NTOrbitConnector(bearer_token_provider=lambda: access_token)
    if orbit.is_healthy():
        print("✅ NT Orbit health check succeeded — this identity is authorized.")
    else:
        print("⚠️  NT Orbit health check did NOT report healthy. The token is valid Entra-side, "
              "but this user/app may not yet be granted access to the Orbit API — "
              "ask NeoTangent/your Orbit admin to grant access, then re-run.")

    print("\n" + "=" * 72)
    print("Store these in the Databricks secret scope 'beproduct':")
    print("=" * 72)
    print(f"  nt_orbit_tenant_id      = {args.tenant_id}")
    print(f"  nt_orbit_client_id      = {args.client_id}")
    if args.client_secret:
        print("  nt_orbit_client_secret  = <unchanged, as provided>")
    print(f"  nt_orbit_refresh_token  = {refresh_token}")
    print("\n(access_token is short-lived and intentionally not shown for storage — "
          "p9b_fill_duty_rates.py mints it on demand from the refresh_token.)")


if __name__ == "__main__":
    main()
