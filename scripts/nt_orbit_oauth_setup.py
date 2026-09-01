#!/usr/bin/env python3
"""
One-time interactive Entra ID (Azure AD) authorization for the NT Orbit Duty
Tools API (Phase 9b).

Run this ONCE per environment, locally, signed in as the delegated user for
whom the Orbit API access is granted (per the project spec:
"auchunkei@lifung.com"). This IS a dedicated confidential app registration
for Phase 9b — it has its own client_secret — but the Entra sign-in itself is
still a DELEGATED USER login (NT Orbit authorizes the signed-in person, not
the app). tenant_id/client_id/client_secret/redirect_uri are ALL parameters
(flags or env vars) precisely so you can point this at a different
environment's app registration (dev/uat/prod each with its own client_id
AND its own registered redirect_uri) without editing this script:

    python scripts/nt_orbit_oauth_setup.py --tenant-id ... --client-id ... \\
        --client-secret ... --redirect-uri ...

Default flow: AUTHCODE (recommended now that you own the app registration and
can add a redirect URI yourself in the Entra portal):

  1. Register --redirect-uri (default http://localhost:8765/callback) as a
     redirect URI on the app in the Entra portal for this client_id, under
     the "Web" platform (Authentication -> Add a platform -> Web) — NOT
     "Mobile and desktop applications". Entra treats ANY redirect_uri
     registered under "Mobile and desktop applications" as a public-client
     request and will reject client_secret with AADSTS700025, even though
     this app registration has one configured. (If you do have it registered
     under "Mobile and desktop applications" already, this script will still
     work — it detects AADSTS700025 and automatically retries without the
     secret — but move it to "Web" to actually use your secret going forward.)
  2. This script opens the Entra /authorize URL, runs a short-lived local
     HTTP server to catch the redirect (?code=...&state=...), and
     automatically exchanges the resulting single-use authorization ``code``
     for an initial access_token + refresh_token pair — no manual copy/paste.
  3. Confirms /api/v1/health succeeds with the resulting access_token, then
     prints the refresh_token so you can store it in the Databricks secret
     scope `beproduct` as key `nt_orbit_refresh_token`.

Two alternate flows are available if you can't add a redirect URI yourself
(e.g. running against someone else's pre-existing client_id):
  --flow manual        The officially-provided fallback dev-test procedure:
                       Entra /authorize with
                       redirect_uri=https://oauth.pstmn.io/v1/browser-callback
                       (Postman's public "display the code back to you" page
                       — already registered on many shared client_ids), with
                       manual copy/paste of the resulting `code` (a
                       single-use AUTHORIZATION CODE, NOT the refresh_token —
                       this script still does the one-time exchange for you).
  --flow devicecode     OAuth2 Device Authorization Grant (RFC 8628, same
                       mechanism as `az login --use-device-code`) — no
                       redirect URI at all, but requires the client_id to have
                       public-client/device-code flows enabled.

Required env vars / flags:
  NT_ORBIT_TENANT_ID       Entra tenant (directory) ID
  NT_ORBIT_CLIENT_ID       The app registration (client) ID for this environment
  NT_ORBIT_CLIENT_SECRET   Optional — only if the app registration is confidential
  NT_ORBIT_REDIRECT_URI    Optional — override the flow's default; MUST match a
                            redirect URI registered on that client_id (authcode
                            flow only; manual/devicecode don't need this)

Usage
-----
    python scripts/nt_orbit_oauth_setup.py
    python scripts/nt_orbit_oauth_setup.py --tenant-id ... --client-id ...

    # Different environment (e.g. UAT app registration + its own callback URI):
    python scripts/nt_orbit_oauth_setup.py --tenant-id <uat_tenant> \\
        --client-id <uat_client_id> --redirect-uri http://localhost:9000/callback

    # No portal access to register a redirect URI on this client_id:
    python scripts/nt_orbit_oauth_setup.py --flow manual
    python scripts/nt_orbit_oauth_setup.py --flow devicecode

After a successful run, store the printed refresh_token as:
    databricks secrets put-secret beproduct nt_orbit_refresh_token
    databricks secrets put-secret beproduct nt_orbit_tenant_id
    databricks secrets put-secret beproduct nt_orbit_client_id
    databricks secrets put-secret beproduct nt_orbit_client_secret   # if used

Refresh tokens rotate on use — dtc/notebooks/p9b_fill_duty_rates.py persists
rotated tokens to a Delta control table automatically after the first run, so
you should only need to run this script again if the credential is revoked or
expires from prolonged disuse, OR you're setting up a new environment.
"""

import argparse
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).parent.parent / "dtc" / "python"))

from client import entra_auth  # noqa: E402
from connectors.nt_orbit import NTOrbitConnector  # noqa: E402


def _verify_and_report(args, tokens, redirect_uri=None):
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
              "but this user may not yet be granted access to the Orbit API — "
              "ask NeoTangent/your Orbit admin to grant access, then re-run.")

    print("\n" + "=" * 72)
    print("Store these in the Databricks secret scope 'beproduct':")
    print("=" * 72)
    print(f"  nt_orbit_tenant_id      = {args.tenant_id}")
    print(f"  nt_orbit_client_id      = {args.client_id}")
    print(f"  nt_orbit_refresh_token  = {refresh_token}")
    if redirect_uri:
        # Not stored/used at Databricks runtime (only the one-time login needs
        # it), but echoed here so you have a record of which redirect_uri was
        # paired with THIS client_id/tenant_id — useful when you swap
        # client_id per environment (dev/uat/prod) and each has its own
        # registered redirect URI.
        print(f"  (redirect_uri used this run, for your records: {redirect_uri})")
    print("\n(access_token is short-lived and intentionally not shown for storage — "
          "p9b_fill_duty_rates.py mints it on demand from the refresh_token.)")


def run_manual(args):
    """
    The officially-provided dev-test procedure: Entra /authorize with
    redirect_uri=Postman's public browser-callback page, manual copy/paste of
    the resulting `code`, then a scripted one-time exchange for tokens.
    """
    redirect_uri = args.redirect_uri or entra_auth.DEFAULT_MANUAL_REDIRECT_URI
    state = args.state or "123456"

    url, state = entra_auth.build_authorize_url(
        tenant_id=args.tenant_id,
        client_id=args.client_id,
        redirect_uri=redirect_uri,
        scope=args.scope,
        state=state,
    )

    print("1. Open this URL and sign in as the delegated user (e.g. auchunkei@lifung.com):\n")
    print(f"   {url}\n")
    if not args.no_browser:
        entra_auth.open_browser_for_login(url)

    print(f"2. The browser will redirect to {redirect_uri}?code=...&state={state}&session_state=...")
    print("   Paste that whole URL, or just the code=... value, below.\n")
    pasted = input("   > ").strip()

    parsed = entra_auth.parse_callback_input(pasted)
    if not parsed["code"]:
        sys.exit("Could not find an authorization code in what you pasted. Try again.")
    if parsed["state"] and parsed["state"] != state:
        print(f"⚠️  state mismatch (expected {state!r}, got {parsed['state']!r}) — "
              "continuing anyway since you pasted this yourself, but double-check you "
              "signed in via the URL printed above and not a stale tab.")

    print("\n3. Got the authorization code — exchanging it ONCE for tokens "
          "(this code cannot be reused) …")
    tokens = entra_auth.exchange_code_for_tokens(
        tenant_id=args.tenant_id,
        client_id=args.client_id,
        redirect_uri=redirect_uri,
        code=parsed["code"],
        client_secret=args.client_secret,
        scope=args.scope,
    )
    _verify_and_report(args, tokens, redirect_uri=redirect_uri)


def run_device_code(args):
    print(f"Requesting a device code from Entra (tenant={args.tenant_id}) …")
    dc = entra_auth.start_device_code_flow(
        tenant_id=args.tenant_id, client_id=args.client_id, scope=args.scope,
    )
    print("\n" + "=" * 72)
    print(dc.get("message") or (
        f"To sign in, open {dc['verification_uri']} and enter code {dc['user_code']}"
    ))
    print("=" * 72)
    print(f"(Sign in as the delegated user, e.g. auchunkei@lifung.com. "
          f"Code expires in {dc.get('expires_in', 900)}s.)\n")

    def _tick():
        print(".", end="", flush=True)

    try:
        tokens = entra_auth.poll_device_code_token(
            tenant_id=args.tenant_id,
            client_id=args.client_id,
            device_code=dc["device_code"],
            interval=dc.get("interval", 5),
            expires_in=dc.get("expires_in", 900),
            on_poll=_tick,
        )
    except entra_auth.DeviceCodeExpired:
        sys.exit("\nDevice code expired before sign-in completed. Try again.")
    print("\n✅ Sign-in complete, tokens received.")
    _verify_and_report(args, tokens)


def run_authcode(args):
    redirect_uri = args.redirect_uri or "http://localhost:8765/callback"
    port = urlparse(redirect_uri).port or 8765

    url, state = entra_auth.build_authorize_url(
        tenant_id=args.tenant_id,
        client_id=args.client_id,
        redirect_uri=redirect_uri,
        scope=args.scope,
    )

    print("Sign in as the delegated user (e.g. auchunkei@lifung.com) at:\n")
    print(f"  {url}\n")
    if not args.no_browser:
        entra_auth.open_browser_for_login(url)

    print(f"Waiting up to {args.timeout}s for the redirect on {redirect_uri} …")
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
        redirect_uri=redirect_uri,
        code=result.code,
        client_secret=args.client_secret,
        scope=args.scope,
    )
    _verify_and_report(args, tokens, redirect_uri=redirect_uri)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tenant-id", default=os.environ.get("NT_ORBIT_TENANT_ID"))
    ap.add_argument("--client-id", default=os.environ.get("NT_ORBIT_CLIENT_ID"))
    ap.add_argument("--flow", choices=["authcode", "manual", "devicecode"], default="authcode",
                    help="authcode (default; fully scripted, no copy/paste — needs "
                         "--redirect-uri registered on the client_id you're using), "
                         "manual (the officially-provided fallback dev-test procedure — "
                         "Postman's browser-callback page + copy/paste, no local server, "
                         "no redirect-URI registration needed), or devicecode (RFC 8628, "
                         "no redirect URI at all, needs public-client flows enabled on "
                         "the client_id)")
    ap.add_argument("--scope", default=entra_auth.DEFAULT_SCOPE)
    ap.add_argument("--state", default=None,
                    help="manual/authcode flow only; default 123456 for manual "
                         "(matches the officially-provided example), random for authcode")
    ap.add_argument("--client-secret", default=os.environ.get("NT_ORBIT_CLIENT_SECRET"),
                    help="manual/authcode flow only; not used by devicecode. Set this when "
                         "using a confidential (has-a-secret) app registration, e.g. via "
                         "NT_ORBIT_CLIENT_SECRET")
    ap.add_argument("--redirect-uri", default=os.environ.get("NT_ORBIT_REDIRECT_URI"),
                    help="manual/authcode flow only — swap this together with --client-id "
                         "when pointing at a different environment's app registration. "
                         "Defaults to http://localhost:8765/callback for --flow authcode, "
                         "https://oauth.pstmn.io/v1/browser-callback for --flow manual. "
                         "Not used by devicecode. Env var: NT_ORBIT_REDIRECT_URI")
    ap.add_argument("--timeout", type=int, default=300,
                    help="authcode flow only: seconds to wait for the browser redirect")
    ap.add_argument("--no-browser", action="store_true",
                    help="manual/authcode flow: print the URL instead of opening it")
    args = ap.parse_args()

    if not args.tenant_id or not args.client_id:
        sys.exit("Set NT_ORBIT_TENANT_ID and NT_ORBIT_CLIENT_ID (env vars or --tenant-id/--client-id).")

    if args.flow == "devicecode":
        run_device_code(args)
    elif args.flow == "manual":
        run_manual(args)
    else:
        run_authcode(args)


if __name__ == "__main__":
    main()
