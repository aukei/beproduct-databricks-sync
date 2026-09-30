# Databricks notebook source
"""
DIAGNOSTIC PROBE -- why does a GET of a DTC-hosted image 403 from Databricks?

READ-ONLY. Not part of any job. Run by hand:
    python scripts/run_v2_task.py v2_probe_image_get

Background (2026-09-28): Phase 3's sibling copy downloads a DTC-hosted image
(`/api/v1/images/<uuid>.png`). From Databricks, every such GET has returned
`403 Forbidden` since 2026-09-18 (last success 2026-09-10). From a local machine
through the local proxy, the SAME URLs return 200 with x-api-key (401
without). The job log keeps only `raise_for_status()`'s one-liner, so this
probe records the full request/response for the DTC team.

It sends ONLY GETs to dtc-api.lfuat.net: the image with and without the key,
the image with the connector's own headers, and a control API call on the
same host. Each result goes in the EXIT VALUE (serverless returns no stdout)
with the status, every response header, the first 400 body chars, the
resolved IPs and the timing. The API key is never included.
"""

# COMMAND ----------

import json
import socket
import time
from datetime import datetime, timezone

import requests

dbutils.widgets.text("environment", "uat", "DTC environment (secret dtc_api_key_<env>)")
dbutils.widgets.text("host", "dtc-api.lfuat.net", "DTC API host")
dbutils.widgets.text("image_paths",
                     "/api/v1/images/57efbd53-2d27-4395-9767-82e4f54d78ad.png,"
                     "/api/v1/images/695f7764-e3bd-413c-8173-23b3eba0402d.png",
                     "Comma-separated image paths to GET")
dbutils.widgets.text("control_path", "/api/v1/views/69f04983501f3d9cf4fc379c",
                     "A normal API path on the same host (control)")
dbutils.widgets.text("module_path", "", "unused; accepted so run_v2_task.py can pass it")

env = dbutils.widgets.get("environment").strip()
host = dbutils.widgets.get("host").strip()
image_paths = [p.strip() for p in dbutils.widgets.get("image_paths").split(",") if p.strip()]
control_path = dbutils.widgets.get("control_path").strip()
api_key = dbutils.secrets.get(scope="beproduct", key=f"dtc_api_key_{env}")


def _probe(label, method, path, headers):
    url = f"https://{host}{path}"
    t0 = time.time()
    out = {"label": label, "method": method, "url": url,
           "request_headers": {k: ("<redacted>" if k.lower() == "x-api-key" else v)
                               for k, v in (headers or {}).items()}}
    try:
        r = requests.request(method, url, headers=headers, timeout=30, allow_redirects=False)
        out["request_headers_sent"] = {
            k: ("<redacted>" if k.lower() == "x-api-key" else v) for k, v in r.request.headers.items()}
        out.update({
            "status": r.status_code, "reason": r.reason,
            "elapsed_ms": int((time.time() - t0) * 1000),
            "response_headers": dict(r.headers),
            "body_len": len(r.content),
            "body_head": (r.text[:400] if "image" not in (r.headers.get("Content-Type") or "")
                          else f"<{len(r.content)} bytes of {r.headers.get('Content-Type')}>"),
        })
    except Exception as e:  # noqa: BLE001
        out.update({"error": f"{type(e).__name__}: {str(e)[:400]}",
                    "elapsed_ms": int((time.time() - t0) * 1000)})
    return out


# COMMAND ----------

summary = {"probe": "v2_probe_image_get", "at_utc": datetime.now(timezone.utc).isoformat(),
           "host": host}
try:
    summary["dns"] = sorted({ai[4][0] for ai in socket.getaddrinfo(host, 443)})
except Exception as e:  # noqa: BLE001
    summary["dns"] = f"{type(e).__name__}: {e}"

key_only = {"x-api-key": api_key}                                     # what Phase 3 sends
connector = {"Content-Type": "application/json", "x-api-key": api_key}  # RestClient._get_headers

results = [_probe("control: normal API path, connector headers", "GET", control_path, connector)]
for p in image_paths:
    results += [
        _probe("image, x-api-key only (exactly what Phase 3 sends)", "GET", p, key_only),
        _probe("image, connector headers", "GET", p, connector),
        _probe("image, no key", "GET", p, None),
    ]
summary["results"] = results
summary["verdict"] = {r["label"] + " " + r["url"].rsplit("/", 1)[-1]: r.get("status", r.get("error"))
                      for r in results}
dbutils.notebook.exit(json.dumps(summary, default=str))
