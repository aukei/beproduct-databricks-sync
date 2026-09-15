# Databricks notebook source
"""
DIAGNOSTIC -- list the live DTC requests for a workspace+document. READ ONLY.

Not part of any job. Answers "what does DTC actually have right now, and does
any name resolve ambiguously?" without writing anything, anywhere -- no DTC
write, no Delta write, not even a registry refresh.

Why it exists
-------------
DTC permits two requests to share a name while BOTH are active, distinguished
only by `requestId`. The registry additionally retains inactive and
`(BACKUP)`-named rows forever. Resolving a request by NAME alone therefore
silently picks the wrong sheet -- live-hit 2026-09-15, when
`KTB SS28 Collaborations` matched an active 60-row request AND an inactive
19-row sibling, and a name lookup returned the sibling.

`registry.find_duplicate_active_names()` exists for this, and
`p1_dtc_request_manager` logs `DUPLICATE_ACTIVE_NAME` rather than guessing.
This notebook is the ad-hoc equivalent: it compares what DTC has RIGHT NOW
against what the registry believes, so a newly-created request that the
registry has not yet discovered is visible immediately.
"""

# COMMAND ----------

import sys

_DEFAULT_MODULE_PATH = "/Workspace/Repos/beproduct-sync/DTC/python"
dbutils.widgets.text("module_path", _DEFAULT_MODULE_PATH, "Python module root")
_MODULE_PATH = (dbutils.widgets.get("module_path") or "").strip() or _DEFAULT_MODULE_PATH
for _p in (_MODULE_PATH, _MODULE_PATH.replace("/DTC/", "/dtc/")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import json

from pyspark.sql import functions as F

from connectors.dtc import DTCConnector
from sync import phase1

dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("customer", "KTB", "Customer")
dbutils.widgets.text("dtc_workspace", "KTB", "DTC workspace")
dbutils.widgets.text("dtc_document", "KTB WIP", "DTC document")
dbutils.widgets.text("dtc_environment", "uat", "DTC environment")
dbutils.widgets.text("name_filter", "", "Only references containing this (blank = all)")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
customer = dbutils.widgets.get("customer")
workspace = dbutils.widgets.get("dtc_workspace")
document = dbutils.widgets.get("dtc_document")
environment = dbutils.widgets.get("dtc_environment")
name_filter = (dbutils.widgets.get("name_filter") or "").strip().lower()

print("=" * 78)
print(f"LIVE DTC REQUESTS -- {workspace} / {document} ({environment})   READ ONLY")
print("=" * 78)

api_key = dbutils.secrets.get(scope="beproduct", key=f"dtc_api_key_{environment}")
dtc = DTCConnector(api_key=api_key, environment=environment, workspace_name=workspace)

# COMMAND ----------

# ── What DTC has right now ─────────────────────────────────────────────────
live = dtc.search_requests(workspace_name=workspace, document_name=document) or []
print(f"\nDTC returned {len(live)} request(s)")

registry = {}
try:
    for r in (spark.table(f"{catalog}.{schema}.dtc_request_registry")
              .where(F.col("environment") == environment).collect()):
        registry[r["request_id"]] = r.asDict()
except Exception as e:  # noqa: BLE001
    print(f"(registry unavailable: {e})")

rows, by_name = [], {}
for r in live:
    ref = r.get("requestReference") or r.get("requestName") or ""
    if name_filter and name_filter not in ref.lower():
        continue
    rid = r.get("requestId") or r.get("id")
    reg = registry.get(rid)
    rec = {
        "request_reference": ref,
        "request_id": rid,
        "in_scope_by_name": phase1.is_in_scope(ref, customer),
        "in_registry": reg is not None,
        "registry_active": (reg or {}).get("request_is_active"),
        "registry_in_scope": (reg or {}).get("in_scope"),
        "registry_rows": (reg or {}).get("row_count"),
        "registry_last_extracted": str((reg or {}).get("last_extracted") or ""),
        "registry_last_pushed": str((reg or {}).get("last_pushed") or ""),
    }
    rows.append(rec)
    if rec["in_scope_by_name"]:
        by_name.setdefault(ref, []).append(rec)

dtc.close()

# COMMAND ----------

print(f"\n{'reference':44} {'in_scope':9} {'in_reg':7} {'active':7} {'rows':>5}")
print("-" * 78)
for rec in sorted(rows, key=lambda x: (not x["in_scope_by_name"], x["request_reference"])):
    print(f"  {rec['request_reference'][:42]:42} "
          f"{str(rec['in_scope_by_name']):9} "
          f"{str(rec['in_registry']):7} "
          f"{str(rec['registry_active']):7} "
          f"{str(rec['registry_rows'] if rec['registry_rows'] is not None else '-'):>5}")

# ── Ambiguity: the thing that silently breaks name-based resolution ────────
dupes = {n: v for n, v in by_name.items() if len(v) > 1}
unknown = [r for r in rows if r["in_scope_by_name"] and not r["in_registry"]]

if dupes:
    print("\n⚠ DUPLICATE IN-SCOPE NAMES -- name-based resolution is AMBIGUOUS:")
    for n, v in dupes.items():
        print(f"    {n!r}")
        for rec in v:
            print(f"      request_id={rec['request_id']}  registry_active="
                  f"{rec['registry_active']}  rows={rec['registry_rows']}  "
                  f"last_extracted={rec['registry_last_extracted']}")
    print("    Resolve by request_id, or deactivate all but one, before pushing.")

if unknown:
    print("\n⚠ IN-SCOPE BUT NOT YET IN THE REGISTRY (created since the last scan):")
    for rec in unknown:
        print(f"    {rec['request_reference']!r}  request_id={rec['request_id']}")
    print("    Run p1_pull_masters_to_delta (refresh_registry=true) to register it.")

summary = {
    "live_requests": len(rows),
    "in_scope": sum(1 for r in rows if r["in_scope_by_name"]),
    "duplicate_in_scope_names": {n: [x["request_id"] for x in v] for n, v in dupes.items()},
    "in_scope_not_in_registry": [
        {"reference": r["request_reference"], "request_id": r["request_id"]} for r in unknown],
    "requests": rows,
}
print("\n" + json.dumps({k: v for k, v in summary.items() if k != "requests"}, indent=2))
dbutils.notebook.exit(json.dumps(summary, default=str))
