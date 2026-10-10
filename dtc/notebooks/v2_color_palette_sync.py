# Databricks notebook source
"""
v2 Stage 60 -- Phase 11: BeProduct color palettes -> DTC "KTB Color Palette".

One-way, BeProduct -> DTC. DTC Fabric requests pick color number + name from
this sheet, so it mirrors every palette in the BeProduct color folder.

  1. ONE `api.color.attributes_list(folder_id)` call returns every palette with
     its colors. No per-palette fetch, and no reliance on `modifiedAt`: the
     palettes are small, so a full pull every run is cheap. It also sees
     removals, which a timestamp filter cannot.
  2. Flatten to one row per palette x color x brand and snapshot it to Delta
     (`beproduct_color_palette`, overwritten each run) so the source is
     queryable.
  3. Read the target DTC request LIVE and diff (`sync/color_palette.py`):
     new -> append_rows, changed -> PATCH only the changed columns,
     gone from BeProduct -> Active = "NO". Nothing changed -> NO write.
  4. If anything was written, re-read and re-plan. Anything still pending
     means a write did not land, and the run says so.

This request is its own DTC write window, separate from the WIP requests
`wip_push` writes. DTC locks per request, so the two never contend.

The target request is resolved by NAME inside the document (it does not follow
the WIP "<customer> <season> <brand>" convention), and the run refuses unless
exactly one ACTIVE request has that name.
"""

# COMMAND ----------

import sys
import subprocess
import time

print("Installing BeProduct SDK …")
_t0 = time.perf_counter()
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "beproduct"])
print(f"  ✅ installed in {time.perf_counter() - _t0:.1f}s")

# COMMAND ----------

_DEFAULT_MODULE_PATH = "/Workspace/Repos/beproduct-sync-v2/DTC/python"
dbutils.widgets.text("module_path", _DEFAULT_MODULE_PATH, "Python module root")
_MODULE_PATH = (dbutils.widgets.get("module_path") or "").strip() or _DEFAULT_MODULE_PATH
for _p in (_MODULE_PATH, _MODULE_PATH.replace("/DTC/", "/dtc/")):
    if _p not in sys.path:
        sys.path.insert(0, _p)
print(f"module_path: {_MODULE_PATH}")

# Python 3.10 compatibility shim -- serverless runs 3.10 and the BeProduct SDK
# uses `datetime.UTC` (3.11+) unconditionally. See v2_pull_bom_segments.
import datetime as _dt_mod

if not hasattr(_dt_mod, "UTC"):
    _dt_mod.UTC = _dt_mod.timezone.utc

import json
from datetime import datetime, timezone

from beproduct.sdk import BeProduct
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from connectors.dtc import DTCConnector
from sync import color_palette as cp

# COMMAND ----------

dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("dtc_environment", "uat", "DTC environment")
dbutils.widgets.text("dtc_workspace", "KTB", "DTC workspace")
dbutils.widgets.text("color_folder", "KTB", "BeProduct COLOR folder name")
dbutils.widgets.text("color_document", "KTB Color Palette", "DTC document")
dbutils.widgets.text("color_request", "Color Palette", "DTC request reference (exact)")
dbutils.widgets.text("color_view", "WIP_ITS_USE", "DTC view")
dbutils.widgets.text("color_palette_table", "beproduct_color_palette", "Delta snapshot table")
dbutils.widgets.text("batch_size", "100", "Rows per DTC call")
dbutils.widgets.text("dry_run", "true", "Plan + log, never write DTC")
dbutils.widgets.text("run_color_palette", "true", "false = no-op")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
environment = dbutils.widgets.get("dtc_environment").strip()
workspace = dbutils.widgets.get("dtc_workspace").strip()
folder_name = dbutils.widgets.get("color_folder").strip()
document = dbutils.widgets.get("color_document").strip()
request_name = dbutils.widgets.get("color_request").strip()
view_name = dbutils.widgets.get("color_view").strip()
snapshot_table = f"{catalog}.{schema}.{dbutils.widgets.get('color_palette_table').strip()}"
sync_log_full = f"{catalog}.{schema}.beproduct_to_dtc_sync_log"
batch_size = max(1, int(dbutils.widgets.get("batch_size") or 100))
dry_run = (dbutils.widgets.get("dry_run") or "true").strip().lower() == "true"

if (dbutils.widgets.get("run_color_palette") or "true").strip().lower() != "true":
    print("run_color_palette=false -> no-op")
    dbutils.notebook.exit(json.dumps({"status": "SKIPPED"}))

now = datetime.now(timezone.utc)
run_id = now.strftime("%Y%m%d%H%M%S")
print(f"dry_run={dry_run}  folder={folder_name!r}  target={document!r} / {request_name!r} / {view_name!r}")

# COMMAND ----------

# ── Step 1: BeProduct palettes ─────────────────────────────────────────────
client = BeProduct(
    client_id=dbutils.secrets.get(scope="beproduct", key="client_id"),
    client_secret=dbutils.secrets.get(scope="beproduct", key="client_secret"),
    refresh_token=dbutils.secrets.get(scope="beproduct", key="refresh_token"),
    company_domain=dbutils.secrets.get(scope="beproduct", key="company_domain"),
)

_folders = [f for f in client.color.folders() if (f.get("name") or "").strip() == folder_name]
if len(_folders) != 1:
    raise RuntimeError(f"BeProduct color folder {folder_name!r}: expected 1 match, got {len(_folders)}")
folder_id = _folders[0]["id"]

palettes = list(client.color.attributes_list(folder_id=folder_id))
targets, flat_report = cp.flatten_palettes(palettes)
print(f"Step 1: {flat_report['palettes']} palette(s), {flat_report['colors']} color(s) "
      f"-> {len(targets)} target row(s)")

# A non-empty folder that yields zero rows is the "silent rename" shape this
# repo has been bitten by before -- refuse rather than untick every DTC row.
if palettes and not targets:
    dbutils.notebook.exit(json.dumps({"status": "NO_TARGETS_FROM_NONEMPTY_FOLDER",
                                      "report": flat_report}))

# COMMAND ----------

# ── Step 2: Delta snapshot of the flattened source ─────────────────────────
_snap_cols = list(cp.OWNED_COLS)
SNAP_SCHEMA = StructType(
    [StructField(c.lower().replace(" ", "_"), StringType()) for c in _snap_cols]
    + [StructField("extracted_at", TimestampType())])
(spark.createDataFrame([tuple(t.get(c) for c in _snap_cols) + (now,) for t in targets], SNAP_SCHEMA)
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(snapshot_table))
print(f"Step 2: wrote {len(targets)} row(s) to {snapshot_table}")

# COMMAND ----------

# ── Step 3: resolve the DTC request, read it live, plan ────────────────────
api_key = dbutils.secrets.get(scope="beproduct", key=f"dtc_api_key_{environment}")
dtc = DTCConnector(api_key=api_key, environment=environment, workspace_name=workspace)

_matches = [r for r in dtc.search_requests(workspace, document_name=document,
                                           filters={"requestIsActive": "Y"})
            if (r.get("requestReference") or "").strip() == request_name]
if len(_matches) != 1:
    raise RuntimeError(f"DTC {document!r}: expected exactly 1 active request named "
                       f"{request_name!r}, got {len(_matches)} -- refusing to guess")
request_id = _matches[0].get("requestId") or _matches[0].get("_id")
sheet_id = dtc.get_request(request_id)["sheetId"]
_views = [v for v in dtc.get_views(request_id) if v.get("viewName") == view_name]
if len(_views) != 1:
    raise RuntimeError(f"request {request_id}: view {view_name!r} not found")
view_id = _views[0]["viewId"]

# Every owned column must exist in the view, or a write would be silently dropped.
_missing = sorted(set(cp.OWNED_COLS) - set(dtc.get_view_column_names(sheet_id, view_id)))
if _missing:
    raise RuntimeError(f"view {view_name!r} lacks column(s) {_missing}")

live_rows = dtc.get_sheet(sheet_id, view_id).get("sheetData", [])
plan = cp.plan_sync(targets, live_rows)
print(f"Step 3: request {request_id} sheet {sheet_id}: {len(live_rows)} live row(s)")
print(f"        plan {plan.summary()}")

# COMMAND ----------

# ── Step 4: the write window -- PATCH, then POST, back to back ─────────────
SYNC_LOG_SCHEMA = StructType([
    StructField("log_time", TimestampType()), StructField("run_id", StringType()),
    StructField("stage", StringType()), StructField("environment", StringType()),
    StructField("dtc_request_name", StringType()), StructField("request_id", StringType()),
    StructField("operation", StringType()), StructField("lf_style_number", StringType()),
    StructField("color", StringType()), StructField("match_key", StringType()),
    StructField("status", StringType()), StructField("reason", StringType()),
    StructField("detail", StringType()), StructField("payload", StringType()),
])
log_rows = []


def log(operation, row, status, reason="", detail="", payload=None):
    k = cp.row_key(row) if row else (None, None, None)
    log_rows.append((now, run_id, "color_palette", environment, request_name, request_id,
                     operation, k[0], k[1], " | ".join(x or "" for x in k), status, reason,
                     detail, json.dumps(payload) if payload is not None else None))


live_by_id = {r.get("rowId"): r for r in live_rows}
calls, failed, inserted_ids = 0, 0, []
status_word = "dry_run" if dry_run else "ok"

for i in range(0, len(plan.updates), batch_size):
    chunk = plan.updates[i:i + batch_size]
    if not dry_run:
        try:
            dtc.patch_rows(sheet_id, view_id, chunk)
            calls += 1
        except Exception as e:  # noqa: BLE001
            failed += len(chunk)
            for u in chunk:
                log("UPDATE", live_by_id.get(u["rowId"]), "error", "patch_failed", str(e)[:300], u)
            continue
    for u in chunk:
        unticked = set(u) == {"rowId", cp.COL_ACTIVE} and u[cp.COL_ACTIVE] == cp.ACTIVE_NO
        log("UPDATE", live_by_id.get(u["rowId"]), status_word,
            "untick_removed" if unticked else "changed", "", u)

for i in range(0, len(plan.inserts), batch_size):
    chunk = plan.inserts[i:i + batch_size]
    if not dry_run:
        try:
            assigned = dtc.append_rows(sheet_id, view_id, chunk)
            calls += 1
            inserted_ids += [a.get("rowId") for a in assigned]
        except Exception as e:  # noqa: BLE001
            failed += len(chunk)
            for r in chunk:
                log("INSERT", r, "error", "append_failed", str(e)[:300], r)
            continue
    for r in chunk:
        log("INSERT", r, status_word, "new", "", r)

for u in plan.unkeyed_dtc_rows:
    log("REPORT", None, "warn", "unkeyed_dtc_row", json.dumps(u))
for k in plan.duplicate_dtc_keys + flat_report["duplicate_keys"]:
    log("REPORT", None, "warn", "duplicate_key", json.dumps(k))
for s in flat_report["skipped_no_color_number"]:
    log("REPORT", None, "warn", "no_color_number", json.dumps(s))

print(f"Step 4: {'DRY RUN, nothing written' if dry_run else f'{calls} call(s)'}; failed rows: {failed}")

# COMMAND ----------

# ── Step 5: verify -- re-read and re-plan; a landed write leaves nothing pending
residual = None
if not dry_run and plan.has_writes:
    residual = cp.plan_sync(targets, dtc.get_sheet(sheet_id, view_id).get("sheetData", [])).summary()
    print(f"Step 5: residual plan after write {residual}")

if log_rows:
    (spark.createDataFrame(log_rows, SYNC_LOG_SCHEMA)
     .write.format("delta").mode("append").option("mergeSchema", "true")
     .saveAsTable(sync_log_full))

summary = {
    "status": "OK",
    "dry_run": dry_run,
    "source": {"folder": folder_name, **{k: (v if isinstance(v, int) else len(v))
                                         for k, v in flat_report.items()},
               "target_rows": len(targets)},
    "target": {"document": document, "request": request_name, "request_id": request_id,
               "view": view_name, "live_rows": len(live_rows)},
    "plan": plan.summary(),
    "write_calls": calls,
    "failed_rows": failed,
    "inserted_row_ids": inserted_ids[:50],
    "residual_after_write": residual,
    "sample_updates": plan.updates[:10],
    "sample_inserts": plan.inserts[:5],
    "unticked": plan.unticked[:20],
}
if residual and (residual["inserts"] or residual["updates"]):
    summary["status"] = "VERIFY_MISMATCH"
if failed:
    summary["status"] = "COMPLETED_WITH_ERRORS"
print(json.dumps(summary, indent=2)[:4000])
dbutils.notebook.exit(json.dumps(summary))
