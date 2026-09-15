# Databricks notebook source
"""
DIAGNOSTIC PROBE -- does a 204-acknowledged DTC sheetData write actually persist?

Not part of any job. Run by hand via scripts/run_v2_task.py when a write is
acknowledged but the value does not appear.

Why this exists
---------------
Live-confirmed 2026-09-15: v1 pushed 13 x {"Sub Class": "..."} against
`KTB SS28 Collaborations`, DTC returned 204 for every one, the sync log
recorded `status=ok` -- and the cells are STILL blank. The column is genuinely
present in the live view (v2_wip_push's `columns_not_seen_in_view` detector
came back empty), so this is not the "FALLBACK_COLS forced a non-existent
column into the payload" case. The mechanism is unexplained, and the working
rule until it is explained is: **a 204 from DTC means "accepted", not
"stored".**

This probe settles it by writing, re-reading, and restoring.

Safety
------
* Targets ONE row of ONE request. Default is the sacrificial request
  `KTB FW26 Wrangler` (UAT `6a26581854e92e7acd8fa71b`), which exists for
  exactly this purpose.
* `dry_run=true` by default -- it will find and report a candidate without
  writing anything.
* ALWAYS restores the original value when `restore=true` (the default), even
  if the verification read shows the write did not take.
* Prefers writing the row's TRUE staging value (so a persisted write is a
  correction, not damage). Falls back to a marked probe string only if no true
  value is available, and in that case restore is forced on.
* Opens at most 2 write windows on that one request (write + restore).
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
from datetime import datetime, timezone

from pyspark.sql import functions as F

from connectors.dtc import DTCConnector
from sync import phase1

# COMMAND ----------

dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("dtc_workspace", "KTB", "DTC workspace")
dbutils.widgets.text("dtc_environment", "uat", "DTC environment")
dbutils.widgets.text("request_reference", "KTB FW26 Wrangler", "Request to probe (sacrificial)")
dbutils.widgets.text("probe_column", "Sub Class", "DTC column to probe")
dbutils.widgets.text("control_column", "Style Description", "A column known to work, same PATCH")
dbutils.widgets.text("dry_run", "true", "true = find + report a candidate, write nothing")
dbutils.widgets.text("restore", "true", "Restore the original value afterwards")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
workspace = dbutils.widgets.get("dtc_workspace")
environment = dbutils.widgets.get("dtc_environment")
request_reference = dbutils.widgets.get("request_reference")
probe_col = dbutils.widgets.get("probe_column")
control_col = dbutils.widgets.get("control_column")
dry_run = (dbutils.widgets.get("dry_run") or "true").strip().lower() == "true"
restore = (dbutils.widgets.get("restore") or "true").strip().lower() == "true"

registry_full = f"{catalog}.{schema}.dtc_request_registry"
staging_full = f"{catalog}.{schema}.beproduct_to_dtc_staging"
STYLE_COL, COLOR_COL = phase1.MATCH_KEY_COLS

out = {"request_reference": request_reference, "probe_column": probe_col,
       "control_column": control_col, "dry_run": dry_run}

print("=" * 78)
print("DTC WRITE-PERSISTENCE PROBE")
print("=" * 78)
print(f"  request : {request_reference}")
print(f"  probe   : {probe_col!r}   control: {control_col!r}")
print(f"  dry_run : {dry_run}   restore: {restore}")

# COMMAND ----------

# ── Resolve the request ────────────────────────────────────────────────────
# Must filter on ACTIVE + IN_SCOPE and refuse on ambiguity. DTC permits two
# concurrently-active requests with the SAME name, distinguished only by
# requestId, and the registry also retains inactive/(BACKUP) rows -- a bare
# name match can silently resolve to the wrong sheet. (Hit while writing this
# probe: 'KTB SS28 Collaborations' matched two registry rows and the first one
# was a 19-row inactive sibling, not the live 60-row request.)
reg = (spark.table(registry_full)
       .where((F.col("environment") == environment)
              & (F.col("request_reference") == request_reference)
              & (F.col("request_is_active") == "Y")
              & (F.col("in_scope")))
       .collect())
if not reg:
    out["status"] = "REQUEST_NOT_FOUND_ACTIVE_IN_SCOPE"
    print(f"❌ no ACTIVE + IN-SCOPE request named {request_reference!r} (env={environment})")
    dbutils.notebook.exit(json.dumps(out))
if len(reg) > 1:
    out["status"] = "DUPLICATE_ACTIVE_NAME"
    out["candidates"] = [x["request_id"] for x in reg]
    print(f"❌ {len(reg)} active in-scope requests share that name: {out['candidates']}")
    print("   Refusing to guess -- pass request_id explicitly.")
    dbutils.notebook.exit(json.dumps(out))

r = reg[0]
sheet_id, view_id, request_id = r["sheet_id"], r["view_id"], r["request_id"]
out.update({"request_id": request_id, "sheet_id": sheet_id, "view_id": view_id})
print(f"  request_id={request_id}  sheet_id={sheet_id}  view_id={view_id}")

api_key = dbutils.secrets.get(scope="beproduct", key=f"dtc_api_key_{environment}")
dtc = DTCConnector(api_key=api_key, environment=environment, workspace_name=workspace)

rows = dtc.get_sheet(sheet_id, view_id).get("sheetData", [])
view_cols = set(dtc.get_view_column_names(sheet_id, view_id))
out["live_rows"] = len(rows)
out["probe_column_in_view_scan"] = probe_col in view_cols
print(f"  live rows: {len(rows)}")
print(f"  {probe_col!r} visible in view scan: {probe_col in view_cols}")

# COMMAND ----------

# ── Pick a candidate row ───────────────────────────────────────────────────
# Prefer a row whose TRUE staging value differs from what DTC holds, so a
# persisted write is a correction rather than damage.
staging = {}
for s in (spark.table(staging_full)
          .where(F.col("dtc_request_name") == request_reference).collect()):
    d = s.asDict()
    staging[(phase1.norm(d.get("bp_style_number")), phase1.norm(d.get("color")))] = d

candidate = None
for row in rows:
    key = (phase1.norm(row.get(STYLE_COL)), phase1.norm(row.get(COLOR_COL)))
    if key == (None, None) or not row.get("rowId"):
        continue
    st = staging.get(key)
    true_val = phase1.norm((st or {}).get("product_sub_category")) if st else None
    current = phase1.norm(row.get(probe_col))
    if true_val is not None and true_val != current:
        candidate = {"row": row, "key": key, "new": true_val,
                     "source": "true_staging_value"}
        break

if candidate is None:
    # Nothing to correct -- fall back to a clearly-marked probe string on the
    # first usable row. Restore is then mandatory.
    stamp = datetime.now(timezone.utc).strftime("%H%M%S")
    for row in rows:
        key = (phase1.norm(row.get(STYLE_COL)), phase1.norm(row.get(COLOR_COL)))
        if key != (None, None) and row.get("rowId"):
            candidate = {"row": row, "key": key, "new": f"PROBE-{stamp}",
                         "source": "probe_marker"}
            break
    restore = True

if candidate is None:
    out["status"] = "NO_CANDIDATE_ROW"
    print("❌ no usable row found")
    dtc.close()
    dbutils.notebook.exit(json.dumps(out))

row = candidate["row"]
row_id = row["rowId"]
original = row.get(probe_col)
control_original = row.get(control_col)
new_value = candidate["new"]

out.update({
    "match_key": list(candidate["key"]),
    "row_id": row_id,
    "value_source": candidate["source"],
    "original_value": original,
    "new_value": new_value,
    "control_original": control_original,
})
print(f"\n  candidate row : {candidate['key']}  rowId={row_id}")
print(f"  {probe_col!r}: current={original!r} -> writing {new_value!r} "
      f"({candidate['source']})")

if dry_run:
    out["status"] = "DRY_RUN_CANDIDATE_FOUND"
    print("\ndry_run=true -- nothing written. Re-run with dry_run=false to probe.")
    dtc.close()
    dbutils.notebook.exit(json.dumps(out, default=str))

# COMMAND ----------

# ── Write, re-read, compare ────────────────────────────────────────────────
# The control column is written back to its OWN CURRENT VALUE in the same
# PATCH. It cannot change anything, but if DTC echoes it back on re-read while
# dropping the probe column, that isolates the behaviour to the probe column
# rather than to the call as a whole.
payload = {probe_col: new_value, "rowId": row_id}
if control_original is not None:
    payload[control_col] = control_original

print(f"\n  PATCH {json.dumps({k: v for k, v in payload.items() if k != 'rowId'})}")
try:
    resp = dtc.patch_rows(sheet_id, view_id, [payload])
    out["patch_ok"] = True
    out["patch_response"] = str(resp)[:300]
    print(f"  -> accepted (no exception raised): {str(resp)[:200]}")
except Exception as e:  # noqa: BLE001
    out["patch_ok"] = False
    out["patch_error"] = str(e)[:500]
    print(f"  ❌ PATCH raised: {e}")

# Fresh read -- a new get_sheet, not a cached object.
after_rows = dtc.get_sheet(sheet_id, view_id).get("sheetData", [])
after = next((x for x in after_rows if x.get("rowId") == row_id), {})
after_val = after.get(probe_col)
out["value_after_write"] = after_val
persisted = phase1.norm(after_val) == phase1.norm(new_value)
out["persisted"] = persisted

print(f"\n  re-read {probe_col!r}: {after_val!r}")
print(f"  PERSISTED: {persisted}")
if not persisted:
    print("  ⚠ DTC acknowledged the write and did NOT store it.")
    print("    A 204 from this endpoint means 'accepted', not 'stored'.")

# COMMAND ----------

# ── Restore ────────────────────────────────────────────────────────────────
if restore and persisted and phase1.norm(original) != phase1.norm(new_value):
    print(f"\n  restoring {probe_col!r} -> {original!r}")
    try:
        dtc.patch_rows(sheet_id, view_id,
                       [{probe_col: original if original is not None else "",
                         "rowId": row_id}])
        back = next((x for x in dtc.get_sheet(sheet_id, view_id).get("sheetData", [])
                     if x.get("rowId") == row_id), {})
        out["restored_value"] = back.get(probe_col)
        out["restored"] = phase1.norm(back.get(probe_col)) == phase1.norm(original)
        print(f"  restored: {out['restored']}  (now {back.get(probe_col)!r})")
    except Exception as e:  # noqa: BLE001
        out["restored"] = False
        out["restore_error"] = str(e)[:500]
        print(f"  ❌ restore failed: {e}")
elif not persisted:
    out["restored"] = True   # nothing changed, so nothing to restore
    print("\n  nothing to restore -- the write never landed.")
elif candidate["source"] == "true_staging_value":
    out["restored"] = "not_needed_value_was_correct"
    print("\n  left in place: the written value is the correct BeProduct value.")

dtc.close()

out["status"] = "PERSISTED" if persisted else "SILENTLY_DROPPED"
print("\n" + json.dumps(out, indent=2, default=str))
dbutils.notebook.exit(json.dumps(out, default=str))
