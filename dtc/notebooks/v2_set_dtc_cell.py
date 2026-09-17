# Databricks notebook source
"""
UTILITY -- set ONE DTC WIP cell, targeted explicitly. Not part of any job.

Exists so an end-to-end path can be exercised with a real value without waiting
for a user to type one. Deliberately narrow: it targets exactly one row, matched
on (BP Style#, Color / Wash, Mill Fabric Article #), and writes exactly one
column.

Safety
------
* `dry_run=true` by default -- reports the target and the current value, writes
  nothing.
* Refuses unless the match is UNIQUE. Never guesses between candidate rows.
* Resolves the request by `request_is_active='Y' AND in_scope`, and refuses on
  duplicate names -- DTC permits two active requests to share a name, and a
  bare name lookup silently picked the wrong sheet once already (2026-09-15).
* Reads the value back after writing: a 204 from DTC means accepted, not
  stored.
* Opens ONE DTC write window on the target request, so do not run it casually
  while users are editing.
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
from sync import bom, phase1

dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("dtc_workspace", "KTB", "DTC workspace")
dbutils.widgets.text("dtc_environment", "uat", "DTC environment")
dbutils.widgets.text("request_reference", "", "Request (blank = the only in-scope one)")
dbutils.widgets.text("bp_style_number", "", "Target BP Style#")
dbutils.widgets.text("color_wash", "", "Target Color / Wash")
dbutils.widgets.text("mill_fabric_article", "", "Target Mill Fabric Article # (blank = any)")
dbutils.widgets.text("column", "", "DTC column to set")
dbutils.widgets.text("value", "", "Value to write (blank string = clear)")
dbutils.widgets.text("dry_run", "true", "true = report only")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
workspace = dbutils.widgets.get("dtc_workspace")
environment = dbutils.widgets.get("dtc_environment")
ref = (dbutils.widgets.get("request_reference") or "").strip()
style = (dbutils.widgets.get("bp_style_number") or "").strip()
colour = (dbutils.widgets.get("color_wash") or "").strip()
article = (dbutils.widgets.get("mill_fabric_article") or "").strip()
column = (dbutils.widgets.get("column") or "").strip()
value = dbutils.widgets.get("value")
dry_run = (dbutils.widgets.get("dry_run") or "true").strip().lower() == "true"

out = {"style": style, "color": colour, "article": article,
       "column": column, "value": value, "dry_run": dry_run}
registry = f"{catalog}.{schema}.dtc_request_registry"

print(f"target: {style} / {colour} / article={article or '(any)'}  column={column!r}")

reg = (spark.table(registry)
       .where((F.col("environment") == environment)
              & (F.col("request_is_active") == "Y") & (F.col("in_scope"))))
if ref:
    reg = reg.where(F.col("request_reference") == ref)
regs = reg.collect()
if len(regs) != 1:
    out["status"] = "REQUEST_NOT_UNIQUE"
    out["candidates"] = [r["request_reference"] for r in regs]
    print(f"❌ expected exactly 1 active in-scope request, found {len(regs)}")
    dbutils.notebook.exit(json.dumps(out))

r = regs[0]
out["request_reference"] = r["request_reference"]
print(f"request: {r['request_reference']}  ({r['request_id']})")

dtc = DTCConnector(
    api_key=dbutils.secrets.get(scope="beproduct", key=f"dtc_api_key_{environment}"),
    environment=environment, workspace_name=workspace)
rows = dtc.get_sheet(r["sheet_id"], r["view_id"]).get("sheetData", [])

matches = [
    x for x in rows
    if phase1.norm(x.get(phase1.MATCH_KEY_COLS[0])) == (style or None)
    and phase1.norm(x.get(phase1.MATCH_KEY_COLS[1])) == (colour or None)
    and (not article
         or phase1.norm(x.get(bom.WIP_FIELD_MILL_FABRIC_ARTICLE)) == article)
    and x.get("rowId")
]
out["matches"] = len(matches)
if len(matches) != 1:
    out["status"] = "TARGET_NOT_UNIQUE"
    print(f"❌ expected exactly 1 matching row, found {len(matches)} -- refusing to guess")
    dtc.close()
    dbutils.notebook.exit(json.dumps(out))

row = matches[0]
out["row_id"] = row["rowId"]
out["value_before"] = row.get(column)
print(f"row {row['rowId']}  {column!r} currently {row.get(column)!r}")

if dry_run:
    out["status"] = "DRY_RUN"
    print("dry_run=true -- nothing written.")
    dtc.close()
    dbutils.notebook.exit(json.dumps(out))

dtc.patch_rows(r["sheet_id"], r["view_id"], [{column: value, "rowId": row["rowId"]}])
after = next((x for x in dtc.get_sheet(r["sheet_id"], r["view_id"]).get("sheetData", [])
              if x.get("rowId") == row["rowId"]), {})
out["value_after"] = after.get(column)
out["persisted"] = (after.get(column) or "") == (value or "")
out["status"] = "OK" if out["persisted"] else "ACCEPTED_BUT_NOT_STORED"
print(f"after: {after.get(column)!r}   persisted={out['persisted']}")
dtc.close()
print(json.dumps(out, indent=2))
dbutils.notebook.exit(json.dumps(out))
