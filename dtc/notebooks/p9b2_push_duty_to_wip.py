# Databricks notebook source
"""
Phase 9b (part 2 of 2) — Push filled HTS/Duty/Tariff values from
`costing_chart` to the live DTC WIP sheet
============================================================================

Split from the original single `p9b_fill_duty_rates.py` notebook (2026-09-03,
job restructuring — see AGENTS.md decisions log). This half:

  - Reads `lft.beproduct.costing_chart` (already filled by
    `p9b1_compute_duty_rates.py`, which may run as a SEPARATE, independently
    scheduled job — this notebook has NO dependency on that notebook having
    just run in the same job; it simply reads costing_chart's CURRENT state).
  - For every row with a filled `hts_code`/`duty_rate_us`/`duty_rate_ca`/
    `duty_rate_mx`/`tariff_rate`, computes the target DTC WIP column values
    (`sync.duty.build_wip_patch_fields`) and PATCHes only the ones that
    actually DIFFER from the WIP row's current value — costing_chart is
    fully rebuilt by every Phase 9a run but usually holds the SAME filled
    values run after run (write-once upstream), so a real diff check avoids
    firing a redundant PATCH call for every row on every run.
  - Runs in the MAIN job (right after Phase 9a's `build_costing_chart`) since
    it's a fast, scoped PATCH — unlike part 1's NT Orbit call chain
    (~30s/call), this is a low DTC-contention window.

"Tariff Rate" columns do not exist in the WIP view yet (confirmed
2026-07-17); those are skipped and logged, the value stays in
`costing_chart` only (same behavior as the original single notebook).

Costing chart table name is a PARAMETER (widget `costing_chart_table`):
  default:  lft.beproduct.costing_chart
  testing:  lft.beproduct.costing_chart_kei
"""

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

import json

from connectors.dtc import DTCConnector
from sync import duty
from sync.phase1 import chunked
from pyspark.sql import functions as F

# ── Parameters ────────────────────────────────────────────────────────────────
dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("customer", "KTB", "Customer code")
dbutils.widgets.text("costing_chart_table", "lft.beproduct.costing_chart",
                     "Costing Chart table (fully-qualified; test override e.g. "
                     "lft.beproduct.costing_chart_kei)")
dbutils.widgets.text("dtc_environment", "uat", "DTC Environment")
dbutils.widgets.text("dtc_workspace", "KTB", "DTC Workspace")
dbutils.widgets.text("dry_run", "true", "Dry run (true/false) — skip writes")
dbutils.widgets.text("batch_size", "100", "Rows per WIP PATCH call")

catalog       = dbutils.widgets.get("catalog")
schema        = dbutils.widgets.get("schema")
customer      = dbutils.widgets.get("customer").strip().upper()
costing_table = dbutils.widgets.get("costing_chart_table").strip()
environment   = dbutils.widgets.get("dtc_environment").strip().lower()
workspace     = dbutils.widgets.get("dtc_workspace").strip()
dry_run       = dbutils.widgets.get("dry_run").strip().lower() == "true"
batch_size    = int(dbutils.widgets.get("batch_size") or 100)

wip_table      = f"{catalog}.{schema}.dtc_wip_{customer.lower()}"
registry_table = f"{catalog}.{schema}.dtc_request_registry"

print("=" * 72)
print("PHASE 9b part 2/2 — Push filled HTS/Duty/Tariff -> live DTC WIP")
print("=" * 72)
print(f"  Costing chart : {costing_table}")
print(f"  WIP table     : {wip_table}")
print(f"  dry_run={dry_run}")

# COMMAND ----------

# ── Step 1: Load costing_chart rows with at least one filled duty field ──────
DUTY_FIELDS = ("hts_code", "duty_rate_us", "duty_rate_ca", "duty_rate_mx", "tariff_rate")
print(f"\nStep 1: Reading {costing_table} …")
chart_rows = [r.asDict() for r in spark.table(costing_table).collect()]
filled_rows = [r for r in chart_rows if any(r.get(c) is not None for c in DUTY_FIELDS)]
print(f"  Total costing_chart rows      : {len(chart_rows)}")
print(f"  Rows with a filled duty field : {len(filled_rows)}")

# COMMAND ----------

# ── Step 2: Load current WIP rows (row_id/sheet_id/view_id + current data) ──
# Indexed by (bp_style_number, color_wash, Mill Fabric Article # / material_no)
# -- NOT (bp_style_number, color_wash) alone (fixed 2026-09-03; REVISED same
# day from an initial "Content" attempt -- owner correction: multiple
# material_no can share the same Content, and multiple styles can share one
# Lineplan Ref#, so material_no is the real per-material discriminator, not
# Content/lineplan_ref). Phase 10 can create MULTIPLE physical WIP rows for
# the same style x color (one "Main Fabric" row + one duplicate per "Fabric"
# segment -- see sync/bom.py); each is an INDEPENDENT row with its OWN "Main
# Factory HTS Code"/"Duty Rate"/etc. cells for that specific material.
# Indexing by (style, color) alone would silently keep only the LAST such row
# (dict overwrite) and could push a material's duty data onto the WRONG
# physical WIP row. "Mill Fabric Article #" is the same per-material
# disambiguator now used in costing_chart's own MERGE key (COSTING_KEY
# includes material_no for the identical reason) -- see AGENTS.md decisions
# log.
print(f"\nStep 2: Loading current WIP state from {wip_table} …")
reg = {r["request_id"]: r.asDict()
       for r in spark.table(registry_table).where(F.col("environment") == environment).collect()}
wip_index: dict = {}   # (bp_style_number, color_wash, material_no) -> {row_id, sheet_id, view_id, data_json}
for r in spark.table(wip_table).collect():
    wr = r.asDict()
    row_fields = json.loads(wr["data_json"]) if wr.get("data_json") else {}
    key = (wr.get("bp_style_number"), wr.get("color_wash"), row_fields.get("Mill Fabric Article #"))
    reg_entry = reg.get(wr.get("request_id"), {})
    wip_index[key] = {
        "row_id": wr.get("row_id"),
        "sheet_id": reg_entry.get("sheet_id"),
        "view_id": reg_entry.get("view_id"),
        "data_json": wr.get("data_json"),
    }
print(f"  WIP rows indexed: {len(wip_index)}")

# COMMAND ----------

# ── Step 3: Compute target fields per row, diff against CURRENT WIP value ────
# Multiple costing_chart rows (one per vendor/factory slot: Main/1/2/3) map to
# the SAME underlying WIP row -- they only differ in which columns they
# target (e.g. "Main Factory HTS Code" vs "Factory 1 - HTS code"), never in
# rowId. Sending them as separate sheetData objects with the same rowId in
# one PATCH call is rejected by DTC with 400 "Duplicate rowId found."
# (confirmed live 2026-09-01) -- merge is safe since each slot's fields
# target disjoint column names.
print("\nStep 3: Computing target fields and diffing against current WIP values …")

merged_by_rowid: dict = {}   # (sheet_id, view_id, row_id) -> merged CHANGED fields dict
push_skipped_reasons: set = set()
push_no_match = 0
push_already_correct = 0

for row in filled_rows:
    wip_key = (row.get("bp_style_no"), row.get("color_name"), row.get("material_no"))
    target = wip_index.get(wip_key)
    if not target or not target.get("row_id"):
        push_no_match += 1
        continue

    filled_fields = {c: row.get(c) for c in DUTY_FIELDS if row.get(c) is not None}
    plan = duty.build_wip_patch_fields(row.get("supplier_type"), filled_fields)
    for reason in plan.skipped:
        push_skipped_reasons.add(reason)
    if not plan.fields:
        continue

    current = json.loads(target["data_json"]) if target.get("data_json") else {}
    changed_fields = {
        col: val for col, val in plan.fields.items()
        if current.get(col) != val
    }
    if not changed_fields:
        push_already_correct += 1
        continue

    merge_key = (target["sheet_id"], target["view_id"], target["row_id"])
    merged_by_rowid.setdefault(merge_key, {}).update(changed_fields)

print(f"  Rows already matching WIP (no-op)     : {push_already_correct}")
print(f"  Rows with no WIP match                : {push_no_match}")
print(f"  WIP rows with a genuine field to push : {len(merged_by_rowid)}")

# COMMAND ----------

# ── Step 4: Push (UPDATE only -- all WIP rows here already exist) ───────────
print(f"\nStep 4: Pushing to DTC (env={environment}) …")

by_sheet: dict = {}
for (sheet_id, view_id, row_id), fields in merged_by_rowid.items():
    sheet_key = (sheet_id, view_id)
    by_sheet.setdefault(sheet_key, []).append({**fields, "rowId": row_id})

pushed, push_errors = 0, 0
if by_sheet:
    secret_key = f"dtc_api_key_{environment}"
    dtc_api_key = dbutils.secrets.get(scope="beproduct", key=secret_key)
    dtc = DTCConnector(api_key=dtc_api_key, environment=environment, workspace_name=workspace)

    for (sheet_id, view_id), sheet_data in by_sheet.items():
        if not sheet_id or not view_id:
            push_errors += len(sheet_data)
            continue
        for chunk in chunked(sheet_data, batch_size):
            try:
                if not dry_run:
                    dtc.patch_rows(sheet_id, view_id, chunk)
                pushed += len(chunk)
            except Exception as e:
                print(f"  ❌ PATCH sheet={sheet_id} view={view_id} failed: {e}")
                push_errors += len(chunk)
    dtc.close()
else:
    print("  Nothing to push -- every filled row already matches its WIP cell.")

print(f"  Pushed rows: {pushed}  (errors: {push_errors})")
for reason in push_skipped_reasons:
    print(f"  ⚠️  {reason}")

# COMMAND ----------

print(f"\n{'='*72}")
print("SUMMARY")
print(f"{'='*72}")
print(f"  Costing chart rows with a filled duty field : {len(filled_rows)}")
print(f"  WIP rows genuinely updated                  : {pushed}")
print(f"  dry_run={dry_run}")
print("✅ Phase 9b part 2/2 complete")
