# Databricks notebook source
"""
Phase 9a — Pull DTC LinePlan sheets → Delta
============================================

Pulls every active request from the DTC "<customer> LinePlan" document
(e.g. "KTB LinePlan") and writes all rows to one Delta table per customer:

    lft.beproduct.dtc_lineplan_<customer>    e.g.  dtc_lineplan_ktb

It also maintains a lightweight registry:

    lft.beproduct.dtc_lineplan_registry

Request naming for "KTB LinePlan" follows the pattern used by the
customer's planning team (not the same as WIP).  All active in-scope
requests are pulled regardless of naming.

View selection (preferred → fallback):
  1. "LINEPLAN_ITS_USE"   — canonical sync view (if added by DTC admin)
  2. "Full"               — current UAT view (30 fields; confirmed 2026-07-17)
  If neither exists for a request, it is skipped and logged.

Key staging columns (LinePlan "Full" view, 30 fields, confirmed 2026-07-17):
  lineplan_ref       ← "Lineplan Ref #"               (join key to WIP)
  projected_volume   ← "PROJECTED VOLUME (season)"     (→ Costing Order Qty)
  target_ldp         ← "TARGET SAP w/ Tariff impact"   (→ Costing Target LDP)
  target_fob         ← "TARGET FOB"                    (→ Costing Target FOB)
  gender             ← "Gender"
  category           ← "Category"
  product_line       ← "Product Line"
  region             ← "REGION"
  internal_sourced   ← "INTERNAL/ SOURCED"             (→ Costing Supplier Type)
  season_launched    ← "SEASON LAUNCHED"

NOTE: Lineplan field names are uppercase in the "Full" view.
"""

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from connectors.dtc import DTCConnector
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType,
)

# ── Schema ────────────────────────────────────────────────────────────────────
FIXED_FIELDS = [
    StructField("customer",           StringType()),
    StructField("workspace_name",     StringType()),
    StructField("document_name",      StringType()),
    StructField("request_id",         StringType()),
    StructField("request_reference",  StringType()),
    StructField("view_name",          StringType()),   # which view was used
    StructField("row_id",             StringType()),
    StructField("row_index",          LongType()),
    # Key fields for Phase 9a Costing Chart join
    StructField("lineplan_ref",       StringType()),   # "Lineplan Ref #"
    StructField("projected_volume",   StringType()),   # "PROJECTED VOLUME (season)"
    StructField("target_ldp",         StringType()),   # "TARGET SAP w/ Tariff impact"
    StructField("target_fob",         StringType()),   # "TARGET FOB"
    # Supplementary fields
    StructField("gender",             StringType()),   # "Gender"
    StructField("category",           StringType()),   # "Category"
    StructField("product_line",       StringType()),   # "Product Line"
    StructField("region",             StringType()),   # "REGION"
    StructField("internal_sourced",   StringType()),   # "INTERNAL/ SOURCED"
    StructField("season_launched",    StringType()),   # "SEASON LAUNCHED"
    # Full row payload for forward-compatibility
    StructField("extracted_at",       TimestampType()),
    StructField("data_json",          StringType()),
]

LINEPLAN_SCHEMA = StructType(FIXED_FIELDS)

# Preferred view name, then fallback. Used for both pull and registry.
PREFERRED_VIEW = "LINEPLAN_ITS_USE"
FALLBACK_VIEW  = "Full"

# ── Registry schema (mirrors dtc_request_registry / dtc_fabric_registry) ─────
REGISTRY_FIELDS = StructType([
    StructField("environment",        StringType()),
    StructField("customer",           StringType()),
    StructField("workspace",          StringType()),
    StructField("document",           StringType()),
    StructField("request_id",         StringType()),
    StructField("request_reference",  StringType()),
    StructField("sheet_id",           StringType()),
    StructField("view_id",            StringType()),
    StructField("view_name",          StringType()),
    StructField("row_count",          LongType()),
    StructField("last_extracted",     TimestampType()),
    StructField("msgs",               StringType()),
    StructField("updated_at",         TimestampType()),
])

# COMMAND ----------

dbutils.widgets.text("dtc_environment",  "uat",          "DTC Environment")
dbutils.widgets.text("customer",         "KTB",          "Customer code")
dbutils.widgets.text("dtc_workspace",    "KTB",          "DTC Workspace")
dbutils.widgets.text("dtc_document",     "KTB LinePlan", "DTC Document name")
dbutils.widgets.text("catalog",          "lft",          "Catalog")
dbutils.widgets.text("schema",           "beproduct",    "Schema")
dbutils.widgets.text("write_mode",       "overwrite",    "overwrite | append")
dbutils.widgets.text("max_workers",      "4",            "Parallel get_sheet() threads")

environment  = dbutils.widgets.get("dtc_environment").strip().lower()
customer     = dbutils.widgets.get("customer").strip().upper()
workspace    = dbutils.widgets.get("dtc_workspace").strip()
document     = dbutils.widgets.get("dtc_document").strip()
catalog      = dbutils.widgets.get("catalog")
schema       = dbutils.widgets.get("schema")
write_mode   = dbutils.widgets.get("write_mode").strip().lower()
max_workers  = int(dbutils.widgets.get("max_workers") or 4)

lineplan_table_full = f"{catalog}.{schema}.dtc_lineplan_{customer.lower()}"
lineplan_reg_full   = f"{catalog}.{schema}.dtc_lineplan_registry"
now = datetime.now(timezone.utc)

print("=" * 72)
print("PHASE 9a — Pull DTC LinePlan sheets → Delta")
print("=" * 72)
print(f"  Document  : {document}  (workspace={workspace}, env={environment})")
print(f"  Output    : {lineplan_table_full}")
print(f"  Registry  : {lineplan_reg_full}")
print(f"  View pref : {PREFERRED_VIEW!r}  → fallback: {FALLBACK_VIEW!r}")

# COMMAND ----------

# ── Auth ──────────────────────────────────────────────────────────────────────
secret_key = f"dtc_api_key_{environment}"
api_key    = dbutils.secrets.get(scope="beproduct", key=secret_key)
connector  = DTCConnector(api_key=api_key, environment=environment,
                          workspace_name=workspace)
print("✅ DTCConnector ready")

# COMMAND ----------

# ── Discover LinePlan requests + resolve view ─────────────────────────────────
# LinePlan does not use the same view-resolution as WIP (no WIP_ITS_USE).
# We discover directly via search_requests + per-request view listing.
print(f"\nDiscovering active requests in '{document}' …")
raw_reqs = connector.search_requests(workspace, document,
                                     filters={"requestIsActive": "Y"})
print(f"  Found {len(raw_reqs)} active request(s)")

eligible   = []   # list of (request_ref, request_id, sheet_id, view_id, view_name)
skipped    = []

for req in raw_reqs:
    req_id = req.get("requestId") or req.get("id", "")
    ref    = req.get("requestReference", "?")
    try:
        detail = connector.get_request(req_id)
        sheets = detail.get("sheets") or detail.get("data", {}).get("sheets", [])
        if not sheets:
            skipped.append((ref, "no sheets"))
            continue
        sheet   = sheets[0]
        sheet_id = sheet.get("sheetId") or sheet.get("id", "")
        views    = sheet.get("views") or []

        # Pick preferred view, then fallback
        chosen_view = None
        for vname in (PREFERRED_VIEW, FALLBACK_VIEW):
            for v in views:
                if (v.get("viewName") or v.get("name", "")) == vname:
                    chosen_view = v
                    break
            if chosen_view:
                break

        if not chosen_view:
            skipped.append((ref, f"no {PREFERRED_VIEW!r} or {FALLBACK_VIEW!r} view"))
            continue

        view_id   = chosen_view.get("viewId") or chosen_view.get("id", "")
        view_name = chosen_view.get("viewName") or chosen_view.get("name", "")
        eligible.append((ref, req_id, sheet_id, view_id, view_name))
        print(f"  ✅  {ref}  view={view_name!r}")
    except Exception as e:
        skipped.append((ref, str(e)))
        print(f"  ⚠️  {ref}: {e}")

if skipped:
    print(f"\n  Skipped {len(skipped)}:")
    for ref, reason in skipped:
        print(f"    {ref}: {reason}")

# COMMAND ----------

# ── Upsert registry ───────────────────────────────────────────────────────────
ts_iso = now.isoformat()

if not spark.catalog.tableExists(lineplan_reg_full):
    print(f"Creating registry table {lineplan_reg_full} …")
    spark.createDataFrame([], REGISTRY_FIELDS).write \
        .format("delta").mode("overwrite") \
        .saveAsTable(lineplan_reg_full)

reg_rows_new = [(
    environment, customer, workspace, document,
    req_id, ref, sheet_id, view_id, view_name,
    0, None, "discovered", now,
) for ref, req_id, sheet_id, view_id, view_name in eligible]

if reg_rows_new:
    df_new = spark.createDataFrame(reg_rows_new, REGISTRY_FIELDS) \
                  .createOrReplaceTempView("_lp_reg_src")
    spark.sql(f"""
      MERGE INTO {lineplan_reg_full} t
      USING _lp_reg_src s
        ON t.environment = s.environment
       AND t.request_id  = s.request_id
      WHEN MATCHED THEN UPDATE SET
        t.request_reference = s.request_reference,
        t.view_id   = s.view_id, t.view_name = s.view_name,
        t.updated_at = s.updated_at
      WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"✅ Registry upserted for {len(reg_rows_new)} request(s)")

# COMMAND ----------

# ── Parallel sheet fetch ──────────────────────────────────────────────────────
def _build_lp_records(ref, req_id, view_name, rows):
    records = []
    for row in rows:
        records.append({
            "customer":          customer,
            "workspace_name":    workspace,
            "document_name":     document,
            "request_id":        req_id,
            "request_reference": ref,
            "view_name":         view_name,
            "row_id":            row.get("rowId"),
            "row_index":         (int(row["rowIndex"])
                                  if row.get("rowIndex") is not None else None),
            # Key Phase 9a fields (exact DTC column names in "Full" view)
            "lineplan_ref":      row.get("Lineplan Ref #"),
            "projected_volume":  row.get("PROJECTED VOLUME (season)"),
            "target_ldp":        row.get("TARGET SAP w/ Tariff impact"),
            "target_fob":        row.get("TARGET FOB"),
            "gender":            row.get("Gender"),
            "category":          row.get("Category"),
            "product_line":      row.get("Product Line"),
            "region":            row.get("REGION"),
            "internal_sourced":  row.get("INTERNAL/ SOURCED"),
            "season_launched":   row.get("SEASON LAUNCHED"),
            "extracted_at":      now,
            "data_json":         json.dumps(row, default=str),
        })
    return records

print(f"\nFetching {len(eligible)} LinePlan sheet(s) with {max_workers} worker(s) …")
all_records  = []
fetch_errors = {}

def _fetch(item):
    ref, req_id, sheet_id, view_id, view_name = item
    sheet = connector.get_sheet(sheet_id, view_id)
    return item, sheet.get("sheetData", [])

with ThreadPoolExecutor(max_workers=max_workers) as pool:
    futures = {pool.submit(_fetch, item): item for item in eligible}
    for future in as_completed(futures):
        item = futures[future]
        ref, req_id, sheet_id, view_id, view_name = item
        try:
            _, rows = future.result()
            recs = _build_lp_records(ref, req_id, view_name, rows)
            all_records.extend(recs)
            with_ref = sum(1 for r in recs if r.get("lineplan_ref"))
            print(f"  ✅ {ref:<55} rows={len(rows):4d}  with_ref={with_ref}")
        except Exception as e:
            fetch_errors[req_id] = str(e)
            print(f"  ❌ {ref}: {e}")

total_rows = len(all_records)
print(f"\n✅ Fetch complete: {total_rows} rows  ({len(fetch_errors)} errors)")

# COMMAND ----------

# ── Write to Delta ────────────────────────────────────────────────────────────
if total_rows == 0:
    print(f"\n⚠️  No rows fetched — {lineplan_table_full} not modified.")
else:
    df_lp = spark.createDataFrame(all_records, schema=LINEPLAN_SCHEMA)
    if write_mode == "overwrite":
        (df_lp.write.format("delta").mode("overwrite")
               .option("overwriteSchema", "true")
               .saveAsTable(lineplan_table_full))
        print(f"✅ Wrote {total_rows} rows → {lineplan_table_full}  (overwrite)")
    else:
        df_lp.write.format("delta").mode("append").saveAsTable(lineplan_table_full)
        print(f"✅ Appended {total_rows} rows → {lineplan_table_full}")

# COMMAND ----------

# ── Batched registry row_count update (single MERGE) ─────────────────────────
from collections import Counter
row_counts = Counter(r["request_id"] for r in all_records)
ts_iso = now.isoformat()

ctrl_rows = [
    (environment, req_id, row_counts.get(req_id, 0),
     f"pulled rows={row_counts.get(req_id, 0)}")
    for _, req_id, *_ in eligible
]
if ctrl_rows:
    ctrl_schema = StructType([
        StructField("environment", StringType()),
        StructField("request_id",  StringType()),
        StructField("row_count",   LongType()),
        StructField("msg",         StringType()),
    ])
    spark.createDataFrame(ctrl_rows, ctrl_schema) \
         .createOrReplaceTempView("_lp_ctrl_src")
    spark.sql(f"""
      MERGE INTO {lineplan_reg_full} t
      USING _lp_ctrl_src s
        ON t.environment = s.environment AND t.request_id = s.request_id
      WHEN MATCHED THEN UPDATE SET
        t.last_extracted = timestamp('{ts_iso}'),
        t.row_count      = s.row_count,
        t.msgs           = s.msg,
        t.updated_at     = timestamp('{ts_iso}')
    """)
    print(f"✅ Registry updated for {len(ctrl_rows)} request(s)  (single MERGE)")

# COMMAND ----------

# ── Summary ───────────────────────────────────────────────────────────────────
print(f"\n{'='*72}")
print("SUMMARY")
print(f"{'='*72}")
print(f"  Requests found   : {len(raw_reqs)}")
print(f"  Eligible         : {len(eligible)}")
print(f"  Skipped          : {len(skipped)}")
print(f"  Fetch errors     : {len(fetch_errors)}")
print(f"  Total rows       : {total_rows}")
print(f"  Output table     : {lineplan_table_full}")
print(f"  Registry table   : {lineplan_reg_full}")

if total_rows > 0:
    spark.table(lineplan_table_full) \
        .groupBy("request_reference", "view_name") \
        .agg(F.count("*").alias("rows"),
             F.count_if(F.col("lineplan_ref").isNotNull()).alias("rows_with_ref")) \
        .orderBy("request_reference").show(truncate=False)

print("\n✅ Phase 9a LinePlan pull complete")
