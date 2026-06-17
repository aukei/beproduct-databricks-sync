# Databricks notebook source
"""
Pull In-Scope DTC Requests -> Delta (Phase 1, requirement 1)
============================================================

Uploads every in-scope request listed in the control table to ONE Delta table
per (workspace + customer):

    lft.beproduct.dtc_wip_<customer>            e.g. dtc_wip_ktb  (customer lowercased)

For each request it reads ONLY the "WIP_ITS_USE" view (the canonical sync
projection); requests whose registered view is anything else are skipped + logged.

Each row carries the request's [request_reference, season_code, brands] plus the
DTC rowId / rowIndex, so the table is keyed for reconciliation and push:
  (customer, season_code, brands, lf_style_number, color_wash)  + row_id/row_index

It also maintains the control table (requirement 1a): for each request it updates
last_extracted, row_count and msgs. Empty requests (no rows) are allowed
(requirement 1b) and recorded with row_count = 0.

Discovery is registry-driven. Run 00_init_request_registry.py first to populate
lft.beproduct.dtc_request_registry (it resolves each request's WIP_ITS_USE
view_id/view_name).

Parameters:
  - dtc_environment (default: uat)
  - customer (default: KTB)
  - dtc_workspace (default: KTB)
  - dtc_document (default: KTB WIP)  -- used when refreshing the registry
  - catalog / schema (default: lft / beproduct)
  - write_mode: overwrite | append (default: overwrite)
  - refresh_registry (default: true) -- scan workspace+document and upsert the
    registry (via sync.registry.refresh) before pulling; set false to use as-is
"""

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

import json
from datetime import datetime, timezone

from connectors.dtc import DTCConnector
from sync import phase1, registry
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType,
)

# Explicit schema for the fixed (non-dynamic) columns. Building the DataFrame with
# an explicit schema avoids Spark's CANNOT_DETERMINE_TYPE error, which is raised
# when a request has few rows and a column is all-NULL (type cannot be inferred).
FIXED_FIELDS = [
    StructField("customer", StringType()),
    StructField("workspace_name", StringType()),
    StructField("document_name", StringType()),
    StructField("request_id", StringType()),
    StructField("request_reference", StringType()),
    StructField("season_code", StringType()),
    StructField("brands", StringType()),
    StructField("row_id", StringType()),
    StructField("row_index", LongType()),
    StructField("lf_style_number", StringType()),
    StructField("color_wash", StringType()),
    StructField("extracted_at", TimestampType()),
    StructField("data_json", StringType()),
]

# Phase 1 only ever reads the "WIP_ITS_USE" view of each request (the canonical
# column projection used for sync). The registry stores each request's WIP view
# under view_id/view_name; we assert it here so the source view is explicit.
WIP_VIEW_NAME = "WIP_ITS_USE"

dbutils.widgets.text("dtc_environment", "uat", "DTC Environment")
dbutils.widgets.text("customer", "KTB", "Customer")
dbutils.widgets.text("dtc_workspace", "KTB", "DTC Workspace")
dbutils.widgets.text("dtc_document", "KTB WIP", "DTC Document")
dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("write_mode", "overwrite", "overwrite | append")
dbutils.widgets.text("refresh_registry", "true", "Scan + refresh registry first")

environment = dbutils.widgets.get("dtc_environment").strip().lower()
customer = dbutils.widgets.get("customer").strip()
workspace = dbutils.widgets.get("dtc_workspace").strip()
document = dbutils.widgets.get("dtc_document").strip()
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
write_mode = dbutils.widgets.get("write_mode").strip().lower()
refresh_registry = dbutils.widgets.get("refresh_registry").strip().lower() in ("true", "1", "yes", "y")

registry_full = f"{catalog}.{schema}.dtc_request_registry"
target_full = f"{catalog}.{schema}.dtc_wip_{customer.lower()}"

print("=" * 80)
print("PULL IN-SCOPE DTC REQUESTS -> DELTA (Phase 1)")
print("=" * 80)
print(f"  Registry: {registry_full}")
print(f"  Target:   {target_full}")
print(f"  Source view: {WIP_VIEW_NAME} (per request, from registry)")
print(f"  Env: {environment} | Customer: {customer} | write_mode: {write_mode}")
print(f"  Refresh registry first: {refresh_registry}")

# COMMAND ----------

secret_key = f"dtc_api_key_{environment}"
api_key = dbutils.secrets.get(scope="beproduct", key=secret_key)
connector = DTCConnector(api_key=api_key, environment=environment, workspace_name=workspace)
now = datetime.now(timezone.utc)

# Scan the workspace+document and upsert the registry BEFORE reading it, so newly
# created/added requests are picked up automatically (mode=merge preserves sync
# state). Set refresh_registry=false to pull against the registry as-is.
if refresh_registry:
    print("\n🔄 Refreshing request registry (scan workspace+document)...")
    registry.refresh(
        spark, connector,
        environment=environment, workspace=workspace, document=document,
        customer=customer, registry_table=registry_full,
    )

# COMMAND ----------

reg_rows = spark.table(registry_full).where(
    (F.col("environment") == environment)
    & (F.col("customer") == customer)
    & (F.col("in_scope") == True)        # noqa: E712
    & (F.upper(F.col("request_is_active")).isin("Y", "TRUE", "1"))
).collect()

print(f"In-scope active requests to pull: {len(reg_rows)}")
if not reg_rows:
    connector.close()
    dbutils.notebook.exit("NO_IN_SCOPE_REQUESTS")

# COMMAND ----------

def normalize(col_name):
    return DTCConnector._normalize_column_name(col_name)

all_spark_dfs = []
control_updates = []  # (request_id, row_count, msg)

for r in reg_rows:
    name = r.request_reference

    # Explicitly require the WIP_ITS_USE view. r.view_id is that view's id (the
    # registry resolves view_id/view_name from get_request_scope); skip + log if a
    # request's registered view is anything else so we never pull the wrong view.
    if r.view_name != WIP_VIEW_NAME:
        print(f"  ⏭️  {name}: skipped (view_name={r.view_name!r}, expected {WIP_VIEW_NAME!r})")
        control_updates.append((r.request_id, None,
                                f"skipped: WIP view missing (view_name={r.view_name})"))
        continue

    try:
        # Pull the WIP_ITS_USE view's sheet data.
        sheet = connector.get_sheet(r.sheet_id, r.view_id)
        rows = sheet.get("sheetData", [])
    except Exception as e:
        print(f"  ❌ {name}: read failed: {e}")
        control_updates.append((r.request_id, None, f"extract_error: {str(e)[:160]}"))
        continue

    print(f"  ✅ {name}: {len(rows)} row(s)")

    records = []
    for row in rows:
        rec = {
            "customer": customer,
            "workspace_name": workspace,
            "document_name": r.document_name,
            "request_id": r.request_id,
            "request_reference": name,
            "season_code": r.season_code,
            "brands": r.brands,
            "row_id": row.get("rowId"),
            "row_index": (int(row["rowIndex"]) if row.get("rowIndex") is not None else None),
            "lf_style_number": phase1.norm(row.get("LF Style#")),
            "color_wash": phase1.norm(row.get("Color / Wash")),
            "extracted_at": now,
            "data_json": json.dumps(row, default=str),
        }
        # All DTC columns, normalized for Delta-friendly names (full fidelity in data_json).
        for k, v in row.items():
            if k in ("rowId", "rowIndex"):
                continue
            rec[f"col_{normalize(k)}"] = None if v is None else str(v)
        records.append(rec)

    control_updates.append((r.request_id, len(rows),
                            "extracted" if rows else "extracted (empty request)"))

    if records:
        # Dynamic col_* set is identical within a request's view; build an explicit
        # schema (all dynamic cols are strings) so all-NULL columns don't break
        # type inference. Feed ordered tuples to avoid dict/schema ambiguity.
        dyn_cols = sorted({k for rec in records for k in rec if k.startswith("col_")})
        schema = StructType(FIXED_FIELDS + [StructField(c, StringType()) for c in dyn_cols])
        ordered = [f.name for f in schema.fields]
        data = [tuple(rec.get(fn) for fn in ordered) for rec in records]
        all_spark_dfs.append(spark.createDataFrame(data, schema))

connector.close()

# COMMAND ----------

# Union all requests (column sets are identical within a Document/view) and write.
if all_spark_dfs:
    from functools import reduce
    # Align columns across requests defensively (in case of view differences).
    all_cols = sorted({c for df in all_spark_dfs for c in df.columns})
    aligned = []
    for df in all_spark_dfs:
        for c in all_cols:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None).cast("string"))
        aligned.append(df.select(*all_cols))
    out = reduce(lambda a, b: a.unionByName(b), aligned)

    (out.write.format("delta").mode(write_mode)
        .option("mergeSchema", "true")
        .option("delta.columnMapping.mode", "name")
        .saveAsTable(target_full))
    total = out.count()
    print(f"✅ Wrote {total} rows to {target_full}")
else:
    print("⚠️  All in-scope requests were empty - no data rows written")

# COMMAND ----------

# Update control table (requirement 1a): last_extracted, row_count, msgs.
ts = now.isoformat()
for request_id, row_count, msg in control_updates:
    rc = "NULL" if row_count is None else str(row_count)
    msg_sql = msg.replace("'", "''")
    spark.sql(f"""
      UPDATE {registry_full}
      SET last_extracted = timestamp('{ts}'),
          row_count = {rc},
          msgs = '{msg_sql}',
          updated_at = timestamp('{ts}')
      WHERE environment = '{environment}' AND request_id = '{request_id}'
    """)
print(f"✅ Updated control table for {len(control_updates)} request(s)")

# COMMAND ----------

print("\nControl table state (in-scope):")
spark.sql(f"""
  SELECT request_reference, season_code, brands, row_count, last_extracted, last_pushed, msgs
  FROM {registry_full}
  WHERE environment = '{environment}' AND customer = '{customer}' AND in_scope = true
  ORDER BY request_reference
""").show(truncate=False)
print("✅ Pull complete")
