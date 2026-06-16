# Databricks notebook source
"""
Pull In-Scope DTC Requests -> Delta (Phase 1, requirement 1)
============================================================

Uploads every in-scope request listed in the control table to ONE Delta table
per (workspace + customer):

    lft.beproduct.dtc_wip_<CUSTOMER>            e.g. dtc_wip_KTB

Each row carries the request's [request_reference, season_code, brands] plus the
DTC rowId / rowIndex, so the table is keyed for reconciliation and push:
  (customer, season_code, brands, lf_style_number, color_wash)  + row_id/row_index

It also maintains the control table (requirement 1a): for each request it updates
last_extracted, row_count and msgs. Empty requests (no rows) are allowed
(requirement 1b) and recorded with row_count = 0.

Discovery is registry-driven (the API key cannot list requests). Run
00_init_request_registry.py first.

Parameters:
  - dtc_environment (default: uat)
  - customer (default: KTB)
  - dtc_workspace (default: KTB)
  - catalog / schema (default: lft / beproduct)
  - write_mode: overwrite | append (default: overwrite)
"""

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

import json
from datetime import datetime, timezone

from connectors.dtc import DTCConnector
from sync import phase1
from pyspark.sql import functions as F

dbutils.widgets.text("dtc_environment", "uat", "DTC Environment")
dbutils.widgets.text("customer", "KTB", "Customer")
dbutils.widgets.text("dtc_workspace", "KTB", "DTC Workspace")
dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("write_mode", "overwrite", "overwrite | append")

environment = dbutils.widgets.get("dtc_environment").strip().lower()
customer = dbutils.widgets.get("customer").strip()
workspace = dbutils.widgets.get("dtc_workspace").strip()
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
write_mode = dbutils.widgets.get("write_mode").strip().lower()

registry_full = f"{catalog}.{schema}.dtc_request_registry"
target_full = f"{catalog}.{schema}.dtc_wip_{customer.lower()}"

print("=" * 80)
print("PULL IN-SCOPE DTC REQUESTS -> DELTA (Phase 1)")
print("=" * 80)
print(f"  Registry: {registry_full}")
print(f"  Target:   {target_full}")
print(f"  Env: {environment} | Customer: {customer} | write_mode: {write_mode}")

# COMMAND ----------

reg_rows = spark.table(registry_full).where(
    (F.col("environment") == environment)
    & (F.col("customer") == customer)
    & (F.col("in_scope") == True)        # noqa: E712
    & (F.upper(F.col("request_is_active")).isin("Y", "TRUE", "1"))
).collect()

print(f"In-scope active requests to pull: {len(reg_rows)}")
if not reg_rows:
    dbutils.notebook.exit("NO_IN_SCOPE_REQUESTS")

secret_key = f"dtc_api_key_{environment}"
api_key = dbutils.secrets.get(scope="beproduct", key=secret_key)
connector = DTCConnector(api_key=api_key, environment=environment, workspace_name=workspace)
now = datetime.now(timezone.utc)

# COMMAND ----------

def normalize(col_name):
    return DTCConnector._normalize_column_name(col_name)

all_spark_dfs = []
control_updates = []  # (request_id, row_count, msg)

for r in reg_rows:
    name = r.request_reference
    try:
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
            "row_index": row.get("rowIndex"),
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
        all_spark_dfs.append(spark.createDataFrame(records))

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
