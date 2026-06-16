# Databricks notebook source
"""
DTC Request Resolver / Validator (Phase 1)
==========================================

Phase 1 does NOT create DTC requests. Per the agreed scope:

  "All would-be requests are pre-created by the project team. If a request that
   BeProduct data targets does not exist (is not registered / in scope), mark an
   error. Phase 2 will look at creating missing DTC requests."

This notebook therefore RESOLVES each distinct request name in the BeProduct
staging table against the Phase 1 control table (dtc_request_registry) and:
  - writes the resolved mapping (request_id, sheet_id, view_id) used by the push
  - logs an error for every staging request that is missing / out of scope /
    inactive / lacking a WIP_ITS_USE view  (=> those rows are NOT pushed)

It never calls DTC to create anything. Run 00_init_request_registry.py first to
populate the registry.

Schedule: after transform (12:00 UTC), before push.

Parameters:
  - catalog / schema (default: lft / beproduct)
  - staging_table (default: beproduct_to_dtc_staging)
  - dtc_environment (default: uat)
  - customer (default: KTB)
"""

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("staging_table", "beproduct_to_dtc_staging", "Staging Table")
dbutils.widgets.text("dtc_environment", "uat", "DTC Environment")
dbutils.widgets.text("customer", "KTB", "In-scope customer token")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
staging_table = dbutils.widgets.get("staging_table")
environment = dbutils.widgets.get("dtc_environment").strip().lower()
customer = dbutils.widgets.get("customer").strip()

staging_full = f"{catalog}.{schema}.{staging_table}"
registry_full = f"{catalog}.{schema}.dtc_request_registry"
mapping_full = f"{catalog}.{schema}.dtc_request_mapping"
sync_log_full = f"{catalog}.{schema}.beproduct_to_dtc_sync_log"

run_id = str(uuid.uuid4())
now = datetime.now(timezone.utc)

print("=" * 80)
print("DTC REQUEST RESOLVER / VALIDATOR (Phase 1 - no creation)")
print("=" * 80)
print(f"  Staging:  {staging_full}")
print(f"  Registry: {registry_full}")
print(f"  Mapping:  {mapping_full}")
print(f"  Sync log: {sync_log_full}")
print(f"  Environment: {environment} | Customer: {customer} | run_id: {run_id}")

# COMMAND ----------

# Ensure sync log exists (shared with the push notebook).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {sync_log_full} (
  log_time TIMESTAMP, run_id STRING, stage STRING, environment STRING,
  dtc_request_name STRING, request_id STRING, operation STRING,
  lf_style_number STRING, color STRING, match_key STRING,
  status STRING, reason STRING, detail STRING, payload STRING
) USING DELTA
TBLPROPERTIES ('description'='BeProduct -> DTC sync log (resolve + push stages)')
""")

# COMMAND ----------

# Distinct pending request names from staging.
df_staging = spark.table(staging_full)
pending = df_staging.where(F.col("sync_status") == "pending")
req_names = [r.dtc_request_name for r in
             pending.select("dtc_request_name").distinct().orderBy("dtc_request_name").collect()]
print(f"Distinct pending request names: {len(req_names)}")
for n in req_names:
    print(f"  - {n}")

if not req_names:
    print("⚠️  No pending staging rows - nothing to resolve")
    dbutils.notebook.exit("NO_PENDING_ROWS")

# In-scope registry entries for this environment.
reg = spark.table(registry_full).where(
    (F.col("environment") == environment) & (F.col("in_scope") == True)  # noqa: E712
)
reg_by_name = {r.request_reference: r for r in reg.collect()}

# COMMAND ----------

resolved_rows = []   # for dtc_request_mapping (rows the push will process)
log_rows = []        # errors for unresolved requests

for name in req_names:
    entry = reg_by_name.get(name)
    if entry is None:
        reason = "request_not_found"
        detail = (f"No in-scope registry entry for '{name}'. Phase 1 does not "
                  f"create requests; register it (00_init_request_registry) or "
                  f"have the project team pre-create it.")
        print(f"  ❌ {name}: {reason}")
        log_rows.append((now, run_id, "resolve", environment, name, None, "REQUEST_NOT_FOUND",
                         None, None, None, "error", reason, detail, None))
        continue

    if str(entry.request_is_active).upper() not in ("Y", "TRUE", "1"):
        reason = "request_inactive"
        print(f"  ❌ {name}: {reason} (is_active={entry.request_is_active})")
        log_rows.append((now, run_id, "resolve", environment, name, entry.request_id,
                         "REQUEST_INACTIVE", None, None, None, "error", reason,
                         f"request_is_active={entry.request_is_active}", None))
        continue

    if not entry.view_id or entry.view_name != "WIP_ITS_USE":
        reason = "wip_view_missing"
        print(f"  ❌ {name}: {reason} (view={entry.view_name})")
        log_rows.append((now, run_id, "resolve", environment, name, entry.request_id,
                         "WIP_VIEW_MISSING", None, None, None, "error", reason,
                         f"view_name={entry.view_name}", None))
        continue

    print(f"  ✅ {name}: resolved -> request_id={entry.request_id}")
    resolved_rows.append({
        "environment": environment,
        "dtc_request_name": name,
        "request_id": entry.request_id,
        "sheet_id": entry.sheet_id,
        "view_id": entry.view_id,
        "season_code": entry.season_code,
        "brands": entry.brands,
        "resolved_at": now,
    })

# COMMAND ----------

# Persist the resolved mapping (overwrite each run; push consumes it).
if resolved_rows:
    df_map = spark.createDataFrame(resolved_rows)
    df_map.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(mapping_full)
    print(f"✅ Wrote {len(resolved_rows)} resolved request(s) to {mapping_full}")
else:
    # Still (re)create an empty mapping so downstream reads don't fail.
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {mapping_full} (
        environment STRING, dtc_request_name STRING, request_id STRING,
        sheet_id STRING, view_id STRING, season_code STRING, brands STRING,
        resolved_at TIMESTAMP
      ) USING DELTA
    """)
    spark.sql(f"DELETE FROM {mapping_full} WHERE environment = '{environment}'")
    print("⚠️  No requests resolved (all missing/out-of-scope/inactive)")

# Log resolution errors.
if log_rows:
    cols = ["log_time", "run_id", "stage", "environment", "dtc_request_name", "request_id",
            "operation", "lf_style_number", "color", "match_key", "status", "reason",
            "detail", "payload"]
    spark.createDataFrame(log_rows, cols).write.format("delta").mode("append").saveAsTable(sync_log_full)
    print(f"⚠️  Logged {len(log_rows)} unresolved-request error(s) to {sync_log_full}")

# COMMAND ----------

print("\n" + "=" * 80)
print("RESOLUTION SUMMARY")
print("=" * 80)
print(f"  Resolved (will push): {len(resolved_rows)}")
print(f"  Errors (NOT pushed):  {len(log_rows)}")
if log_rows:
    print("\n  Unresolved requests this run:")
    spark.sql(f"""
      SELECT dtc_request_name, operation, reason
      FROM {sync_log_full}
      WHERE run_id = '{run_id}' AND stage = 'resolve'
      ORDER BY dtc_request_name
    """).show(truncate=False)
print("\nNext: run beproduct_to_dtc_push.py")
