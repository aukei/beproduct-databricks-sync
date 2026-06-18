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
  - request_ids (default: "") -- comma-separated DTC request IDs to pull;
    blank = all in-scope. When provided the pull is targeted: only the listed
    requests are read and their rows are replaced in the Delta table (DELETE +
    append) rather than overwriting the whole table. Typically passed by the
    orchestrator for the Step 7 post-Phase-1 re-pull (only INSERT'd requests).
  - max_workers (default: 4) -- ThreadPoolExecutor size for parallel get_sheet()
    calls. Capped at 4 to respect the 2-node K8S cluster backing the DTC UAT API.
"""

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
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
dbutils.widgets.text("request_ids", "", "Comma-separated request IDs (blank = all in-scope)")
dbutils.widgets.text("max_workers", "4", "Parallel get_sheet() workers (max 4 for UAT K8S)")

environment = dbutils.widgets.get("dtc_environment").strip().lower()
customer = dbutils.widgets.get("customer").strip()
workspace = dbutils.widgets.get("dtc_workspace").strip()
document = dbutils.widgets.get("dtc_document").strip()
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
write_mode = dbutils.widgets.get("write_mode").strip().lower()
refresh_registry = dbutils.widgets.get("refresh_registry").strip().lower() in ("true", "1", "yes", "y")
_raw_request_ids = dbutils.widgets.get("request_ids").strip()
filter_request_ids = (
    {i.strip() for i in _raw_request_ids.split(",") if i.strip()}
    if _raw_request_ids else None
)
max_workers = min(int(dbutils.widgets.get("max_workers").strip() or "4"), 4)  # hard cap at 4

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
print(f"  filter_request_ids: {filter_request_ids or '(all in-scope)'}")
print(f"  max_workers: {max_workers}")

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

_reg_all = spark.table(registry_full).where(
    (F.col("environment") == environment)
    & (F.col("customer") == customer)
    & (F.col("in_scope") == True)        # noqa: E712
    & (F.upper(F.col("request_is_active")).isin("Y", "TRUE", "1"))
).collect()

# When request_ids filter is given (targeted re-pull from orchestrator Step 7),
# only pull the specified requests instead of the full in-scope set.
if filter_request_ids:
    reg_rows = [r for r in _reg_all if r.request_id in filter_request_ids]
    print(f"Targeted pull: {len(reg_rows)} of {len(_reg_all)} in-scope requests "
          f"(filtered to {len(filter_request_ids)} request_id(s))")
else:
    reg_rows = _reg_all
    print(f"In-scope active requests to pull: {len(reg_rows)}")

if not reg_rows:
    connector.close()
    dbutils.notebook.exit("NO_IN_SCOPE_REQUESTS")

# COMMAND ----------

def normalize(col_name):
    return DTCConnector._normalize_column_name(col_name)

def _build_records(r, rows):
    """Convert raw DTC sheet rows into flat record dicts for Delta."""
    records = []
    name = r.request_reference
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
    return records

all_spark_dfs = []
control_updates = []  # (request_id, row_count, msg)

# Split eligible (WIP view present) vs skipped (wrong view) before parallel fetch.
eligible = [r for r in reg_rows if r.view_name == WIP_VIEW_NAME]
for r in reg_rows:
    if r.view_name != WIP_VIEW_NAME:
        print(f"  ⏭️  {r.request_reference}: skipped "
              f"(view_name={r.view_name!r}, expected {WIP_VIEW_NAME!r})")
        control_updates.append((r.request_id, None,
                                f"skipped: WIP view missing (view_name={r.view_name})"))

# Parallel get_sheet() calls — max_workers=4 to respect the 2-node K8S DTC API cluster.
# requests.Session is safe for concurrent GET calls (no shared mutable state per request).
fetch_results = {}   # request_id -> list[dict]  (sheetData rows)
fetch_errors  = {}   # request_id -> str

def _fetch(r):
    sheet = connector.get_sheet(r.sheet_id, r.view_id)
    return r.request_id, sheet.get("sheetData", [])

print(f"\nFetching {len(eligible)} sheet(s) with {max_workers} parallel worker(s)...")
with ThreadPoolExecutor(max_workers=max_workers) as pool:
    futures = {pool.submit(_fetch, r): r for r in eligible}
    for fut in as_completed(futures):
        r = futures[fut]
        try:
            rid, rows = fut.result()
            fetch_results[rid] = rows
        except Exception as e:
            fetch_errors[r.request_id] = str(e)

# Process results in registry order (deterministic DataFrame union ordering).
for r in eligible:
    name = r.request_reference
    if r.request_id in fetch_errors:
        err = fetch_errors[r.request_id]
        print(f"  ❌ {name}: read failed: {err}")
        control_updates.append((r.request_id, None, f"extract_error: {err[:160]}"))
        continue

    rows = fetch_results[r.request_id]
    print(f"  ✅ {name}: {len(rows)} row(s)")

    control_updates.append((r.request_id, len(rows),
                            "extracted" if rows else "extracted (empty request)"))

    if rows:
        records = _build_records(r, rows)
        # Dynamic col_* set is identical within a request's view; build an explicit
        # schema (all dynamic cols are strings) so all-NULL columns don't break
        # type inference. Feed ordered tuples to avoid dict/schema ambiguity.
        dyn_cols = sorted({k for rec in records for k in rec if k.startswith("col_")})
        row_schema = StructType(FIXED_FIELDS + [StructField(c, StringType()) for c in dyn_cols])
        ordered = [f.name for f in row_schema.fields]
        data = [tuple(rec.get(fn) for fn in ordered) for rec in records]
        all_spark_dfs.append(spark.createDataFrame(data, row_schema))

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

    # Targeted re-pull (request_ids filter active): replace only the rows for the
    # specified requests — DELETE their stale rows then APPEND the fresh data.
    # This preserves all other requests' rows in the table.
    # Full pull (no filter): normal overwrite replaces the whole table.
    if filter_request_ids:
        ids_sql = "', '".join(filter_request_ids)
        spark.sql(f"DELETE FROM {target_full} WHERE request_id IN ('{ids_sql}')")
        effective_write_mode = "append"
        print(f"  Targeted write: deleted stale rows for {len(filter_request_ids)} "
              f"request(s), appending fresh data")
    else:
        effective_write_mode = write_mode

    (out.write.format("delta").mode(effective_write_mode)
        .option("mergeSchema", "true")
        .option("delta.columnMapping.mode", "name")
        .saveAsTable(target_full))
    total = out.count()
    print(f"✅ Wrote {total} rows to {target_full}")
else:
    if filter_request_ids:
        # Targeted pull where all fetched requests came back empty — still remove
        # their stale rows from the table so dtc_wip is consistent.
        ids_sql = "', '".join(filter_request_ids)
        spark.sql(f"DELETE FROM {target_full} WHERE request_id IN ('{ids_sql}')")
        print("⚠️  All targeted requests were empty — removed stale rows if any")
    else:
        print("⚠️  All in-scope requests were empty - no data rows written")

# COMMAND ----------

# Update control table (requirement 1a): last_extracted, row_count, msgs.
# Batched as a SINGLE `MERGE INTO` (one Spark job) instead of one `UPDATE` per
# request. The old per-request loop fired ~66 separate Delta UPDATE jobs and was
# the second-largest cost in this notebook (~179 s in run 3 — see
# docs/PERFORMANCE.md "Validation run 3"). Because Step 7 runs THIS SAME notebook
# (with a targeted request_ids filter), it gets the same speedup automatically;
# in targeted mode `control_updates` only holds the filtered requests, so the
# MERGE touches just those rows.
ts_iso = now.isoformat()
if control_updates:
    ctrl_schema = StructType([
        StructField("environment", StringType()),
        StructField("request_id", StringType()),
        StructField("row_count", LongType()),   # nullable: None -> NULL (skipped)
        StructField("msg", StringType()),
    ])
    ctrl_data = [
        (environment, rid, (None if rc is None else int(rc)), msg)
        for (rid, rc, msg) in control_updates
    ]
    spark.createDataFrame(ctrl_data, ctrl_schema).createOrReplaceTempView("control_updates_src")
    spark.sql(f"""
      MERGE INTO {registry_full} t
      USING control_updates_src s
        ON t.environment = s.environment AND t.request_id = s.request_id
      WHEN MATCHED THEN UPDATE SET
        t.last_extracted = timestamp('{ts_iso}'),
        t.row_count = s.row_count,
        t.msgs = s.msg,
        t.updated_at = timestamp('{ts_iso}')
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
