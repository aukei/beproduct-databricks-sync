# Databricks notebook source
"""
Phase 1 Control Table: DTC Request Registry
===========================================

Creates and populates the Phase 1 control table (requirement 1a):

    lft.beproduct.dtc_request_registry

Request discovery is registry-driven by design: an admin seeds the in-scope
request IDs here, and this notebook enriches each one via by-id reads
(get_request + get_views). This gives an explicit, auditable in-scope list and
keeps out-of-scope developer requests (e.g. "KON ...") out of the pipeline.

(Listing IS available if needed: GET /v1/requests works when workspaceName +
filters are sent in the JSON BODY - see DTCConnector.search_requests(). An
earlier note here claimed the key could not list; that was a client bug from
sending workspaceName as a query param, not an API/permission limitation.)

In scope == request reference parses as "<customer> <seasonCode> <brand>" AND its
customer token equals `customer` (e.g. "KTB FW26 Wrangler"). Developer test
requests such as "KON FW26 Wrangler" are out of scope and recorded with
in_scope = false (kept for audit, ignored by pull/push).

Control columns (per requirement 1a):
  request_id, view_id, customer, season_code, brands, last_extracted, msgs
plus operational fields (sheet_id, request_reference, row_count, last_pushed...).

Parameters:
  - request_ids: comma-separated DTC request IDs to register (REQUIRED)
  - dtc_environment: uat | prod (default: uat)
  - customer: in-scope customer token (default: KTB)
  - dtc_workspace: DTC workspace name (default: KTB)
  - dtc_document: DTC document name (default: KTB WIP)
  - catalog / schema: target location (default: lft / beproduct)
  - mode: "merge" (upsert by request_id) or "replace" (default: merge)
"""

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

from datetime import datetime, timezone
from connectors.dtc import DTCConnector
from sync import phase1
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, LongType, TimestampType,
)

dbutils.widgets.text("request_ids", "", "Comma-separated DTC request IDs")
dbutils.widgets.text("dtc_environment", "uat", "DTC Environment")
dbutils.widgets.text("customer", "KTB", "In-scope customer token")
dbutils.widgets.text("dtc_workspace", "KTB", "DTC Workspace Name")
dbutils.widgets.text("dtc_document", "KTB WIP", "DTC Document Name")
dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("mode", "merge", "merge | replace")

request_ids = [r.strip() for r in dbutils.widgets.get("request_ids").split(",") if r.strip()]
environment = dbutils.widgets.get("dtc_environment").strip().lower()
customer = dbutils.widgets.get("customer").strip()
workspace = dbutils.widgets.get("dtc_workspace").strip()
document = dbutils.widgets.get("dtc_document").strip()
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
mode = dbutils.widgets.get("mode").strip().lower()

registry_table = f"{catalog}.{schema}.dtc_request_registry"

print("=" * 80)
print("DTC REQUEST REGISTRY (Phase 1 control table)")
print("=" * 80)
print(f"  Registry: {registry_table}")
print(f"  Environment: {environment} | Customer: {customer}")
print(f"  Workspace/Document: {workspace} / {document}")
print(f"  Request IDs to register: {len(request_ids)}")
print(f"  Mode: {mode}")

if not request_ids:
    raise ValueError("Parameter 'request_ids' is required (comma-separated DTC request IDs)")

# COMMAND ----------

# Create the registry table if needed.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {registry_table} (
  environment        STRING,
  workspace_name     STRING,
  document_name      STRING,
  customer           STRING,
  request_id         STRING,
  sheet_id           STRING,
  view_id            STRING,
  view_name          STRING,
  request_reference  STRING,
  season_code        STRING,
  brands             STRING,
  request_is_active  STRING,
  in_scope           BOOLEAN,
  row_count          BIGINT,
  last_extracted     TIMESTAMP,
  last_pushed        TIMESTAMP,
  msgs               STRING,
  registered_at      TIMESTAMP,
  updated_at         TIMESTAMP
) USING DELTA
TBLPROPERTIES ('description'='Phase 1 control table: in-scope DTC requests + sync state')
""")
print(f"✅ Registry table ready: {registry_table}")

# COMMAND ----------

# Enrich each request id via by-id reads (no listing required).
secret_key = f"dtc_api_key_{environment}"
api_key = dbutils.secrets.get(scope="beproduct", key=secret_key)
connector = DTCConnector(api_key=api_key, environment=environment, workspace_name=workspace)

now = datetime.now(timezone.utc)
rows = []
for rid in request_ids:
    try:
        scope = connector.get_request_scope(rid)
    except Exception as e:
        print(f"  ❌ {rid}: failed to read request: {e}")
        rows.append({
            "environment": environment, "workspace_name": workspace, "document_name": document,
            "customer": customer, "request_id": rid, "sheet_id": None, "view_id": None,
            "view_name": None, "request_reference": None, "season_code": None, "brands": None,
            "request_is_active": None, "in_scope": False, "row_count": None,
            "last_extracted": None, "last_pushed": None,
            "msgs": f"read_error: {str(e)[:200]}", "registered_at": now, "updated_at": now,
        })
        continue

    ref = scope.get("request_reference") or ""
    in_scope = bool(scope.get("parse_ok")) and phase1.is_in_scope(ref, customer)
    if scope.get("view_name") != "WIP_ITS_USE":
        msg = f"WARNING: WIP_ITS_USE view not found (using {scope.get('view_name')})"
    elif not in_scope:
        msg = f"OUT_OF_SCOPE for customer {customer} (ref={ref!r})"
    else:
        msg = "registered"

    print(f"  {'✅' if in_scope else '⚠️ '} {rid}  ref={ref!r}  "
          f"season={scope.get('season_code')} brand={scope.get('brand')}  in_scope={in_scope}")

    rows.append({
        "environment": environment, "workspace_name": workspace, "document_name": document,
        "customer": customer, "request_id": scope.get("request_id") or rid,
        "sheet_id": scope.get("sheet_id"), "view_id": scope.get("wip_view_id"),
        "view_name": scope.get("view_name"), "request_reference": ref,
        "season_code": scope.get("season_code"), "brands": scope.get("brand"),
        "request_is_active": scope.get("request_is_active"), "in_scope": in_scope,
        "row_count": None, "last_extracted": None, "last_pushed": None,
        "msgs": msg, "registered_at": now, "updated_at": now,
    })

connector.close()

schema_struct = StructType([
    StructField("environment", StringType()), StructField("workspace_name", StringType()),
    StructField("document_name", StringType()), StructField("customer", StringType()),
    StructField("request_id", StringType()), StructField("sheet_id", StringType()),
    StructField("view_id", StringType()), StructField("view_name", StringType()),
    StructField("request_reference", StringType()), StructField("season_code", StringType()),
    StructField("brands", StringType()), StructField("request_is_active", StringType()),
    StructField("in_scope", BooleanType()), StructField("row_count", LongType()),
    StructField("last_extracted", TimestampType()), StructField("last_pushed", TimestampType()),
    StructField("msgs", StringType()), StructField("registered_at", TimestampType()),
    StructField("updated_at", TimestampType()),
])
df_new = spark.createDataFrame(rows, schema=schema_struct)

# COMMAND ----------

if mode == "replace":
    df_new.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(registry_table)
    print(f"✅ Registry REPLACED with {df_new.count()} rows")
else:
    # MERGE upsert by (environment, request_id). Preserve existing sync-state
    # columns (last_extracted/last_pushed/row_count) on update.
    df_new.createOrReplaceTempView("incoming_registry")
    spark.sql(f"""
        MERGE INTO {registry_table} t
        USING incoming_registry s
        ON t.environment = s.environment AND t.request_id = s.request_id
        WHEN MATCHED THEN UPDATE SET
          t.workspace_name = s.workspace_name, t.document_name = s.document_name,
          t.customer = s.customer, t.sheet_id = s.sheet_id, t.view_id = s.view_id,
          t.view_name = s.view_name, t.request_reference = s.request_reference,
          t.season_code = s.season_code, t.brands = s.brands,
          t.request_is_active = s.request_is_active, t.in_scope = s.in_scope,
          t.msgs = s.msgs, t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"✅ Registry MERGED ({df_new.count()} incoming rows)")

# COMMAND ----------

print("\nIn-scope active requests in registry:")
spark.sql(f"""
  SELECT request_reference, season_code, brands, request_id, view_id,
         last_extracted, last_pushed, msgs
  FROM {registry_table}
  WHERE environment = '{environment}' AND in_scope = true
  ORDER BY request_reference
""").show(truncate=False)
print("✅ Registry init complete")
