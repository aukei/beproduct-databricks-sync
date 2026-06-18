# Databricks notebook source
"""
DTC Request Resolver / Creator (Phase 1)
========================================

RESOLVES each distinct request name in the BeProduct staging table against the
control table (dtc_request_registry) and writes the resolved mapping
(request_id, sheet_id, view_id) consumed by the push.

For request names that are **missing but in scope** (parse as
`<customer> <seasonCode> <brand>` and the customer matches), this notebook will
**create** the DTC request/sheet via `connector.create_sheet`, then re-scan the
registry so they resolve. Creation is gated by `dry_run`:
  - dry_run=true  (default): logs "would create", creates nothing.
  - dry_run=false:           creates the request in DTC, then registers + resolves.

Names that are NOT in scope (e.g. a brand-less `KTB SS26`) are never created and
are logged as errors. Inactive / WIP-view-missing requests are also logged.

Discovery scan: by default it refreshes the registry (sync.registry.refresh) at the
start so existing DTC requests aren't mistaken for missing (avoids duplicate
creation), and again after creating new ones.

Schedule: after transform (12:00 UTC), before push.

Parameters:
  - catalog / schema (default: lft / beproduct)
  - staging_table (default: beproduct_to_dtc_staging)
  - dtc_environment (default: uat)
  - customer (default: KTB)
  - dtc_workspace (default: KTB)
  - dtc_document (default: KTB WIP)   -- document new requests are created in
  - dry_run (default: true)           -- when true, never creates (preview only)
  - refresh_registry (default: true)  -- scan + upsert registry before resolving
"""

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

import uuid
from datetime import datetime, timezone

from connectors.dtc import DTCConnector
from sync import phase1, registry
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Explicit schemas so createDataFrame never has to infer types from all-NULL
# columns (e.g. a REQUEST_NOT_FOUND log row has request_id/lf_style_number/
# color/match_key/payload = None), which raises CANNOT_DETERMINE_TYPE.
SYNC_LOG_SCHEMA = StructType([
    StructField("log_time", TimestampType()),
    StructField("run_id", StringType()),
    StructField("stage", StringType()),
    StructField("environment", StringType()),
    StructField("dtc_request_name", StringType()),
    StructField("request_id", StringType()),
    StructField("operation", StringType()),
    StructField("lf_style_number", StringType()),
    StructField("color", StringType()),
    StructField("match_key", StringType()),
    StructField("status", StringType()),
    StructField("reason", StringType()),
    StructField("detail", StringType()),
    StructField("payload", StringType()),
])
MAPPING_SCHEMA = StructType([
    StructField("environment", StringType()),
    StructField("dtc_request_name", StringType()),
    StructField("request_id", StringType()),
    StructField("sheet_id", StringType()),
    StructField("view_id", StringType()),
    StructField("season_code", StringType()),
    StructField("brands", StringType()),
    StructField("resolved_at", TimestampType()),
])

dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("staging_table", "beproduct_to_dtc_staging", "Staging Table")
dbutils.widgets.text("dtc_environment", "uat", "DTC Environment")
dbutils.widgets.text("customer", "KTB", "In-scope customer token")
dbutils.widgets.text("dtc_workspace", "KTB", "DTC Workspace")
dbutils.widgets.text("dtc_document", "KTB WIP", "DTC Document (for created requests)")
dbutils.widgets.text("dry_run", "true", "Dry run (true = never create)")
dbutils.widgets.text("refresh_registry", "true", "Scan + refresh registry first")
# Sharing: a freshly created request is visible only to its creator until shared.
dbutils.widgets.text("share_on_create", "true", "Share newly created requests")
dbutils.widgets.text("share_user_email", "aiagentwip@lifung.com", "Share-all-views user email")
dbutils.widgets.text("share_user_group", "Fabric Group", "User group (blank = skip)")
dbutils.widgets.text("group_view_names", "Full Version", "Views shared to group (CSV)")
dbutils.widgets.text("send_email", "N", "Email share recipients (Y/N)")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
staging_table = dbutils.widgets.get("staging_table")
environment = dbutils.widgets.get("dtc_environment").strip().lower()
customer = dbutils.widgets.get("customer").strip()
workspace = dbutils.widgets.get("dtc_workspace").strip()
document = dbutils.widgets.get("dtc_document").strip()
dry_run = dbutils.widgets.get("dry_run").strip().lower() in ("true", "1", "yes", "y")
refresh_registry = dbutils.widgets.get("refresh_registry").strip().lower() in ("true", "1", "yes", "y")
share_on_create = dbutils.widgets.get("share_on_create").strip().lower() in ("true", "1", "yes", "y")
share_user_email = dbutils.widgets.get("share_user_email").strip()
share_user_group = dbutils.widgets.get("share_user_group").strip()
group_view_names = [v.strip() for v in dbutils.widgets.get("group_view_names").split(",") if v.strip()]
send_email = dbutils.widgets.get("send_email").strip().upper() or "N"

staging_full = f"{catalog}.{schema}.{staging_table}"
registry_full = f"{catalog}.{schema}.dtc_request_registry"
mapping_full = f"{catalog}.{schema}.dtc_request_mapping"
sync_log_full = f"{catalog}.{schema}.beproduct_to_dtc_sync_log"

run_id = str(uuid.uuid4())
now = datetime.now(timezone.utc)

print("=" * 80)
print("DTC REQUEST RESOLVER / CREATOR (Phase 1)")
print("=" * 80)
print(f"  Staging:  {staging_full}")
print(f"  Registry: {registry_full}")
print(f"  Mapping:  {mapping_full}")
print(f"  Sync log: {sync_log_full}")
print(f"  Environment: {environment} | Customer: {customer} | run_id: {run_id}")
print(f"  Workspace/Document: {workspace} / {document}")
print(f"  dry_run: {dry_run} (false = create missing in-scope requests) | "
      f"refresh_registry: {refresh_registry}")

# DTC connector (for the registry scan and request creation).
secret_key = f"dtc_api_key_{environment}"
api_key = dbutils.secrets.get(scope="beproduct", key=secret_key)
connector = DTCConnector(api_key=api_key, environment=environment, workspace_name=workspace)

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
    connector.close()
    dbutils.notebook.exit("NO_PENDING_ROWS")

# Scan the workspace+document and upsert the registry first, so requests that
# already exist in DTC are present here (prevents trying to re-create them).
if refresh_registry:
    print("\n🔄 Refreshing request registry (scan workspace+document)...")
    registry.refresh(
        spark, connector,
        environment=environment, workspace=workspace, document=document,
        customer=customer, registry_table=registry_full,
    )


def load_reg_by_name():
    reg = spark.table(registry_full).where(
        (F.col("environment") == environment) & (F.col("in_scope") == True)  # noqa: E712
    )
    return {r.request_reference: r for r in reg.collect()}


def resolve_name(name, reg_by_name):
    """('ok', mapping_dict) | ('error', (op, reason, detail, request_id)) | ('missing', None)."""
    entry = reg_by_name.get(name)
    if entry is None:
        return ("missing", None)
    if str(entry.request_is_active).upper() not in ("Y", "TRUE", "1"):
        return ("error", ("REQUEST_INACTIVE", "request_inactive",
                          f"request_is_active={entry.request_is_active}", entry.request_id))
    if not entry.view_id or entry.view_name != "WIP_ITS_USE":
        return ("error", ("WIP_VIEW_MISSING", "wip_view_missing",
                          f"view_name={entry.view_name}", entry.request_id))
    return ("ok", {
        "environment": environment, "dtc_request_name": name,
        "request_id": entry.request_id, "sheet_id": entry.sheet_id,
        "view_id": entry.view_id, "season_code": entry.season_code,
        "brands": entry.brands, "resolved_at": now,
    })

# COMMAND ----------

reg_by_name = load_reg_by_name()
resolved_rows = []   # for dtc_request_mapping (rows the push will process)
log_rows = []        # errors / create events
missing = []         # names not present in the registry

# Pass 1: resolve against the current registry.
for name in req_names:
    status, payload = resolve_name(name, reg_by_name)
    if status == "ok":
        print(f"  ✅ {name}: resolved -> request_id={payload['request_id']}")
        resolved_rows.append(payload)
    elif status == "error":
        op, reason, detail, rid = payload
        print(f"  ❌ {name}: {reason}")
        log_rows.append((now, run_id, "resolve", environment, name, rid, op,
                         None, None, None, "error", reason, detail, None))
    else:
        missing.append(name)

# Partition missing names: in-scope can be created; others cannot.
to_create = [n for n in missing if phase1.is_in_scope(n, customer)]
not_creatable = [n for n in missing if not phase1.is_in_scope(n, customer)]

for name in not_creatable:
    detail = (f"'{name}' does not parse as '<customer> <seasonCode> <brand>' for "
              f"customer {customer} (e.g. missing brand) - cannot create.")
    print(f"  ❌ {name}: not_in_scope (cannot create)")
    log_rows.append((now, run_id, "resolve", environment, name, None, "NOT_IN_SCOPE",
                     None, None, None, "error", "not_in_scope", detail, None))

# COMMAND ----------

# Create missing in-scope requests (gated by dry_run).
created_any = False
if to_create:
    print(f"\n🛠️  Missing in-scope requests: {len(to_create)} "
          f"({'DRY RUN - none will be created' if dry_run else 'creating in DTC'})")
    for name in to_create:
        if dry_run:
            print(f"  🅓 {name}: would CREATE (dry_run)")
            log_rows.append((now, run_id, "create", environment, name, None, "CREATE_REQUEST",
                             None, None, None, "skipped", "dry_run",
                             f"would create in document '{document}'", None))
            continue
        try:
            resp = connector.create_sheet(workspace, document, name)
            rid, sid = resp.get("requestId"), resp.get("sheetId")
            created_any = True
            print(f"  ✅ {name}: CREATED request_id={rid} sheet_id={sid}")
            log_rows.append((now, run_id, "create", environment, name, rid, "CREATE_REQUEST",
                             None, None, None, "created", "", f"sheet_id={sid}", None))

            # Share the new request so it is visible to the team (creator-only
            # by default). All views -> service user; group views -> user group.
            if share_on_create and rid:
                try:
                    views = connector.get_views(rid)
                    all_views = [v.get("viewName") for v in views if v.get("viewName")]
                except Exception as ve:
                    all_views = []
                    print(f"     ⚠️  get_views failed for share: {ve}")
                if share_user_email and all_views:
                    try:
                        connector.share_request_with_user(
                            rid, share_user_email, all_views, send_email=send_email)
                        print(f"     🔗 shared {len(all_views)} views -> {share_user_email}")
                        log_rows.append((now, run_id, "share", environment, name, rid, "SHARE_USER",
                                         None, None, None, "ok", "", f"{len(all_views)} views -> {share_user_email}", None))
                    except Exception as se:
                        print(f"     ❌ user share failed: {se}")
                        log_rows.append((now, run_id, "share", environment, name, rid, "SHARE_USER",
                                         None, None, None, "error", "share_failed", str(se)[:300], None))
                if share_user_group and group_view_names:
                    try:
                        connector.share_request_with_usergroup(
                            rid, share_user_group, group_view_names, send_email=send_email)
                        print(f"     🔗 shared {group_view_names} -> group {share_user_group!r}")
                        log_rows.append((now, run_id, "share", environment, name, rid, "SHARE_GROUP",
                                         None, None, None, "ok", "", f"{group_view_names} -> {share_user_group}", None))
                    except Exception as se:
                        print(f"     ❌ group share failed: {se}")
                        log_rows.append((now, run_id, "share", environment, name, rid, "SHARE_GROUP",
                                         None, None, None, "error", "share_failed", str(se)[:300], None))
        except Exception as e:
            print(f"  ❌ {name}: create failed: {e}")
            log_rows.append((now, run_id, "create", environment, name, None, "CREATE_REQUEST",
                             None, None, None, "error", "create_failed", str(e)[:300], None))

# Re-scan + re-resolve the newly created requests.
if created_any:
    print("\n🔄 Re-scanning registry to register newly created requests...")
    registry.refresh(
        spark, connector,
        environment=environment, workspace=workspace, document=document,
        customer=customer, registry_table=registry_full,
    )
    reg_by_name = load_reg_by_name()
    for name in to_create:
        status, payload = resolve_name(name, reg_by_name)
        if status == "ok":
            print(f"  ✅ {name}: resolved after create -> request_id={payload['request_id']}")
            resolved_rows.append(payload)
        elif status == "error":
            op, reason, detail, rid = payload
            log_rows.append((now, run_id, "resolve", environment, name, rid, op,
                             None, None, None, "error", reason, detail, None))
        else:
            log_rows.append((now, run_id, "resolve", environment, name, None, "REQUEST_NOT_FOUND",
                             None, None, None, "error", "not_found_after_create",
                             "created but not yet discoverable", None))

connector.close()

# COMMAND ----------

# Persist the resolved mapping (overwrite each run; push consumes it).
if resolved_rows:
    _ordered = [f.name for f in MAPPING_SCHEMA.fields]
    _map_data = [tuple(r[fn] for fn in _ordered) for r in resolved_rows]
    df_map = spark.createDataFrame(_map_data, MAPPING_SCHEMA)
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
    spark.createDataFrame(log_rows, SYNC_LOG_SCHEMA).write.format("delta").mode("append").saveAsTable(sync_log_full)
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
