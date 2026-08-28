# Databricks notebook source
"""
DTC Request Sharing (post-create visibility)
============================================

A newly created DTC request grants FULL rights to its CREATOR only (the API
identity). For the data to be visible to the team, the request must be SHARED.

This notebook applies the project sharing policy to a set of requests,
idempotently:
  1. Share ALL views with the AI Agent service user (default
     aiagentwip@lifung.com)  -> POST /v1/requests/{id}/shares/{userEmail}
  2. Share the "Full Version" view with the "Kontoor Project Team" user group
     -> POST /v1/requests/{id}/shares/usergroups/{userGroupName}

It is safe to re-run: existing shares are detected (GET .../shares[/usergroups])
and skipped. Use it to backfill already-created requests; new requests are also
auto-shared by dtc_request_manager at create time (share_on_create).

Parameters:
  - catalog / schema (default: lft / beproduct)
  - dtc_environment (default: uat)
  - dtc_workspace (default: KTB)
  - request_names    CSV of request references to share; blank = ALL requests in
                     dtc_request_mapping for this environment.
  - share_user_email (default: aiagentwip@lifung.com)
  - user_view_scope  "ALL" (default) = every view; or a CSV of view names.
  - share_user_group (default: Kontoor Project Team)   blank = skip group share
  - group_view_names (default: Full Version)    CSV of views shared to the group
  - send_email       "Y" | "N" (default N) -- whether DTC emails the recipients
  - dry_run          (default: true) -- compute & log only, no share calls
"""

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

import json
import uuid
from datetime import datetime, timezone

from connectors.dtc import DTCConnector
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

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

dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("dtc_environment", "uat", "DTC Environment")
dbutils.widgets.text("dtc_workspace", "KTB", "DTC Workspace")
dbutils.widgets.text("request_names", "", "Request names CSV (blank = all in mapping)")
dbutils.widgets.text("share_user_email", "aiagentwip@lifung.com", "Share-all-views user email")
dbutils.widgets.text("user_view_scope", "ALL", "User views: ALL or CSV of view names")
dbutils.widgets.text("share_user_group", "Kontoor Project Team", "User group (blank = skip)")
dbutils.widgets.text("group_view_names", "Full Version", "Views shared to group (CSV)")
dbutils.widgets.text("send_email", "N", "Email recipients (Y/N)")
dbutils.widgets.text("dry_run", "true", "Dry Run (true/false)")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
environment = dbutils.widgets.get("dtc_environment").strip().lower()
workspace = dbutils.widgets.get("dtc_workspace").strip()
request_names = [r.strip() for r in dbutils.widgets.get("request_names").split(",") if r.strip()]
share_user_email = dbutils.widgets.get("share_user_email").strip()
user_view_scope = dbutils.widgets.get("user_view_scope").strip()
share_user_group = dbutils.widgets.get("share_user_group").strip()
group_view_names = [v.strip() for v in dbutils.widgets.get("group_view_names").split(",") if v.strip()]
send_email = dbutils.widgets.get("send_email").strip().upper() or "N"
dry_run = dbutils.widgets.get("dry_run").strip().lower() == "true"

mapping_full = f"{catalog}.{schema}.dtc_request_mapping"
sync_log_full = f"{catalog}.{schema}.beproduct_to_dtc_sync_log"

run_id = str(uuid.uuid4())
now = datetime.now(timezone.utc)

print("=" * 80)
print("DTC REQUEST SHARING")
print("=" * 80)
print(f"  Mapping:  {mapping_full}")
print(f"  Env: {environment} | dry_run={dry_run} | send_email={send_email}")
print(f"  user (all views unless scoped): {share_user_email} | scope={user_view_scope}")
print(f"  group: {share_user_group or '(none)'} | group views: {group_view_names}")
print(f"  target names: {request_names or 'ALL in mapping'}")
print(f"  run_id: {run_id}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {sync_log_full} (
  log_time TIMESTAMP, run_id STRING, stage STRING, environment STRING,
  dtc_request_name STRING, request_id STRING, operation STRING,
  lf_style_number STRING, color STRING, match_key STRING,
  status STRING, reason STRING, detail STRING, payload STRING
) USING DELTA
""")

df_map = spark.table(mapping_full).where(F.col("environment") == environment)
if request_names:
    df_map = df_map.where(F.col("dtc_request_name").isin(request_names))
targets = [(r.dtc_request_name, r.request_id) for r in df_map.collect()]
print(f"Requests to share: {len(targets)}")
if not targets:
    dbutils.notebook.exit("NO_TARGET_REQUESTS")

# COMMAND ----------

secret_key = f"dtc_api_key_{environment}"
api_key = dbutils.secrets.get(scope="beproduct", key=secret_key)
connector = DTCConnector(api_key=api_key, environment=environment, workspace_name=workspace)

log_rows = []
totals = {"requests": 0, "user_shared": 0, "user_already": 0,
          "group_shared": 0, "group_already": 0, "errors": 0}

def log(name, request_id, operation, status, reason="", detail="", payload=None):
    log_rows.append((now, run_id, "share", environment, name, request_id, operation,
                     None, None, None, status, reason, detail,
                     (json.dumps(payload) if payload is not None else None)))

for name, request_id in targets:
    totals["requests"] += 1
    print(f"\n--- {name}  (request_id={request_id}) ---")

    # Resolve the view list for this request.
    try:
        views = connector.get_views(request_id)
        all_view_names = [v.get("viewName") for v in views if v.get("viewName")]
    except Exception as e:
        print(f"  ❌ get_views failed: {e}")
        log(name, request_id, "SHARE", "error", "get_views_failed", str(e)[:300])
        totals["errors"] += 1
        continue

    user_views = all_view_names if user_view_scope.upper() == "ALL" \
        else [v.strip() for v in user_view_scope.split(",") if v.strip()]

    # 1) Share with the service user (all views) - idempotent.
    if share_user_email:
        try:
            shared_users = {(u.get("userEmail") or "").lower()
                            for u in connector.get_request_shares(request_id)}
            if share_user_email.lower() in shared_users:
                print(f"  ⏭  user {share_user_email} already shared")
                log(name, request_id, "SHARE_USER", "skipped", "already_shared",
                    share_user_email)
                totals["user_already"] += 1
            else:
                if not dry_run:
                    connector.share_request_with_user(
                        request_id, share_user_email, user_views, send_email=send_email)
                print(f"  ✅ shared {len(user_views)} views -> {share_user_email}")
                log(name, request_id, "SHARE_USER", "ok",
                    "dry_run" if dry_run else "",
                    f"{len(user_views)} views -> {share_user_email}",
                    {"viewNames": user_views})
                totals["user_shared"] += 1
        except Exception as e:
            print(f"  ❌ user share failed: {e}")
            log(name, request_id, "SHARE_USER", "error", "share_failed", str(e)[:300])
            totals["errors"] += 1

    # 2) Share with the user group (group views) - idempotent.
    if share_user_group and group_view_names:
        try:
            shared_groups = {(g.get("userGroupName") or "")
                             for g in connector.get_request_share_usergroups(request_id)}
            if share_user_group in shared_groups:
                print(f"  ⏭  group {share_user_group!r} already shared")
                log(name, request_id, "SHARE_GROUP", "skipped", "already_shared",
                    share_user_group)
                totals["group_already"] += 1
            else:
                if not dry_run:
                    connector.share_request_with_usergroup(
                        request_id, share_user_group, group_view_names, send_email=send_email)
                print(f"  ✅ shared {group_view_names} -> group {share_user_group!r}")
                log(name, request_id, "SHARE_GROUP", "ok",
                    "dry_run" if dry_run else "",
                    f"{group_view_names} -> {share_user_group}",
                    {"viewNames": group_view_names})
                totals["group_shared"] += 1
        except Exception as e:
            print(f"  ❌ group share failed: {e}")
            log(name, request_id, "SHARE_GROUP", "error", "share_failed", str(e)[:300])
            totals["errors"] += 1

connector.close()

# COMMAND ----------

if log_rows:
    spark.createDataFrame(log_rows, SYNC_LOG_SCHEMA).write.format("delta").mode("append").saveAsTable(sync_log_full)
    print(f"\n✅ Logged {len(log_rows)} share rows to {sync_log_full}")

print("\n" + "=" * 80)
print("SHARE SUMMARY")
print("=" * 80)
for k, v in totals.items():
    print(f"  {k}: {v}")
if dry_run:
    print("\n⚠️  DRY RUN - no shares were applied. Set dry_run=false to apply.")

dbutils.notebook.exit(
    f"OK user_shared={totals['user_shared']} group_shared={totals['group_shared']} "
    f"already={totals['user_already']+totals['group_already']} errors={totals['errors']}"
)
