# Databricks notebook source
"""
BeProduct -> DTC Upsert & Push (Phase 1)
========================================

Massages BeProduct (denormalized) staging rows onto each in-scope DTC request
and pushes the changes back via the DTC PATCH API.

Implements dtc/PHASE1_WORKFLOW.md steps 3 & 4 using the validated, unit-tested
core in dtc/python/sync/phase1.py and connectors/dtc.py:

  3a. Upsert on the in-request row key (LF Style#, Color / Wash) - season & brand
      are fixed by the request (one brand per request). Update indicated non-key
      fields, EXCEPT "Style Image". Insert new rows with key + mapped fields.
  3b. RowIndex: keep original rowIndex on UPDATE; on INSERT assign
      max(rowIndex)+1 within the request (= partition by season+brand),
      sparse-aware.
  3c. Log exceptions (scope mismatch, dup keys, missing rowId...) to the sync log.
  4a. Delta push: only consider staging rows modified since the request's last
      push (beproduct_modified_at > registry.last_pushed). compute_upsert also
      emits NOOP for rows whose mapped fields already match, so unchanged rows
      are never pushed.
  4b. UPDATE: PATCH .../views/{viewId} with {"sheetData":[{...,"rowId":id}]}.
  4c. INSERT: PATCH .../views/{viewId} with {"sheetData":[{...,"rowIndex":n}]}.
      (Updates and inserts are sent as SEPARATE batches - the API rejects a mix.)
  4d. Log detailed per-row results/exceptions to the sync log table.

Phase 1 never creates requests: requests are resolved upstream by
dtc_request_manager.py (validate-only). Requests that BeProduct targets but that
are missing/out-of-scope are logged there as errors and are absent from the
resolved mapping, so they are skipped here.

Parameters:
  - catalog / schema (default: lft / beproduct)
  - staging_table (default: beproduct_to_dtc_staging)
  - dtc_environment (default: uat)
  - dtc_workspace (default: KTB)
  - dry_run (default: true)   -- when true, computes & logs but does NOT PATCH
  - delta_only (default: true) -- only rows modified since last push
  - batch_size (default: 100)  -- rows per PATCH call
"""

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

import json
import uuid
from datetime import datetime, timezone

from connectors.dtc import DTCConnector
from sync import phase1
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("staging_table", "beproduct_to_dtc_staging", "Staging Table")
dbutils.widgets.text("dtc_environment", "uat", "DTC Environment")
dbutils.widgets.text("dtc_workspace", "KTB", "DTC Workspace Name")
dbutils.widgets.text("dry_run", "true", "Dry Run (true/false)")
dbutils.widgets.text("delta_only", "true", "Only push rows modified since last push")
dbutils.widgets.text("batch_size", "100", "Rows per PATCH call")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
staging_table = dbutils.widgets.get("staging_table")
environment = dbutils.widgets.get("dtc_environment").strip().lower()
workspace = dbutils.widgets.get("dtc_workspace").strip()
dry_run = dbutils.widgets.get("dry_run").strip().lower() == "true"
delta_only = dbutils.widgets.get("delta_only").strip().lower() == "true"
batch_size = int(dbutils.widgets.get("batch_size"))

staging_full = f"{catalog}.{schema}.{staging_table}"
mapping_full = f"{catalog}.{schema}.dtc_request_mapping"
registry_full = f"{catalog}.{schema}.dtc_request_registry"
sync_log_full = f"{catalog}.{schema}.beproduct_to_dtc_sync_log"

run_id = str(uuid.uuid4())
now = datetime.now(timezone.utc)

print("=" * 80)
print("BEPRODUCT -> DTC UPSERT & PUSH (Phase 1)")
print("=" * 80)
print(f"  Staging:  {staging_full}")
print(f"  Mapping:  {mapping_full}")
print(f"  Registry: {registry_full}")
print(f"  Sync log: {sync_log_full}")
print(f"  Env: {environment} | dry_run={dry_run} | delta_only={delta_only} | batch_size={batch_size}")
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

# Resolved mapping (from dtc_request_manager). If empty -> nothing to push.
try:
    df_map = spark.table(mapping_full).where(F.col("environment") == environment)
except Exception:
    df_map = None
if df_map is None or df_map.count() == 0:
    print("⚠️  No resolved requests in mapping - run dtc_request_manager first. Exiting.")
    dbutils.notebook.exit("NO_RESOLVED_REQUESTS")

mapping = {r.dtc_request_name: r for r in df_map.collect()}

# Registry (for last_pushed delta + state update).
reg = {r.request_reference: r for r in
       spark.table(registry_full).where(F.col("environment") == environment).collect()}

# Staging pending rows.
df_staging = spark.table(staging_full).where(F.col("sync_status") == "pending")
total_pending = df_staging.count()
print(f"Pending staging rows: {total_pending}")
if total_pending == 0:
    dbutils.notebook.exit("NO_PENDING_ROWS")

# COMMAND ----------

# DTC connector (only needed for live reads/writes).
secret_key = f"dtc_api_key_{environment}"
api_key = dbutils.secrets.get(scope="beproduct", key=secret_key)
connector = DTCConnector(api_key=api_key, environment=environment, workspace_name=workspace)

# Fallback allowed-columns for an empty request sheet (we still must not send
# columns absent from the view). These are the FIELD_MAPPING targets, which were
# validated to exist in the KTB WIP WIP_ITS_USE view.
FALLBACK_COLS = {c for c in phase1.FIELD_MAPPING.values() if c != phase1.STYLE_IMAGE_COL}

LOG_COLS = ["log_time", "run_id", "stage", "environment", "dtc_request_name", "request_id",
            "operation", "lf_style_number", "color", "match_key", "status", "reason",
            "detail", "payload"]

def log(rows, name, request_id, operation, key, status, reason="", detail="", payload=None):
    lf, color = (key or (None, None))
    rows.append((now, run_id, "push", environment, name, request_id, operation,
                 lf, color, f"{lf} | {color}", status, reason, detail,
                 (json.dumps(payload) if payload is not None else None)))

# COMMAND ----------

log_rows = []
totals = {"requests": 0, "updates": 0, "inserts": 0, "noops": 0, "exceptions": 0,
          "pushed_ok": 0, "push_failed": 0}
pushed_keys = {}    # request_name -> set of (lf,color) that reached DTC (or would, dry_run)
error_keys = {}     # request_name -> set of (lf,color) with exceptions

for name, m in mapping.items():
    totals["requests"] += 1
    request_id, sheet_id, view_id = m.request_id, m.sheet_id, m.view_id
    reg_entry = reg.get(name)
    last_pushed = getattr(reg_entry, "last_pushed", None) if reg_entry else None
    print(f"\n--- {name}  (request_id={request_id}) ---")

    # Staging rows for this request (optionally delta-filtered).
    sdf = df_staging.where(F.col("dtc_request_name") == name)
    if delta_only and last_pushed is not None:
        sdf = sdf.where(F.col("beproduct_modified_at") > F.lit(last_pushed))
    bp_rows = [r.asDict() for r in sdf.collect()]
    print(f"  BeProduct rows considered: {len(bp_rows)}"
          + (f" (delta since {last_pushed})" if (delta_only and last_pushed) else ""))
    if not bp_rows:
        continue

    # Current DTC rows (live) from the WIP_ITS_USE view.
    try:
        sheet = connector.get_sheet(sheet_id, view_id)
        dtc_rows = sheet.get("sheetData", [])
        allowed = set(connector.get_view_column_names(sheet_id, view_id)) or set(FALLBACK_COLS)
    except Exception as e:
        print(f"  ❌ Failed to read DTC sheet: {e}")
        log(log_rows, name, request_id, "ERROR", None, "error", "sheet_read_failed", str(e)[:300])
        totals["push_failed"] += 1
        continue

    scope = {"season_code": m.season_code, "brand": m.brands}
    plan = phase1.compute_upsert(scope, dtc_rows, bp_rows, allowed_cols=allowed)
    s = plan.summary()
    totals["updates"] += s["updates"]; totals["inserts"] += s["inserts"]
    totals["noops"] += s["noops"]; totals["exceptions"] += s["exceptions"]
    print(f"  plan: {s}")

    pushed_keys.setdefault(name, set())
    error_keys.setdefault(name, set())

    # Log exceptions (3c / 4d).
    for ex in plan.exceptions:
        log(log_rows, name, request_id, "EXCEPTION", ex.match_key, "error", ex.reason, ex.detail)
        error_keys[name].add(ex.match_key)
    # NOOPs (informational) + count as already-synced.
    for op in plan.noops:
        log(log_rows, name, request_id, "NOOP", op.match_key, "ok", "no_field_changes")
        pushed_keys[name].add(op.match_key)

    # ---- UPDATES (PATCH by rowId, batched) ----
    upd_sd = phase1.update_sheet_data(plan)
    for chunk_ops, chunk_sd in zip(phase1.chunked(plan.updates, batch_size),
                                   phase1.chunked(upd_sd, batch_size)):
        try:
            if not dry_run:
                connector.patch_rows(sheet_id, view_id, chunk_sd)
            for op in chunk_ops:
                log(log_rows, name, request_id, "UPDATE", op.match_key, "ok",
                    "dry_run" if dry_run else "", "", op.fields)
                pushed_keys[name].add(op.match_key); totals["pushed_ok"] += 1
        except Exception as e:
            for op in chunk_ops:
                log(log_rows, name, request_id, "UPDATE", op.match_key, "error",
                    "patch_failed", str(e)[:300], op.fields)
                error_keys[name].add(op.match_key); totals["push_failed"] += 1

    # ---- INSERTS (PATCH by rowIndex, batched) ----
    ins_sd = phase1.insert_sheet_data(plan)
    for chunk_ops, chunk_sd in zip(phase1.chunked(plan.inserts, batch_size),
                                   phase1.chunked(ins_sd, batch_size)):
        try:
            if not dry_run:
                connector.patch_rows(sheet_id, view_id, chunk_sd)
            for op in chunk_ops:
                log(log_rows, name, request_id, "INSERT", op.match_key, "ok",
                    "dry_run" if dry_run else "", f"rowIndex={op.row_index}", op.fields)
                pushed_keys[name].add(op.match_key); totals["pushed_ok"] += 1
        except Exception as e:
            for op in chunk_ops:
                log(log_rows, name, request_id, "INSERT", op.match_key, "error",
                    "patch_failed", str(e)[:300], op.fields)
                error_keys[name].add(op.match_key); totals["push_failed"] += 1

    # Update registry last_pushed/msgs for this request (skip in dry_run).
    if not dry_run:
        reg_msg = "pushed u={u} i={i} noop={n} exc={e}".format(
            u=s["updates"], i=s["inserts"], n=s["noops"], e=s["exceptions"])
        ts = now.isoformat()
        spark.sql(f"""
          UPDATE {registry_full}
          SET last_pushed = timestamp('{ts}'),
              msgs = '{reg_msg}',
              updated_at = timestamp('{ts}')
          WHERE environment = '{environment}' AND request_id = '{request_id}'
        """)

connector.close()

# COMMAND ----------

# Write the sync log (4d).
if log_rows:
    spark.createDataFrame(log_rows, LOG_COLS).write.format("delta").mode("append").saveAsTable(sync_log_full)
    print(f"\n✅ Logged {len(log_rows)} sync-log rows to {sync_log_full}")

# COMMAND ----------

# Update staging sync_status (skip in dry_run). pushed -> rows that reached DTC
# (incl NOOP); error -> rows with exceptions/failed pushes.
if not dry_run:
    def _flatten(d):
        out = []
        for nm, keys in d.items():
            for (lf, color) in keys:
                out.append((nm, lf, color))
        return out

    pushed_list = _flatten(pushed_keys)
    error_list = _flatten(error_keys)

    if pushed_list or error_list:
        upd = spark.table(staging_full)
        pushed_set = F.array(*[F.array(F.lit(a), F.lit(b), F.lit(c)) for (a, b, c) in pushed_list]) \
            if pushed_list else F.array().cast("array<array<string>>")
        error_set = F.array(*[F.array(F.lit(a), F.lit(b), F.lit(c)) for (a, b, c) in error_list]) \
            if error_list else F.array().cast("array<array<string>>")
        keytuple = F.array(F.col("dtc_request_name"),
                           F.trim(F.col("lf_style_number")), F.trim(F.col("color")))
        upd = upd.withColumn(
            "sync_status",
            F.when(F.array_contains(error_set, keytuple), F.lit("error"))
             .when(F.array_contains(pushed_set, keytuple), F.lit("pushed"))
             .otherwise(F.col("sync_status"))
        )
        if "pushed_at" not in upd.columns:
            upd = upd.withColumn("pushed_at", F.lit(None).cast("timestamp"))
        upd = upd.withColumn(
            "pushed_at",
            F.when(F.array_contains(pushed_set, keytuple), F.lit(now)).otherwise(F.col("pushed_at"))
        )
        upd.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(staging_full)
        print(f"✅ Updated staging sync_status (pushed={len(pushed_list)}, error={len(error_list)})")

# COMMAND ----------

print("\n" + "=" * 80)
print("PUSH SUMMARY")
print("=" * 80)
for k, v in totals.items():
    print(f"  {k}: {v}")
if dry_run:
    print("\n⚠️  DRY RUN - no PATCH calls were made. Set dry_run=false to apply.")
print(f"\nReview details:\n  SELECT * FROM {sync_log_full} WHERE run_id = '{run_id}' ORDER BY status DESC;")
print("\n✅ Push complete")
