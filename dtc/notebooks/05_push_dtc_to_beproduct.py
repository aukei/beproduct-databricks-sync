# Databricks notebook source
"""
Phase 2: Push DTC-owned fields back to BeProduct (DTC -> BeProduct)
==================================================================

Reverse of Phase 1. A small set of DTC-OWNED columns are written from the pulled
DTC data back into the corresponding BeProduct style:

    DTC column                  BeProduct target (fieldId)             level
    Customer Style#             customer_style_number                  header
    Main Vendor (Sampling)      parent_vendor                          header
    Main Factory (Sampling)     factory                                header
    Lot#                        drawing_number_walmart                 colorway
    Main Factory Customer ID    (no target yet -> skipped/logged)      -

Phase 6 update (2026-06-26):
    "Legacy Code" replaced by "Customer Style#" as the DTC->BP vehicle for
    customer_style_number. "Legacy Code" is now BeProduct->DTC (Phase 1).

Inputs (already produced by the daily pipeline):
  - DTC pulled table:  lft.beproduct.dtc_wip_<customer>   (pull_requests_to_delta)
        -> request_reference, bp_style_number, color_wash, data_json, row_id
  - BeProduct staging: lft.beproduct.<staging_table>      (beproduct_to_dtc_transform)
        -> dtc_request_name, bp_style_number, color, colorway_id, beproduct_style_id

Identity resolution: DTC row (request, bp_style, color) -> BeProduct (style_id, colorway_id)
via the staging table. Current BeProduct values are read LIVE (attributes_get) per
candidate style so NOOP diffing is accurate (the staging 'lot_code' is the legacy
header value, not the colorway Lot#). Writes go through the BeProduct SDK:

    api.style.attributes_update(header_id=<style>, fields={...}, colorways=[{id, fields}])

The deterministic mapping/diff lives in dtc/python/sync/phase2.py (unit-tested).

Parameters:
  - catalog / schema (default: lft / beproduct)
  - customer (default: KTB)               -- selects dtc_wip_<customer>
  - staging_table (default: beproduct_to_dtc_staging)
  - dtc_environment (default: uat)         -- log/labelling only
  - dry_run (default: true)                -- compute & log but do NOT write BeProduct
  - push_blanks (default: false)           -- if true, blank DTC values clear BeProduct
"""

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

import json
import uuid
import subprocess
from datetime import datetime, timezone

# BeProduct SDK (installed at runtime, like the style sync job).
try:
    from beproduct.sdk import BeProduct
except Exception:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "beproduct"])
    from beproduct.sdk import BeProduct

from sync import phase1, phase2
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Explicit log schema so createDataFrame never infers from all-NULL columns
# (raises CANNOT_DETERMINE_TYPE). Matches the sync_log CREATE TABLE below.
SYNC_LOG_SCHEMA = StructType([
    StructField("log_time", TimestampType()),
    StructField("run_id", StringType()),
    StructField("environment", StringType()),
    StructField("customer", StringType()),
    StructField("beproduct_style_id", StringType()),
    StructField("colorway_id", StringType()),
    # Phase 6 note: this column now stores bp_style_number values (renamed in BP).
    # Column kept as "lf_style_number" for Delta schema backward compatibility.
    StructField("lf_style_number", StringType()),
    StructField("color", StringType()),
    StructField("dtc_request_name", StringType()),
    StructField("dtc_row_id", StringType()),
    StructField("scope", StringType()),
    StructField("operation", StringType()),
    StructField("status", StringType()),
    StructField("reason", StringType()),
    StructField("detail", StringType()),
    StructField("payload", StringType()),
])

dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("customer", "KTB", "Customer")
dbutils.widgets.text("staging_table", "beproduct_to_dtc_staging", "Staging Table")
dbutils.widgets.text("dtc_environment", "uat", "DTC Environment")
dbutils.widgets.text("dry_run", "true", "Dry Run (true/false)")
dbutils.widgets.text("push_blanks", "false", "Clear BeProduct on blank DTC value")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
customer = dbutils.widgets.get("customer").strip()
staging_table = dbutils.widgets.get("staging_table").strip()
environment = dbutils.widgets.get("dtc_environment").strip().lower()
dry_run = dbutils.widgets.get("dry_run").strip().lower() == "true"
push_blanks = dbutils.widgets.get("push_blanks").strip().lower() == "true"

dtc_wip_full = f"{catalog}.{schema}.dtc_wip_{customer.lower()}"
staging_full = f"{catalog}.{schema}.{staging_table}"
sync_log_full = f"{catalog}.{schema}.dtc_to_beproduct_sync_log"

run_id = str(uuid.uuid4())
now = datetime.now(timezone.utc)

print("=" * 80)
print("PHASE 2: DTC -> BEPRODUCT PUSHBACK")
print("=" * 80)
print(f"  DTC source:  {dtc_wip_full}")
print(f"  Staging:     {staging_full}")
print(f"  Sync log:    {sync_log_full}")
print(f"  Env: {environment} | customer: {customer} | dry_run={dry_run} | push_blanks={push_blanks}")
print(f"  run_id: {run_id}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {sync_log_full} (
  log_time TIMESTAMP, run_id STRING, environment STRING, customer STRING,
  beproduct_style_id STRING, colorway_id STRING,
  lf_style_number STRING, color STRING, dtc_request_name STRING, dtc_row_id STRING,
  scope STRING, operation STRING, status STRING, reason STRING,
  detail STRING, payload STRING
) USING DELTA
TBLPROPERTIES ('description'='DTC -> BeProduct (Phase 2) pushback log')
""")

log_rows = []
def log(beproduct_style_id, colorway_id, lf, color, req, row_id, scope, operation,
        status, reason="", detail="", payload=None):
    log_rows.append((now, run_id, environment, customer, beproduct_style_id, colorway_id,
                     lf, color, req, row_id, scope, operation, status, reason, detail,
                     json.dumps(payload) if payload is not None else None))

LOG_COLS = ["log_time", "run_id", "environment", "customer", "beproduct_style_id",
            "colorway_id", "lf_style_number", "color", "dtc_request_name", "dtc_row_id",
            "scope", "operation", "status", "reason", "detail", "payload"]

# COMMAND ----------

# Identity map from staging: (request, norm bp_style, norm color) -> (style_id, colorway_id)
# Phase 6: match key is now bp_style_number (was lf_style_number).
try:
    sdf = spark.table(staging_full).select(
        "dtc_request_name", "bp_style_number", "color", "colorway_id", "beproduct_style_id")
except Exception as e:
    raise RuntimeError(f"Cannot read staging table {staging_full}: {e}")

identity = {}
for r in sdf.collect():
    key = (r["dtc_request_name"], phase1.norm(r["bp_style_number"]), phase1.norm(r["color"]))
    # last write wins; staging keys are unique per (request, bp_style, color)
    identity[key] = (r["beproduct_style_id"], r["colorway_id"])
print(f"Staging identity rows: {len(identity)}")

# COMMAND ----------

# DTC pulled rows -> extract the 5 DTC-owned values from data_json (exact names).
try:
    ddf = spark.table(dtc_wip_full)
except Exception as e:
    print(f"⚠️  DTC pulled table not found ({e}); nothing to push.")
    dbutils.notebook.exit("NO_DTC_TABLE")

dtc_rows = ddf.select(
    "request_reference", "bp_style_number", "color_wash", "row_id", "data_json"
).collect()
print(f"DTC rows pulled: {len(dtc_rows)}")

joined = []        # phase2 input rows
unmatched = 0
for r in dtc_rows:
    req = r["request_reference"]
    bp_style = phase1.norm(r["bp_style_number"])    # Phase 6: was lf_style_number
    color = phase1.norm(r["color_wash"])
    try:
        full = json.loads(r["data_json"]) if r["data_json"] else {}
    except Exception:
        full = {}
    dtc_vals = {c: full.get(c) for c in phase2.ALL_PHASE2_COLUMNS}
    # skip rows with nothing to push
    if not any(phase1.norm(v) is not None for v in dtc_vals.values()):
        continue
    ident = identity.get((req, bp_style, color))
    if not ident:
        unmatched += 1
        log(None, None, bp_style, color, req, r["row_id"], "match", "UNMATCHED", "error",
            "no_beproduct_identity",
            "DTC row has no matching BeProduct staging (style moved/not in BeProduct?)")
        continue
    style_id, colorway_id = ident
    joined.append({
        "beproduct_style_id": style_id, "colorway_id": colorway_id,
        "bp_style_number": bp_style, "color": color,       # Phase 6: was lf_style_number
        "_req": req, "_row_id": r["row_id"], "dtc": dtc_vals,
    })

print(f"Candidate rows with DTC values: {len(joined)} | unmatched: {unmatched}")

# COMMAND ----------

# Read current BeProduct values LIVE (per candidate style) for accurate NOOP diff.
client_id = dbutils.secrets.get(scope="beproduct", key="client_id")
client_secret = dbutils.secrets.get(scope="beproduct", key="client_secret")
refresh_token = dbutils.secrets.get(scope="beproduct", key="refresh_token")
company_domain = dbutils.secrets.get(scope="beproduct", key="company_domain")
api = BeProduct(client_id=client_id, client_secret=client_secret,
                refresh_token=refresh_token, company_domain=company_domain)

# fieldId -> DTC column (to read current values back out of the BeProduct record)
HDR_ID_TO_COL = {v: k for k, v in phase2.REVERSE_HEADER_FIELDS.items()}
CW_FID = phase2.REVERSE_COLORWAY_FIELDS["Lot#"]

style_cache = {}   # style_id -> {"header": {fid: val}, "cw_lot": {cw_id: val}}
def load_current(style_id):
    if style_id in style_cache:
        return style_cache[style_id]
    rec = api.style.attributes_get(header_id=style_id)
    header = {f.get("id"): f.get("value") for f in rec.get("headerData", {}).get("fields", [])}
    cw_lot = {}
    for cw in rec.get("colorways", []) or []:
        cw_lot[cw.get("id")] = (cw.get("fields") or {}).get(CW_FID)
    style_cache[style_id] = {"header": header, "cw_lot": cw_lot}
    return style_cache[style_id]

for j in joined:
    cur = load_current(j["beproduct_style_id"])
    bp = {}
    for fid, col in HDR_ID_TO_COL.items():
        bp[col] = cur["header"].get(fid)
    bp["Lot#"] = cur["cw_lot"].get(j["colorway_id"])
    j["bp"] = bp

# COMMAND ----------

# Compute the deterministic plan and the SDK calls.
plan = phase2.build_beproduct_updates(joined, push_blanks=push_blanks)
print("Phase 2 plan:", plan.summary())

for ex in plan.exceptions:
    log(ex.style_id, None, ex.key[0], ex.key[1], None, None, "compute", ex.reason,
        "error", ex.reason, ex.detail)

calls = phase2.to_sdk_calls(plan)
print(f"Styles to update: {len(calls)}")

# Map style_id -> a representative joined row (for logging bp_style/req).
rep = {}
for j in joined:
    rep.setdefault(j["beproduct_style_id"], j)

# COMMAND ----------

ok = failed = 0
for c in calls:
    sid = c["header_id"]
    r = rep.get(sid, {})
    try:
        if not dry_run:
            api.style.attributes_update(
                header_id=sid, fields=c["fields"], colorways=c["colorways"])
        ok += 1
        log(sid, None, r.get("bp_style_number"), r.get("color"), r.get("_req"),
            r.get("_row_id"), "push", "UPDATE", "ok",
            "dry_run" if dry_run else "",
            f"header={len(c['fields'])} colorways={len(c['colorways'])}",
            {"fields": c["fields"], "colorways": c["colorways"]})
    except Exception as e:
        failed += 1
        log(sid, None, r.get("bp_style_number"), r.get("color"), r.get("_req"),
            r.get("_row_id"), "push", "UPDATE", "error", "attributes_update_failed",
            str(e)[:300], {"fields": c["fields"], "colorways": c["colorways"]})

# COMMAND ----------

if log_rows:
    spark.createDataFrame(log_rows, SYNC_LOG_SCHEMA).write.format("delta").mode("append").saveAsTable(sync_log_full)
    print(f"✅ Logged {len(log_rows)} rows to {sync_log_full}")

print("\n" + "=" * 80)
print("PHASE 2 PUSHBACK SUMMARY")
print("=" * 80)
print(f"  candidate rows:        {len(joined)}")
print(f"  unmatched (skipped):   {unmatched}")
for k, v in plan.summary().items():
    print(f"  {k}: {v}")
print(f"  styles pushed ok:      {ok}")
print(f"  styles failed:         {failed}")
if dry_run:
    print("\n⚠️  DRY RUN - no BeProduct writes were made. Set dry_run=false to apply.")
print(f"\nReview: SELECT * FROM {sync_log_full} WHERE run_id = '{run_id}' ORDER BY status DESC;")
print("\n✅ Phase 2 pushback complete")
