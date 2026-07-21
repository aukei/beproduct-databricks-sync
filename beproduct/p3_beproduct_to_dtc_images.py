# Databricks notebook source
"""
BeProduct -> DTC Image Upload (Phase 3)
=======================================

Uploads the BeProduct front image into each in-scope DTC request's "Style Image"
cell. The Style Image column is a BINARY cell and cannot ride the JSON sheetData
PATCH used by Phase 1; it has its own multipart endpoint that operates on an
EXISTING row:

    POST /v1/sheets/{sheetId}/views/{viewId}/images
         ?rowindex={int}&columnname=Style Image
    body: multipart/form-data, image bytes as a file part   -> (status TBD)

Because the endpoint targets an existing row by rowIndex, this MUST run AFTER
beproduct_to_dtc_push (Phase 1), which creates/updates the rows. The orchestrator
runs a fresh DTC pull (pull_masters_to_delta) just before this so dtc_wip_<cust>
reflects the rows Phase 1 just inserted; this notebook itself ALSO re-reads each
sheet live (connector.get_sheet) so it sees the freshest rowIndex + Style Image
state and never acts on stale data.

Workflow (per in-scope resolved request):
  1. Reload the sheet live and note, per row, whether "Style Image" is populated.
  2. Match each row to its BeProduct staging row on (BP Style#, Color / Wash).
     Phase 6: match key changed from (LF Style#, ...) to (BP Style#, ...).
  3. For rows that are blank-image AND whose BeProduct row has a valid
     front_image_url: download the image from the BeProduct CDN, then POST it to
     the DTC image endpoint at that rowIndex / columnname="Style Image".
  4. Log every decision (uploaded / skipped / failed) to the shared sync log.

Image sync is strictly BeProduct -> DTC and one-directional: it never clears a
DTC image and never reads an image back into BeProduct. Already-imaged rows are
left untouched, so re-runs are idempotent.

Parameters:
  - catalog / schema (default: lft / beproduct)
  - staging_table (default: beproduct_to_dtc_staging)
  - dtc_environment (default: uat)
  - dtc_workspace (default: KTB)
  - dry_run (default: true)  -- when true, computes & logs but does NOT upload
  - http_timeout (default: 30)  -- seconds for the CDN download
  - max_uploads (default: 0)  -- 0 = no cap; >0 caps uploads this run (safety)
"""

# COMMAND ----------

import sys
import subprocess

# Pillow is needed to transcode non-native images (e.g. webp -> png) since DTC's
# image endpoint rejects webp. Install quietly if missing.
try:
    from PIL import Image  # noqa: F401
except Exception:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "Pillow"])

sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

import io
import json
import uuid
from datetime import datetime, timezone

import requests
from PIL import Image

from connectors.dtc import DTCConnector
from sync import phase1, phase3
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Explicit log schema (matches beproduct_to_dtc_sync_log, shared with Phase 1).
SYNC_LOG_SCHEMA = StructType([
    StructField("log_time", TimestampType()),
    StructField("run_id", StringType()),
    StructField("stage", StringType()),
    StructField("environment", StringType()),
    StructField("dtc_request_name", StringType()),
    StructField("request_id", StringType()),
    StructField("operation", StringType()),
    # Phase 6 note: this column now stores bp_style_number values (renamed in BP).
    # Column kept as "lf_style_number" for Delta schema backward compatibility.
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
dbutils.widgets.text("staging_table", "beproduct_to_dtc_staging", "Staging Table")
dbutils.widgets.text("dtc_environment", "uat", "DTC Environment")
dbutils.widgets.text("dtc_workspace", "KTB", "DTC Workspace Name")
dbutils.widgets.text("dry_run", "true", "Dry Run (true/false)")
dbutils.widgets.text("http_timeout", "30", "CDN download timeout (s)")
dbutils.widgets.text("max_uploads", "0", "Max uploads this run (0 = no cap)")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
staging_table = dbutils.widgets.get("staging_table")
environment = dbutils.widgets.get("dtc_environment").strip().lower()
workspace = dbutils.widgets.get("dtc_workspace").strip()
dry_run = dbutils.widgets.get("dry_run").strip().lower() == "true"
http_timeout = int(dbutils.widgets.get("http_timeout"))
max_uploads = int(dbutils.widgets.get("max_uploads"))

staging_full = f"{catalog}.{schema}.{staging_table}"
mapping_full = f"{catalog}.{schema}.dtc_request_mapping"
sync_log_full = f"{catalog}.{schema}.beproduct_to_dtc_sync_log"

run_id = str(uuid.uuid4())
now = datetime.now(timezone.utc)

print("=" * 80)
print("BEPRODUCT -> DTC IMAGE UPLOAD (Phase 3)")
print("=" * 80)
print(f"  Staging:  {staging_full}")
print(f"  Mapping:  {mapping_full}")
print(f"  Sync log: {sync_log_full}")
print(f"  Env: {environment} | dry_run={dry_run} | http_timeout={http_timeout} | max_uploads={max_uploads or '∞'}")
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

# Resolved mapping (from dtc_request_manager). If empty -> nothing to image.
try:
    df_map = spark.table(mapping_full).where(F.col("environment") == environment)
except Exception:
    df_map = None
if df_map is None or df_map.count() == 0:
    print("⚠️  No resolved requests in mapping - run dtc_request_manager first. Exiting.")
    dbutils.notebook.exit("NO_RESOLVED_REQUESTS")

mapping = {r.dtc_request_name: r for r in df_map.collect()}

# Staging rows carry front_image_url + the match key. We consider ALL staging
# rows here (not just sync_status='pending'): an image can be missing on a row
# whose text fields were already pushed in an earlier run.
df_staging = spark.table(staging_full)
total_staging = df_staging.count()
print(f"Staging rows available: {total_staging}")
if total_staging == 0:
    dbutils.notebook.exit("NO_STAGING_ROWS")

# COMMAND ----------

# DTC connector.
secret_key = f"dtc_api_key_{environment}"
api_key = dbutils.secrets.get(scope="beproduct", key=secret_key)
connector = DTCConnector(api_key=api_key, environment=environment, workspace_name=workspace)

LOG_COLS = ["log_time", "run_id", "stage", "environment", "dtc_request_name", "request_id",
            "operation", "lf_style_number", "color", "match_key", "status", "reason",
            "detail", "payload"]

def log(rows, name, request_id, operation, key, status, reason="", detail="", payload=None):
    lf, color = (key or (None, None))
    rows.append((now, run_id, "images", environment, name, request_id, operation,
                 lf, color, f"{lf} | {color}", status, reason, detail,
                 (json.dumps(payload) if payload is not None else None)))


def download_image(url, timeout):
    """Fetch image bytes from the BeProduct CDN; return (bytes, content_type)."""
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip() or None
    return resp.content, ctype


_CT_EXT = {"image/jpeg": "image.jpg", "image/png": "image.png"}

def prepare_for_dtc(img_bytes, content_type, url):
    """
    Decide + produce the bytes to upload, honoring DTC's accepted types.

    Returns (out_bytes, out_content_type, filename, note) on success, or
    (None, None, None, reason) to SKIP. DTC accepts jpg/png natively; webp/gif/
    bmp/tiff are transcoded to PNG; vector/unknown are skipped.
    """
    enc = phase3.classify_image_type(content_type, url)
    if enc.action == "skip":
        return None, None, None, enc.reason

    if enc.action == "upload":
        # Trust classification but guard against a mislabeled/corrupt payload.
        try:
            im = Image.open(io.BytesIO(img_bytes)); im.load()
        except Exception as e:
            return None, None, None, f"decode_failed:{str(e)[:80]}"
        return img_bytes, enc.content_type, _CT_EXT.get(enc.content_type, "image.png"), ""

    # action == 'convert' -> transcode to PNG.
    try:
        im = Image.open(io.BytesIO(img_bytes)); im.load()
    except Exception as e:
        return None, None, None, f"decode_failed:{str(e)[:80]}"
    out = io.BytesIO()
    try:
        im.save(out, format="PNG")
    except Exception:
        # palette/CMYK/LA etc. -> normalise to RGBA then save
        im.convert("RGBA").save(out, format="PNG")
    return out.getvalue(), "image/png", "image.png", enc.reason

# COMMAND ----------

log_rows = []
totals = {"requests": 0, "uploads_ok": 0, "uploads_failed": 0, "converted": 0,
          "skipped": 0, "already_imaged_rows": 0, "download_failed": 0,
          "unsupported_type": 0}
uploaded_count = 0  # respects max_uploads cap
cap_reached = False

for name, m in mapping.items():
    if cap_reached:
        break
    totals["requests"] += 1
    request_id, sheet_id, view_id = m.request_id, m.sheet_id, m.view_id
    print(f"\n--- {name}  (request_id={request_id}) ---")

    # 1. Reload sheet live -> freshest rowIndex + Style Image state.
    try:
        sheet = connector.get_sheet(sheet_id, view_id)
        dtc_rows = sheet.get("sheetData", [])
    except Exception as e:
        print(f"  ❌ Failed to read DTC sheet: {e}")
        log(log_rows, name, request_id, "ERROR", None, "error", "sheet_read_failed", str(e)[:300])
        continue

    already = sum(1 for r in dtc_rows if phase3.is_image_populated(r))
    totals["already_imaged_rows"] += already

    # 2. BeProduct staging rows for this request -> source of front_image_url.
    sdf = df_staging.where(F.col("dtc_request_name") == name)
    bp_rows = [r.asDict() for r in sdf.collect()]

    # 3. Plan: blank-image rows whose BeProduct source has a valid URL.
    plan = phase3.compute_image_uploads(dtc_rows, bp_rows)
    print(f"  rows={len(dtc_rows)} already_imaged={already} "
          f"plan={plan.summary()}")

    # Informational skips (blank image but no usable source URL / no rowIndex).
    for sk in plan.skips:
        log(log_rows, name, request_id, "SKIP", sk.match_key, "skipped", sk.reason, sk.detail)
        totals["skipped"] += 1

    # 4. Upload each planned image (download CDN -> POST multipart).
    for op in plan.uploads:
        if max_uploads and uploaded_count >= max_uploads:
            print(f"  ⏸  max_uploads={max_uploads} reached; stopping.")
            cap_reached = True
            break

        # Download from BeProduct CDN.
        try:
            raw_bytes, ctype = download_image(op.image_url, http_timeout)
        except Exception as e:
            log(log_rows, name, request_id, "IMAGE_UPLOAD", op.match_key, "error",
                "download_failed", str(e)[:300], {"url": op.image_url})
            totals["download_failed"] += 1
            continue

        # Classify + transcode (webp/etc -> png); skip vector/unknown types.
        img_bytes, out_ctype, fname, note = prepare_for_dtc(raw_bytes, ctype, op.image_url)
        if img_bytes is None:
            log(log_rows, name, request_id, "IMAGE_UPLOAD", op.match_key, "skipped",
                "unsupported_type", note, {"url": op.image_url, "content_type": ctype})
            totals["unsupported_type"] += 1
            continue
        converted = note.startswith("transcode")
        if converted:
            totals["converted"] += 1

        # Upload to DTC.
        try:
            if not dry_run:
                connector.upload_row_image(
                    sheet_id, view_id, op.row_index, img_bytes,
                    column_name=phase1.STYLE_IMAGE_COL,
                    filename=fname, content_type=out_ctype,
                )
            log(log_rows, name, request_id, "IMAGE_UPLOAD", op.match_key, "ok",
                "dry_run" if dry_run else ("converted" if converted else ""),
                f"rowIndex={op.row_index} bytes={len(img_bytes)} type={out_ctype}"
                + (f" ({note})" if converted else ""),
                {"url": op.image_url, "rowIndex": op.row_index})
            uploaded_count += 1
            totals["uploads_ok"] += 1
        except Exception as e:
            log(log_rows, name, request_id, "IMAGE_UPLOAD", op.match_key, "error",
                "upload_failed", str(e)[:300],
                {"url": op.image_url, "rowIndex": op.row_index})
            totals["uploads_failed"] += 1

connector.close()

# COMMAND ----------

# Write the sync log.
if log_rows:
    spark.createDataFrame(log_rows, SYNC_LOG_SCHEMA).write.format("delta").mode("append").saveAsTable(sync_log_full)
    print(f"\n✅ Logged {len(log_rows)} sync-log rows to {sync_log_full}")

# COMMAND ----------

print("\n" + "=" * 80)
print("IMAGE UPLOAD SUMMARY")
print("=" * 80)
for k, v in totals.items():
    print(f"  {k}: {v}")
if dry_run:
    print("\n⚠️  DRY RUN - no images were uploaded. Set dry_run=false to apply.")
print(f"\nReview details:\n  SELECT * FROM {sync_log_full} WHERE run_id = '{run_id}' ORDER BY status DESC;")
print("\n✅ Image sync complete")

dbutils.notebook.exit(
    f"OK uploads_ok={totals['uploads_ok']} failed={totals['uploads_failed'] + totals['download_failed']} "
    f"skipped={totals['skipped']}"
)
