# Databricks notebook source
"""
Phase 0 — Pull DTC "XTS Master" (Supplier/Factory) -> Delta
=============================================================

Logically precedes Style/Material/Costing sync: pulls DTC's partner master
data (Supplier/Factory) from workspace "KTB", document "XTS Master", so
`beproduct/p0_xts_master_to_directory_upsert.py` can upsert it into
`lft.beproduct.beproduct_directory` before any Style/Material/Costing steps run.

Pulls exactly 2 requests by EXACT `requestReference` (never the "(BACKUP)"-named
siblings that also exist in this document):

    "XTS Supplier Master"  -> partner_type SUPPLIER  (view "Supplier")
    "XTS Factory Master"   -> partner_type FACTORY   (view "Factory")

SCOPE (clarified 2026-08-28): "XTS Mill Master" is intentionally out of scope
for now — see `dtc/python/sync/xts_master.py` module docstring.

IMPORTANT (live-validated 2026-08-28, see `dtc/python/sync/xts_master.py`
docstring for full detail): this document is a request-sharing/access-config
sheet, not a rich vendor-master sheet — none of address/state/zip/city/phone/
fax/website/notes exist anywhere in it. Partner type comes from WHICH
request/view a row was read from, never from the sheet's own "Type" column
(which is instead used to filter out brand-level config rows — see
xts_master.is_brand_row()).

Output (full overwrite every run — this document is small, ~40-60 rows total):
    lft.beproduct.dtc_xts_master_ktb        one row per (partner_type, sheet row)
    lft.beproduct.dtc_xts_master_registry   row_count / last_extracted per request

Parameters:
  - dtc_environment (default: uat)
  - dtc_workspace (default: KTB)
  - xts_document (default: XTS Master)

IMPORTANT — widget name (live-debugged 2026-09-01): this widget is
deliberately named "xts_document", NOT "dtc_document". Databricks Jobs
auto-injects EVERY job-level parameter into EVERY task's widgets by name; the
job also has an unrelated job-level parameter literally named "dtc_document"
(default "KTB WIP", used by the WIP-pulling tasks). If this notebook's widget
were also named "dtc_document", that auto-injection SILENTLY WINS OVER this
task's own explicit base_parameters mapping in scripts/deploy_job.py, causing
this notebook to search the wrong DTC document ("KTB WIP" instead of "XTS
Master") on every scheduled run — confirmed live by decoding an actual run's
notebook output, which printed "Document : KTB WIP" and "0/2 exact matches"
despite the job's "xts_document" parameter correctly resolving to "XTS
Master" the whole time. Do not rename this widget back to "dtc_document".
  - catalog / schema (default: lft / beproduct)
"""

# COMMAND ----------

import sys

# ── Python module root ──────────────────────────────────────────────────────
# The `sync` / `connectors` / `client` packages are deployed as Workspace FILES
# (not notebooks) and added to sys.path here. This is a PARAMETER rather than a
# literal because v2 deploys them under its own workspace root
# (/Workspace/Repos/beproduct-sync-v2/DTC/python), so checking out the v2 branch
# can never change what the live v1 job imports. The default is the v1 path, so
# a task that does not pass `module_path` behaves exactly as before.
# See docs/MIGRATION_V1_V2.md ("Workspace isolation").
_DEFAULT_MODULE_PATH = "/Workspace/Repos/beproduct-sync/DTC/python"
dbutils.widgets.text("module_path", _DEFAULT_MODULE_PATH, "Python module root")
_MODULE_PATH = (dbutils.widgets.get("module_path") or "").strip() or _DEFAULT_MODULE_PATH
# The lowercase-"dtc" sibling is a harmless no-op when it does not exist; it
# preserves the defensive both-cases behaviour some notebooks already had.
for _p in (_MODULE_PATH, _MODULE_PATH.replace("/DTC/", "/dtc/")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ── Run flag (v2 has NO condition tasks) ────────────────────────────────────
# v2 reads every run_* flag HERE and exits as a genuine SUCCESS no-op when
# disabled, instead of sitting behind a gate_* condition task. Databricks
# propagates a condition task's EXCLUDED outcome to every downstream dependent
# UNCONDITIONALLY, ignoring run_if -- and v2's chain is linear enough that one
# gate evaluating false would silently excise the whole DTC push. v1 learned
# this twice (gate_phase1, gate_phase10). See docs/PIPELINE.md design rule 4.
#
# Defaults to "true", so an ad-hoc/interactive run that passes nothing is
# completely unaffected; only an explicit "false" skips.
dbutils.widgets.text("run_phase0", "true", "Run Phase 0 (false = no-op)")
if (dbutils.widgets.get("run_phase0") or "true").strip().lower() != "true":
    print("run_phase0=false -- skipping entirely (no reads, no writes).")
    dbutils.notebook.exit("SKIPPED_run_phase0_false")

import json
from datetime import datetime, timezone

from connectors.dtc import DTCConnector
from sync import xts_master as xm
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType,
)

XTS_SCHEMA = StructType([
    StructField("partner_type",       StringType()),
    StructField("name",               StringType()),
    StructField("directory_id",       StringType()),
    StructField("country",            StringType()),
    StructField("address",            StringType()),
    StructField("state",              StringType()),
    StructField("zip",                StringType()),
    StructField("city",               StringType()),
    StructField("phone",              StringType()),
    StructField("fax",                StringType()),
    StructField("website",            StringType()),
    StructField("notes",              StringType()),
    StructField("row_id",             StringType()),
    StructField("row_index",          LongType()),
    StructField("request_id",         StringType()),
    StructField("request_reference",  StringType()),
    StructField("view_name",          StringType()),
    StructField("extracted_at",       TimestampType()),
    StructField("data_json",          StringType()),
])

REGISTRY_SCHEMA = StructType([
    StructField("environment",        StringType()),
    StructField("workspace",          StringType()),
    StructField("document",           StringType()),
    StructField("partner_type",       StringType()),
    StructField("request_id",         StringType()),
    StructField("request_reference",  StringType()),
    StructField("sheet_id",           StringType()),
    StructField("view_id",            StringType()),
    StructField("view_name",          StringType()),
    StructField("row_count",          LongType()),
    StructField("last_extracted",     TimestampType()),
    StructField("msgs",               StringType()),
    StructField("updated_at",         TimestampType()),
])

# COMMAND ----------

dbutils.widgets.text("dtc_environment", "uat",         "DTC Environment")
dbutils.widgets.text("dtc_workspace",   "KTB",         "DTC Workspace")
dbutils.widgets.text("xts_document",    "XTS Master",  "DTC Document name (XTS Master, NOT dtc_document -- see module docstring)")
dbutils.widgets.text("catalog",         "lft",         "Catalog")
dbutils.widgets.text("schema",          "beproduct",   "Schema")

environment = dbutils.widgets.get("dtc_environment").strip().lower()
workspace   = dbutils.widgets.get("dtc_workspace").strip()
document    = dbutils.widgets.get("xts_document").strip()
catalog     = dbutils.widgets.get("catalog").strip()
schema      = dbutils.widgets.get("schema").strip()

xts_table_full = f"{catalog}.{schema}.dtc_xts_master_ktb"
xts_reg_full   = f"{catalog}.{schema}.dtc_xts_master_registry"
now = datetime.now(timezone.utc)

print("=" * 72)
print("PHASE 0 — Pull DTC XTS Master (Supplier/Factory) -> Delta")
print("=" * 72)
print(f"  Document  : {document}  (workspace={workspace}, env={environment})")
print(f"  Requests  : {list(xm.XTS_REQUESTS.keys())}")
print(f"  Output    : {xts_table_full}")
print(f"  Registry  : {xts_reg_full}")

# COMMAND ----------

secret_key = f"dtc_api_key_{environment}"
api_key    = dbutils.secrets.get(scope="beproduct", key=secret_key)
connector  = DTCConnector(api_key=api_key, environment=environment, workspace_name=workspace)
print("✅ DTCConnector ready")

# COMMAND ----------

# ── Discover the exact requests (never the "(BACKUP)"-named siblings) ────────
print(f"\nSearching '{document}' for the {len(xm.XTS_REQUESTS)} exact request name(s)…")
raw_reqs = connector.search_requests(workspace, document)
by_ref = {r.get("requestReference"): r for r in raw_reqs}
print(f"  {len(raw_reqs)} total request(s) found in document; "
      f"{sum(1 for k in xm.XTS_REQUESTS if k in by_ref)}/{len(xm.XTS_REQUESTS)} exact matches")

eligible = []  # (ref, partner_type, req_id, sheet_id, view_id, view_name)
skipped = []

for ref, spec in xm.XTS_REQUESTS.items():
    partner_type = spec["partner_type"]
    want_view = spec["view_name"]
    req = by_ref.get(ref)
    if req is None:
        skipped.append((ref, partner_type, "request not found in document"))
        print(f"  ❌ {ref}: not found")
        continue
    req_id = req.get("requestId") or req.get("id", "")
    if str(req.get("requestIsActive", "Y")).upper() not in ("Y", "TRUE", "1"):
        skipped.append((ref, partner_type, "request_is_active is not Y"))
        print(f"  ❌ {ref}: not active")
        continue
    try:
        views = connector.get_views(req_id)
        view = next((v for v in views
                     if (v.get("viewName") or v.get("name")) == want_view), None)
        if view is None:
            found = [v.get("viewName") or v.get("name") for v in views]
            skipped.append((ref, partner_type, f"view {want_view!r} not found (have {found})"))
            print(f"  ❌ {ref}: view {want_view!r} not found (have {found})")
            continue
        sheet_id = req.get("sheetId") or req.get("id", "")
        view_id = view.get("viewId") or view.get("id", "")
        eligible.append((ref, partner_type, req_id, sheet_id, view_id, want_view))
        print(f"  ✅ {ref}  partner_type={partner_type}  view={want_view!r}")
    except Exception as e:
        skipped.append((ref, partner_type, str(e)))
        print(f"  ❌ {ref}: {e}")

if skipped:
    print(f"\n  Skipped {len(skipped)}:")
    for ref, ptype, reason in skipped:
        print(f"    {ref} ({ptype}): {reason}")

# COMMAND ----------

# ── Fetch each sheet, extract via the pure xts_master helpers ────────────────
# NOTE: xts_master.extract_directory_row() drops two kinds of rows and both
# are counted separately below so nothing silently vanishes from the log:
#   - unnamed rows (no name = can't be Directory-matched)
#   - "brand row" config entries interleaved in the Supplier sheet
#     (Type="Brand" - NOT a real company; see xts_master.py module docstring
#     for the live-verified 2026-08-28 discovery)
all_rows = []
fetch_errors = {}

for ref, partner_type, req_id, sheet_id, view_id, view_name in eligible:
    try:
        sheet = connector.get_sheet(sheet_id, view_id)
        raw_rows = sheet.get("sheetData", [])
        n_extracted = 0
        n_brand_excluded = 0
        n_unnamed = 0
        for raw in raw_rows:
            if xm.is_brand_row(partner_type, raw):
                n_brand_excluded += 1
                continue
            rec = xm.extract_directory_row(
                partner_type, raw, request_id=req_id, request_reference=ref
            )
            if rec is None:
                n_unnamed += 1
                continue
            rec["view_name"] = view_name
            rec["extracted_at"] = now
            rec["data_json"] = json.dumps(raw, default=str)
            all_rows.append(rec)
            n_extracted += 1
        print(f"  ✅ {ref:<22} raw_rows={len(raw_rows):3d}  kept={n_extracted:3d}  "
              f"brand_rows_excluded={n_brand_excluded:3d}  unnamed_skipped={n_unnamed:3d}")
    except Exception as e:
        fetch_errors[req_id] = str(e)
        print(f"  ❌ {ref}: fetch failed: {e}")

total_rows = len(all_rows)
print(f"\n✅ Fetch complete: {total_rows} company row(s) across {len(eligible)} request(s) "
      f"({len(fetch_errors)} fetch errors)")

# COMMAND ----------

# ── Duplicate (name, partner_type) audit (informational here; dedupe happens
#    in the upsert notebook right before the MERGE, so nothing is silently
#    dropped twice). NOTE: BeProduct's Directory key is (name, partner_type)
#    TOGETHER - the same name across DIFFERENT partner types (e.g. the same
#    entity as both a SUPPLIER and a FACTORY) is expected and NOT flagged
#    here; only a truly repeated (name, partner_type) pair is a collision. ──
dups = xm.find_duplicate_keys(all_rows)
if dups:
    print(f"\n⚠️  {len(dups)} (name, partner_type) pair(s) appear on 2+ rows (BeProduct "
          f"matches Directory by name+type together - the upsert step will need to pick "
          f"one, see beproduct/p0_xts_master_to_directory_upsert.py):")
    for (name, ptype), rows in dups.items():
        print(f"    {name!r} [{ptype}]: {[r['request_reference'] for r in rows]}")
else:
    print("\n✅ No duplicate (name, partner_type) pairs across the pulled rows")

# COMMAND ----------

# ── Write to Delta (full overwrite - this document is small) ─────────────────
connector.close()

df = (spark.createDataFrame(all_rows, schema=XTS_SCHEMA) if all_rows
      else spark.createDataFrame([], schema=XTS_SCHEMA))
(df.write.format("delta").mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(xts_table_full))
print(f"✅ Wrote {total_rows} row(s) -> {xts_table_full}  (overwrite)")

# COMMAND ----------

# ── Registry (row_count / last_extracted per request, for auditability) ──────
if not spark.catalog.tableExists(xts_reg_full):
    spark.createDataFrame([], REGISTRY_SCHEMA).write \
        .format("delta").mode("overwrite").saveAsTable(xts_reg_full)

from collections import Counter
row_counts = Counter(r["request_id"] for r in all_rows)
ts_iso = now.isoformat()

reg_rows = []
for ref, partner_type, req_id, sheet_id, view_id, view_name in eligible:
    reg_rows.append((
        environment, workspace, document, partner_type, req_id, ref,
        sheet_id, view_id, view_name, row_counts.get(req_id, 0), now,
        "extracted", now,
    ))
for ref, partner_type, reason in skipped:
    reg_rows.append((
        environment, workspace, document, partner_type, None, ref,
        None, None, None, None, None, f"skipped: {reason}", now,
    ))

if reg_rows:
    spark.createDataFrame(reg_rows, REGISTRY_SCHEMA).createOrReplaceTempView("_xts_reg_src")
    spark.sql(f"""
      MERGE INTO {xts_reg_full} t
      USING _xts_reg_src s
        ON t.environment = s.environment AND t.request_reference = s.request_reference
      WHEN MATCHED THEN UPDATE SET
        t.document = s.document, t.request_id = s.request_id, t.sheet_id = s.sheet_id,
        t.view_id = s.view_id, t.view_name = s.view_name,
        t.row_count = s.row_count, t.last_extracted = s.last_extracted,
        t.msgs = s.msgs, t.updated_at = s.updated_at
      WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"✅ Registry upserted for {len(reg_rows)} request(s)")

# COMMAND ----------

print(f"\n{'='*72}")
print("SUMMARY")
print(f"{'='*72}")
print(f"  Requests eligible : {len(eligible)}/{len(xm.XTS_REQUESTS)}")
print(f"  Requests skipped  : {len(skipped)}")
print(f"  Total named rows  : {total_rows}")
print(f"  Duplicate (name, partner_type) pairs : {len(dups)}")
print(f"  Output table      : {xts_table_full}")
print(f"  Registry table    : {xts_reg_full}")

if total_rows > 0:
    spark.table(xts_table_full).groupBy("partner_type").agg(
        F.count("*").alias("rows"),
        F.count_if(F.col("directory_id").isNotNull()).alias("with_code"),
        F.count_if(F.col("country").isNotNull()).alias("with_country"),
    ).orderBy("partner_type").show(truncate=False)

print("\nNext: run beproduct/p0_xts_master_to_directory_upsert.py to merge these "
      "rows into beproduct_directory (match key = name + partner_type), then run "
      "p5utl_beproduct_master_data_sync.py mode=PUSH_DIRECTORY to push to BeProduct.")
print("\n✅ Phase 0 XTS Master pull complete")
