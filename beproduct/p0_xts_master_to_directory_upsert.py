# Databricks notebook source
"""
Phase 0 — Upsert DTC XTS Master rows -> lft.beproduct.beproduct_directory
==========================================================================

Runs AFTER `dtc/notebooks/p0_pull_xts_master_to_delta.py` and BEFORE
`p5utl_beproduct_master_data_sync.py` (mode=PUSH_DIRECTORY). This is the
concrete implementation of the upsert p5utl's Cell 5b only sketches as a
generic template.

MATCH KEY: `name` + `partner_type` TOGETHER — NOT `directory_id`/`id`.
BeProduct's Directory record is keyed by name+type (clarified by the project
team), despite `id`/`directory_id` columns existing. This means the SAME name
across DIFFERENT partner types is expected and fine (e.g. the same physical
entity legitimately exists as both a SUPPLIER record and a FACTORY record —
19/34 real Supplier rows in UAT have exactly this cross-type name match with
a Factory row) — it is NOT a collision and both records are kept. This
intentionally differs from p5utl's Cell 5b template comment (which uses
`directory_id` alone) — that template predates this clarification and should
be read as superseded for XTS-sourced data.

SAFETY — never destroys existing BeProduct-sourced data:
  `dtc_xts_master_ktb` never carries address/state/zip/city/phone/fax/website/
  notes (XTS Master has no such fields at all — see `sync/xts_master.py`), and
  `directory_id`/`country` are only sometimes populated. If a MATCHED
  beproduct_directory row already has real values there from the original
  full BeProduct pull, blindly overwriting them with XTS's NULLs would
  silently destroy real data. Every field in the MERGE therefore uses
  `COALESCE(src.field, tgt.field)` — a field is only ever CHANGED when the
  XTS source actually provides a non-null value; a NULL from XTS always
  preserves whatever is already in `beproduct_directory`.

partner_type is part of the match key itself, so it can never be changed by
this MERGE's UPDATE branch (a MATCHED row's partner_type is by definition
already identical to the source's). It is only ever set on brand-new
(NOT MATCHED) inserts.

Duplicate (name, partner_type) pairs: a TRUE collision only happens when the
identical pair repeats (e.g. a duplicate row within one sheet) —
`xts_master.dedupe_by_key()` collapses any such collision deterministically
(prefers a row with a real code, then row_index) and reports every collision
so it's never silently dropped without a trace — see the printed "Duplicate"
section. The same name under a DIFFERENT partner_type is never reported here.

Parameters:
  - catalog / schema (default: lft / beproduct)
  - source_table (default: dtc_xts_master_ktb)
  - dry_run (default: true) — compute + print the plan, never write
"""

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")
sys.path.append("/Workspace/Repos/beproduct-sync/dtc/python")

from datetime import datetime, timezone

from sync import xts_master as xm
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
)

dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("source_table", "dtc_xts_master_ktb", "XTS Master staging table")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry Run")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
source_table = dbutils.widgets.get("source_table").strip()
dry_run = dbutils.widgets.get("dry_run").strip().lower() == "true"

source_full = f"{catalog}.{schema}.{source_table}"
directory_full = f"{catalog}.{schema}.beproduct_directory"
now = datetime.now(timezone.utc)

print("=" * 72)
print("PHASE 0 — Upsert XTS Master -> beproduct_directory  (match key = name + partner_type)")
print("=" * 72)
print(f"  Source    : {source_full}")
print(f"  Target    : {directory_full}")
print(f"  dry_run   : {dry_run}")

# COMMAND ----------

if not spark.catalog.tableExists(source_full):
    raise RuntimeError(
        f"{source_full} does not exist — run "
        f"dtc/notebooks/p0_pull_xts_master_to_delta.py first."
    )
if not spark.catalog.tableExists(directory_full):
    raise RuntimeError(
        f"{directory_full} does not exist — run "
        f"p5utl_beproduct_master_data_sync.py mode=PULL_ONLY first "
        f"(creates the table from a full BeProduct Directory pull)."
    )

src_rows = [r.asDict() for r in spark.table(source_full).collect()]
print(f"\n{len(src_rows)} raw row(s) in {source_full}")

# COMMAND ----------

# ── Dedupe by (name, partner_type) - BeProduct's real match key together -
#    never silently drop a TRUE collision without reporting it. The same name
#    under a DIFFERENT partner_type is NOT a collision (both are kept). ──────
winners, duplicates = xm.dedupe_by_key(src_rows)
print(f"\nDeduped to {len(winners)} distinct (name, partner_type) pair(s) "
      f"({len(src_rows) - len(winners)} row(s) collapsed)")

if duplicates:
    print(f"\n⚠️  {len(duplicates)} colliding (name, partner_type) pair(s) — picked "
          f"deterministically (prefer a row with a real code, then row_index; ALL "
          f"colliding rows shown so nothing is silently dropped):")
    for (name, ptype), rows in duplicates.items():
        chosen = next(w for w in winners if w["name"] == name and w["partner_type"] == ptype)
        for r in rows:
            marker = "→ KEPT" if (
                r["request_id"] == chosen["request_id"] and r["row_id"] == chosen["row_id"]
            ) else "  (dropped)"
            print(f"    {name!r} [{ptype}]: directory_id={r['directory_id']!r} "
                  f"request={r['request_reference']!r} {marker}")
else:
    print("✅ No duplicate (name, partner_type) pairs")

if not winners:
    print("\n⚠️  Nothing to upsert — exiting.")
    dbutils.notebook.exit("NO_ROWS")

# COMMAND ----------

# ── Build the MERGE source DataFrame ──────────────────────────────────────────
UPSERT_COLS = [
    "name", "directory_id", "partner_type", "country",
    "address", "state", "zip", "city", "phone", "fax", "website", "notes",
]
SRC_SCHEMA = StructType([StructField(c, StringType()) for c in UPSERT_COLS])
src_data = [tuple(w.get(c) for c in UPSERT_COLS) for w in winners]
df_src = spark.createDataFrame(src_data, SRC_SCHEMA)
df_src.createOrReplaceTempView("_xts_directory_src")

# COMMAND ----------

# ── Plan preview (new vs. changed vs. unchanged) - always computed, even in
# dry_run, so the plan is visible before anything is written. Existing keys
# are (name, partner_type) TOGETHER - matches the MERGE's ON clause. ─────────
existing_keys = set(
    (r["name"], r["partner_type"])
    for r in spark.table(directory_full).select("name", "partner_type").collect()
)
new_keys = [(w["name"], w["partner_type"]) for w in winners
            if (w["name"], w["partner_type"]) not in existing_keys]
matched_keys = [(w["name"], w["partner_type"]) for w in winners
                if (w["name"], w["partner_type"]) in existing_keys]

print(f"\nPlan: {len(new_keys)} NEW (Directory/Add on next PUSH_DIRECTORY), "
      f"{len(matched_keys)} MATCHED by (name, partner_type) (updated only if a field "
      f"actually changes)")
if new_keys:
    print(f"  New: {new_keys[:20]}{' ...' if len(new_keys) > 20 else ''}")

# COMMAND ----------

if dry_run:
    print("\n⚠️  DRY RUN — no changes written. Re-run with dry_run=false to apply.")
    dbutils.notebook.exit(f"DRY_RUN new={len(new_keys)} matched={len(matched_keys)}")

# COMMAND ----------

# ── The actual MERGE. Match key is (name, partner_type) TOGETHER - the same
#    name under a different partner_type is a SEPARATE valid Directory record,
#    not a collision. COALESCE on every non-key field so a NULL from XTS
#    (which is most fields, by design - see module docstring) never
#    overwrites real data already in beproduct_directory. partner_type can
#    never change on a MATCHED row - it's part of the join key itself, so a
#    match is only possible when it's already identical. ────────────────────
merge_sql = f"""
MERGE INTO {directory_full} AS tgt
USING _xts_directory_src AS src
ON tgt.name = src.name AND tgt.partner_type = src.partner_type

WHEN MATCHED AND (
       COALESCE(src.directory_id, tgt.directory_id) IS DISTINCT FROM tgt.directory_id
    OR COALESCE(src.country,      tgt.country)      IS DISTINCT FROM tgt.country
    OR COALESCE(src.address,      tgt.address)       IS DISTINCT FROM tgt.address
    OR COALESCE(src.state,        tgt.state)         IS DISTINCT FROM tgt.state
    OR COALESCE(src.zip,          tgt.zip)           IS DISTINCT FROM tgt.zip
    OR COALESCE(src.city,         tgt.city)          IS DISTINCT FROM tgt.city
    OR COALESCE(src.phone,        tgt.phone)         IS DISTINCT FROM tgt.phone
    OR COALESCE(src.fax,          tgt.fax)           IS DISTINCT FROM tgt.fax
    OR COALESCE(src.website,      tgt.website)       IS DISTINCT FROM tgt.website
    OR COALESCE(src.notes,        tgt.notes)         IS DISTINCT FROM tgt.notes
)
THEN UPDATE SET
    tgt.directory_id = COALESCE(src.directory_id, tgt.directory_id),
    tgt.country      = COALESCE(src.country,      tgt.country),
    tgt.address      = COALESCE(src.address,      tgt.address),
    tgt.state        = COALESCE(src.state,        tgt.state),
    tgt.zip          = COALESCE(src.zip,          tgt.zip),
    tgt.city         = COALESCE(src.city,         tgt.city),
    tgt.phone        = COALESCE(src.phone,        tgt.phone),
    tgt.fax          = COALESCE(src.fax,          tgt.fax),
    tgt.website      = COALESCE(src.website,      tgt.website),
    tgt.notes        = COALESCE(src.notes,        tgt.notes),
    tgt.modified_at  = current_timestamp()   -- flags this row for PUSH_DIRECTORY

WHEN NOT MATCHED THEN INSERT (
    id, directory_id, name, partner_type,
    address, country, state, zip, city, phone, fax, website, notes,
    active, data_json, extracted_at, bp_modified_at, modified_at
) VALUES (
    NULL,                    -- id = NULL -> Directory/Add on next PUSH_DIRECTORY
    src.directory_id, src.name, src.partner_type,
    src.address, src.country, src.state, src.zip, src.city,
    src.phone, src.fax, src.website, src.notes,
    true,                    -- active: XTS Master carries no active flag - default true
    NULL,                    -- data_json: populated on the next PULL_ONLY from BeProduct
    NULL,                    -- extracted_at = NULL = never pulled from BeProduct yet
    NULL,                    -- bp_modified_at = NULL = never pulled yet
    current_timestamp()      -- modified_at flags the row for push immediately
)
"""

spark.sql(merge_sql)
print(f"✅ MERGE applied: {len(new_keys)} inserted, up to {len(matched_keys)} candidates "
      f"updated (only rows with an actual field change were touched)")

# COMMAND ----------

print(f"\n{'='*72}")
print("SUMMARY")
print(f"{'='*72}")
print(f"  Source rows              : {len(src_rows)}")
print(f"  Deduped (name+type)      : {len(winners)}  ({len(duplicates)} collision(s))")
print(f"  New (Add pending)        : {len(new_keys)}")
print(f"  Matched (name+type)      : {len(matched_keys)}")
print(f"\nNext: run p5utl_beproduct_master_data_sync.py mode=PUSH_DIRECTORY "
      f"(dry_run=true first) to push these to BeProduct.")
print("\n✅ Phase 0 XTS Master upsert complete")
