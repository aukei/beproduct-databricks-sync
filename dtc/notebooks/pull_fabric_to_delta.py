# Databricks notebook source
"""
Phase 8a — Pull DTC FABRIC sheets → Delta
==========================================

Pulls every active in-scope request from the DTC "<customer> FABRIC" document
(e.g. "KTB FABRIC") and lands all **Adoption (Y/N) = Y** rows into one Delta
table per customer:

    lft.beproduct.dtc_fabric_<customer>     e.g.  dtc_fabric_ktb

It also maintains a lightweight registry:

    lft.beproduct.dtc_fabric_registry

which mirrors the shape of dtc_request_registry so the same tooling can be
reused for Phase 8b (potential BeProduct Material Master upsert).

Request naming in the KTB FABRIC document follows two patterns:
  "<customer> <seasoncode> <brand> - DEV"      master development sheet per brand
  "<customer> <seasoncode> <brand>-<MILLCODE>" mill-specific sheet

Both types are pulled.  season_code and brand are extracted from the reference
so they can be used as join keys in Phase 8b.

Key staging columns sourced from the WIP_ITS_USE view (119 fields, id
6a0ac943fedfa0ca7ff2bf48):

    its_key            ← DTC "ITS_Key"              (system row key; future LF MATERIAL ID)
    mill_fabric_code   ← DTC "Mill Fabric Article #" (MILL FABRIC CODE)
    mill_name          ← DTC "Mill Name"             (MILL/SUPPLIER NAME)
    material_class     ← DTC "Material Class"        (MATERIAL CATEGORY)
    fabric_type        ← DTC "Fabric Type"           (FABRIC/MATERIAL TYPE)
    fabric_content     ← DTC "Fabric Content"        (MATERIAL DESCRIPTION proxy)
    kb_fabric_code     ← DTC "KB Fabric Code (SAP Code)"
    adoption           ← DTC "Adoption (Y/N)"        (filter: keep Y only)

Fields "LF MATERIAL ID" and "MATERIAL DESCRIPTION" are not yet in the view;
they will be added by DTC admin as part of Phase 8b preparation.

Parameters (widgets):
    dtc_environment   uat | prod        (default: uat)
    customer          e.g. KTB          (default: KTB)
    dtc_workspace     DTC workspace     (default: KTB)
    dtc_document      DTC document name (default: KTB FABRIC)
    catalog           Unity Catalog     (default: lft)
    schema            schema            (default: beproduct)
    write_mode        overwrite|append  (default: overwrite)
    refresh_registry  true|false        (default: true)
    max_workers       parallel threads  (default: 4)
"""

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from connectors.dtc import DTCConnector
from sync import registry as reg_module
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType, BooleanType,
)

# ── Schema ────────────────────────────────────────────────────────────────────
# Fixed columns that are always present regardless of which DTC fields have data.
# Dynamic columns (all 119 WIP_ITS_USE fields) are stored verbatim in data_json
# and promoted to typed columns for the identified key fields below.
FIXED_FIELDS = [
    StructField("customer",           StringType()),
    StructField("workspace_name",     StringType()),
    StructField("document_name",      StringType()),
    StructField("request_id",         StringType()),
    StructField("request_reference",  StringType()),
    StructField("season_code",        StringType()),
    StructField("brand",              StringType()),
    StructField("sheet_type",         StringType()),   # "DEV" | "MILL"
    StructField("mill_code",          StringType()),   # mill suffix for MILL sheets; NULL for DEV
    StructField("row_id",             StringType()),
    StructField("row_index",          LongType()),
    # Key fabric identity fields (Phase 8b compulsory / known mapping)
    StructField("its_key",            StringType()),   # DTC "ITS_Key"     → LF MATERIAL ID
    StructField("mill_fabric_code",   StringType()),   # DTC "Mill Fabric Article #"
    StructField("mill_name",          StringType()),   # DTC "Mill Name"
    StructField("material_class",     StringType()),   # DTC "Material Class"
    StructField("fabric_type",        StringType()),   # DTC "Fabric Type"
    StructField("fabric_content",     StringType()),   # DTC "Fabric Content" (≈ MATERIAL DESCRIPTION)
    StructField("kb_fabric_code",     StringType()),   # DTC "KB Fabric Code (SAP Code)"
    StructField("adoption",           StringType()),   # DTC "Adoption (Y/N)"
    # Full row payload for forward-compatibility
    StructField("extracted_at",       TimestampType()),
    StructField("data_json",          StringType()),
]

FABRIC_SCHEMA = StructType(FIXED_FIELDS)

# The only view used for fabric sync; get_request_scope() returns its per-request id.
FABRIC_VIEW_NAME = "WIP_ITS_USE"

# COMMAND ----------

dbutils.widgets.text("dtc_environment",  "uat",        "DTC Environment")
dbutils.widgets.text("customer",         "KTB",        "Customer code")
dbutils.widgets.text("dtc_workspace",    "KTB",        "DTC Workspace")
dbutils.widgets.text("dtc_document",     "KTB FABRIC", "DTC Document name")
dbutils.widgets.text("catalog",          "lft",        "Catalog")
dbutils.widgets.text("schema",           "beproduct",  "Schema")
dbutils.widgets.text("write_mode",       "overwrite",  "overwrite | append")
dbutils.widgets.text("refresh_registry", "true",       "Refresh registry before pull")
dbutils.widgets.text("max_workers",      "4",          "Parallel get_sheet() threads")

environment       = dbutils.widgets.get("dtc_environment").strip().lower()
customer          = dbutils.widgets.get("customer").strip().upper()
workspace         = dbutils.widgets.get("dtc_workspace").strip()
document          = dbutils.widgets.get("dtc_document").strip()
catalog           = dbutils.widgets.get("catalog")
schema            = dbutils.widgets.get("schema")
write_mode        = dbutils.widgets.get("write_mode").strip().lower()
refresh_registry  = dbutils.widgets.get("refresh_registry").strip().lower() == "true"
max_workers       = int(dbutils.widgets.get("max_workers") or 4)

fabric_table_full    = f"{catalog}.{schema}.dtc_fabric_{customer.lower()}"
fabric_reg_full      = f"{catalog}.{schema}.dtc_fabric_registry"

now = datetime.now(timezone.utc)

print("=" * 72)
print("PHASE 8a — Pull DTC FABRIC sheets → Delta")
print("=" * 72)
print(f"  Document  : {document}  (workspace={workspace}, env={environment})")
print(f"  Output    : {fabric_table_full}")
print(f"  Registry  : {fabric_reg_full}")
print(f"  write_mode: {write_mode}  |  refresh_registry: {refresh_registry}")

# COMMAND ----------

# ── Auth ──────────────────────────────────────────────────────────────────────
secret_key = f"dtc_api_key_{environment}"
api_key    = dbutils.secrets.get(scope="beproduct", key=secret_key)
connector  = DTCConnector(api_key=api_key, environment=environment,
                          workspace_name=workspace)
print("✅ DTCConnector ready")

# COMMAND ----------

# ── Registry refresh (discover all active FABRIC requests) ────────────────────
# Re-uses sync.registry.refresh, pointing it at the FABRIC document and registry
# table. The registry stores request_id, sheet_id, view_id, etc. for Phase 8b.
if refresh_registry:
    print(f"\n{'='*72}")
    print("Registry refresh: discovering active FABRIC requests …")
    reg_module.refresh(
        connector    = connector,
        spark        = spark,
        registry_full= fabric_reg_full,
        workspace    = workspace,
        document     = document,
        environment  = environment,
        customer     = customer,
        mode         = "merge",
    )

# Load registry rows for this environment
try:
    reg_rows = (spark.table(fabric_reg_full)
                .where(F.col("environment") == environment)
                .collect())
    print(f"✅ Registry rows loaded: {len(reg_rows)}")
except Exception as e:
    print(f"❌ Cannot read {fabric_reg_full}: {e}")
    raise

# COMMAND ----------

# ── Parse request reference into (season_code, brand, sheet_type, mill_code) ──
_FABRIC_REF_RE = re.compile(
    r"^(?P<customer>[A-Z0-9]+)\s+"
    r"(?P<season>[A-Z]{2}\d{2})\s+"
    r"(?P<rest>.+)$"
)

def _parse_fabric_ref(ref: str):
    """
    Parse a FABRIC request reference into parts.

    Patterns:
      "KTB SS28 Blue Bell - DEV"     → season_code=SS28, brand=Blue Bell, type=DEV
      "KTB SS28 Blue Bell-HUBO"      → season_code=SS28, brand=Blue Bell, type=MILL, mill=HUBO
      "KTB FW28 Wrangler - DEV"      → season_code=FW28, brand=Wrangler, type=DEV
    """
    m = _FABRIC_REF_RE.match(ref or "")
    if not m:
        return None, None, "UNKNOWN", None
    season_code = m.group("season")
    rest = m.group("rest").strip()

    # DEV sheet: ends with " - DEV" (space-dash-space)
    if rest.endswith(" - DEV"):
        brand = rest[:-6].strip()
        return season_code, brand, "DEV", None

    # Mill sheet: "<brand>-<MILLCODE>" — last hyphen-separated token is the mill code
    # Mill codes are short alphanumeric (no spaces); brand may contain spaces
    mill_split = rest.rsplit("-", 1)
    if len(mill_split) == 2 and " " not in mill_split[1].strip():
        brand    = mill_split[0].strip()
        mill_code = mill_split[1].strip()
        return season_code, brand, "MILL", mill_code

    # Fallback: whole rest is the brand
    return season_code, rest, "UNKNOWN", None


# COMMAND ----------

# ── Parallel sheet fetch with Adoption = Y filter ────────────────────────────
def _norm_adoption(val) -> str:
    """Normalise 'Adoption (Y/N)' cell value to uppercase stripped string."""
    return (val or "").strip().upper()

def _build_records(r, rows, season_code, brand, sheet_type, mill_code):
    """Convert DTC sheet rows (Adoption=Y only) into flat record dicts."""
    records = []
    ref = r.request_reference
    for row in rows:
        # Filter: only Adoption = Y
        if _norm_adoption(row.get("Adoption (Y/N)")) != "Y":
            continue
        records.append({
            "customer":          customer,
            "workspace_name":    workspace,
            "document_name":     document,
            "request_id":        r.request_id,
            "request_reference": ref,
            "season_code":       season_code,
            "brand":             brand,
            "sheet_type":        sheet_type,
            "mill_code":         mill_code,
            "row_id":            row.get("rowId"),
            "row_index":         (int(row["rowIndex"])
                                  if row.get("rowIndex") is not None else None),
            # Key fabric identity fields
            "its_key":           row.get("ITS_Key"),
            "mill_fabric_code":  row.get("Mill Fabric Article #"),
            "mill_name":         row.get("Mill Name"),
            "material_class":    row.get("Material Class"),
            "fabric_type":       row.get("Fabric Type"),
            "fabric_content":    row.get("Fabric Content"),
            "kb_fabric_code":    row.get("KB Fabric Code (SAP Code)"),
            "adoption":          row.get("Adoption (Y/N)"),
            "extracted_at":      now,
            "data_json":         json.dumps(row, default=str),
        })
    return records


# Filter to eligible requests (WIP_ITS_USE view present)
eligible    = [r for r in reg_rows if r.view_name == FABRIC_VIEW_NAME]
skipped_cnt = len(reg_rows) - len(eligible)
if skipped_cnt:
    print(f"  ⏭️  {skipped_cnt} request(s) skipped (view_name ≠ {FABRIC_VIEW_NAME!r})")

print(f"\nFetching {len(eligible)} FABRIC sheet(s) with {max_workers} worker(s) …")

all_records    = []
fetch_errors   = {}

def _fetch(r):
    sheet = connector.get_sheet(r.sheet_id, r.view_id)
    return r, sheet.get("sheetData", [])

with ThreadPoolExecutor(max_workers=max_workers) as pool:
    futures = {pool.submit(_fetch, r): r for r in eligible}
    for future in as_completed(futures):
        r = futures[future]
        ref = r.request_reference
        try:
            r_obj, rows = future.result()
            season_code, brand, sheet_type, mill_code = _parse_fabric_ref(ref)
            adopted_rows = [row for row in rows
                            if _norm_adoption(row.get("Adoption (Y/N)")) == "Y"]
            recs = _build_records(r_obj, rows, season_code, brand, sheet_type, mill_code)
            all_records.extend(recs)
            print(f"  ✅ {ref:<55} rows={len(rows):3d}  adopted={len(adopted_rows)}")
        except Exception as e:
            fetch_errors[r.request_id] = str(e)
            print(f"  ❌ {ref}: {e}")

total_rows    = len(all_records)
total_adopted = total_rows  # all stored records are already filtered to Adoption=Y
print(f"\n✅ Fetch complete: {total_adopted} Adoption=Y rows across "
      f"{len(eligible)} sheets  ({len(fetch_errors)} errors)")

# COMMAND ----------

# ── Write to Delta ────────────────────────────────────────────────────────────
if total_rows == 0:
    print(f"\n⚠️  No Adoption=Y rows found — {fabric_table_full} not modified.")
    print("   (This is expected while DTC users have not yet set Adoption (Y/N) = Y.)")
    print("   The registry has been refreshed and is ready for when adoption data arrives.")
else:
    df_fabric = spark.createDataFrame(all_records, schema=FABRIC_SCHEMA)

    if write_mode == "overwrite":
        (df_fabric.write.format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .saveAsTable(fabric_table_full))
        print(f"✅ Wrote {total_rows} rows → {fabric_table_full}  (overwrite)")
    else:
        (df_fabric.write.format("delta")
         .mode("append")
         .saveAsTable(fabric_table_full))
        print(f"✅ Appended {total_rows} rows → {fabric_table_full}")

# COMMAND ----------

# ── Update registry row_count + last_extracted ───────────────────────────────
# Count how many Adoption=Y rows were loaded per request_id.
from collections import Counter
adopted_counts = Counter(r["request_id"] for r in all_records)

ts = now.isoformat()
updated_regs = 0
for r in eligible:
    row_count = adopted_counts.get(r.request_id, 0)
    msg = f"pulled adopted={row_count}"
    try:
        spark.sql(f"""
          UPDATE {fabric_reg_full}
          SET last_extracted = timestamp('{ts}'),
              row_count      = {row_count},
              msgs           = '{msg}',
              updated_at     = timestamp('{ts}')
          WHERE environment  = '{environment}'
            AND request_id   = '{r.request_id}'
        """)
        updated_regs += 1
    except Exception as e:
        print(f"  ⚠️  Registry update failed for {r.request_reference}: {e}")

print(f"\n✅ Registry updated for {updated_regs} request(s)")

# COMMAND ----------

# ── Summary ───────────────────────────────────────────────────────────────────
print(f"\n{'='*72}")
print("SUMMARY")
print(f"{'='*72}")
print(f"  Document        : {document}")
print(f"  Requests in registry: {len(reg_rows)}")
print(f"  Eligible (WIP_ITS_USE view): {len(eligible)}")
print(f"  Fetch errors    : {len(fetch_errors)}")
print(f"  Adoption=Y rows : {total_rows}")
print(f"  Output table    : {fabric_table_full}")
print(f"  Registry table  : {fabric_reg_full}")

if total_rows > 0:
    spark.table(fabric_table_full).groupBy("season_code", "brand", "sheet_type") \
        .count().orderBy("season_code", "brand").show(50, truncate=False)

print("\n✅ Phase 8a complete")
