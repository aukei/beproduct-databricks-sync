# Databricks notebook source
"""
Initialise the BeProduct Style Application registry
===================================================

Caches the BeProduct application (page) IDs for a style FOLDER so the daily Step 1
sync (`p1p7_beproduct_style_sync`) can enrich styles with sample-app submit status
WITHOUT re-discovering IDs on every run.

Why a registry: application IDs are constant per FOLDER (not per style) — every
style in a folder shares the same app IDs (https://python.beproduct.com/075-apps/).
So we resolve them ONCE here and store them. Re-run this notebook ONLY when the
folder's application setup changes (apps added / removed / renamed).

Writes `lft.beproduct.beproduct_style_app_registry` (replaces the rows for this
folder):
    folder_name, app_id, app_title, app_type, is_sample, column_prefix, registered_at

`is_sample = true` rows (Proto / PreLine / SMS / Fit / PP / TOP) are the ones the
sync reads; `column_prefix` is the Delta column-prefix used for the 2 columns each
sample app contributes (`<prefix>_status`, `<prefix>_status_date`).

Parameters:
  - folder_name   (default: KTB)
  - catalog       (default: lft)
  - schema        (default: beproduct)
  - source_table  (default: ktb_styles)   — used to find one header_id cheaply
  - table_name    (default: beproduct_style_app_registry)
"""

# COMMAND ----------

import sys
import subprocess

print("📦 Installing BeProduct SDK...")
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "beproduct"])
print("✅ BeProduct SDK installed")

# COMMAND ----------

from datetime import datetime, timezone
from beproduct.sdk import BeProduct
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, TimestampType,
)

# SSOT for the sample-app title -> Delta column-prefix mapping. MUST match
# `SAMPLE_APPS` in beproduct/p1p7_beproduct_style_sync.py.
SAMPLE_APPS = {
    "Proto Sample":   "proto_sample",
    "PreLine Sample": "preline_sample",
    "SMS Sample":     "sms_sample",
    "Fit Sample":     "fit_sample",
    "PP Sample":      "pp_sample",
    "TOP Sample":     "top_sample",
}

dbutils.widgets.text("folder_name", "KTB", "BeProduct Folder Name")
dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("source_table", "ktb_styles", "Source table (for a header_id)")
dbutils.widgets.text("table_name", "beproduct_style_app_registry", "Registry table")

folder_name = dbutils.widgets.get("folder_name").strip()
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
source_table = dbutils.widgets.get("source_table").strip()
table_name = dbutils.widgets.get("table_name").strip()

registry_full = f"{catalog}.{schema}.{table_name}"
source_full = f"{catalog}.{schema}.{source_table}"

print("=" * 80)
print("INIT STYLE APPLICATION REGISTRY")
print("=" * 80)
print(f"  folder:   {folder_name}")
print(f"  registry: {registry_full}")

# COMMAND ----------

# SDK client
api = BeProduct(
    client_id=dbutils.secrets.get(scope="beproduct", key="client_id"),
    client_secret=dbutils.secrets.get(scope="beproduct", key="client_secret"),
    refresh_token=dbutils.secrets.get(scope="beproduct", key="refresh_token"),
    company_domain=dbutils.secrets.get(scope="beproduct", key="company_domain"),
)

# COMMAND ----------

# Find ONE header_id in the folder — cheap path is the existing source table; fall
# back to the (slower) attributes_list scan if the table is missing/empty.
header_id = None
try:
    rows = spark.sql(
        f"SELECT id FROM {source_full} WHERE folder_name = '{folder_name}' LIMIT 1"
    ).collect()
    if rows:
        header_id = rows[0]["id"]
        print(f"  header_id from {source_full}: {header_id}")
except Exception as e:
    print(f"  ({source_full} unavailable: {str(e)[:120]})")

if not header_id:
    print(f"  Scanning api.style.attributes_list() for a '{folder_name}' style…")
    for s in api.style.attributes_list():
        if (s.get("folder") or {}).get("name") == folder_name:
            header_id = s["id"]
            break
    print(f"  header_id from API scan: {header_id}")

if not header_id:
    raise RuntimeError(f"No style found in folder '{folder_name}' — cannot list apps")

# COMMAND ----------

# Discover the folder's applications (folder-constant id set).
apps = api.style.app_list(header_id=header_id)
print(f"\nDiscovered {len(apps)} application(s) for folder '{folder_name}':")

now = datetime.now(timezone.utc)
reg_rows = []
for a in apps:
    title = a.get("title")
    is_sample = title in SAMPLE_APPS
    reg_rows.append((
        folder_name,
        a.get("id"),
        title,
        a.get("type"),
        is_sample,
        SAMPLE_APPS.get(title),   # column_prefix (None unless sample)
        now,
    ))
    flag = "  ⭐ sample" if is_sample else ""
    print(f"  {a.get('id')}  {str(a.get('type')):22}  {title}{flag}")

# Warn about any expected sample app that is missing from this folder.
found_titles = {a.get("title") for a in apps}
missing = [t for t in SAMPLE_APPS if t not in found_titles]
if missing:
    print(f"\n⚠️  Expected sample apps NOT found in folder: {missing}")
else:
    print(f"\n✅ All {len(SAMPLE_APPS)} sample apps present.")

# COMMAND ----------

# Write: replace this folder's rows (single DELETE + single append = grouped).
schema_struct = StructType([
    StructField("folder_name", StringType()),
    StructField("app_id", StringType()),
    StructField("app_title", StringType()),
    StructField("app_type", StringType()),
    StructField("is_sample", BooleanType()),
    StructField("column_prefix", StringType()),
    StructField("registered_at", TimestampType()),
])
df = spark.createDataFrame(reg_rows, schema_struct)

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

if spark.catalog.tableExists(registry_full):
    spark.sql(f"DELETE FROM {registry_full} WHERE folder_name = '{folder_name}'")
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(registry_full)
else:
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(registry_full)

n_sample = sum(1 for r in reg_rows if r[4])
print(f"\n✅ Wrote {len(reg_rows)} app(s) ({n_sample} sample) for '{folder_name}' to {registry_full}")
spark.sql(
    f"SELECT app_title, app_type, is_sample, column_prefix FROM {registry_full} "
    f"WHERE folder_name = '{folder_name}' ORDER BY is_sample DESC, app_title"
).show(truncate=False)
