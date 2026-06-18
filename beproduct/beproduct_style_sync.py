# Databricks notebook source
"""
BeProduct STYLE Master Data Sync Job
=====================================

Retrieves STYLE master data from BeProduct for folder 'KTB' and stores in Delta Lake.
Supports both FULL and INCREMENTAL refresh modes.

Schedule: Daily at 7pm HKT (11am UTC)

Parameters:
  - refresh_mode: "FULL" (default) or "INCREMENTAL"
  - catalog: Target Databricks catalog (default: "lft")
  - schema: Target Databricks schema (default: "beproduct")
  - table_name: Table name (default: "ktb_styles")
"""

# COMMAND ----------

# ============================================================================
# CELL 1: Install BeProduct SDK  (ISOLATED so its time is measured separately)
# ============================================================================
# Kept in its own command cell on purpose: the per-cell timing in the exported
# run model then shows EXACTLY how long the pip install takes, isolated from the
# imports / fetch / write. See docs/PERFORMANCE.md "Validation run 3" — the SDK
# install is a fixed per-run cost; if it's material, bake `beproduct` into the
# cluster image / init script and this cell becomes a no-op.

import sys
import subprocess
import time

print("=" * 80)
print("INSTALL CELL: Install BeProduct SDK")
print("=" * 80)

print("\n📦 Installing BeProduct SDK...")
_install_t0 = time.perf_counter()
try:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "beproduct"])
    _install_secs = time.perf_counter() - _install_t0
    print(f"✅ BeProduct SDK installed in {_install_secs:.1f}s")
except Exception as e:
    print(f"❌ Failed after {time.perf_counter() - _install_t0:.1f}s: {str(e)}")
    raise

# COMMAND ----------

# ============================================================================
# CELL 2: Setup - Import Libraries, Configure Parameters
# ============================================================================

print("=" * 80)
print("SETUP CELL: Import Libraries, Configure Parameters")
print("=" * 80)

# Import libraries
print("\n📚 Importing libraries...")
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from beproduct.sdk import BeProduct
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType
from pyspark.sql import Row

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
print("✅ All libraries imported")

# Configure job parameters with widgets
print("\n⚙️  Configuring job parameters...")
dbutils.widgets.text("folder_name", "KTB", "BeProduct Folder Name")
dbutils.widgets.text("refresh_mode", "FULL", "Refresh Mode (FULL or INCREMENTAL)")
dbutils.widgets.text("catalog", "lft", "Catalog Name")
dbutils.widgets.text("schema", "beproduct", "Schema Name")
dbutils.widgets.text("table_name", "ktb_styles", "Table Name")

folder_name = dbutils.widgets.get("folder_name")
refresh_mode = dbutils.widgets.get("refresh_mode").upper()
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_name = dbutils.widgets.get("table_name")

print("✅ Parameters configured:")
print(f"   folder_name: {folder_name}")
print(f"   refresh_mode: {refresh_mode}")
print(f"   catalog: {catalog}")
print(f"   schema: {schema}")
print(f"   table_name: {table_name}")

print("\n" + "=" * 80)
print("✅ SETUP COMPLETE - Ready to sync")
print("=" * 80)

# COMMAND ----------

# ============================================================================
# CELL 2: Main Sync Logic
# ============================================================================

print("\n" + "=" * 80)
print("SYNC CELL: Fetch, Transform, and Write Data")
print("=" * 80)

# Get parameters from previous cell
folder_name_val = dbutils.widgets.get("folder_name")
refresh_mode_val = dbutils.widgets.get("refresh_mode").upper()
catalog_val = dbutils.widgets.get("catalog")
schema_val = dbutils.widgets.get("schema")
table_name_val = dbutils.widgets.get("table_name")

# Field mapping configuration
# Keys are BeProduct field names (from headerData.fields[].name)
# Values are Delta table column names
# Note: Field names are case-sensitive and must match exactly!
COMPULSORY_FIELDS = {
    "LF Style Number": "lf_style_number",
    "DESCRIPTION": "description",
    "TEAM": "team",
    "SEASON": "season",
    "YEAR": "year",
}

INTERESTED_FIELDS = {
    "PRODUCT STATUS": "product_status",
    "CUSTOMER STYLE NUMBER / PLM #": "customer_style_number",
    "PRODUCT CATEGORY": "product_category",
    "PRODUCT SUB CATEGORY": "product_sub_category",
    "Division": "division",
    "BRANDS": "brands",
    "GARMENT FINISH": "garment_finish",
    "TECHPACK STAGE": "techpack_stage",
    "Lot Code": "lot_code",
    "PARENT VENDOR": "parent_vendor",
    "FACTORY": "factory",
}

# BOM and Material fields (extracted by field ID from headerData)
# Per requirements: colorways, BOM materials, material category/content, front image
BOM_MATERIAL_FIELDS = {
    "core_main_material": "bom_material_1",        # Main Fabric material
    "Core_main_material2": "bom_material_2",       # Secondary fabric material
    "main_material_category": "main_material_category",
    "main_material_content": "main_material_content",
}

EXTRACTED_FIELDS = {**COMPULSORY_FIELDS, **INTERESTED_FIELDS}

print(f"\n📋 Configuration:")
print(f"   Folder: {folder_name_val}")
print(f"   Mode: {refresh_mode_val}")
print(f"   Target: {catalog_val}.{schema_val}.{table_name_val}")
print(f"   Standard fields: {len(EXTRACTED_FIELDS)}")
print(f"   BOM/Material fields: {len(BOM_MATERIAL_FIELDS)}")
print(f"   Extended: colorways array, front image URL")

# ============================================================================
# Step 1: Get Credentials and Initialize Client
# ============================================================================

print(f"\n{'='*80}")
print("Step 1: Initialize BeProduct SDK")
print("=" * 80)

try:
    print("🔐 Retrieving credentials from Databricks secrets...")
    client_id = dbutils.secrets.get(scope="beproduct", key="client_id")
    client_secret = dbutils.secrets.get(scope="beproduct", key="client_secret")
    refresh_token = dbutils.secrets.get(scope="beproduct", key="refresh_token")
    company_domain = dbutils.secrets.get(scope="beproduct", key="company_domain")
    print("   ✓ client_id, client_secret, refresh_token, company_domain retrieved")
    
    print("🚀 Creating BeProduct SDK client...")
    api = BeProduct(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        company_domain=company_domain,
    )
    print("✅ BeProduct SDK client initialized")
except Exception as e:
    print(f"❌ Failed to initialize: {str(e)}")
    raise

# ============================================================================
# Step 2: Check Sync Metadata
# ============================================================================

print(f"\n{'='*80}")
print("Step 2: Check Sync Metadata")
print("=" * 80)

def get_last_sync_timestamp(table_name: str) -> Optional[str]:
    """Get last sync timestamp for incremental refresh.
    
    Each table has its own metadata table: {table_name}_sync_meta
    This allows different streams (folders/entities) to track sync independently.
    """
    metadata_table = f"{table_name}_sync_meta"
    
    try:
        spark.sql(f"USE CATALOG {catalog_val}")
        spark.sql(f"USE SCHEMA {schema_val}")
        
        tables = spark.sql(
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_catalog = '{catalog_val}' "
            f"  AND table_schema = '{schema_val}' "
            f"  AND table_name = '{metadata_table}'"
        ).collect()
        
        if not tables:
            logger.info(f"Metadata table {metadata_table} does not exist (first run)")
            return None
        
        result = spark.sql(
            f"SELECT last_sync_at FROM {catalog_val}.{schema_val}.{metadata_table} LIMIT 1"
        ).collect()
        
        if result:
            timestamp = result[0]["last_sync_at"]
            logger.info(f"Last sync for {metadata_table}: {timestamp}")
            return timestamp
        return None
    except Exception as e:
        logger.warning(f"Could not retrieve metadata from {metadata_table}: {str(e)}")
        return None

if refresh_mode_val == "FULL":
    print("🔄 FULL REFRESH mode")
    since_iso = None
else:
    print("🔄 INCREMENTAL REFRESH mode")
    since_iso = get_last_sync_timestamp(table_name_val)
    if since_iso:
        print(f"   Last sync: {since_iso}")
    else:
        print("   No previous sync found, switching to FULL refresh")
        refresh_mode_val = "FULL"
        since_iso = None

# ============================================================================
# Step 3: Fetch Styles
# ============================================================================

print(f"\n{'='*80}")
print("Step 3: Fetch Styles from BeProduct")
print("=" * 80)

try:
    print(f"📥 Fetching styles from folder '{folder_name_val}'...")
    print(f"   (This may take a moment...)")
    
    filters = None
    if since_iso:
        filters = [{
            "field": "FolderModifiedAt",
            "operator": "Gt",
            "value": since_iso,
        }]
        print(f"   Filter: FolderModifiedAt > {since_iso}")
    else:
        print(f"   No filter (fetching all styles)")
    
    styles = []
    all_styles = []
    count = 0
    
    print(f"\n   Calling api.style.attributes_list(filters={filters})...")
    
    # Get iterator
    iterator = api.style.attributes_list(filters=filters)
    print(f"   Iterator created: {type(iterator)}")
    
    # Iterate through results
    for style in iterator:
        all_styles.append(style)
        
        # Show first result with FULL structure for debugging
        if len(all_styles) == 1:
            print(f"\n   🔍 FIRST RESULT STRUCTURE (id={style.get('id', '?')[:16]}...):")
            print(f"      Top-level keys: {list(style.keys())}")
            
            # Check top-level for LF_Style_number
            if "LF_Style_number" in style:
                print(f"      LF_Style_number (top-level): {style['LF_Style_number']}")
            
            # Check attributes (might be in headerData instead)
            attrs = style.get("attributes", {})
            if attrs:
                print(f"\n      Attributes ({len(attrs)} fields):")
                # Show all attribute values for first style
                for key, val in sorted(attrs.items()):
                    print(f"        - '{key}': {repr(val)[:80]}")
            
            # Check headerData - this might contain the attributes
            header_data = style.get("headerData", {})
            if header_data:
                print(f"\n      headerData ({len(header_data)} fields):")
                # Show all header data values for first style
                for key, val in sorted(header_data.items()):
                    val_str = repr(val)[:100]
                    print(f"        - '{key}': {val_str}")
            
            # Check folder
            folder = style.get("folder", {})
            if folder:
                print(f"\n      Folder info:")
                print(f"        - {folder}")
            
            # Extract fields from headerData.fields
            fields_list = header_data.get("fields", [])
            if fields_list:
                print(f"\n      Fields from headerData.fields ({len(fields_list)} fields):")
                fields_dict = {}
                for field in fields_list:
                    field_name = field.get("name", "?")
                    field_value = field.get("value", "")
                    fields_dict[field_name] = field_value
                    print(f"        - '{field_name}': {repr(field_value)[:80]}")
                
                # Verify expected fields exist
                print(f"\n      ✅ VERIFICATION - Checking for expected fields:")
                expected = {
                    "LF Style Number": "LFBP-WM1MJ-002",
                    "Lot Code": "112394630",
                    "BRANDS": "['Wrangler']",  # Array
                    "CUSTOMER STYLE NUMBER / PLM #": "127-WM1MJ-XXXX-009",
                    "DESCRIPTION": "MOD MALE T1 WASHED LEATHER JACKET",
                    "GARMENT FINISH": "LEATHER JACKET + TBC Wash",
                    "PRODUCT CATEGORY": "Jackets",
                    "PRODUCT SUB CATEGORY": "Jacket",
                    "PRODUCT STATUS": "Proto",
                    "SEASON": "Spring",
                    "TECHPACK STAGE": "Draft",
                    "YEAR": "2027",
                    "TEAM": "KTB",
                }
                
                for field_name, expected_value in expected.items():
                    actual_value = fields_dict.get(field_name, "NOT_FOUND")
                    status = "✓" if actual_value != "NOT_FOUND" else "✗"
                    print(f"        {status} {field_name}: {actual_value}")
            
            print()
        
        # Show first few results with detailed info
        if len(all_styles) <= 5:
            style_id = style.get("id", "NO_ID")[:16]
            
            # Try multiple ways to get folder name
            folder_obj = style.get("folder", {})
            folder_name = folder_obj.get("name", "?") if folder_obj else "?"
            
            # Try multiple ways to get LF Style number
            lf_style = (
                style.get("LF_Style_number") or 
                style.get("attributes", {}).get("LF Sytle Number") or
                style.get("attributes", {}).get("LF_Style_number") or
                style.get("attributes", {}).get("LF Style Number") or
                "NO_LF"
            )
            
            print(f"     Result {len(all_styles)}: folder='{folder_name}', lf_style={lf_style}, id={style_id}...")
        
        # Filter by specified folder (case-sensitive match)
        # Folder is nested: style.get("folder", {}).get("name")
        folder_obj = style.get("folder", {})
        actual_folder = folder_obj.get("name", "") if folder_obj else ""
        if actual_folder == folder_name_val:
            styles.append(style)
            count += 1
            if count % 50 == 0:
                print(f"     Matched {count} styles so far...")
    
    print(f"\n✅ Fetch complete:")
    print(f"   Total results from API: {len(all_styles)}")
    print(f"   Styles with folder='{folder_name_val}': {len(styles)}")
    
    if len(all_styles) == 0:
        print(f"\n   ⚠️  API returned 0 results!")
        print(f"   Possible reasons:")
        print(f"     - No styles exist in your BeProduct instance")
        print(f"     - Credentials are invalid")
        print(f"     - Filter is too restrictive")
    
    if len(all_styles) > 0 and len(styles) == 0:
        unique_folders = set(s.get("folder", {}).get("name", "?") for s in all_styles if s.get("folder"))
        print(f"\n   ⚠️  WARNING: API returned {len(all_styles)} styles, but NONE matched folder '{folder_name_val}'")
        print(f"   Unique folders in results: {unique_folders}")
        print(f"   (Check folder name spelling and case sensitivity)")

except Exception as e:
    print(f"❌ Failed to fetch styles: {str(e)}")
    print(f"   Exception type: {type(e).__name__}")
    import traceback
    traceback.print_exc()
    raise

# Check if we got any data
print(f"\n   Checking data...")
print(f"   styles list length: {len(styles)}")

HAS_DATA = len(styles) > 0

if not HAS_DATA:
    print(f"\n❌ No styles to sync")
    print(f"   Total API results: {len(all_styles)}")
    if len(all_styles) > 0:
        print(f"   But none matched folder '{folder_name_val}'")
    print(f"\n⚠️  No data to process - skipping transformation and write steps")
else:
    print(f"\n✅ {len(styles)} styles ready for processing")

# Only proceed if we have data
if HAS_DATA:
    
    # ============================================================================
    # Step 4: Transform Records
    # ============================================================================

    print(f"\n{'='*80}")
    print("Step 4: Transform Records")
    print("=" * 80)

    def extract_colorways(record: Dict) -> List[str]:
        """
        Extract colorway names from record.colorways array.
        Returns list of color names.
        """
        colorways = record.get("colorways", [])
        if not colorways or not isinstance(colorways, list):
            return []
        
        color_names = []
        for cw in colorways:
            if isinstance(cw, dict):
                color_name = cw.get("colorName")
                if color_name:
                    color_names.append(str(color_name))
        
        return color_names

    def extract_colorways_detail(record: Dict) -> List[Dict]:
        """
        Extract colorway detail (id + name + number) from record.colorways.

        Phase 2 (DTC -> BeProduct) writes the colorway-level "Lot Code"
        (fieldId drawing_number_walmart) back to a SPECIFIC colorway, which the
        BeProduct SDK addresses by colorway *id*. The denormalized transform only
        carries the color *name*, so we must persist the colorway id here and
        carry it through to the staging table.

        Returns a list of {colorway_id, color_name, color_number}; serialized as
        JSON into the 'colorways_json' column (kept as a string so the dynamic
        Delta schema builder treats it as a plain column).
        """
        colorways = record.get("colorways", [])
        if not colorways or not isinstance(colorways, list):
            return []

        detail = []
        for cw in colorways:
            if isinstance(cw, dict) and cw.get("colorName"):
                detail.append({
                    "colorway_id": cw.get("id"),
                    "color_name": str(cw.get("colorName")),
                    "color_number": cw.get("colorNumber"),
                })
        return detail

    def extract_bom_materials(header_data: Dict) -> Dict[str, Optional[str]]:
        """
        Extract BOM material fields by field ID.
        
        Per requirements:
          - core_main_material → BOM Line 1 (Main Fabric)
          - Core_main_material2 → BOM Line 2 (Fabric)
        
        Returns dict with bom_material_1, bom_material_2, etc.
        """
        fields_list = header_data.get("fields", [])
        
        # Build dict keyed by field ID
        fields_by_id = {}
        for field in fields_list:
            field_id = field.get("id", "")
            field_value = field.get("value")
            if field_id:
                fields_by_id[field_id] = field_value
        
        # Extract BOM fields
        bom_data = {}
        for field_id, column_name in BOM_MATERIAL_FIELDS.items():
            value = fields_by_id.get(field_id)
            # Convert arrays to comma-separated strings
            if isinstance(value, list):
                value = ", ".join(str(v) for v in value) if value else None
            bom_data[column_name] = value
        
        return bom_data

    def extract_front_image_url(header_data: Dict) -> Optional[str]:
        """
        Extract front image URL from headerData.frontImage.origin.
        """
        front_image = header_data.get("frontImage")
        if not front_image or not isinstance(front_image, dict):
            return None
        
        origin_url = front_image.get("origin")
        return origin_url if origin_url else None

    def transform_style_record(record: Dict) -> Dict:
        """Transform a BeProduct Style record into a Delta table row with extended fields."""
        # Extract folder info
        folder_obj = record.get("folder", {})
        folder_name = folder_obj.get("name", "") if folder_obj else ""
        
        # Current extraction timestamp
        extracted_now = datetime.now(timezone.utc)
        
        row = {
            "id": record.get("id"),
            "folder_name": folder_name,
            "synced_at": extracted_now,  # Legacy name, kept for compatibility
            "extracted": extracted_now,   # NEW: Unified extraction timestamp
        }
        
        # Parse ISO 8601 strings to datetime objects for proper TIMESTAMP storage
        if "createdAt" in record:
            try:
                created_str = record["createdAt"]
                row["created_at"] = datetime.fromisoformat(created_str.replace('Z', '+00:00'))
            except:
                row["created_at"] = None
        
        # NEW: last_modified from source (modifiedAt)
        if "modifiedAt" in record:
            try:
                modified_str = record["modifiedAt"]
                last_mod = datetime.fromisoformat(modified_str.replace('Z', '+00:00'))
                row["modified_at"] = last_mod     # Legacy name
                row["last_modified"] = last_mod   # NEW: Unified change tracking
            except:
                row["modified_at"] = None
                row["last_modified"] = None
        
        # Extract attributes from headerData.fields
        header_data = record.get("headerData", {})
        fields_list = header_data.get("fields", [])
        
        # Convert fields list to dict keyed by field name
        attributes = {}
        for field in fields_list:
            field_name = field.get("name", "")
            field_value = field.get("value")
            if field_name:
                attributes[field_name] = field_value
        
        # Extract standard fields
        for beproduct_name, column_name in EXTRACTED_FIELDS.items():
            value = attributes.get(beproduct_name)
            # Convert arrays to comma-separated strings
            if isinstance(value, list):
                value = ", ".join(str(v) for v in value) if value else None
            row[column_name] = value
        
        # NEW: Extract colorways array
        colorways = extract_colorways(record)
        row["colorways_array"] = colorways
        row["colorways_count"] = len(colorways)
        # NEW: colorway detail (id + name + number) as JSON, so the DTC transform
        # can carry colorway_id for Phase 2 (DTC -> BeProduct Lot# pushback).
        row["colorways_json"] = json.dumps(extract_colorways_detail(record))
        
        # NEW: Extract BOM material fields
        bom_data = extract_bom_materials(header_data)
        row.update(bom_data)  # Adds bom_material_1, bom_material_2, etc.
        
        # NEW: Extract front image URL
        row["front_image_url"] = extract_front_image_url(header_data)
        
        # Store full record as JSON
        row["data_json"] = json.dumps(record)
        
        return row

    try:
        print(f"🔄 Transforming {len(styles)} records with extended fields...")
        print(f"   - Standard fields: {len(EXTRACTED_FIELDS)}")
        print(f"   - BOM/Material fields: {len(BOM_MATERIAL_FIELDS)}")
        print(f"   - Colorways array + front image")
        print(f"   - Change tracking: last_modified, extracted")
        
        rows = [transform_style_record(s) for s in styles]
        
        # Print sample for verification
        if rows:
            sample = rows[0]
            print(f"\n   📋 Sample row:")
            print(f"      - colorways: {sample.get('colorways_array', [])} ({sample.get('colorways_count', 0)} colors)")
            print(f"      - bom_material_1: {sample.get('bom_material_1', 'N/A')}")
            print(f"      - bom_material_2: {sample.get('bom_material_2', 'N/A')}")
            print(f"      - last_modified: {sample.get('last_modified', 'N/A')}")
            print(f"      - extracted: {sample.get('extracted', 'N/A')}")
        
        print(f"✅ Transformed {len(rows)} rows")
    except Exception as e:
        print(f"❌ Failed to transform: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

    # ============================================================================
    # Step 5: Create Spark DataFrame
    # ============================================================================

    print(f"\n{'='*80}")
    print("Step 5: Create Spark DataFrame")
    print("=" * 80)

    try:
        print(f"📊 Creating DataFrame from {len(rows)} rows...")
        
        # Get all column names
        all_cols = set()
        for row in rows:
            all_cols.update(row.keys())
        sorted_cols = sorted(all_cols)
        
        print(f"   Columns: {len(sorted_cols)}")
        
        # Create schema with proper types for timestamp and array fields
        timestamp_cols = {"synced_at", "created_at", "modified_at", "last_modified", "extracted"}
        array_cols = {"colorways_array"}
        
        fields = []
        for col in sorted_cols:
            if col in timestamp_cols:
                fields.append(StructField(col, TimestampType(), True))
            elif col in array_cols:
                fields.append(StructField(col, ArrayType(StringType()), True))
            else:
                fields.append(StructField(col, StringType(), True))
        schema = StructType(fields)
        
        # Convert to Spark rows (preserve types for timestamp and array fields)
        def row_to_spark_row(row_dict, cols):
            row_data = {}
            for col in cols:
                val = row_dict.get(col)
                if val is None:
                    row_data[col] = None
                elif col in timestamp_cols:
                    # Keep datetime objects as-is for timestamp columns
                    row_data[col] = val
                elif col in array_cols:
                    # Keep list/array as-is for array columns
                    row_data[col] = val if isinstance(val, list) else []
                else:
                    # Convert everything else to string
                    row_data[col] = str(val)
            return Row(**row_data)
        
        spark_rows = [row_to_spark_row(row, sorted_cols) for row in rows]
        df = spark.createDataFrame(spark_rows, schema=schema)
        
        row_count = df.count()
        print(f"✅ DataFrame created: {row_count} rows")
        
    except Exception as e:
        print(f"❌ Failed to create DataFrame: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

    # ============================================================================
    # Step 6: Write to Delta Table
    # ============================================================================

    print(f"\n{'='*80}")
    print("Step 6: Write to Delta Table")
    print("=" * 80)

    full_table_path = f"{catalog_val}.{schema_val}.{table_name_val}"

    try:
        print(f"💾 Writing to {full_table_path}...")
        
        spark.sql(f"USE CATALOG {catalog_val}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_val}.{schema_val}")
        
        if refresh_mode_val == "FULL":
            # FULL: overwrite the whole table with the complete fresh snapshot.
            write_mode = "overwrite"
            print(f"   Write mode: overwrite (FULL refresh, {len(rows)} styles)")
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("mergeSchema", "true")
                .saveAsTable(full_table_path)
            )
        else:
            # INCREMENTAL: MERGE by BeProduct style id so that already-present
            # styles are UPDATED (not duplicated) and genuinely new styles are
            # INSERTED. Plain "append" would create duplicate rows because the
            # BeProduct FolderModifiedAt filter is folder-scoped — any modification
            # in the KTB folder causes all styles in that folder to qualify, so
            # previously-synced styles can re-appear in the incremental result.
            from delta.tables import DeltaTable
            table_exists = spark.catalog.tableExists(full_table_path)
            if table_exists:
                write_mode = "merge (upsert by id)"
                print(f"   Write mode: merge/upsert (INCREMENTAL, key=id, {len(rows)} styles)")
                dt = DeltaTable.forName(spark, full_table_path)
                (
                    dt.alias("target")
                    .merge(df.alias("source"), "target.id = source.id")
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                # Table doesn't exist yet (first run in INCREMENTAL mode) — treat
                # as initial load.
                write_mode = "overwrite (first run)"
                print(f"   Write mode: overwrite (first run, {len(rows)} styles)")
                (
                    df.write.format("delta")
                    .mode("overwrite")
                    .option("mergeSchema", "true")
                    .saveAsTable(full_table_path)
                )
        
        final_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table_path}").collect()[0]["cnt"]
        print(f"✅ Data written successfully")
        print(f"   Total rows in table: {final_count}")
        
    except Exception as e:
        print(f"❌ Failed to write: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

    # ============================================================================
    # Step 7: Save Sync Metadata
    # ============================================================================

    print(f"\n{'='*80}")
    print("Step 7: Save Sync Metadata")
    print("=" * 80)

    try:
        sync_timestamp = datetime.now(timezone.utc).isoformat()
        metadata_table = f"{table_name_val}_sync_meta"
        
        spark.sql(f"USE CATALOG {catalog_val}")
        spark.sql(f"USE SCHEMA {schema_val}")
        
        # Build summary
        summary = f'{len(rows)} styles synced from folder "{folder_name_val}" (mode: {refresh_mode_val})'
        
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {catalog_val}.{schema_val}.{metadata_table}
            (last_sync_at STRING, sync_type STRING, records_synced LONG, summary STRING)
            USING DELTA
            """
        )
        
        spark.sql(
            f"""
            INSERT INTO {catalog_val}.{schema_val}.{metadata_table}
            SELECT 
                '{sync_timestamp}' AS last_sync_at,
                '{refresh_mode_val}' AS sync_type,
                {len(rows)} AS records_synced,
                '{summary}' AS summary
            """
        )
        print(f"✅ Metadata saved to {metadata_table}:")
        print(f"   Timestamp: {sync_timestamp}")
        print(f"   Type: {refresh_mode_val}")
        print(f"   Records: {len(rows)}")
        print(f"   Summary: {summary}")
    except Exception as e:
        print(f"⚠️  Could not save metadata: {str(e)}")

# ============================================================================
# Summary
# ============================================================================

    print(f"\n{'='*80}")
    print("SYNC SUMMARY")
    print("=" * 80)

    print(f"\n✅ Job completed successfully!")
    print(f"\n   Mode: {refresh_mode_val}")
    print(f"   Rows synced: {len(rows)}")
    print(f"   Write mode: {write_mode}")
    print(f"   Table: {full_table_path}")
    print(f"   Total rows: {final_count}")
    print(f"   Timestamp: {sync_timestamp}")
    
    print(f"\n📜 SYNC HISTORY (last 5 syncs):")
    try:
        metadata_table = f"{table_name_val}_sync_meta"
        history = spark.sql(f"""
            SELECT last_sync_at, sync_type, records_synced, summary
            FROM {catalog_val}.{schema_val}.{metadata_table}
            ORDER BY last_sync_at DESC
            LIMIT 5
        """).collect()
        
        for i, row in enumerate(history, 1):
            print(f"   {i}. {row['last_sync_at'][:10]} | {row['sync_type']:12} | {row['records_synced']:3} records | {row['summary']}")
    except Exception as e:
        print(f"   (Could not retrieve history: {str(e)})")

    print(f"\n{'='*80}")

else:
    # No data to process
    print(f"\n{'='*80}")
    print("NO DATA TO SYNC")
    print("=" * 80)
    print(f"\n⚠️  Job completed with no data")
    print(f"\n   API returned: {len(all_styles)} total styles")
    print(f"   Matched folder '{folder_name_val}': 0 styles")
    
    if len(all_styles) > 0:
        unique_folders = set(s.get("folder", {}).get("name", "?") for s in all_styles if s.get("folder"))
        print(f"\n   Available folders in your account:")
        for folder in sorted(unique_folders):
            print(f"     - {folder}")
        print(f"\n   Please check:")
        print(f"     1. Folder name spelling (is it '{folder_name_val}' or something else?)")
        print(f"     2. Folder name case-sensitivity (should be exactly: {folder_name_val})")
    else:
        print(f"\n   Please check:")
        print(f"     1. BeProduct credentials are valid")
        print(f"     2. Your account has styles defined")
    
    print(f"\n{'='*80}")
