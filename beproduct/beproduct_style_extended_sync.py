# Databricks notebook source
"""
BeProduct STYLE Extended Sync Job
==================================

Extended version of beproduct_style_sync.py that extracts additional fields for DTC integration:
  - Colorways array ($.colorways[].colorName) 
  - BOM material fields (core_main_material, Core_main_material2)
  - Material category and content
  - Front image URL (frontImage.origin)

This creates an intermediate table that will be denormalized in the next step.

Schedule: Daily at 11am UTC (before denormalization)

Parameters:
  - refresh_mode: "FULL" (default) or "INCREMENTAL"
  - catalog: Target Databricks catalog (default: "lft")
  - schema: Target Databricks schema (default: "beproduct")
  - folder_name: BeProduct folder name (default: "KTB")
  - table_name: Table name (default: "ktb_styles_extended")
"""

# COMMAND ----------

# ============================================================================
# CELL 1: Setup - Install SDK, Import, Configure Parameters
# ============================================================================

import sys
import subprocess

print("=" * 80)
print("SETUP CELL: Install SDK, Import Libraries, Configure Parameters")
print("=" * 80)

# Install BeProduct SDK
print("\n📦 Installing BeProduct SDK...")
try:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "beproduct"])
    print("✅ BeProduct SDK installed")
except Exception as e:
    print(f"❌ Failed: {str(e)}")
    raise

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
dbutils.widgets.text("table_name", "ktb_styles_extended", "Table Name")

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
print("EXTENDED SYNC CELL: Fetch with Colorways, BOM, Materials, Images")
print("=" * 80)

# Get parameters from previous cell
folder_name_val = dbutils.widgets.get("folder_name")
refresh_mode_val = dbutils.widgets.get("refresh_mode").upper()
catalog_val = dbutils.widgets.get("catalog")
schema_val = dbutils.widgets.get("schema")
table_name_val = dbutils.widgets.get("table_name")

# Field mapping configuration - ALL fields from requirements
# Compulsory fields
COMPULSORY_FIELDS = {
    "LF Style Number": "lf_style_number",
    "SEASON": "season",
    "YEAR": "year",
    "BRANDS": "brands",
}

# Interested fields
INTERESTED_FIELDS = {
    "DESCRIPTION": "description",
    "TEAM": "team",
    "PRODUCT STATUS": "product_status",
    "CUSTOMER STYLE NUMBER / PLM #": "customer_style_number",
    "PRODUCT CATEGORY": "product_category",
    "PRODUCT SUB CATEGORY": "product_sub_category",
    "Division": "division",
    "GARMENT FINISH": "garment_finish",
    "TECHPACK STAGE": "techpack_stage",
    "Lot Code": "lot_code",
    "PARENT VENDOR": "parent_vendor",
    "FACTORY": "factory",
}

# NEW: BOM and Material fields (field IDs, not display names)
# These are extracted from headerData by field ID
BOM_MATERIAL_FIELDS = {
    "core_main_material": "bom_material_1",        # Main Fabric material
    "Core_main_material2": "bom_material_2",       # Secondary fabric material
    "main_material_category": "main_material_category",  # Material category
    "main_material_content": "main_material_content",    # Material content
}

EXTRACTED_FIELDS = {**COMPULSORY_FIELDS, **INTERESTED_FIELDS}

print(f"\n📋 Configuration:")
print(f"   Folder: {folder_name_val}")
print(f"   Mode: {refresh_mode_val}")
print(f"   Target: {catalog_val}.{schema_val}.{table_name_val}")
print(f"   Standard fields: {len(EXTRACTED_FIELDS)}")
print(f"   BOM/Material fields: {len(BOM_MATERIAL_FIELDS)}")
print(f"   NEW: Colorways extraction enabled")
print(f"   NEW: Front image extraction enabled")

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
    print("   ✓ All credentials retrieved")
    
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
    """Get last sync timestamp for incremental refresh."""
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
    
    # Get iterator
    iterator = api.style.attributes_list(filters=filters)
    
    # Iterate through results
    for style in iterator:
        all_styles.append(style)
        
        # Filter by specified folder
        folder_obj = style.get("folder", {})
        actual_folder = folder_obj.get("name", "") if folder_obj else ""
        if actual_folder == folder_name_val:
            styles.append(style)
            if len(styles) % 50 == 0:
                print(f"     Matched {len(styles)} styles so far...")
    
    print(f"\n✅ Fetch complete:")
    print(f"   Total results from API: {len(all_styles)}")
    print(f"   Styles with folder='{folder_name_val}': {len(styles)}")

except Exception as e:
    print(f"❌ Failed to fetch styles: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

HAS_DATA = len(styles) > 0

if not HAS_DATA:
    print(f"\n❌ No styles to sync")
    print(f"\n⚠️  No data to process - exiting")
    dbutils.notebook.exit("NO_DATA")
else:
    print(f"\n✅ {len(styles)} styles ready for processing")

# ============================================================================
# Step 4: Transform Records with EXTENDED FIELDS
# ============================================================================

print(f"\n{'='*80}")
print("Step 4: Transform Records (EXTENDED)")
print("=" * 80)

def extract_colorways(record: Dict) -> List[str]:
    """
    Extract color names from colorways array.
    
    Per requirements: $.colorways[].colorName
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


def extract_bom_materials(header_data: Dict) -> Dict[str, Optional[str]]:
    """
    Extract BOM material fields by field ID.
    
    Per requirements (line 65):
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
    
    Per requirements (line 115): Use frontImage.origin URL
    """
    front_image = header_data.get("frontImage")
    if not front_image or not isinstance(front_image, dict):
        return None
    
    origin_url = front_image.get("origin")
    return origin_url if origin_url else None


def transform_style_record_extended(record: Dict) -> Dict:
    """Transform a BeProduct Style record with EXTENDED fields."""
    # Extract folder info
    folder_obj = record.get("folder", {})
    folder_name = folder_obj.get("name", "") if folder_obj else ""
    
    row = {
        "id": record.get("id"),
        "folder_name": folder_name,
        "synced_at": datetime.now(timezone.utc),
    }
    
    # Parse timestamps
    if "createdAt" in record:
        try:
            created_str = record["createdAt"]
            row["created_at"] = datetime.fromisoformat(created_str.replace('Z', '+00:00'))
        except:
            row["created_at"] = None
    
    if "modifiedAt" in record:
        try:
            modified_str = record["modifiedAt"]
            row["modified_at"] = datetime.fromisoformat(modified_str.replace('Z', '+00:00'))
        except:
            row["modified_at"] = None
    
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
    row["colorways_array"] = colorways  # Store as array
    row["colorways_count"] = len(colorways)
    
    # NEW: Extract BOM material fields
    bom_data = extract_bom_materials(header_data)
    row.update(bom_data)  # Add bom_material_1, bom_material_2, etc.
    
    # NEW: Extract front image URL
    row["front_image_url"] = extract_front_image_url(header_data)
    
    # Store full record as JSON
    row["data_json"] = json.dumps(record)
    
    return row

try:
    print(f"🔄 Transforming {len(styles)} records with EXTENDED fields...")
    print(f"   - Standard fields: {len(EXTRACTED_FIELDS)}")
    print(f"   - BOM/Material fields: {len(BOM_MATERIAL_FIELDS)}")
    print(f"   - Colorways: array extraction")
    print(f"   - Front image: URL extraction")
    
    rows = [transform_style_record_extended(s) for s in styles]
    
    # Print sample for verification
    if rows:
        sample = rows[0]
        print(f"\n   📋 Sample row structure:")
        print(f"      - colorways_array: {sample.get('colorways_array', [])} ({sample.get('colorways_count', 0)} colors)")
        print(f"      - bom_material_1: {sample.get('bom_material_1', 'N/A')}")
        print(f"      - bom_material_2: {sample.get('bom_material_2', 'N/A')}")
        print(f"      - front_image_url: {sample.get('front_image_url', 'N/A')[:50] if sample.get('front_image_url') else 'N/A'}...")
    
    print(f"✅ Transformed {len(rows)} rows")
except Exception as e:
    print(f"❌ Failed to transform: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

# ============================================================================
# Step 5: Create Spark DataFrame with EXTENDED Schema
# ============================================================================

print(f"\n{'='*80}")
print("Step 5: Create Spark DataFrame")
print("=" * 80)

try:
    print(f"📊 Creating DataFrame from {len(rows)} rows...")
    
    # Define schema with proper types
    schema = StructType([
        # Standard fields
        StructField("id", StringType(), True),
        StructField("folder_name", StringType(), True),
        StructField("synced_at", TimestampType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("modified_at", TimestampType(), True),
        
        # Extracted standard fields
        StructField("lf_style_number", StringType(), True),
        StructField("season", StringType(), True),
        StructField("year", StringType(), True),
        StructField("brands", StringType(), True),
        StructField("description", StringType(), True),
        StructField("team", StringType(), True),
        StructField("product_status", StringType(), True),
        StructField("customer_style_number", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_sub_category", StringType(), True),
        StructField("division", StringType(), True),
        StructField("garment_finish", StringType(), True),
        StructField("techpack_stage", StringType(), True),
        StructField("lot_code", StringType(), True),
        StructField("parent_vendor", StringType(), True),
        StructField("factory", StringType(), True),
        
        # NEW: Extended fields
        StructField("colorways_array", ArrayType(StringType()), True),  # Array of color names
        StructField("colorways_count", StringType(), True),
        StructField("bom_material_1", StringType(), True),              # Main Fabric
        StructField("bom_material_2", StringType(), True),              # Secondary Fabric
        StructField("main_material_category", StringType(), True),
        StructField("main_material_content", StringType(), True),
        StructField("front_image_url", StringType(), True),
        
        # Full JSON
        StructField("data_json", StringType(), True),
    ])
    
    # Convert to Spark rows
    spark_rows = [Row(**row) for row in rows]
    
    # Create DataFrame
    df = spark.createDataFrame(spark_rows, schema=schema)
    
    print(f"✅ Created DataFrame:")
    print(f"   Rows: {df.count()}")
    print(f"   Columns: {len(df.columns)}")
    
    # Show sample
    print(f"\n   Sample data:")
    df.select(
        "lf_style_number", "season", "brands", 
        "colorways_count", "bom_material_1", "front_image_url"
    ).show(3, truncate=50)

except Exception as e:
    print(f"❌ Failed to create DataFrame: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

# ============================================================================
# Step 6: Write to Delta Lake
# ============================================================================

print(f"\n{'='*80}")
print("Step 6: Write to Delta Lake")
print("=" * 80)

full_table_name = f"{catalog_val}.{schema_val}.{table_name_val}"

try:
    print(f"💾 Writing to {full_table_name}...")
    
    # Use overwrite for FULL, append for INCREMENTAL
    write_mode = "overwrite" if refresh_mode_val == "FULL" else "append"
    
    df.write.format("delta") \
        .mode(write_mode) \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true" if refresh_mode_val == "FULL" else "false") \
        .saveAsTable(full_table_name)
    
    print(f"✅ Write complete:")
    print(f"   Mode: {write_mode}")
    print(f"   Table: {full_table_name}")

except Exception as e:
    print(f"❌ Failed to write: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

# ============================================================================
# Step 7: Update Sync Metadata
# ============================================================================

print(f"\n{'='*80}")
print("Step 7: Update Sync Metadata")
print("=" * 80)

metadata_table = f"{catalog_val}.{schema_val}.{table_name_val}_sync_meta"

try:
    print(f"📝 Updating metadata: {metadata_table}...")
    
    # Create metadata record
    sync_time = datetime.now(timezone.utc).isoformat()
    metadata = spark.createDataFrame([{
        "last_sync_at": sync_time,
        "rows_synced": len(rows),
        "refresh_mode": refresh_mode_val,
        "folder_name": folder_name_val,
    }])
    
    # Write metadata (overwrite - only keep latest)
    metadata.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(metadata_table)
    
    print(f"✅ Metadata updated:")
    print(f"   Last sync: {sync_time}")
    print(f"   Rows synced: {len(rows)}")

except Exception as e:
    print(f"⚠️  Failed to update metadata: {str(e)}")
    # Don't fail the job for metadata errors

# ============================================================================
# Final Summary
# ============================================================================

print(f"\n{'='*80}")
print("✅ SYNC COMPLETE")
print("=" * 80)
print(f"\nSummary:")
print(f"  Folder: {folder_name_val}")
print(f"  Mode: {refresh_mode_val}")
print(f"  Styles synced: {len(rows)}")
print(f"  Target table: {full_table_name}")
print(f"  Colorways extracted: ✅")
print(f"  BOM materials extracted: ✅")
print(f"  Front images extracted: ✅")
print(f"\nNext step:")
print(f"  Run beproduct_to_dtc_transform.py to denormalize data")

# COMMAND ----------

# Validation Query
print("\n" + "=" * 80)
print("Validation: Check Extended Data")
print("=" * 80)

spark.sql(f"""
SELECT 
    lf_style_number,
    season,
    brands,
    colorways_count,
    colorways_array,
    bom_material_1,
    bom_material_2,
    CASE WHEN front_image_url IS NOT NULL THEN 'Yes' ELSE 'No' END as has_image
FROM {full_table_name}
LIMIT 5
""").show(truncate=False)

print("\n✅ Extended sync complete!")
