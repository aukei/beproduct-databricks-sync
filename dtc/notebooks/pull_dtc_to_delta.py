# Databricks notebook source
"""
DTC Master Chart Sync Notebook

Syncs a specific DTC request to a Databricks Delta table.
Can be run as a scheduled job.

Target Table: lft.beproduct.dtc_master_chart_uat
Source: DTC API (request ID: 69f076f0b7247a661226be9a)

Change Tracking Strategy:
--------------------------
The notebook tracks row-level changes to support bi-directional sync:

1. extracted_time: Timestamp when data was pulled from DTC (current sync)
2. last_modified: 
   - For modified rows: extracted_time (we just changed it)
   - For unchanged rows: DTC's updated_at (original timestamp)
3. Brand_modified: Boolean flag indicating if Brand was overwritten in THIS sync

Change Detection (Within Current Snapshot):
- Rows we modified: WHERE Brand_modified = True
- Rows unchanged: WHERE Brand_modified = False
- Rows needing push to DTC: Use Change Log table (see below)

Change Log Table (PRIMARY source for push operations):
- Table: {target_table}_change_log (e.g., dtc_master_chart_uat_change_log)
- Records: ALL Brand overwrites with old_value → new_value, timestamps
- Append-only: Never overwritten, full audit trail
- Usage: 
  * Audit trail of all modifications
  * Identify rows for push back to DTC
  * Track sync history

Query Rows That Need Push to DTC:
----------------------------------
-- Find all rows modified in recent syncs that haven't been pushed yet
SELECT DISTINCT row_id, lf_style, new_value as Brand, modified_at
FROM lft.beproduct.dtc_master_chart_uat_change_log
WHERE modification_type = 'brand_overwrite'
  AND sync_date >= current_date() - INTERVAL 1 DAYS
ORDER BY modified_at DESC;

Query Current Snapshot Modified Rows:
--------------------------------------
-- Find rows modified in THIS sync (from main table)
SELECT row_id, lf_style, Brand, last_modified, extracted_time
FROM lft.beproduct.dtc_master_chart_uat
WHERE Brand_modified = True;

Query Change History:
---------------------
SELECT * FROM lft.beproduct.dtc_master_chart_uat_change_log
WHERE lf_style = 'YOUR_STYLE_NUMBER'
ORDER BY modified_at DESC;
"""

# COMMAND ----------

# Import libraries
import sys
import logging
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("=" * 80)
print("DTC MASTER CHART SYNC")
print("=" * 80)
print(f"Start time: {datetime.now(timezone.utc).isoformat()}")

# COMMAND ----------

# CELL 1: Configuration & Secrets
print("\n[CELL 1] Configuration & Secrets")
print("-" * 80)

# Define widgets with defaults (will be overridden by job parameters if provided)
try:
    # These lines create the widgets with default values for interactive runs
    dbutils.widgets.text("dtc_workspace_name", "KTB", "DTC Workspace Name")
    dbutils.widgets.text("dtc_request_id", "6a26581854e92e7acd8fa71b", "DTC Request ID")
    dbutils.widgets.text("dtc_environment", "uat", "DTC Environment (uat/prod)")
    dbutils.widgets.text("dtc_customer", "KTB", "Customer code in DTC (e.g., KTB)")
    dbutils.widgets.text("beproduct_customer", "KTB", "Customer code in BeProduct (e.g., KTB)")
    dbutils.widgets.text("target_catalog", "lft", "Target Catalog")
    dbutils.widgets.text("target_schema", "beproduct", "Target Schema")
    dbutils.widgets.text("target_table", "dtc_master_chart_uat", "Target Table Name")
    dbutils.widgets.text("write_mode", "overwrite", "Write Mode (overwrite/append)")
except Exception as e:
    # If widgets already exist (running as job), this is expected
    pass

# Parameters (can be overridden by Databricks job)
DTC_WORKSPACE_NAME = dbutils.widgets.get("dtc_workspace_name")
DTC_REQUEST_ID = dbutils.widgets.get("dtc_request_id")
DTC_ENVIRONMENT = dbutils.widgets.get("dtc_environment")
DTC_CUSTOMER = dbutils.widgets.get("dtc_customer")
BEPRODUCT_CUSTOMER = dbutils.widgets.get("beproduct_customer")
TARGET_CATALOG = dbutils.widgets.get("target_catalog")
TARGET_SCHEMA = dbutils.widgets.get("target_schema")
TARGET_TABLE = dbutils.widgets.get("target_table")
WRITE_MODE = dbutils.widgets.get("write_mode")

print(f"Workspace: {DTC_WORKSPACE_NAME}")
print(f"Request ID: {DTC_REQUEST_ID}")
print(f"Environment: {DTC_ENVIRONMENT}")
print(f"Customer: DTC={DTC_CUSTOMER}, BeProduct={BEPRODUCT_CUSTOMER}")
print(f"Target: {TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}")

# Get DTC API key from Databricks secrets
try:
    dtc_api_key = dbutils.secrets.get("beproduct", "dtc_api_key_uat")
    print("✅ DTC API key loaded from secrets")
except Exception as e:
    print(f"❌ Failed to load DTC API key: {e}")
    print("   You need to set up the secret:")
    print("   databricks secrets put-secret beproduct dtc_api_key_uat --string-value YOUR_KEY")
    raise

# COMMAND ----------

# CELL 2: Import DTCConnector
print("\n[CELL 2] Import DTCConnector")
print("-" * 80)

# Add python library path for imports
# Notebook location: /Workspace/Repos/beproduct-sync/DTC/notebooks/pull_dtc_to_delta
# Python modules location: /Workspace/Repos/beproduct-sync/DTC/python/
python_path = "/Workspace/Repos/beproduct-sync/DTC/python"
sys.path.insert(0, python_path)
print(f"📁 Python path: {python_path}")

try:
    from connectors.dtc import DTCConnector
    print("✅ DTCConnector imported successfully")
except ImportError as e:
    print(f"❌ Failed to import DTCConnector: {e}")
    print(f"   Python path: {sys.path}")
    print("   Make sure the databricks/dtc/python folder exists in the workspace")
    raise

# COMMAND ----------

# CELL 3: Pull Data from DTC
print("\n[CELL 3] Pull Data from DTC")
print("-" * 80)

try:
    # Initialize connector
    connector = DTCConnector(
        api_key=dtc_api_key,
        environment=DTC_ENVIRONMENT,
        workspace_name=DTC_WORKSPACE_NAME,
    )
    print(f"✅ DTCConnector initialized")
    print(f"   Workspace: {DTC_WORKSPACE_NAME}")
    print(f"   Environment: {DTC_ENVIRONMENT}")

    # Get request details
    request = connector.get_request(DTC_REQUEST_ID)
    request_ref = request.get("requestReference", "UNKNOWN")
    sheet_id = request.get("sheetId")
    print(f"✅ Request loaded: {request_ref} (sheet: {sheet_id})")

    # Get available views
    views = connector.get_views(DTC_REQUEST_ID)
    print(f"✅ Found {len(views)} views")
    
    # IMPORTANT: Always use "WIP_ITS_USE" view to get complete, unfiltered data
    # Other views may hide columns or rows, compromising data integrity
    view_id = None
    full_version_view = None
    
    for v in views:
        print(f"   - {v.get('viewName')}")
        if v.get("viewName") == "WIP_ITS_USE":
            full_version_view = v
            view_id = v.get("viewId")
    
    if not view_id:
        print(f"\n❌ ERROR: 'WIP_ITS_USE' view not found!")
        print(f"   Available views: {[v.get('viewName') for v in views]}")
        print(f"\n   REQUIREMENT: DTC request must have a 'WIP_ITS_USE' view")
        print(f"   to ensure complete, unfiltered data is pulled.")
        print(f"\n   Contact DTC admin to configure the 'WIP_ITS_USE' view.")
        raise ValueError(f"'WIP_ITS_USE' view not found for request {DTC_REQUEST_ID}")
    
    print(f"✅ Using view: WIP_ITS_USE (id: {view_id})")

    # Pull data to DataFrame and Document metadata
    print(f"Pulling sheet data...")
    df, document_metadata = connector.pull_request_to_dataframe(DTC_REQUEST_ID, view_id)
    print(f"✅ Pulled {len(df)} rows, {len(df.columns)} columns")
    
    # Display document metadata
    print(f"\nDocument Metadata:")
    for key, value in document_metadata.items():
        print(f"  {key}: {value}")
    
    # Display sample
    print(f"\nDataFrame shape: {df.shape}")
    print(f"\nColumn names (normalized for Delta Lake):")
    print(f"  Note: HTML tags and spaces removed from DTC field names")
    print(f"  Example: 'Product Status' → 'Product_Status'")
    print(f"  Example: 'Proto Sample<BR/>Date' → 'Proto_SampleDate'")
    print(f"\nSample columns (first 10):")
    for i, col in enumerate(list(df.columns)[:10], 1):
        print(f"  {i}. {col}")
    if len(df.columns) > 10:
        print(f"  ... and {len(df.columns) - 10} more")
    
    connector.close()

except Exception as e:
    print(f"❌ Failed to pull from DTC: {e}")
    import traceback
    traceback.print_exc()
    raise

# COMMAND ----------

# CELL 4: Convert to Spark DataFrame
print("\n[CELL 4] Convert to Spark DataFrame")
print("-" * 80)

try:
    # BUSINESS LOGIC: Brand column population + Change Tracking
    # The request name (e.g., "KTB FW26 Wrangler") is the source of truth for Brand.
    # All rows should have the brand from the request name, not from the sheet data.
    # 
    # Change Tracking:
    # - extracted_time: when we pulled data from DTC (now)
    # - last_modified: DTC's updated_at OR extracted_time if we modified
    # - Change detection: last_modified > previous extracted_time = changed rows
    #
    # Change Log:
    # - Track all modifications (Brand overwrites) for push back to DTC
    
    from collections import Counter
    import pandas as pd
    import numpy as np
    from datetime import datetime, timezone
    
    print(f"\n🔧 Applying Brand Business Logic + Change Tracking:")
    print(f"   Request name is source of truth for Brand")
    
    # Add extracted_time timestamp (when we pulled from DTC)
    extraction_timestamp = datetime.now(timezone.utc).isoformat()
    df['extracted_time'] = extraction_timestamp
    print(f"   ✅ Added extracted_time: {extraction_timestamp}")
    
    # Initialize change log (for recording modifications)
    change_log_records = []
    
    if 'brand' in df.columns:
        metadata_brand = df['brand'].iloc[0] if len(df) > 0 else None
        print(f"   Metadata brand (from request name): '{metadata_brand}'")
        
        if 'Brand' in df.columns:
            # Store original Brand values for comparison and change log
            original_brand = df['Brand'].copy()
            
            # Check what's different
            unique_brands = df['Brand'].unique()
            different_values = [b for b in unique_brands if b != metadata_brand and pd.notna(b)]
            
            if different_values:
                print(f"   ⚠️  Found {len(different_values)} different Brand values in sheet:")
                for val in different_values[:5]:
                    count = (df['Brand'] == val).sum()
                    print(f"      '{val}': {count} rows")
                if len(different_values) > 5:
                    print(f"      ... and {len(different_values) - 5} more")
            
            # Mark rows as modified only if original value differs from metadata brand
            df['Brand_modified'] = (
                (df['Brand'] != metadata_brand) | 
                (df['Brand'].isna()) | 
                (df['Brand'] == '')
            )
            
            # For modified rows, collect change log entries
            for idx in df[df['Brand_modified'] == True].index:
                change_log_records.append({
                    'row_id': df.loc[idx, 'row_id'] if 'row_id' in df.columns else None,
                    'row_index': df.loc[idx, 'row_index'] if 'row_index' in df.columns else idx,
                    'lf_style': df.loc[idx, 'LF_Style'] if 'LF_Style' in df.columns else None,
                    'request_id': df.loc[idx, 'request_id'] if 'request_id' in df.columns else None,
                    'column_name': 'Brand',
                    'old_value': str(original_brand.loc[idx]) if pd.notna(original_brand.loc[idx]) else None,
                    'new_value': metadata_brand,
                    'modified_at': extraction_timestamp,
                    'modification_type': 'brand_overwrite'
                })
            
            modified_count = df['Brand_modified'].sum()
            unchanged_count = len(df) - modified_count
            
            # Overwrite Brand column with metadata brand for ALL rows
            df['Brand'] = metadata_brand
            
            # Update last_modified for modified rows
            # Use DTC's updated_at if available, otherwise use extracted_time
            if 'updated_at' in df.columns:
                # For modified rows, set last_modified to extraction time (we just changed it)
                # For unchanged rows, keep DTC's updated_at
                df['last_modified'] = df.apply(
                    lambda row: extraction_timestamp if row['Brand_modified'] else row['updated_at'],
                    axis=1
                )
            else:
                # No DTC updated_at, use extracted_time for all
                df['last_modified'] = extraction_timestamp
            
            print(f"   ✅ Set all rows: Brand = '{metadata_brand}'")
            print(f"   ✅ Brand_modified flag:")
            print(f"      - Modified: {modified_count} rows (were different/empty)")
            print(f"      - Unchanged: {unchanged_count} rows (already matched)")
            print(f"   ✅ Updated last_modified for {modified_count} modified rows")
            print(f"   ✅ Collected {len(change_log_records)} change log entries")
            
            # Drop lowercase 'brand' to avoid case-insensitive duplicate
            df = df.drop(columns=['brand'])
            print(f"   ✅ Removed metadata 'brand' column (merged into 'Brand')")
        else:
            # No Brand column in sheet, rename metadata brand to Brand
            df = df.rename(columns={'brand': 'Brand'})
            df['Brand_modified'] = True  # All rows are new values
            df['last_modified'] = extraction_timestamp
            
            # Log all as new
            for idx in df.index:
                change_log_records.append({
                    'row_id': df.loc[idx, 'row_id'] if 'row_id' in df.columns else None,
                    'row_index': df.loc[idx, 'row_index'] if 'row_index' in df.columns else idx,
                    'lf_style': df.loc[idx, 'LF_Style'] if 'LF_Style' in df.columns else None,
                    'request_id': df.loc[idx, 'request_id'] if 'request_id' in df.columns else None,
                    'column_name': 'Brand',
                    'old_value': None,
                    'new_value': metadata_brand,
                    'modified_at': extraction_timestamp,
                    'modification_type': 'brand_populated'
                })
            
            print(f"   ℹ️  No 'Brand' column in sheet, using metadata brand")
            print(f"   ✅ Renamed: 'brand' → 'Brand'")
            print(f"   ✅ Brand_modified = True (all rows are new)")
    elif 'Brand' in df.columns:
        # Only sheet Brand exists, no metadata brand
        print(f"   ⚠️  No metadata 'brand' found (request name parsing failed?)")
        print(f"   Keeping existing 'Brand' column as-is")
        df['Brand_modified'] = False  # Not modified
        df['last_modified'] = df['updated_at'] if 'updated_at' in df.columns else extraction_timestamp
    
    # Store change log in DataFrame for later write
    change_log_df = pd.DataFrame(change_log_records) if change_log_records else pd.DataFrame()
    
    print(f"\n✅ Brand business logic + change tracking applied")
    print(f"   Columns added: extracted_time, last_modified, Brand_modified")
    print(f"   Change log entries: {len(change_log_records)}")
    
    # Convert Pandas to Spark
    spark_df = spark.createDataFrame(df)
    print(f"✅ Created Spark DataFrame: {spark_df.count()} rows")
    
    # Show schema
    print("\nSchema:")
    spark_df.printSchema()

except Exception as e:
    print(f"❌ Failed to create Spark DataFrame: {e}")
    raise

# COMMAND ----------

# CELL 5: Join SeasonCode Mapping
print("\n[CELL 5] Join SeasonCode Mapping")
print("-" * 80)

from pyspark.sql.functions import lit, current_timestamp, col, substring, regexp_extract, when
from pyspark.sql.types import IntegerType
from collections import Counter

# Map DTC seasonCode to BeProduct (Season, Year)
# 
# Mapping table structure (lft.beproduct.dtc_seasoncode_mapping):
#   CUSTOMER (BeProduct customer code, e.g., "KTB")
#   BPSEASON (BeProduct season name, e.g., "SPRING", "FALL")
#   DTCCODE (DTC season code prefix, e.g., "SS", "FW")
#
# DTC season_code format: "<prefix><year>" (e.g., "SS28", "FW27")
#   - First 2 chars = season code prefix (maps to DTCCODE in mapping)
#   - Remaining chars = year (2 or 4 digits)

mapping_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.dtc_seasoncode_mapping"

try:
    # DIAGNOSTIC: Check columns before join
    print(f"\n🔍 BEFORE JOIN:")
    print(f"   Columns in spark_df: {len(spark_df.columns)}")
    col_counts_before = Counter(spark_df.columns)
    duplicates_before = {col: count for col, count in col_counts_before.items() if count > 1}
    if duplicates_before:
        print(f"   ⚠️  DUPLICATE COLUMNS DETECTED:")
        for col, count in duplicates_before.items():
            print(f"      '{col}' appears {count} times")
    
    # Step 1: Add beproduct_customer column from parameter
    print(f"\nAdding beproduct_customer column from parameter")
    spark_df = spark_df.withColumn("beproduct_customer", lit(BEPRODUCT_CUSTOMER))
    print(f"  beproduct_customer = {BEPRODUCT_CUSTOMER}")
    
    # Step 2: Extract season code prefix and year
    # Examples: "SS28" → prefix="SS", year="28"
    print(f"Extracting season code components from season_code")
    spark_df = spark_df.withColumn(
        "season_code_prefix",
        substring(col("season_code"), 1, 2)
    ).withColumn(
        "season_code_year",
        regexp_extract(col("season_code"), r"(\d+)$", 1)
    )
    
    # Step 3: Load mapping table
    mapping_df = spark.table(mapping_table)
    print(f"✅ Loaded mapping table: {mapping_table}")
    
    # Step 4: Create mapping lookup (simple dict approach via broadcast)
    mapping_data = mapping_df.select(
        col("CUSTOMER"),
        col("DTCCODE"),
        col("BPSEASON").alias("mapped_season")
    )
    
    # Step 5: Join using the mapping table
    # Strategy: keep all columns from spark_df, add mapped_season from lookup
    spark_df = spark_df.join(
        mapping_data,
        (spark_df["beproduct_customer"] == mapping_data["CUSTOMER"]) &
        (spark_df["season_code_prefix"] == mapping_data["DTCCODE"]),
        how="left"
    )
    
    # DIAGNOSTIC: Check columns after join
    print(f"\n🔍 AFTER JOIN:")
    print(f"   Columns in joined spark_df: {len(spark_df.columns)}")
    col_counts_after = Counter(spark_df.columns)
    duplicates_after = {col: count for col, count in col_counts_after.items() if count > 1}
    if duplicates_after:
        print(f"   ⚠️  DUPLICATE COLUMNS DETECTED AFTER JOIN:")
        for col, count in duplicates_after.items():
            print(f"      '{col}' appears {count} times")
        print(f"\n   All columns after join:")
        for i, col in enumerate(spark_df.columns, 1):
            marker = "⚠️ " if col in duplicates_after else "  "
            print(f"   {marker}{i:3d}. {col}")
    
    # Step 6: Drop the join key columns from mapping_data that we don't need
    # (CUSTOMER and DTCCODE were only used for the join condition)
    spark_df = spark_df.drop("CUSTOMER", "DTCCODE")
    
    # Step 7: Rename mapped_season to beproduct_season
    spark_df = spark_df.withColumnRenamed("mapped_season", "beproduct_season")
    
    # Step 8: Add beproduct_year from extracted year
    spark_df = spark_df.withColumn(
        "beproduct_year",
        col("season_code_year").cast(IntegerType())
    )
    
    # Step 9: Clean up temporary columns
    spark_df = spark_df.drop("season_code_prefix", "season_code_year")
    
    # DIAGNOSTIC: Check final columns
    print(f"\n🔍 FINAL (after cleanup):")
    print(f"   Columns: {len(spark_df.columns)}")
    col_counts_final = Counter(spark_df.columns)
    duplicates_final = {col: count for col, count in col_counts_final.items() if count > 1}
    if duplicates_final:
        print(f"   ⚠️  DUPLICATE COLUMNS STILL PRESENT:")
        for col, count in duplicates_final.items():
            print(f"      '{col}' appears {count} times")
    else:
        print(f"   ✅ No duplicate columns")
    
    joined_count = spark_df.count()
    print(f"\n✅ Joined with seasonCode mapping")
    print(f"   Join condition: beproduct_customer = CUSTOMER AND season_code_prefix = DTCCODE")
    print(f"   Added columns: beproduct_season, beproduct_year")
    print(f"   Temporary columns removed: season_code_prefix, season_code_year")
    print(f"   Total rows after join: {joined_count}")
    
except Exception as e:
    print(f"⚠️  Warning: Could not join with mapping table: {e}")
    print(f"   Mapping table path: {mapping_table}")
    print(f"   Adding NULL columns for beproduct_season and beproduct_year")
    import traceback
    traceback.print_exc()
    spark_df = spark_df.withColumn("beproduct_season", lit(None)) \
                       .withColumn("beproduct_year", lit(None))

# COMMAND ----------

# CELL 6: Add Metadata Columns
print("\n[CELL 6] Add Metadata Columns")
print("-" * 80)

# Add sync metadata
spark_df = spark_df.withColumn("sync_timestamp", current_timestamp()) \
                   .withColumn("sync_date", lit(datetime.now().date()))

print(f"✅ Added metadata columns")

# COMMAND ----------

# CELL 7: Write to Delta Table
print("\n[CELL 7] Write to Delta Table")
print("-" * 80)

target_table_path = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}"
print(f"Writing to: {target_table_path}")

try:
    # Write mode options:
    # - overwrite: replace entire table
    # - append: add to existing
    # - merge: upsert based on row_id
    
    if WRITE_MODE == "overwrite":
        print(f"Write mode: OVERWRITE (replace entire table)")
        spark_df.write.format("delta").mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(target_table_path)
    elif WRITE_MODE == "append":
        print(f"Write mode: APPEND (add rows)")
        spark_df.write.format("delta").mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(target_table_path)
    else:
        print(f"Write mode: {WRITE_MODE}")
        spark_df.write.format("delta").mode(WRITE_MODE) \
            .option("mergeSchema", "true") \
            .saveAsTable(target_table_path)
    
    print(f"✅ Data written to {target_table_path}")
    
    # Store Document metadata as table properties
    print(f"\nStoring Document metadata as table properties...")
    try:
        # Build ALTER TABLE statement for properties
        properties_statements = []
        for key, value in document_metadata.items():
            # Escape values for SQL
            sql_value = str(value).replace("'", "''") if value else ""
            properties_statements.append(f"'{key}'='{sql_value}'")
        
        if properties_statements:
            props_sql = ", ".join(properties_statements)
            alter_sql = f"ALTER TABLE {target_table_path} SET TBLPROPERTIES ({props_sql})"
            spark.sql(alter_sql)
            print(f"✅ Document metadata stored as table properties")
            print(f"   Document: {document_metadata.get('document_name')}")
            print(f"   Request: {document_metadata.get('request_reference')}")
            print(f"   Owner: {document_metadata.get('owner_name')}")
    except Exception as prop_error:
        print(f"⚠️  Warning: Could not set table properties: {prop_error}")
        # Don't fail the entire sync for this

except Exception as e:
    print(f"❌ Failed to write to Delta table: {e}")
    import traceback
    traceback.print_exc()
    raise

# COMMAND ----------

# CELL 8: Write Change Log
print("\n[CELL 8] Write Change Log")
print("-" * 80)

try:
    if len(change_log_df) > 0:
        # Convert change log to Spark DataFrame
        change_log_spark = spark.createDataFrame(change_log_df)
        
        # Add sync metadata
        change_log_spark = change_log_spark.withColumn("sync_timestamp", current_timestamp()) \
                                           .withColumn("sync_date", lit(datetime.now().date())) \
                                           .withColumn("request_reference", lit(request_ref)) \
                                           .withColumn("environment", lit(DTC_ENVIRONMENT))
        
        # Change log table name
        change_log_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}_change_log"
        
        print(f"Writing {len(change_log_df)} change log entries to: {change_log_table}")
        
        # Append changes to log table (never overwrite)
        change_log_spark.write.format("delta").mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(change_log_table)
        
        print(f"✅ Change log written successfully")
        print(f"   Table: {change_log_table}")
        print(f"   Entries: {len(change_log_df)}")
        print(f"\n   Change types:")
        for mod_type, count in change_log_df['modification_type'].value_counts().items():
            print(f"      - {mod_type}: {count}")
        
        # Show sample of changes
        print(f"\n   Sample changes (first 5):")
        change_log_spark.select("row_index", "lf_style", "column_name", "old_value", "new_value", "modified_at") \
                        .limit(5).display()
    else:
        print(f"ℹ️  No changes detected, skipping change log write")
        
except Exception as e:
    print(f"⚠️  Warning: Could not write change log: {e}")
    import traceback
    traceback.print_exc()
    # Don't fail the entire sync for change log issues

# COMMAND ----------

# CELL 9: Verify Write
print("\n[CELL 9] Verify Write")
print("-" * 80)

try:
    # Read back and verify
    verify_df = spark.read.table(target_table_path)
    row_count = verify_df.count()
    col_count = len(verify_df.columns)
    
    print(f"✅ Table verified:")
    print(f"   Rows: {row_count}")
    print(f"   Columns: {col_count}")
    print(f"   Last updated: {datetime.now(timezone.utc).isoformat()}")
    
    # Display sample
    print(f"\nSample data (first 3 rows):")
    verify_df.select("request_reference", "row_index", "lf_style", "sync_timestamp").limit(3).display()

except Exception as e:
    print(f"⚠️  Could not verify table: {e}")

# COMMAND ----------

print("\n" + "=" * 80)
print("✅ SYNC COMPLETE")
print("=" * 80)
print(f"End time: {datetime.now(timezone.utc).isoformat()}")
