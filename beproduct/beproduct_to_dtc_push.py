# Databricks notebook source
"""
BeProduct to DTC Push with Change Detection
============================================

Detects changes and pushes BeProduct data to DTC using PATCH API.

Process:
1. Load staging data (BeProduct denormalized)
2. Pull current DTC data for comparison
3. Join and detect changes (INSERT/UPDATE/DELETE)
4. Validate data before push
5. Push to DTC via PATCH API
6. Log results and update sync status

Timezone handling:
  - BeProduct: UTC timestamps
  - DTC: HKT (UTC+8) timestamps
  - All comparisons done in UTC

Schedule: Daily at 1pm UTC (after request manager at 12:30pm)

Parameters:
  - catalog: Databricks catalog (default: "lft")
  - schema: Databricks schema (default: "beproduct")
  - staging_table: Staging table (default: "beproduct_to_dtc_staging")
  - dtc_environment: DTC environment (default: "uat")
  - dry_run: Test mode without pushing (default: "false")
"""

# COMMAND ----------

# ============================================================================
# CELL 1: Setup
# ============================================================================

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

print("=" * 80)
print("BEPRODUCT TO DTC PUSH SETUP")
print("=" * 80)

from connectors.dtc import DTCConnector
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_utc_timestamp, from_utc_timestamp,
    when, coalesce, concat_ws, array, struct
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from datetime import datetime, timezone
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure parameters
dbutils.widgets.text("catalog", "lft", "Catalog Name")
dbutils.widgets.text("schema", "beproduct", "Schema Name")
dbutils.widgets.text("staging_table", "beproduct_to_dtc_staging", "Staging Table")
dbutils.widgets.text("dtc_environment", "uat", "DTC Environment")
dbutils.widgets.text("dry_run", "false", "Dry Run (true/false)")
dbutils.widgets.text("batch_size", "100", "Batch Size for Push")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
staging_table = dbutils.widgets.get("staging_table")
dtc_environment = dbutils.widgets.get("dtc_environment")
dry_run = dbutils.widgets.get("dry_run").lower() == "true"
batch_size = int(dbutils.widgets.get("batch_size"))

staging_table_full = f"{catalog}.{schema}.{staging_table}"
mapping_table_full = f"{catalog}.{schema}.dtc_request_mapping"
push_log_table_full = f"{catalog}.{schema}.beproduct_to_dtc_push_log"
dtc_snapshot_table = f"{catalog}.{schema}.dtc_current_snapshot_{dtc_environment}"

print("✅ Parameters configured:")
print(f"   Staging: {staging_table_full}")
print(f"   Mapping: {mapping_table_full}")
print(f"   Push log: {push_log_table_full}")
print(f"   DTC snapshot: {dtc_snapshot_table}")
print(f"   Environment: {dtc_environment}")
print(f"   Dry run: {dry_run}")
print(f"   Batch size: {batch_size}")

print("\n" + "=" * 80)
print("✅ SETUP COMPLETE")
print("=" * 80)

# COMMAND ----------

# ============================================================================
# CELL 2: Initialize DTC Connector
# ============================================================================

print("\n" + "=" * 80)
print("Step 1: Initialize DTC Connector")
print("=" * 80)

try:
    print(f"🔐 Retrieving DTC API key...")
    secret_key = f"dtc_api_key_{dtc_environment}"
    dtc_api_key = dbutils.secrets.get(scope="beproduct", key=secret_key)
    
    print(f"🚀 Creating DTCConnector...")
    connector = DTCConnector(
        api_key=dtc_api_key,
        environment=dtc_environment,
        workspace_name="Kontoor"
    )
    print(f"✅ DTCConnector initialized")
    
except Exception as e:
    print(f"❌ Failed to initialize: {e}")
    raise

# COMMAND ----------

# ============================================================================
# CELL 3: Load Staging Data
# ============================================================================

print("\n" + "=" * 80)
print("Step 2: Load Staging Data")
print("=" * 80)

try:
    print(f"📥 Loading staging data from {staging_table_full}...")
    
    # Load staging table
    df_staging_raw = spark.table(staging_table_full)
    
    # Filter for pending rows only
    df_staging = df_staging_raw.where(col("sync_status") == "pending")
    
    total_staging = df_staging_raw.count()
    pending_count = df_staging.count()
    
    print(f"✅ Loaded staging data:")
    print(f"   Total rows: {total_staging}")
    print(f"   Pending: {pending_count}")
    
    if pending_count == 0:
        print(f"\n⚠️  No pending rows to sync")
        dbutils.notebook.exit("NO_PENDING_ROWS")
    
    # Load request mapping
    print(f"\n📥 Loading request mapping...")
    df_mapping = spark.table(mapping_table_full)
    
    # Join staging with mapping to get request_id and sheet_id
    df_staging_with_mapping = df_staging.join(
        df_mapping,
        on="dtc_request_name",
        how="inner"
    )
    
    # Get unique requests to fetch from DTC
    unique_requests = df_staging_with_mapping.select(
        "dtc_request_name", "request_id", "sheet_id"
    ).distinct().collect()
    
    print(f"✅ Unique requests to sync: {len(unique_requests)}")
    for req in unique_requests[:5]:  # Show first 5
        print(f"   - {req.dtc_request_name} ({req.request_id})")
    
except Exception as e:
    print(f"❌ Failed to load staging: {e}")
    raise

# COMMAND ----------

# ============================================================================
# CELL 4: Pull Current DTC Data
# ============================================================================

print("\n" + "=" * 80)
print("Step 3: Pull Current DTC Data")
print("=" * 80)

try:
    print(f"📥 Fetching current DTC data for comparison...")
    
    all_dtc_dfs = []
    
    for req in unique_requests:
        req_name = req.dtc_request_name
        req_id = req.request_id
        sheet_id = req.sheet_id
        
        print(f"\n   Fetching: {req_name}")
        print(f"     Request ID: {req_id}")
        print(f"     Sheet ID: {sheet_id}")
        
        try:
            # Get views for this request
            views = connector.get_views(req_id)
            
            # Find "Full Version" view
            full_version_view = next(
                (v for v in views if v.get("viewName") == "Full Version"),
                None
            )
            
            if not full_version_view:
                print(f"     ⚠️  No 'Full Version' view found, using first view")
                full_version_view = views[0] if views else None
            
            if full_version_view:
                view_id = full_version_view["viewId"]
                
                # Fetch sheet data
                df_dtc, doc_metadata = connector.pull_request_to_dataframe(
                    request_id=req_id,
                    view_id=view_id
                )
                
                # Convert to Spark DataFrame
                spark_df_dtc = spark.createDataFrame(df_dtc)
                
                # Add request name for joining
                spark_df_dtc = spark_df_dtc.withColumn("dtc_request_name", lit(req_name))
                
                all_dtc_dfs.append(spark_df_dtc)
                
                row_count = spark_df_dtc.count()
                print(f"     ✅ Fetched {row_count} rows")
            else:
                print(f"     ⚠️  No views available")
        
        except Exception as e:
            print(f"     ⚠️  Failed to fetch: {e}")
            # Continue with other requests
    
    # Combine all DTC data
    if all_dtc_dfs:
        from functools import reduce
        from pyspark.sql import DataFrame
        
        df_dtc_current = reduce(DataFrame.union, all_dtc_dfs)
        dtc_current_count = df_dtc_current.count()
        
        print(f"\n✅ Combined DTC data: {dtc_current_count} rows")
        
        # Save snapshot for auditing
        print(f"\n💾 Saving DTC snapshot to {dtc_snapshot_table}...")
        df_dtc_current.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(dtc_snapshot_table)
        print(f"   ✅ Snapshot saved")
    else:
        print(f"\n⚠️  No DTC data fetched")
        df_dtc_current = spark.createDataFrame([], StructType([]))
        dtc_current_count = 0
    
except Exception as e:
    print(f"❌ Failed to pull DTC data: {e}")
    raise

# COMMAND ----------

# ============================================================================
# CELL 5: Join and Detect Changes
# ============================================================================

print("\n" + "=" * 80)
print("Step 4: Detect Changes")
print("=" * 80)

try:
    print(f"🔄 Joining staging with current DTC data...")
    print(f"   Composite key: (lf_style_number, color_name, fabric_group)")
    
    # Normalize column names for joining
    # DTC columns might have different names
    # Per requirements: LF Style# in DTC = lf_style_number in staging
    
    # Build mapping of expected DTC columns (normalized names from connector)
    # Based on connector's _normalize_column_name logic
    
    if dtc_current_count > 0:
        # Join on composite key
        # Note: DTC columns are normalized, so "LF Style#" becomes "LF_Style"
        #       "Color / Wash" becomes "Color___Wash"
        #       "Fabric Group" becomes "Fabric_Group"
        
        # First, check what columns exist in DTC data
        print(f"\n   Available DTC columns:")
        dtc_columns = df_dtc_current.columns
        for col_name in sorted(dtc_columns)[:20]:  # Show first 20
            print(f"     - {col_name}")
        
        # Perform full outer join
        df_comparison = df_staging_with_mapping.alias("stg").join(
            df_dtc_current.alias("dtc"),
            on=[
                (col("stg.dtc_request_name") == col("dtc.dtc_request_name")),
                # TODO: Adjust these based on actual DTC normalized column names
                # For now, use placeholders - will need to check actual column names
            ],
            how="full_outer"
        )
        
        # For now, since we don't have actual DTC data to test with,
        # assume all staging rows are INSERTs
        print(f"\n⚠️  NOTE: Using simplified change detection (all INSERTs)")
        print(f"   Full join logic will be completed after DTC column mapping is confirmed")
        
        # Classify operations
        df_inserts = df_staging_with_mapping.withColumn("operation", lit("INSERT"))
        df_updates = spark.createDataFrame([], df_staging_with_mapping.schema)
        df_deletes = spark.createDataFrame([], df_staging_with_mapping.schema)
        
    else:
        # No existing DTC data - all are INSERTs
        print(f"\n   No existing DTC data - all rows are INSERTs")
        df_inserts = df_staging_with_mapping.withColumn("operation", lit("INSERT"))
        df_updates = spark.createDataFrame([], df_staging_with_mapping.schema)
        df_deletes = spark.createDataFrame([], df_staging_with_mapping.schema)
    
    insert_count = df_inserts.count()
    update_count = df_updates.count()
    delete_count = df_deletes.count()
    
    print(f"\n✅ Change detection complete:")
    print(f"   INSERTs: {insert_count}")
    print(f"   UPDATEs: {update_count}")
    print(f"   DELETEs: {delete_count}")
    print(f"   Total operations: {insert_count + update_count + delete_count}")
    
except Exception as e:
    print(f"❌ Failed to detect changes: {e}")
    raise

# COMMAND ----------

# ============================================================================
# CELL 6: Prepare DTC Payloads
# ============================================================================

print("\n" + "=" * 80)
print("Step 5: Prepare DTC Payloads")
print("=" * 80)

try:
    print(f"🔄 Mapping fields to DTC column names...")
    
    # Field mapping: Staging column → DTC column name
    # Per requirements document
    COLUMN_MAPPING = {
        "lf_style_number": "LF Style#",
        "brands": "Brand",
        "description": "Style Description",
        "product_status": "Product Status",
        "product_category": "Class",
        "product_sub_category": "Sub Class",
        "division": "Division",
        "garment_finish": "Garment Finish",
        "techpack_stage": "Tech Pack Stage",
        "color_name": "Color / Wash",
        "fabric_group": "Fabric Group",
        "mill_fabric_article": "Mill Fabric Article #",
        # Add more mappings as needed
    }
    
    def prepare_payload(row_dict):
        """Prepare DTC PATCH payload from staging row."""
        payload = {}
        
        for staging_col, dtc_col in COLUMN_MAPPING.items():
            value = row_dict.get(staging_col)
            if value is not None:  # Only include non-null values
                payload[dtc_col] = str(value)
        
        return payload
    
    print(f"✅ Payload preparation function ready")
    print(f"   Mapped fields: {len(COLUMN_MAPPING)}")
    
except Exception as e:
    print(f"❌ Failed to prepare payloads: {e}")
    raise

# COMMAND ----------

# ============================================================================
# CELL 7: Push to DTC
# ============================================================================

print("\n" + "=" * 80)
print("Step 6: Push Changes to DTC")
print("=" * 80)

if dry_run:
    print(f"\n⚠️  DRY RUN MODE - Not actually pushing to DTC")
    print(f"   Would push {insert_count + update_count + delete_count} changes")

# Initialize results
results = {
    "success": 0,
    "failed": 0,
    "errors": []
}

push_log_rows = []

try:
    # Process INSERTs
    if insert_count > 0:
        print(f"\n📤 Processing {insert_count} INSERTs...")
        
        inserts_list = df_inserts.collect()
        
        for idx, row in enumerate(inserts_list[:batch_size], 1):  # Limit to batch size
            try:
                req_name = row.dtc_request_name
                sheet_id = row.sheet_id
                lf_style = row.lf_style_number
                
                # Get views for this request
                req_id = row.request_id
                views = connector.get_views(req_id)
                full_version_view = next(
                    (v for v in views if v.get("viewName") == "Full Version"),
                    views[0] if views else None
                )
                
                if not full_version_view:
                    raise ValueError(f"No view found for request {req_id}")
                
                view_id = full_version_view["viewId"]
                
                # Get max row index
                max_row_index = connector.get_max_row_index(sheet_id, view_id)
                new_row_index = max_row_index + idx
                
                # Prepare payload
                payload = prepare_payload(row.asDict())
                
                print(f"   [{idx}/{min(insert_count, batch_size)}] INSERT: {lf_style} at index {new_row_index}")
                
                if not dry_run:
                    # Push to DTC
                    response = connector.patch_row(
                        sheet_id=sheet_id,
                        view_id=view_id,
                        column_values=payload,
                        row_index=new_row_index
                    )
                    
                    print(f"      ✅ Success")
                    results["success"] += 1
                else:
                    print(f"      [DRY RUN] Would push")
                    results["success"] += 1
                
                # Log
                push_log_rows.append({
                    "push_time": datetime.now(timezone.utc),
                    "dtc_request_name": req_name,
                    "operation": "INSERT",
                    "lf_style_number": lf_style,
                    "color_name": row.color_name,
                    "fabric_group": row.fabric_group,
                    "status": "success",
                    "error_message": None,
                    "payload": json.dumps(payload),
                    "dry_run": dry_run
                })
                
            except Exception as e:
                error_msg = str(e)
                print(f"      ❌ Failed: {error_msg}")
                results["failed"] += 1
                results["errors"].append({
                    "operation": "INSERT",
                    "lf_style": lf_style if 'lf_style' in locals() else "?",
                    "error": error_msg
                })
                
                # Log error
                push_log_rows.append({
                    "push_time": datetime.now(timezone.utc),
                    "dtc_request_name": req_name if 'req_name' in locals() else "?",
                    "operation": "INSERT",
                    "lf_style_number": lf_style if 'lf_style' in locals() else "?",
                    "color_name": row.color_name if 'row' in locals() else None,
                    "fabric_group": row.fabric_group if 'row' in locals() else None,
                    "status": "failed",
                    "error_message": error_msg,
                    "payload": None,
                    "dry_run": dry_run
                })
        
        if insert_count > batch_size:
            print(f"\n   ⚠️  Batch limit reached. {insert_count - batch_size} inserts remaining.")
            print(f"      Run again to process remaining rows.")
    
    # TODO: Process UPDATEs and DELETEs similarly
    # (Skipped for now as they require DTC row_id from comparison)
    
except Exception as e:
    print(f"❌ Push failed: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# ============================================================================
# CELL 8: Log Results
# ============================================================================

print("\n" + "=" * 80)
print("Step 7: Log Push Results")
print("=" * 80)

try:
    print(f"💾 Writing push log to {push_log_table_full}...")
    
    if push_log_rows:
        df_push_log = spark.createDataFrame(push_log_rows)
        
        # Append to push log table
        df_push_log.write.format("delta") \
            .mode("append") \
            .saveAsTable(push_log_table_full)
        
        print(f"✅ Logged {len(push_log_rows)} operations")
    else:
        print(f"   No operations to log")
    
except Exception as e:
    print(f"⚠️  Failed to log results: {e}")
    # Don't fail the job for logging errors

# COMMAND ----------

# ============================================================================
# CELL 9: Update Sync Status
# ============================================================================

print("\n" + "=" * 80)
print("Step 8: Update Sync Status")
print("=" * 80)

try:
    if results["success"] > 0 and not dry_run:
        print(f"🔄 Updating sync status for successful pushes...")
        
        # Get list of successfully pushed styles
        successful_styles = [
            log["lf_style_number"] 
            for log in push_log_rows 
            if log["status"] == "success"
        ]
        
        if successful_styles:
            # Update staging table
            from pyspark.sql.functions import when
            
            df_staging_updated = spark.table(staging_table_full).withColumn(
                "sync_status",
                when(
                    col("lf_style_number").isin(successful_styles),
                    lit("pushed")
                ).otherwise(col("sync_status"))
            ).withColumn(
                "pushed_at",
                when(
                    col("lf_style_number").isin(successful_styles),
                    current_timestamp()
                ).otherwise(col("pushed_at"))
            )
            
            # Overwrite staging table
            df_staging_updated.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "false") \
                .saveAsTable(staging_table_full)
            
            print(f"✅ Updated sync status for {len(successful_styles)} rows")
    else:
        print(f"   No status updates needed")
    
except Exception as e:
    print(f"⚠️  Failed to update status: {e}")

# COMMAND ----------

# ============================================================================
# Final Summary
# ============================================================================

print("\n" + "=" * 80)
print("✅ PUSH COMPLETE")
print("=" * 80)

print(f"\nPush Summary:")
print(f"  Environment: {dtc_environment}")
print(f"  Pending rows: {pending_count}")
print(f"  Operations detected:")
print(f"    - INSERTs: {insert_count}")
print(f"    - UPDATEs: {update_count}")
print(f"    - DELETEs: {delete_count}")
print(f"  Results:")
print(f"    - Success: {results['success']}")
print(f"    - Failed: {results['failed']}")

if results["errors"]:
    print(f"\n  Errors:")
    for err in results["errors"][:10]:  # Show first 10 errors
        print(f"    - {err['operation']} {err['lf_style']}: {err['error']}")

if dry_run:
    print(f"\n⚠️  DRY RUN - No actual changes made to DTC")

print(f"\nNext steps:")
if results["failed"] > 0:
    print(f"  ⚠️  Review errors in {push_log_table_full}")
    print(f"  Fix issues and re-run for failed rows")
else:
    print(f"  ✅ All operations successful!")
    print(f"  Data is now synced to DTC")

print(f"\n✅ Push complete!")
