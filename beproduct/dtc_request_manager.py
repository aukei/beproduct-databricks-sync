# Databricks notebook source
"""
DTC Request/Sheet Manager
==========================

Ensures all DTC requests exist before pushing data.

Process:
1. Get unique request names from staging table
2. Search DTC for existing requests
3. Create missing requests/sheets
4. Store request/sheet ID mapping

Schedule: Daily at 12:30pm UTC (after denormalization at 12pm)

Parameters:
  - catalog: Databricks catalog (default: "lft")
  - schema: Databricks schema (default: "beproduct")
  - staging_table: Staging table (default: "beproduct_to_dtc_staging")
  - dtc_environment: DTC environment (default: "uat")
  - dtc_workspace: DTC workspace name (default: "Kontoor")
  - dtc_document: DTC document name (default: "KTB WIP")
  - dry_run: Test mode without creating requests (default: "false")
"""

# COMMAND ----------

# ============================================================================
# CELL 1: Setup
# ============================================================================

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

print("=" * 80)
print("DTC REQUEST/SHEET MANAGER SETUP")
print("=" * 80)

from connectors.dtc import DTCConnector
from pyspark.sql.functions import col, lit, current_timestamp
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure parameters
dbutils.widgets.text("catalog", "lft", "Catalog Name")
dbutils.widgets.text("schema", "beproduct", "Schema Name")
dbutils.widgets.text("staging_table", "beproduct_to_dtc_staging", "Staging Table")
dbutils.widgets.text("dtc_environment", "uat", "DTC Environment")
dbutils.widgets.text("dtc_workspace", "Kontoor", "DTC Workspace Name")
dbutils.widgets.text("dtc_document", "KTB WIP", "DTC Document Name")
dbutils.widgets.text("dry_run", "false", "Dry Run (true/false)")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
staging_table = dbutils.widgets.get("staging_table")
dtc_environment = dbutils.widgets.get("dtc_environment")
dtc_workspace = dbutils.widgets.get("dtc_workspace")
dtc_document = dbutils.widgets.get("dtc_document")
dry_run = dbutils.widgets.get("dry_run").lower() == "true"

staging_table_full = f"{catalog}.{schema}.{staging_table}"
mapping_table_full = f"{catalog}.{schema}.dtc_request_mapping"

print("✅ Parameters configured:")
print(f"   Staging: {staging_table_full}")
print(f"   Mapping: {mapping_table_full}")
print(f"   DTC: {dtc_workspace} / {dtc_document} ({dtc_environment})")
print(f"   Dry run: {dry_run}")

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
    print(f"🔐 Retrieving DTC API key from secrets...")
    secret_key = f"dtc_api_key_{dtc_environment}"
    dtc_api_key = dbutils.secrets.get(scope="beproduct", key=secret_key)
    print(f"   ✓ {secret_key} retrieved")
    
    print(f"🚀 Creating DTCConnector...")
    connector = DTCConnector(
        api_key=dtc_api_key,
        environment=dtc_environment,
        workspace_name=dtc_workspace
    )
    print(f"✅ DTCConnector initialized")
    
except Exception as e:
    print(f"❌ Failed to initialize connector: {e}")
    raise

# COMMAND ----------

# ============================================================================
# CELL 3: Get Unique Request Names from Staging
# ============================================================================

print("\n" + "=" * 80)
print("Step 2: Get Unique Request Names")
print("=" * 80)

try:
    print(f"📥 Loading from {staging_table_full}...")
    
    # Get unique request names that need to be synced
    df_staging = spark.table(staging_table_full)
    
    # Filter for pending rows only
    df_pending = df_staging.where(col("sync_status") == "pending")
    
    # Get unique request names
    unique_requests = df_pending.select("dtc_request_name") \
        .distinct() \
        .orderBy("dtc_request_name") \
        .collect()
    
    request_names = [row.dtc_request_name for row in unique_requests]
    
    print(f"✅ Found {len(request_names)} unique request names:")
    for name in request_names:
        print(f"   - {name}")
    
    if len(request_names) == 0:
        print(f"\n⚠️  No pending requests to sync")
        dbutils.notebook.exit("NO_PENDING_REQUESTS")
    
except Exception as e:
    print(f"❌ Failed to get request names: {e}")
    raise

# COMMAND ----------

# ============================================================================
# CELL 4: Search Existing Requests in DTC
# ============================================================================

print("\n" + "=" * 80)
print("Step 3: Search Existing DTC Requests")
print("=" * 80)

try:
    print(f"🔍 Searching DTC...")
    print(f"   Workspace: {dtc_workspace}")
    print(f"   Document: {dtc_document}")
    
    # Search for all requests in this workspace/document
    existing_requests = connector.search_requests(
        workspace_name=dtc_workspace,
        document_name=dtc_document
    )
    
    print(f"✅ Found {len(existing_requests)} existing requests")
    
    # Build mapping: request name → (request_id, sheet_id)
    request_map = {}
    for req in existing_requests:
        req_name = req.get("requestReference", "")
        req_id = req.get("requestId", "")
        sheet_id = req.get("sheetId", "")
        
        if req_name and req_id:
            request_map[req_name] = {
                "request_id": req_id,
                "sheet_id": sheet_id,
                "status": "exists"
            }
    
    print(f"\n   Mapped {len(request_map)} requests")
    
    # Identify missing requests
    missing_requests = [name for name in request_names if name not in request_map]
    existing_count = len(request_names) - len(missing_requests)
    
    print(f"\n📊 Status:")
    print(f"   Existing: {existing_count}")
    print(f"   Missing: {len(missing_requests)}")
    
    if missing_requests:
        print(f"\n   Missing requests:")
        for name in missing_requests:
            print(f"     - {name}")
    
except Exception as e:
    print(f"❌ Failed to search requests: {e}")
    raise

# COMMAND ----------

# ============================================================================
# CELL 5: Create Missing Requests
# ============================================================================

print("\n" + "=" * 80)
print("Step 4: Create Missing Requests")
print("=" * 80)

if len(missing_requests) == 0:
    print(f"✅ All requests already exist - nothing to create")
else:
    print(f"🔄 Creating {len(missing_requests)} missing requests...")
    
    if dry_run:
        print(f"\n⚠️  DRY RUN MODE - Not actually creating requests")
    
    created_count = 0
    failed_count = 0
    errors = []
    
    for request_name in missing_requests:
        try:
            print(f"\n   Creating: {request_name}")
            
            if dry_run:
                print(f"      [DRY RUN] Would create request")
                # Simulate success in dry run
                request_map[request_name] = {
                    "request_id": f"DRY_RUN_{created_count}",
                    "sheet_id": f"DRY_SHEET_{created_count}",
                    "status": "dry_run"
                }
                created_count += 1
            else:
                # Actually create the request
                response = connector.create_sheet(
                    workspace_name=dtc_workspace,
                    document_name=dtc_document,
                    request_name=request_name,
                    request_description=f"Created by BeProduct sync on {str(current_timestamp())}"
                )
                
                req_id = response.get("requestId")
                sheet_id = response.get("sheetId")
                
                if req_id and sheet_id:
                    request_map[request_name] = {
                        "request_id": req_id,
                        "sheet_id": sheet_id,
                        "status": "created"
                    }
                    created_count += 1
                    print(f"      ✅ Created: {req_id} / {sheet_id}")
                else:
                    raise ValueError(f"Response missing requestId or sheetId: {response}")
            
        except Exception as e:
            failed_count += 1
            error_msg = str(e)
            errors.append({
                "request_name": request_name,
                "error": error_msg
            })
            print(f"      ❌ Failed: {error_msg}")
    
    print(f"\n📊 Creation Summary:")
    print(f"   Created: {created_count}")
    print(f"   Failed: {failed_count}")
    
    if errors:
        print(f"\n   Errors:")
        for err in errors:
            print(f"     - {err['request_name']}: {err['error']}")
        
        if failed_count > 0 and not dry_run:
            raise ValueError(f"Failed to create {failed_count} requests")

# COMMAND ----------

# ============================================================================
# CELL 6: Create/Update Mapping Table
# ============================================================================

print("\n" + "=" * 80)
print("Step 5: Store Request/Sheet Mapping")
print("=" * 80)

try:
    print(f"💾 Writing mapping to {mapping_table_full}...")
    
    # Convert request_map to DataFrame
    mapping_rows = []
    for req_name, mapping in request_map.items():
        mapping_rows.append({
            "dtc_request_name": req_name,
            "request_id": mapping["request_id"],
            "sheet_id": mapping["sheet_id"],
            "workspace_name": dtc_workspace,
            "document_name": dtc_document,
            "environment": dtc_environment,
            "status": mapping["status"],
            "last_updated_at": str(current_timestamp()),
        })
    
    if mapping_rows:
        df_mapping = spark.createDataFrame(mapping_rows)
        
        # Write to Delta table (overwrite to keep only latest)
        df_mapping.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(mapping_table_full)
        
        print(f"✅ Mapping table updated:")
        print(f"   Rows: {len(mapping_rows)}")
        print(f"   Table: {mapping_table_full}")
        
        # Show mapping
        print(f"\n   Current mappings:")
        spark.table(mapping_table_full).select(
            "dtc_request_name", "request_id", "sheet_id", "status"
        ).show(truncate=False)
    else:
        print(f"⚠️  No mappings to store")
    
except Exception as e:
    print(f"❌ Failed to store mapping: {e}")
    raise

# COMMAND ----------

# ============================================================================
# CELL 7: Validate All Requests Mapped
# ============================================================================

print("\n" + "=" * 80)
print("Step 6: Validation")
print("=" * 80)

try:
    print(f"🔍 Validating all staging rows have request mapping...")
    
    # Join staging with mapping
    df_validation = df_staging.alias("stg").join(
        spark.table(mapping_table_full).alias("map"),
        on=col("stg.dtc_request_name") == col("map.dtc_request_name"),
        how="left"
    )
    
    # Check for unmapped rows
    unmapped_count = df_validation.where(col("map.request_id").isNull()).count()
    
    if unmapped_count > 0:
        print(f"❌ {unmapped_count} rows without request mapping")
        
        # Show unmapped request names
        unmapped_requests = df_validation.where(col("map.request_id").isNull()) \
            .select("stg.dtc_request_name").distinct().collect()
        
        print(f"\n   Unmapped requests:")
        for row in unmapped_requests:
            print(f"     - {row.dtc_request_name}")
        
        raise ValueError(f"{unmapped_count} rows have no request mapping")
    else:
        print(f"✅ All staging rows have request mapping")
    
    # Show statistics
    total_staging = df_staging.count()
    pending_staging = df_pending.count()
    mapped_requests = len(request_map)
    
    print(f"\n📊 Statistics:")
    print(f"   Total staging rows: {total_staging}")
    print(f"   Pending rows: {pending_staging}")
    print(f"   Mapped requests: {mapped_requests}")
    
except Exception as e:
    print(f"❌ Validation failed: {e}")
    raise

# COMMAND ----------

# ============================================================================
# Final Summary
# ============================================================================

print("\n" + "=" * 80)
print("✅ REQUEST MANAGEMENT COMPLETE")
print("=" * 80)

print(f"\nSummary:")
print(f"  DTC workspace: {dtc_workspace}")
print(f"  DTC document: {dtc_document}")
print(f"  Environment: {dtc_environment}")
print(f"  Total requests: {len(request_map)}")
print(f"  Existing: {existing_count}")
print(f"  Created: {created_count}")
print(f"  Mapping table: {mapping_table_full}")

if dry_run:
    print(f"\n⚠️  DRY RUN - No actual changes made to DTC")

print(f"\nNext step:")
print(f"  Run beproduct_to_dtc_push.py to sync data to DTC")

print(f"\n✅ Request manager complete!")
