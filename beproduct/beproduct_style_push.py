# Databricks notebook source
"""
BeProduct STYLE Data Push-Back Job
===================================

Syncs changes from Databricks Delta Lake back to BeProduct.

Detects changes by comparing:
  - modified_at: timestamp of last change in Databricks
  - synced_at: timestamp of last pull from BeProduct

If modified_at > synced_at, the record was edited locally and should be pushed back.

Updates all extracted fields (compulsory + interested) for changed records.

Schedule: Can be triggered manually or on a schedule (e.g., hourly)

Parameters:
  - folder_name: BeProduct folder name (e.g., "KTB")
  - source_table_name: Source Delta table (e.g., "ktb_styles")
  - dry_run: "true" to preview changes without pushing (default: "false")
"""

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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
print("✅ All libraries imported")

# Configure job parameters with widgets
print("\n⚙️  Configuring job parameters...")
dbutils.widgets.text("folder_name", "KTB", "BeProduct Folder Name")
dbutils.widgets.text("source_table_name", "ktb_styles", "Source Delta Table Name")
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "beproduct", "Schema Name")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry Run (preview only, no actual push)")

folder_name = dbutils.widgets.get("folder_name")
source_table_name = dbutils.widgets.get("source_table_name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
dry_run = dbutils.widgets.get("dry_run").lower() == "true"

print("✅ Parameters configured:")
print(f"   folder_name: {folder_name}")
print(f"   source_table_name: {source_table_name}")
print(f"   catalog: {catalog}")
print(f"   schema: {schema}")
print(f"   dry_run: {dry_run}")

print("\n" + "=" * 80)
print("✅ SETUP COMPLETE - Ready to push")
print("=" * 80)

# COMMAND ----------

print("\n" + "=" * 80)
print("PUSH CELL: Detect Changes and Push to BeProduct")
print("=" * 80)

# Get parameters from previous cell
folder_name_val = dbutils.widgets.get("folder_name")
source_table_name_val = dbutils.widgets.get("source_table_name")
catalog_val = dbutils.widgets.get("catalog")
schema_val = dbutils.widgets.get("schema")
dry_run_val = dbutils.widgets.get("dry_run").lower() == "true"

# Field mapping (same as pull job)
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

# Create reverse mapping: column_name → beproduct_field_name
COLUMN_TO_FIELD = {}
for bp_name, col_name in {**COMPULSORY_FIELDS, **INTERESTED_FIELDS}.items():
    COLUMN_TO_FIELD[col_name] = bp_name

print(f"\n📋 Configuration:")
print(f"   Source table: {catalog_val}.{schema_val}.{source_table_name_val}")
print(f"   Folder: {folder_name_val}")
print(f"   Dry run: {dry_run_val}")
print(f"   Extracted fields: {len(COLUMN_TO_FIELD)}")

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
# Step 2: Query Changed Records
# ============================================================================

print(f"\n{'='*80}")
print("Step 2: Query Changed Records")
print("=" * 80)

try:
    source_table_path = f"{catalog_val}.{schema_val}.{source_table_name_val}"
    
    print(f"📊 Querying {source_table_path}...")
    print(f"   Looking for records where modified_at > synced_at")
    
    # Query for changed records
    changed_records_df = spark.sql(f"""
        SELECT
            id,
            lf_style_number,
            description,
            team,
            season,
            year,
            product_status,
            customer_style_number,
            product_category,
            product_sub_category,
            division,
            brands,
            garment_finish,
            techpack_stage,
            lot_code,
            parent_vendor,
            factory,
            modified_at,
            synced_at,
            data_json
        FROM {source_table_path}
        WHERE modified_at > synced_at
            AND modified_at IS NOT NULL
            AND synced_at IS NOT NULL
        ORDER BY modified_at DESC
    """)
    
    changed_records = changed_records_df.collect()
    
    print(f"✅ Query complete:")
    print(f"   Total records with changes: {len(changed_records)}")
    
    if len(changed_records) > 0:
        print(f"\n   Sample changed records:")
        for i, rec in enumerate(changed_records[:5]):
            style_num = rec["lf_style_number"] if rec["lf_style_number"] is not None else "?"
            modified = rec["modified_at"] if rec["modified_at"] is not None else "?"
            synced = rec["synced_at"] if rec["synced_at"] is not None else "?"
            print(f"     {i+1}. {style_num}: modified={modified}, last_synced={synced}")

except Exception as e:
    print(f"❌ Failed to query changes: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

# Flag for conditional execution
HAS_CHANGES = len(changed_records) > 0

if not HAS_CHANGES:
    print(f"\n✅ No changes to push")
    print(f"   All records are in sync (modified_at == synced_at)")

def build_update_payload(record: Dict) -> Dict:
    """
    Build BeProduct API update payload from Databricks record.
    
    Uses field IDs (not field names) by extracting the mapping from data_json.
    Returns dict with 'id' and 'fields' for the SDK's attributes_update() method.
    """
    style_id = record.get("id")
    
    # Extract field ID mapping from data_json
    # Maps: BeProduct field name → field ID
    field_name_to_id = {}
    try:
        data_json_str = record.get("data_json")
        if isinstance(data_json_str, str):
            data_json = json.loads(data_json_str)
        else:
            data_json = data_json_str
        
        # Extract field mappings from headerData.fields
        header_data = data_json.get("headerData", {})
        fields_list = header_data.get("fields", [])
        
        for field_obj in fields_list:
            field_name = field_obj.get("name")
            field_id = field_obj.get("id")
            if field_name and field_id:
                field_name_to_id[field_name] = field_id
    except Exception as e:
        logger.warning(f"Failed to extract field mapping from data_json: {str(e)}")
    
    # Build fields dict: use field IDs (not names)
    # Also identify field types for validation
    field_id_to_type = {}
    try:
        data_json_str = record.get("data_json")
        if isinstance(data_json_str, str):
            data_json = json.loads(data_json_str)
        else:
            data_json = data_json_str
        
        header_data = data_json.get("headerData", {})
        fields_list = header_data.get("fields", [])
        
        for field_obj in fields_list:
            field_id = field_obj.get("id")
            field_type = field_obj.get("type")
            if field_id:
                field_id_to_type[field_id] = field_type
    except:
        pass
    
    fields = {}
    
    for col_name, bp_field_name in COLUMN_TO_FIELD.items():
        value = record.get(col_name)
        
        # Handle array fields (Brands, etc.)
        if isinstance(value, str) and value.startswith("["):
            try:
                value = json.loads(value)
            except:
                pass
        
        # Only include non-None and non-empty values
        # Skip empty strings on dropdown/multiselect fields (they get rejected silently)
        if value is not None and value != "":
            # Use field ID if available, fallback to field name
            field_id = field_name_to_id.get(bp_field_name, bp_field_name)
            
            # Log warning if sending to dropdown/multiselect with potentially invalid value
            field_type = field_id_to_type.get(field_id, "")
            if field_type in ["DropDown", "MultiSelect"] and isinstance(value, str):
                logger.warning(f"Field {field_id} ({bp_field_name}) is {field_type} type - ensure value '{value}' is in valid Master Data list")
            
            fields[field_id] = value
    
    return {
        "id": style_id,
        "fields": fields,
    }

# Initialize counters (will be populated if HAS_CHANGES)
pushed_count = 0
failed_count = 0
failed_records = []

if HAS_CHANGES:

    # ============================================================================
    # Step 3: Build Update Payloads
    # ============================================================================

    print(f"\n{'='*80}")
    print("Step 3: Build Update Payloads")
    print("=" * 80)

    try:
        print(f"🔨 Building update payloads for {len(changed_records)} records...")
        
        payloads = []
        for record in changed_records:
            # Convert Row object to dictionary
            record_dict = record.asDict()
            payload = build_update_payload(record_dict)
            payloads.append(payload)
        
        print(f"✅ Built {len(payloads)} payloads")
        
        # Show sample payload (all fields, not just first 5)
        if payloads:
            print(f"\n   Sample payload (first record):")
            sample = payloads[0]
            print(f"     id: {sample['id']}")
            print(f"     fields to update: {len(sample['fields'])}")
            print(f"     Using field IDs (extracted from data_json):")
            for field_id, value in sample['fields'].items():
                val_str = str(value)[:60]
                print(f"       - {field_id}: {val_str}")

    except Exception as e:
        print(f"❌ Failed to build payloads: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

    # ============================================================================
    # Step 4: Push to BeProduct
    # ============================================================================

    print(f"\n{'='*80}")
    print("Step 4: Push to BeProduct")
    print("=" * 80)

    if dry_run_val:
        print(f"🔍 DRY RUN MODE - No actual pushes will be made")

    try:
        print(f"\n🚀 Pushing {len(payloads)} records to BeProduct...")
        
        for i, payload in enumerate(payloads, 1):
            style_id = payload["id"]
            fields = payload["fields"]
            
            try:
                if dry_run_val:
                    # Dry run: just log what would be pushed
                    print(f"  [{i}/{len(payloads)}] DRY RUN: {style_id[:16]}... would push {len(fields)} fields")
                    print(f"         Fields: {fields}")
                else:
                    # Real push: call SDK to update
                    print(f"  [{i}/{len(payloads)}] Pushing {style_id[:16]}... ({len(fields)} fields)")
                    print(f"         Fields: {fields}")
                    
                    response = api.style.attributes_update(
                        header_id=style_id,
                        fields=fields
                    )
                    
                    pushed_count += 1
                    print(f"         ✓ Success - API response: {response}")
            
            except Exception as e:
                failed_count += 1
                failed_records.append({
                    "id": style_id,
                    "error": str(e)
                })
                print(f"         ✗ Failed: {str(e)[:100]}")
        
        print(f"\n✅ Push complete:")
        print(f"   Pushed: {pushed_count}")
        print(f"   Failed: {failed_count}")
        
        if failed_records:
            print(f"\n   Failed records:")
            for rec in failed_records[:10]:
                print(f"     - {rec['id'][:16]}...: {rec['error'][:80]}")

    except Exception as e:
        print(f"❌ Push operation failed: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

    # ============================================================================
    # Step 5: Update Local synced_at Timestamp
    # ============================================================================

    print(f"\n{'='*80}")
    print("Step 5: Update Local synced_at Timestamp")
    print("=" * 80)

    if dry_run_val:
        print(f"🔍 DRY RUN - Skipping local update")
    else:
        try:
            if pushed_count > 0:
                source_table_path = f"{catalog_val}.{schema_val}.{source_table_name_val}"
                current_timestamp = datetime.now(timezone.utc).isoformat()
                
                print(f"📝 Updating synced_at for pushed records...")
                print(f"   New timestamp: {current_timestamp}")
                
                # Get list of successfully pushed record IDs
                pushed_ids = [p["id"] for p in payloads[:pushed_count]]
                
                # Update synced_at
                # Note: This assumes we have write access to the table
                # If table is read-only, this will fail gracefully
                try:
                    # Create a temporary dataframe with updated timestamps
                    update_df = spark.createDataFrame(
                        [(id_val, current_timestamp) for id_val in pushed_ids],
                        ["id", "new_synced_at"]
                    )
                    
                    # Merge back into source table
                    spark.sql(f"USE CATALOG {catalog_val}")
                    
                    # Write to temp location
                    temp_table = f"{source_table_name_val}_temp_update"
                    update_df.write.mode("overwrite").saveAsTable(f"{catalog_val}.{schema_val}.{temp_table}")
                    
                    # Update main table
                    spark.sql(f"""
                        MERGE INTO {source_table_path} t
                        USING {catalog_val}.{schema_val}.{temp_table} u
                        ON t.id = u.id
                        WHEN MATCHED THEN
                            UPDATE SET t.synced_at = u.new_synced_at
                    """)
                
                    # Clean up temp table
                    spark.sql(f"DROP TABLE IF EXISTS {catalog_val}.{schema_val}.{temp_table}")
                    
                    print(f"✅ Updated synced_at for {pushed_count} records")
                
                except Exception as e:
                    print(f"⚠️  Could not update synced_at in Delta table: {str(e)}")
                    print(f"   Records were pushed to BeProduct but local sync timestamp wasn't updated")
                    print(f"   Please manually update or re-sync to reset modified_at markers")

            else:
                print(f"⚠️  No records were successfully pushed, skipping local update")

        except Exception as e:
            print(f"⚠️  Failed to update local timestamps: {str(e)}")

    # ============================================================================
    # Step 6: Log Push Metadata
    # ============================================================================

    print(f"\n{'='*80}")
    print("Step 6: Log Push Metadata")
    print("=" * 80)

    if not dry_run_val:
        try:
            push_timestamp = datetime.now(timezone.utc).isoformat()
            metadata_table = f"{source_table_name_val}_push_log"
            
            spark.sql(f"USE CATALOG {catalog_val}")
            spark.sql(f"USE SCHEMA {schema_val}")
            
            # Build summary
            summary = f"{pushed_count} styles pushed to BeProduct"
            if failed_count > 0:
                summary += f" ({failed_count} failed)"
            
            spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {catalog_val}.{schema_val}.{metadata_table}
                (pushed_at STRING, records_pushed LONG, records_failed LONG, summary STRING)
                USING DELTA
                """
            )
            
            spark.sql(
                f"""
                INSERT INTO {catalog_val}.{schema_val}.{metadata_table}
                SELECT 
                    '{push_timestamp}' AS pushed_at,
                    {pushed_count} AS records_pushed,
                    {failed_count} AS records_failed,
                    '{summary}' AS summary
                """
            )
            print(f"✅ Push log saved to {metadata_table}:")
            print(f"   Timestamp: {push_timestamp}")
            print(f"   Pushed: {pushed_count}")
            print(f"   Failed: {failed_count}")
            print(f"   Summary: {summary}")
        except Exception as e:
            print(f"⚠️  Could not save push log: {str(e)}")

# ============================================================================
# Summary
# ============================================================================

print(f"\n{'='*80}")
print("PUSH SUMMARY")
print("=" * 80)

print(f"\n✅ Push job complete!")

if HAS_CHANGES:
    if dry_run_val:
        print(f"\n   Mode: DRY RUN (no actual pushes)")
        print(f"   Records that would be pushed: {len(payloads)}")
        print(f"   \n   Re-run with dry_run=false to actually push")
    else:
        print(f"\n   Records pushed: {pushed_count}")
        print(f"   Records failed: {failed_count}")
        print(f"   Success rate: {100*pushed_count/(pushed_count+failed_count) if (pushed_count+failed_count) > 0 else 0:.1f}%")
        
        print(f"\n📜 PUSH HISTORY (last 5 pushes):")
        try:
            push_log_table = f"{source_table_name_val}_push_log"
            
            # Check if table exists before querying
            table_exists = spark.sql(f"""
                SELECT 1 FROM information_schema.tables 
                WHERE table_catalog = '{catalog_val}'
                AND table_schema = '{schema_val}'
                AND table_name = '{push_log_table}'
            """).count() > 0
            
            if table_exists:
                history = spark.sql(f"""
                    SELECT pushed_at, records_pushed, records_failed, summary
                    FROM {catalog_val}.{schema_val}.{push_log_table}
                    ORDER BY pushed_at DESC
                    LIMIT 5
                """).collect()
                
                for i, row in enumerate(history, 1):
                    status = "✓" if row['records_failed'] == 0 else "✗"
                    print(f"   {i}. {row['pushed_at'][:10]} {status} | {row['records_pushed']} pushed, {row['records_failed']} failed | {row['summary']}")
            else:
                print(f"   No push history available (first run)")
        except Exception as e:
            print(f"   (Could not retrieve history: {str(e)})")

else:
    print(f"\n   No changes detected")
    print(f"   All records are in sync (modified_at == synced_at)")
    
    print(f"\n📜 LAST PUSH (from history):")
    try:
        push_log_table = f"{source_table_name_val}_push_log"
        
        # Check if table exists before querying
        table_exists = spark.sql(f"""
            SELECT 1 FROM information_schema.tables 
            WHERE table_catalog = '{catalog_val}'
            AND table_schema = '{schema_val}'
            AND table_name = '{push_log_table}'
        """).count() > 0
        
        if table_exists:
            last_push = spark.sql(f"""
                SELECT pushed_at, records_pushed, records_failed, summary
                FROM {catalog_val}.{schema_val}.{push_log_table}
                ORDER BY pushed_at DESC
                LIMIT 1
            """).collect()
            
            if last_push:
                row = last_push[0]
                print(f"   Last push: {row['pushed_at']}")
                print(f"   Records: {row['records_pushed']} pushed, {row['records_failed']} failed")
                print(f"   Summary: {row['summary']}")
            else:
                print(f"   No previous push history found (first run)")
        else:
            print(f"   No push history available (first run)")
    except Exception as e:
        print(f"   (Could not retrieve history: {str(e)})")

print(f"\n{'='*80}")
