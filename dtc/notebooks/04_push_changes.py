# Databricks notebook source
# MAGIC %md
# MAGIC # Push Changes to DTC
# MAGIC
# MAGIC Applies detected changes back to DTC:
# MAGIC - **INSERT**: Create new rows in DTC
# MAGIC - **UPDATE**: Patch existing rows in DTC
# MAGIC - **DELETE**: Remove rows from DTC
# MAGIC
# MAGIC After successful push, creates a new snapshot to reset the baseline.

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

from connectors.dtc import DTCConnector
from sync.change_detection import ChangeDetector
from sync.snapshot import SnapshotManager

# COMMAND ----------

# Define widgets with defaults
try:
    dbutils.widgets.text("dtc_workspace_name", "KTB", "DTC Workspace Name")
    dbutils.widgets.text("dtc_request_id", "69f076f0b7247a661226be9a", "DTC Request ID")
    dbutils.widgets.text("dtc_environment", "uat", "DTC Environment (uat/prod)")
    dbutils.widgets.text("target_catalog", "lft", "Target Catalog")
    dbutils.widgets.text("target_schema", "beproduct", "Target Schema")
    dbutils.widgets.text("target_table", "dtc_master_chart_uat", "Target Table Name")
except Exception as e:
    pass

# Parameters
DTC_WORKSPACE_NAME = dbutils.widgets.get("dtc_workspace_name")
DTC_REQUEST_ID = dbutils.widgets.get("dtc_request_id")
DTC_ENVIRONMENT = dbutils.widgets.get("dtc_environment")
TARGET_CATALOG = dbutils.widgets.get("target_catalog")
TARGET_SCHEMA = dbutils.widgets.get("target_schema")
TARGET_TABLE = dbutils.widgets.get("target_table")

# Extract environment and build table names
environment = TARGET_TABLE.split("_")[-1]  # uat or prod

metadata_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.dtc_sync_metadata_{environment}"
changes_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.dtc_master_chart_changes_{environment}"
data_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}"

print(f"Pushing changes to DTC ({environment.upper()})")
print(f"  Workspace: {DTC_WORKSPACE_NAME}")
print(f"  Request ID: {DTC_REQUEST_ID}")
print(f"  Changes table: {changes_table}")
print("-" * 80)

# COMMAND ----------

# Get DTC API key from secrets
print("\n1️⃣ Initializing DTC Connector...")

secret_key = f"dtc_api_key_{environment}"
try:
    dtc_api_key = dbutils.secrets.get(scope="beproduct", key=secret_key)
except Exception as e:
    print(f"❌ Error: Could not retrieve {secret_key} from secrets")
    print(f"   Error: {e}")
    dbutils.notebook.exit(1)

# Initialize connector
try:
    connector = DTCConnector(
        api_key=dtc_api_key,
        environment=environment,
        workspace_name=DTC_WORKSPACE_NAME
    )
    print("✅ DTCConnector initialized")
except Exception as e:
    print(f"❌ Error initializing connector: {e}")
    dbutils.notebook.exit(1)

# COMMAND ----------

# Get request details
print("\n2️⃣ Getting request details from DTC...")

try:
    request = connector.get_request(DTC_REQUEST_ID)
    sheet_id = request.get("sheetId")
    
    if not sheet_id:
        print(f"❌ Request {DTC_REQUEST_ID} has no sheetId")
        dbutils.notebook.exit(1)
    
    # The validated write contract is PATCH /v1/sheets/{sheetId}/views/{viewId},
    # so a view_id is required. Use WIP_ITS_USE (complete, unfiltered).
    views = connector.get_views(DTC_REQUEST_ID)
    view_id = next((v.get("viewId") for v in views if v.get("viewName") == "WIP_ITS_USE"), None)
    if not view_id:
        print("❌ 'WIP_ITS_USE' view not found for this request")
        dbutils.notebook.exit(1)
    # Running max rowIndex used to assign indexes for INSERTs.
    next_row_index = connector.get_max_row_index(sheet_id, view_id)

    print(f"   Sheet ID: {sheet_id} | View ID: {view_id} | max rowIndex: {next_row_index}")
except Exception as e:
    print(f"❌ Error getting request: {e}")
    dbutils.notebook.exit(1)

# COMMAND ----------

# Get pending changes
print("\n3️⃣ Retrieving pending changes...")

detector = ChangeDetector(spark, changes_table)
pending_changes = detector.get_pending_changes(DTC_REQUEST_ID)

print(f"   Pending changes: {len(pending_changes)}")

if len(pending_changes) == 0:
    print("   ✅ No pending changes to push")
    dbutils.notebook.exit(0)

# Summarize
inserts = len([c for c in pending_changes if c['operation'] == 'INSERT'])
updates = len([c for c in pending_changes if c['operation'] == 'UPDATE'])
deletes = len([c for c in pending_changes if c['operation'] == 'DELETE'])

print(f"   - INSERTs: {inserts}")
print(f"   - UPDATEs: {updates}")
print(f"   - DELETEs: {deletes}")

# COMMAND ----------

# Push changes
print("\n4️⃣ Pushing changes to DTC...")

results = {
    'success': 0,
    'error': 0,
    'conflict': 0,
    'errors': []
}

for i, change in enumerate(pending_changes, 1):
    change_id = change['change_id']
    row_id = change['row_id']
    operation = change['operation']
    columns_changed = change['columns_changed']
    
    # change_detection stores columns_changed as {col: {old_value, new_value}}.
    # Flatten to {col: new_value} for the DTC write contract.
    flat_values = {
        col: (info.get("new_value") if isinstance(info, dict) else info)
        for col, info in (columns_changed or {}).items()
    }

    try:
        print(f"\n   [{i}/{len(pending_changes)}] {operation} - row_id: {row_id}")
        
        if operation == 'INSERT':
            # INSERT: PATCH .../views/{viewId} with a fresh rowIndex
            next_row_index += 1
            response = connector.create_row(sheet_id, view_id, flat_values, next_row_index)
            print(f"      ✅ Created at rowIndex {next_row_index}")
            detector.mark_as_pushed(change_id, response)
            results['success'] += 1
            
        elif operation == 'UPDATE':
            # UPDATE: PATCH .../views/{viewId} with rowId
            response = connector.update_row(sheet_id, view_id, row_id, flat_values)
            print(f"      ✅ Updated")
            detector.mark_as_pushed(change_id, response)
            results['success'] += 1
            
        elif operation == 'DELETE':
            # The DTC API exposes no row-delete endpoint (validated 2026-06-17).
            # Mark as rejected so it is visible for manual handling (Phase 2+).
            msg = "DELETE not supported by DTC API (no row-delete endpoint)"
            print(f"      ⚠️  {msg}")
            detector.mark_as_rejected(change_id, msg)
            results['conflict'] = results.get('conflict', 0) + 1
            continue
            
    except Exception as e:
        print(f"      ❌ Error: {str(e)}")
        detector.mark_as_rejected(change_id, str(e))
        results['error'] += 1
        results['errors'].append({
            'change_id': change_id,
            'operation': operation,
            'row_id': row_id,
            'error': str(e)
        })

# COMMAND ----------

# Show results
print("\n5️⃣ Push Results:")
print(f"   ✅ Successful: {results['success']}")
print(f"   ❌ Errors: {results['error']}")

if results['errors']:
    print("\n   Failed changes:")
    for err in results['errors']:
        print(f"     - {err['operation']} (row_id: {err['row_id']}): {err['error']}")

# COMMAND ----------

# Create new snapshot after successful push
if results['success'] > 0:
    print("\n6️⃣ Creating new snapshot after push...")
    
    # Pull latest data from table
    current_df = spark.sql(f"""
    SELECT *
    FROM {data_table}
    WHERE request_id = '{DTC_REQUEST_ID}'
    """)
    
    row_count = current_df.count()
    
    # Get metadata
    metadata_df = spark.sql(f"""
    SELECT DISTINCT document_name, request_reference, request_description
    FROM {data_table}
    WHERE request_id = '{DTC_REQUEST_ID}'
    LIMIT 1
    """).collect()
    
    if metadata_df:
        metadata_row = metadata_df[0]
        document_name = metadata_row.document_name
        request_reference = metadata_row.request_reference
        request_description = metadata_row.request_description
        
        # Create snapshot
        manager = SnapshotManager(spark, metadata_table)
        
        snapshot_hash, metadata_record = manager.create_snapshot(
            request_id=DTC_REQUEST_ID,
            environment=environment,
            data_df=current_df,
            document_name=document_name,
            row_count=row_count,
            details={
                "request_reference": request_reference,
                "pushed_changes": results['success'],
                "created_by": "push_notebook"
            }
        )
        
        manager.store_snapshot(metadata_record)
        print(f"   ✅ New snapshot created (Hash: {snapshot_hash[:16]}...)")
    else:
        print("   ⚠️  Could not create new snapshot (no data found)")

# COMMAND ----------

# Final summary
print("\n✅ Push complete!")
print(f"\n   Summary:")
print(f"     Pushed: {results['success']}")
print(f"     Failed: {results['error']}")
print(f"     Remaining: {len(pending_changes) - results['success'] - results['error']}")

if results['error'] > 0:
    print(f"\n   ⚠️  Some changes failed. Review in {changes_table}")
    print(f"   Status 'rejected' = push failed (manual retry needed)")
else:
    print(f"\n   All changes pushed successfully!")
    print(f"   Ready for next sync cycle")
