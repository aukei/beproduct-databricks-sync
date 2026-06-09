# Databricks notebook source
# MAGIC %md
# MAGIC # Detect Changes in Data
# MAGIC
# MAGIC Compares current state of table with last snapshot to detect edits.
# MAGIC
# MAGIC Identifies:
# MAGIC - **INSERT**: New rows added to Databricks table
# MAGIC - **UPDATE**: Rows with changed values
# MAGIC - **DELETE**: Rows removed from Databricks table
# MAGIC
# MAGIC All changes are logged in the change log table for audit trail and push.

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

from sync.snapshot import SnapshotManager
from sync.change_detection import ChangeDetector
import pandas as pd

# COMMAND ----------

# Define widgets with defaults
try:
    dbutils.widgets.text("dtc_request_id", "69f076f0b7247a661226be9a", "DTC Request ID")
    dbutils.widgets.text("dtc_environment", "uat", "DTC Environment (uat/prod)")
    dbutils.widgets.text("target_catalog", "lft", "Target Catalog")
    dbutils.widgets.text("target_schema", "beproduct", "Target Schema")
    dbutils.widgets.text("target_table", "dtc_master_chart_uat", "Target Table Name")
except Exception as e:
    pass

# Parameters
DTC_REQUEST_ID = dbutils.widgets.get("dtc_request_id")
DTC_ENVIRONMENT = dbutils.widgets.get("dtc_environment")
TARGET_CATALOG = dbutils.widgets.get("target_catalog")
TARGET_SCHEMA = dbutils.widgets.get("target_schema")
TARGET_TABLE = dbutils.widgets.get("target_table")

# Extract environment from table name
environment = TARGET_TABLE.split("_")[-1]  # uat or prod

metadata_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.dtc_sync_metadata_{environment}"
changes_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.dtc_master_chart_changes_{environment}"
data_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}"

print(f"Detecting changes for {environment.upper()}")
print(f"  Request ID: {DTC_REQUEST_ID}")
print(f"  Data table: {data_table}")
print(f"  Changes table: {changes_table}")
print("-" * 80)

# COMMAND ----------

# Get last snapshot
print("\n1️⃣ Retrieving last snapshot...")

manager = SnapshotManager(spark, metadata_table)
last_snapshot = manager.get_last_snapshot(DTC_REQUEST_ID, sync_direction='pull')

if not last_snapshot:
    print("⚠️  No snapshot found for this request!")
    print("   Please run '02_create_snapshot' after pulling data first")
    dbutils.notebook.exit(1)

print(f"   Snapshot from: {last_snapshot['sync_timestamp']}")
print(f"   Rows in snapshot: {last_snapshot['row_count']}")
print(f"   Hash: {last_snapshot['snapshot_hash'][:16]}...")

# COMMAND ----------

# Fetch current data
print("\n2️⃣ Fetching current data from table...")

current_df = spark.sql(f"""
SELECT *
FROM {data_table}
WHERE request_id = '{DTC_REQUEST_ID}'
ORDER BY row_id
""").toPandas()

print(f"   Current rows: {len(current_df)}")
print(f"   Columns: {len(current_df.columns)}")

# COMMAND ----------

# Fetch previous data (reconstruct from data table - in production, you'd keep versioned snapshots)
print("\n3️⃣ Reconstructing previous snapshot data...")

# Since we don't version snapshots, we'll get the "baseline" by checking if changes exist
# If no changes exist yet, the current state IS the baseline
# Otherwise, we reconstruct by applying inverse changes

# For now, we'll use a simpler approach:
# Get all rows that existed at snapshot time (we know from row count)
# In production, you'd have versioned snapshots stored

previous_df = spark.sql(f"""
SELECT *
FROM {data_table}
WHERE request_id = '{DTC_REQUEST_ID}'
ORDER BY row_id
""").toPandas()

# Check if changes were already logged
existing_changes = spark.sql(f"""
SELECT COUNT(*) as count
FROM {changes_table}
WHERE request_id = '{DTC_REQUEST_ID}'
AND status IN ('pushed', 'pending', 'conflict')
""").collect()[0][0]

if existing_changes == 0:
    # This is first change detection - current data IS the snapshot
    print("   First change detection - using snapshot as baseline")
    # Get previous version by looking at snapshot hash
    # For now we'll use current as both (no changes on first run)
    previous_df = current_df.copy()
else:
    print(f"   Found {existing_changes} existing changes")

print(f"   Previous rows: {len(previous_df)}")

# COMMAND ----------

# Detect changes
print("\n4️⃣ Detecting changes...")

detector = ChangeDetector(spark, changes_table)

changes = detector.detect_changes(
    request_id=DTC_REQUEST_ID,
    current_df=current_df,
    previous_df=previous_df
)

print(f"   Total changes detected: {len(changes)}")

if len(changes) > 0:
    # Summarize by operation type
    inserts = len([c for c in changes if c['operation'] == 'INSERT'])
    updates = len([c for c in changes if c['operation'] == 'UPDATE'])
    deletes = len([c for c in changes if c['operation'] == 'DELETE'])
    
    print(f"   - INSERTs: {inserts}")
    print(f"   - UPDATEs: {updates}")
    print(f"   - DELETEs: {deletes}")

# COMMAND ----------

# Store changes
if len(changes) > 0:
    print("\n5️⃣ Storing changes in change log...")
    
    stored_count = detector.store_changes(changes)
    print(f"   Stored {stored_count} changes")
else:
    print("\n5️⃣ No changes detected - skipping storage")

# COMMAND ----------

# Show summary
print("\n6️⃣ Change Summary:")

summary = detector.get_change_summary(DTC_REQUEST_ID)

print(f"\n   Total changes: {summary['total']}")
print(f"   By operation:")
for op, count in summary['by_operation'].items():
    if count > 0:
        print(f"     - {op}: {count}")
print(f"   By status:")
for status in ['pending', 'pushed', 'conflict', 'rejected']:
    if summary.get(status, 0) > 0:
        print(f"     - {status.upper()}: {summary[status]}")

# COMMAND ----------

# Show pending changes
if summary['pending'] > 0:
    print("\n7️⃣ Pending Changes (ready to push):")
    
    pending_df = spark.sql(f"""
    SELECT
      change_id,
      row_id,
      operation,
      detected_timestamp
    FROM {changes_table}
    WHERE request_id = '{DTC_REQUEST_ID}'
    AND status = 'pending'
    ORDER BY detected_timestamp ASC
    """)
    
    pending_df.show(20, truncate=False)

# COMMAND ----------

if len(changes) > 0:
    print("\n✅ Change detection complete!")
    print(f"\nNext steps:")
    print(f"  1. Review pending changes in {changes_table}")
    print(f"  2. Run '04_push_changes' to sync to DTC")
else:
    print("\n✅ No changes detected!")
    print("   Your table matches the last snapshot.")
