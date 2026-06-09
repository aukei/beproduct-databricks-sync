# Databricks notebook source
# MAGIC %md
# MAGIC # Create Snapshot After Pull
# MAGIC
# MAGIC Creates a baseline snapshot after pulling from DTC.
# MAGIC
# MAGIC This should be called immediately after `pull_dtc_to_delta.py` to record
# MAGIC the state of the data for change detection.
# MAGIC
# MAGIC Call this once per pull to establish the baseline for change tracking.

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

from sync.snapshot import SnapshotManager
from datetime import datetime

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
data_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}"

print(f"Creating snapshot for {environment.upper()}")
print(f"  Request ID: {DTC_REQUEST_ID}")
print(f"  Data table: {data_table}")
print(f"  Metadata table: {metadata_table}")
print("-" * 80)

# COMMAND ----------

# Fetch the data from the data table
print("\n1️⃣ Fetching data from table...")

df = spark.sql(f"""
SELECT *
FROM {data_table}
WHERE request_id = '{DTC_REQUEST_ID}'
""")

row_count = df.count()
print(f"   Rows fetched: {row_count}")

# COMMAND ----------

# Get document name and details
print("\n2️⃣ Getting request metadata...")

metadata_df = spark.sql(f"""
SELECT DISTINCT
  document_name,
  request_reference,
  request_description
FROM {data_table}
WHERE request_id = '{DTC_REQUEST_ID}'
LIMIT 1
""")

if metadata_df.count() == 0:
    print("   ⚠️  No data found for this request")
    dbutils.notebook.exit(1)

metadata_row = metadata_df.collect()[0]
document_name = metadata_row.document_name
request_reference = metadata_row.request_reference
request_description = metadata_row.request_description

print(f"   Document: {document_name}")
print(f"   Reference: {request_reference}")
print(f"   Description: {request_description}")

# COMMAND ----------

# Create snapshot
print("\n3️⃣ Creating snapshot hash...")

manager = SnapshotManager(spark, metadata_table)

snapshot_hash, metadata_record = manager.create_snapshot(
    request_id=DTC_REQUEST_ID,
    environment=environment,
    data_df=df,
    document_name=document_name,
    row_count=row_count,
    details={
        "request_reference": request_reference,
        "request_description": request_description,
        "created_by": "snapshot_notebook",
        "created_at": datetime.now().isoformat()
    }
)

print(f"   Snapshot Hash: {snapshot_hash}")
print(f"   Rows in snapshot: {row_count}")

# COMMAND ----------

# Store snapshot
print("\n4️⃣ Storing snapshot in metadata table...")

manager.store_snapshot(metadata_record)

print(f"✅ Snapshot stored in {metadata_table}")

# COMMAND ----------

# Verify
print("\n5️⃣ Verifying snapshot...")

verify_df = spark.sql(f"""
SELECT
  request_id,
  environment,
  sync_direction,
  sync_timestamp,
  snapshot_hash,
  row_count
FROM {metadata_table}
WHERE request_id = '{DTC_REQUEST_ID}'
ORDER BY sync_timestamp DESC
LIMIT 3
""")

print("Recent snapshots:")
verify_df.show(truncate=False)

# COMMAND ----------

print("\n✅ Snapshot creation complete!")
print(f"\nNext steps:")
print(f"  1. Make edits to {data_table}")
print(f"  2. Run '03_detect_changes' to find modifications")
print(f"  3. Run '04_push_changes' to sync back to DTC")
