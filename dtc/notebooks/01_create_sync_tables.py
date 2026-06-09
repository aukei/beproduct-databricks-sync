# Databricks notebook source
# MAGIC %md
# MAGIC # Create Sync Metadata & Change Log Tables
# MAGIC
# MAGIC Initialize Phase 2 infrastructure for bi-directional sync:
# MAGIC - `dtc_sync_metadata_{environment}`: Snapshots after each pull
# MAGIC - `dtc_master_chart_changes_{environment}`: Change audit trail
# MAGIC
# MAGIC Run this once per environment (uat, prod) to set up tables.

# COMMAND ----------

# Define widgets with defaults
try:
    dbutils.widgets.text("target_catalog", "lft", "Target Catalog")
    dbutils.widgets.text("target_schema", "beproduct", "Target Schema")
    dbutils.widgets.text("target_table", "dtc_master_chart_uat", "Target Table Name (e.g., dtc_master_chart_uat)")
except Exception as e:
    pass

# Parameters
TARGET_CATALOG = dbutils.widgets.get("target_catalog")
TARGET_SCHEMA = dbutils.widgets.get("target_schema")
TARGET_TABLE = dbutils.widgets.get("target_table")

# Extract environment from table name (e.g., "dtc_master_chart_uat" → "uat")
environment = TARGET_TABLE.split("_")[-1]  # uat or prod
metadata_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.dtc_sync_metadata_{environment}"
changes_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.dtc_master_chart_changes_{environment}"
full_data_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}"

print(f"Creating sync tables for {environment.upper()} environment")
print(f"  Base table: {full_data_table}")
print(f"  Metadata table: {metadata_table}")
print(f"  Changes table: {changes_table}")
print("-" * 80)

# COMMAND ----------

# Create Metadata Table
print("\n1️⃣ Creating metadata table...")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {metadata_table} (
  request_id STRING NOT NULL,
  environment STRING NOT NULL,
  sync_direction STRING NOT NULL,           -- 'pull' or 'push'
  sync_timestamp TIMESTAMP NOT NULL,
  snapshot_hash STRING,                     -- SHA256 hash of all row data
  row_count INT,
  document_name STRING,
  details MAP<STRING, STRING>,
  PRIMARY KEY (request_id, sync_timestamp)
)
USING DELTA
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '1',
  'description' = 'Sync metadata and snapshots for {full_data_table}'
)
""")

print(f"✅ Created: {metadata_table}")

# COMMAND ----------

# Create Change Log Table
print("\n2️⃣ Creating change log table...")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {changes_table} (
  change_id STRING NOT NULL,                            -- UUID
  request_id STRING NOT NULL,
  row_id STRING NOT NULL,                               -- DTC rowId
  operation STRING NOT NULL,                            -- 'INSERT', 'UPDATE', 'DELETE'
  detected_timestamp TIMESTAMP NOT NULL,
  columns_changed MAP<STRING, STRUCT<
    old_value: STRING,
    new_value: STRING
  >>,
  change_source STRING NOT NULL,                        -- 'databricks' or 'dtc'
  status STRING NOT NULL DEFAULT 'pending',             -- 'pending', 'pushed', 'conflict', 'rejected'
  push_timestamp TIMESTAMP,
  conflict_reason STRING,
  push_response MAP<STRING, STRING>,                    -- Response from DTC API after push
  PRIMARY KEY (change_id)
)
USING DELTA
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '1',
  'description' = 'Change audit trail for {full_data_table}'
)
""")

print(f"✅ Created: {changes_table}")

# COMMAND ----------

# Verify tables
print("\n3️⃣ Verifying tables...")

metadata_info = spark.sql(f"DESCRIBE TABLE {metadata_table}").toPandas()
print(f"Metadata table columns ({len(metadata_info)}):")
print(metadata_info[['col_name', 'data_type']].to_string(index=False))

print()

changes_info = spark.sql(f"DESCRIBE TABLE {changes_table}").toPandas()
print(f"Changes table columns ({len(changes_info)}):")
print(changes_info[['col_name', 'data_type']].to_string(index=False))

# COMMAND ----------

# Show row counts
print("\n4️⃣ Table Summary:")

metadata_count = spark.sql(f"SELECT COUNT(*) as count FROM {metadata_table}").collect()[0][0]
changes_count = spark.sql(f"SELECT COUNT(*) as count FROM {changes_table}").collect()[0][0]

print(f"  Metadata table: {metadata_count} rows")
print(f"  Changes table: {changes_count} rows")

print("\n✅ Phase 2 infrastructure ready!")
print(f"   Run '02_create_snapshot' after each pull to baseline the data")
print(f"   Run '03_detect_changes' to find edits since last snapshot")
print(f"   Run '04_push_changes' to sync back to DTC")
