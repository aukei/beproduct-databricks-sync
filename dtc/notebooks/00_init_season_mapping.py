# Databricks notebook source
# MAGIC %md
# MAGIC # Initialize SeasonCode Mapping Table
# MAGIC
# MAGIC Creates and populates the mapping table for DTC seasonCode → BeProduct (Season, Year).
# MAGIC
# MAGIC **Important**: Run this once to set up the mapping table.
# MAGIC Then update with actual mappings for each customer/seasonCode combination.

# COMMAND ----------

# Define widgets
try:
    dbutils.widgets.text("target_catalog", "lft", "Target Catalog")
    dbutils.widgets.text("target_schema", "beproduct", "Target Schema")
except Exception as e:
    pass

# Parameters
TARGET_CATALOG = dbutils.widgets.get("target_catalog")
TARGET_SCHEMA = dbutils.widgets.get("target_schema")

mapping_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.dtc_season_code_mapping"

print(f"Initializing seasonCode mapping table: {mapping_table}")
print("-" * 80)

# COMMAND ----------

print("\n1️⃣ Creating mapping table...")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {mapping_table} (
  dtc_customer STRING NOT NULL,
  season_code STRING NOT NULL,
  beproduct_season STRING NOT NULL,
  beproduct_year INT NOT NULL,
  description STRING,
  created_date TIMESTAMP DEFAULT current_timestamp(),
  PRIMARY KEY (dtc_customer, season_code)
)
USING DELTA
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '1',
  'description' = 'Maps DTC seasonCode to BeProduct (Season, Year)'
)
""")

print(f"✅ Created: {mapping_table}")

# COMMAND ----------

print("\n2️⃣ Checking for existing data...")

count = spark.sql(f"SELECT COUNT(*) as cnt FROM {mapping_table}").collect()[0][0]

if count == 0:
    print(f"   Table is empty. Insert sample mappings:")
    
    # Insert sample mappings
    # IMPORTANT: Update these with actual mappings from your environment
    sample_mappings = [
        ("KTB", "SS28", "Spring", 2028, "Spring 2028"),
        ("KTB", "SS26", "Spring", 2026, "Spring 2026"),
        ("KTB", "FW27", "Fall", 2027, "Fall 2027"),
        ("KTB", "FW26", "Fall", 2026, "Fall 2026"),
    ]
    
    for dtc_cust, season_code, bp_season, bp_year, desc in sample_mappings:
        print(f"   Sample: {dtc_cust} {season_code} → {bp_season} {bp_year}")
        spark.sql(f"""
        INSERT INTO {mapping_table} 
        (dtc_customer, season_code, beproduct_season, beproduct_year, description)
        VALUES ('{dtc_cust}', '{season_code}', '{bp_season}', {bp_year}, '{desc}')
        """)
    
    print(f"\n✅ Inserted {len(sample_mappings)} sample mappings")
    print(f"   ⚠️  UPDATE THESE WITH ACTUAL MAPPINGS FOR YOUR ENVIRONMENT!")
else:
    print(f"   Table has {count} existing mappings")

# COMMAND ----------

print("\n3️⃣ Current mappings:")

mapping_df = spark.sql(f"""
SELECT dtc_customer, season_code, beproduct_season, beproduct_year, description
FROM {mapping_table}
ORDER BY dtc_customer, season_code
""")

mapping_df.show(truncate=False)

# COMMAND ----------

print("\n✅ Mapping table ready!")
print(f"\nTo add new mappings:")
print(f"""
INSERT INTO {mapping_table}
(dtc_customer, season_code, beproduct_season, beproduct_year, description)
VALUES ('KTB', 'SS25', 'Spring', 2025, 'Spring 2025');
""")

print(f"\nTo update existing mapping:")
print(f"""
UPDATE {mapping_table}
SET beproduct_season = 'Summer', beproduct_year = 2028
WHERE dtc_customer = 'KTB' AND season_code = 'SS28';
""")

print(f"\nTo view mappings for a customer:")
print(f"""
SELECT * FROM {mapping_table}
WHERE dtc_customer = 'KTB'
ORDER BY beproduct_year DESC, beproduct_season;
""")
