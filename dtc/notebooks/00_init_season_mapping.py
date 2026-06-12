# Databricks notebook source
# MAGIC %md
# MAGIC # Initialize SeasonCode Mapping Table
# MAGIC
# MAGIC Creates and populates `lft.beproduct.dtc_seasoncode_mapping`, the table that
# MAGIC maps a BeProduct (Customer, Season) to a DTC season code **prefix**.
# MAGIC
# MAGIC ## DTC <> BeProduct season identification
# MAGIC - **DTC** identifies a season with 2 values: `(Customer, SeasonCode)`
# MAGIC   e.g. `(KTB, SS28)`, `(KTB, FW26)`
# MAGIC - **BeProduct** identifies a season with 3 values: `(Customer, Season, Year)`
# MAGIC   e.g. `(KTB, Spring, 2028)`, `(KTB, Fall, 2026)`
# MAGIC
# MAGIC ## What this table stores (prefix only)
# MAGIC | Column   | Meaning                                   | Example          |
# MAGIC |----------|-------------------------------------------|------------------|
# MAGIC | CUSTOMER | BeProduct customer code                   | `KTB`            |
# MAGIC | SEASON   | BeProduct season name                     | `SPRING`, `FALL` |
# MAGIC | DTCCODE  | DTC season code **prefix**                | `SS`, `FW`       |
# MAGIC
# MAGIC ## Derivation of the full DTC SeasonCode
# MAGIC `DTC SeasonCode = DTCCODE + last 2 digits (YY) of the BeProduct Year`
# MAGIC e.g. `SPRING + 2028 -> "SS" + "28" -> "SS28"`
# MAGIC
# MAGIC The year is **not** stored here; it comes from each style's `year` field at
# MAGIC runtime. Forward direction (BeProduct -> DTC):
# MAGIC `beproduct/beproduct_to_dtc_transform.py`. Reverse direction (DTC -> BeProduct):
# MAGIC `dtc/notebooks/pull_dtc_to_delta.py`. Both read this same table.
# MAGIC
# MAGIC **Important**: Run this once to set up the mapping table, then add a row for
# MAGIC each (customer, season) prefix combination.

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

# NOTE: table name is `dtc_seasoncode_mapping` (no underscore between season/code)
# to match the readers in beproduct_to_dtc_transform.py and pull_dtc_to_delta.py.
mapping_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.dtc_seasoncode_mapping"

print(f"Initializing seasonCode mapping table: {mapping_table}")
print("-" * 80)

# COMMAND ----------

print("\n1️⃣ Creating mapping table...")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {mapping_table} (
  CUSTOMER STRING NOT NULL COMMENT 'BeProduct Customer Code',
  SEASON   STRING NOT NULL COMMENT 'BeProduct Season Name (e.g. SPRING, FALL)',
  DTCCODE  STRING NOT NULL COMMENT 'DTC Season Code prefix (e.g. SS, FW)'
)
USING DELTA
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '1',
  'description' = 'Maps BeProduct (CUSTOMER, SEASON) to DTC season code prefix DTCCODE. Full DTC SeasonCode = DTCCODE + YY of BeProduct year.'
)
""")

print(f"✅ Created: {mapping_table}")

# COMMAND ----------

print("\n2️⃣ Checking for existing data...")

count = spark.sql(f"SELECT COUNT(*) as cnt FROM {mapping_table}").collect()[0][0]

if count == 0:
    print(f"   Table is empty. Inserting sample mappings:")

    # (CUSTOMER, SEASON, DTCCODE) -- prefix only; year is supplied at runtime.
    # IMPORTANT: Add a row for every customer/season prefix combination you use.
    sample_mappings = [
        ("KTB", "SPRING", "SS"),
        ("KTB", "FALL", "FW"),
    ]

    for cust, season, dtccode in sample_mappings:
        print(f"   Sample: ({cust}, {season}) -> DTCCODE '{dtccode}'  (e.g. {season} 2028 -> {dtccode}28)")
        spark.sql(f"""
        INSERT INTO {mapping_table} (CUSTOMER, SEASON, DTCCODE)
        VALUES ('{cust}', '{season}', '{dtccode}')
        """)

    print(f"\n✅ Inserted {len(sample_mappings)} sample mappings")
    print(f"   ⚠️  ADD ANY ADDITIONAL CUSTOMER/SEASON PREFIXES FOR YOUR ENVIRONMENT!")
else:
    print(f"   Table has {count} existing mappings")

# COMMAND ----------

print("\n3️⃣ Current mappings:")

mapping_df = spark.sql(f"""
SELECT CUSTOMER, SEASON, DTCCODE
FROM {mapping_table}
ORDER BY CUSTOMER, SEASON
""")

mapping_df.show(truncate=False)

# COMMAND ----------

print("\n✅ Mapping table ready!")
print(f"\nReminder: full DTC SeasonCode = DTCCODE + last 2 digits of BeProduct year.")
print(f"   e.g. (KTB, SPRING, SS) + year 2028  ->  SS28")
print(f"        (KTB, FALL,   FW) + year 2027  ->  FW27")

print(f"\nTo add a new prefix mapping:")
print(f"""
INSERT INTO {mapping_table} (CUSTOMER, SEASON, DTCCODE)
VALUES ('KTB', 'SUMMER', 'SU');
""")

print(f"\nTo update an existing mapping:")
print(f"""
UPDATE {mapping_table}
SET DTCCODE = 'SP'
WHERE CUSTOMER = 'KTB' AND SEASON = 'SPRING';
""")

print(f"\nTo view mappings for a customer:")
print(f"""
SELECT * FROM {mapping_table}
WHERE CUSTOMER = 'KTB'
ORDER BY SEASON;
""")
