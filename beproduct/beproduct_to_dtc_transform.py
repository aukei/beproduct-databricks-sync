# Databricks notebook source
"""
BeProduct to DTC Denormalization Transform
===========================================

Transforms BeProduct's normalized data structure into DTC's flat denormalized structure.

Process:
1. Read BeProduct styles (with colorways)
2. Explode colorways: 1 style → N rows (one per color)
3. Add fabric row: Each (style × color) → 1 hardcoded row
   (Fabric Group = "MAIN MATERIAL CONTENT", Placement = main_material_content)
4. Map BeProduct season/year to DTC season code (SS26, FW27, etc.)
5. Derive DTC request name: "<Customer> <SeasonCode> <Brand>"
6. Map all fields to DTC column names
7. Write to staging table

Result: N colors × 1 fabric row = N rows per style

Schedule: Daily at 12pm UTC (after style sync at 11am)

Parameters:
  - catalog: Databricks catalog (default: "lft")
  - schema: Databricks schema (default: "beproduct")
  - source_table: Styles table with colorways (default: "ktb_styles")
  - staging_table: Output staging table (default: "beproduct_to_dtc_staging")
  - folder_name: BeProduct folder name (default: "KTB")
"""

# COMMAND ----------

# ============================================================================
# CELL 1: Setup
# ============================================================================

print("=" * 80)
print("DENORMALIZATION TRANSFORM SETUP")
print("=" * 80)

from pyspark.sql.functions import (
    explode, col, lit, concat, concat_ws, current_timestamp, 
    current_date, array, when, coalesce, upper, trim, size, right, substring,
    from_json
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, ArrayType
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure parameters
dbutils.widgets.text("catalog", "lft", "Catalog Name")
dbutils.widgets.text("schema", "beproduct", "Schema Name")
dbutils.widgets.text("source_table", "ktb_styles", "Source Table")
dbutils.widgets.text("staging_table", "beproduct_to_dtc_staging", "Staging Table")
dbutils.widgets.text("folder_name", "KTB", "Folder Name")
dbutils.widgets.text("customer_code", "KTB", "DTC Customer Code (e.g., KTB)")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_table = dbutils.widgets.get("source_table")
staging_table = dbutils.widgets.get("staging_table")
folder_name = dbutils.widgets.get("folder_name")
customer_code = dbutils.widgets.get("customer_code")

source_table_full = f"{catalog}.{schema}.{source_table}"
staging_table_full = f"{catalog}.{schema}.{staging_table}"
season_mapping_table = f"{catalog}.{schema}.dtc_seasoncode_mapping"

print("✅ Parameters configured:")
print(f"   Source: {source_table_full}")
print(f"   Staging: {staging_table_full}")
print(f"   Season mapping: {season_mapping_table}")
print(f"   Folder: {folder_name}")
print(f"   DTC Customer: {customer_code}")

print("\n" + "=" * 80)
print("✅ SETUP COMPLETE")
print("=" * 80)

# COMMAND ----------

# ============================================================================
# CELL 2: Load Source Data
# ============================================================================

print("\n" + "=" * 80)
print("Step 1: Load Source Data")
print("=" * 80)

try:
    print(f"📥 Loading from {source_table_full}...")
    
    # Load styles
    df_source = spark.table(source_table_full)
    
    source_count = df_source.count()
    print(f"✅ Loaded {source_count} styles")
    
    # Check for duplicate style keys
    print(f"\n🔍 Checking for duplicate style keys (LF Style # + Brands + Season + Year)...")
    duplicate_styles = df_source.groupBy("lf_style_number", "brands", "season", "year") \
        .count() \
        .filter(col("count") > 1)
    
    dup_count = duplicate_styles.count()
    if dup_count > 0:
        print(f"⚠️  WARNING: Found {dup_count} duplicate style keys in source data!")
        duplicate_styles.show(truncate=False)
        
        # Log warning to the existing sync_meta table
        metadata_table = f"{catalog}.{schema}.{source_table}_sync_meta"
        warning_summary = f"WARNING: Found {dup_count} duplicate style keys (LF Style # + Brands + Season + Year). Pipeline proceeding."
        
        spark.sql(f"""
            INSERT INTO {metadata_table}
            SELECT 
                CAST(current_timestamp() AS STRING) AS last_sync_at,
                'WARNING' AS sync_type,
                {dup_count} AS records_synced,
                '{warning_summary.replace("'", "''")}' AS summary
        """)
        print(f"✅ Logged duplicate style key warning to {metadata_table}")
        print(f"⚠️  Pipeline will proceed with the data.")
    else:
        print(f"✅ No duplicate style keys found")
    
    # Show sample
    print(f"\n   Sample source data:")
    df_source.select(
        "lf_style_number", "season", "year", "brands", "main_material_content"
    ).show(3, truncate=50)
    
except Exception as e:
    print(f"❌ Failed to load source data: {e}")
    raise

# COMMAND ----------

# ============================================================================
# CELL 3: Explode Colorways (1 Style → N Colors)
# ============================================================================

print("\n" + "=" * 80)
print("Step 2: Explode Colorways")
print("=" * 80)

try:
    print(f"🔄 Exploding colorway detail (id + name)...")

    # Parse colorways_json (array of {colorway_id, color_name, color_number}).
    # We explode the DETAIL (not the names-only colorways_array) so each row
    # carries the colorway_id needed for Phase 2 DTC -> BeProduct Lot# pushback.
    cw_schema = ArrayType(StructType([
        StructField("colorway_id", StringType()),
        StructField("color_name", StringType()),
        StructField("color_number", StringType()),
    ]))
    df_parsed = df_source.withColumn("cw_detail", from_json(col("colorways_json"), cw_schema))

    # Filter out styles with no colorways
    df_with_colors_raw = df_parsed.where(
        col("cw_detail").isNotNull() & (size(col("cw_detail")) > 0)
    )
    styles_with_colors = df_with_colors_raw.count()

    print(f"   Styles with colorways: {styles_with_colors} / {source_count}")

    # Explode to one row per color, carrying color name + colorway_id.
    df_exploded_colors = (
        df_with_colors_raw
        .withColumn("cw", explode(col("cw_detail")))
        .withColumn("color", col("cw.color_name"))
        .withColumn("colorway_id", col("cw.colorway_id"))
        .drop("cw", "cw_detail")
    )

    exploded_count = df_exploded_colors.count()
    print(f"✅ Exploded to {exploded_count} rows (style × color)")
    if styles_with_colors:
        print(f"   Avg colors per style: {exploded_count / styles_with_colors:.1f}")

    # Show sample
    print(f"\n   Sample after colorway explosion:")
    df_exploded_colors.select(
        "lf_style_number", "brands", "color", "colorway_id", "main_material_content"
    ).show(5, truncate=50)

except Exception as e:
    print(f"❌ Failed to explode colorways: {e}")
    raise

# COMMAND ----------

# ============================================================================
# CELL 4: Add Fabric Group and Placement (1 Row per Style × Color)
# ============================================================================

print("\n" + "=" * 80)
print("Step 3: Add Fabric Group and Placement")
print("=" * 80)

try:
    print(f"🔄 Adding Fabric Group and Placement columns...")
    print(f"   Per requirements: 1 row per (style × color)")
    print(f"     - Fabric Group: hardcoded to 'MAIN MATERIAL CONTENT'")
    print(f"     - Placement: value of main_material_content")
    
    df_denormalized = df_exploded_colors.withColumn("fabric_group", lit("MAIN MATERIAL CONTENT")) \
                                        .withColumn("placement", col("main_material_content"))
    
    denorm_count = df_denormalized.count()
    print(f"✅ Processed to {denorm_count} rows (style × color)")
    print(f"   Expected: {exploded_count}")
    print(f"   Actual: {denorm_count}")
    
    # Show sample
    print(f"\n   Sample denormalized data:")
    df_denormalized.select(
        "lf_style_number", "color", "fabric_group", "placement"
    ).orderBy("lf_style_number", "color").show(6, truncate=50)
    
except Exception as e:
    print(f"❌ Failed to add Fabric Group and Placement: {e}")
    raise

# COMMAND ----------

# ============================================================================
# CELL 5: Map Season Code (BeProduct Season + Year → DTC SeasonCode)
# ============================================================================
#
# DTC <> BeProduct season identification differs:
#   - DTC identifies a season with 2 values:  (Customer, SeasonCode)
#       e.g. (KTB, SS28), (KTB, FW26)
#   - BeProduct identifies a season with 3 values: (Customer, Season, Year)
#       e.g. (KTB, Spring, 2028), (KTB, Fall, 2026)
#
# The mapping table `lft.beproduct.dtc_seasoncode_mapping` only stores the
# *prefix* part of the relationship:
#       CUSTOMER  STRING  -- BeProduct customer code, e.g. "KTB"
#       BPSEASON  STRING  -- BeProduct season name,   e.g. "SPRING", "FALL"
#       DTCCODE   STRING  -- DTC season code prefix,  e.g. "SS", "FW"
#   Sample rows: (KTB, SPRING, SS), (KTB, FALL, FW)
#
# The full DTC SeasonCode is derived at runtime:
#       DTC SeasonCode = DTCCODE + last 2 digits (YY) of the BeProduct Year
#   e.g. SPRING + 2028 -> "SS" + "28" -> "SS28"
#        FALL   + 2027 -> "FW" + "27" -> "FW27"
#
# NOTE: `year` in the source styles table is a STRING (e.g. "2026"), and may
# contain the sentinel "N/A"; such rows simply fail the join / stay NULL and
# are reported as unmapped below.
# The reverse direction (DTC -> BeProduct) lives in
# dtc/notebooks/pull_dtc_to_delta.py and uses the same table.
# ============================================================================

print("\n" + "=" * 80)
print("Step 4: Map Season Code")
print("=" * 80)

try:
    print(f"📋 Loading season code mapping from {season_mapping_table}...")
    
    # Load season mapping table (schema: CUSTOMER, BPSEASON, DTCCODE)
    df_season_mapping = spark.table(season_mapping_table)
    
    mapping_count = df_season_mapping.count()
    print(f"   Mappings available: {mapping_count}")
    
    df_season_mapping.show(truncate=False)
    
    # Rename mapping columns to non-colliding names. The styles side already has a
    # `season` column; the mapping's BeProduct-season column is named BPSEASON to
    # avoid the case-insensitive collision that would make `col("season")` ambiguous.
    df_map = (df_season_mapping
              .withColumnRenamed("CUSTOMER", "map_customer")
              .withColumnRenamed("BPSEASON", "map_season")
              .withColumnRenamed("DTCCODE", "map_dtccode"))
    
    # Join with denormalized data on (CUSTOMER, BPSEASON), case-insensitive.
    # The mapping is on the season *prefix* only; the year supplies the YY suffix.
    print(f"\n🔄 Joining with season mapping...")
    print(f"   Match on: CUSTOMER={customer_code}, BPSEASON=season (case-insensitive)")
    
    df_with_season = df_denormalized.join(
        df_map,
        on=[
            (upper(lit(customer_code)) == upper(col("map_customer"))),
            (upper(col("season")) == upper(col("map_season")))
        ],
        how="left"
    )
    
    # Derive DTC SeasonCode = DTCCODE + last 2 digits of year
    # (e.g. 'SS' + '28' = 'SS28'). `year` is a STRING, so right() is applied
    # directly; rows with no mapping match (map_dtccode NULL) stay NULL.
    df_with_season = df_with_season.withColumn(
        "season_code",
        when(col("map_dtccode").isNotNull() & col("year").isNotNull(), 
             concat(col("map_dtccode"), right(col("year"), lit(2))))
        .otherwise(lit(None))
    )
    
    # Check for unmapped seasons
    unmapped = df_with_season.where(col("season_code").isNull()).count()
    
    if unmapped > 0:
        print(f"\n⚠️  WARNING: {unmapped} rows have no season code mapping")
        print(f"   Unmapped season/year combinations:")
        df_with_season.where(col("season_code").isNull()) \
            .select("season", "year").distinct().show()
        print(f"\n   Action: Update {season_mapping_table} with missing mappings")
    else:
        print(f"✅ All rows mapped successfully")
    
    mapped_count = df_with_season.where(col("season_code").isNotNull()).count()
    print(f"   Mapped: {mapped_count} / {denorm_count}")
    
except Exception as e:
    print(f"❌ Failed to map season code: {e}")
    raise

# COMMAND ----------

# ============================================================================
# CELL 6: Derive DTC Request Name
# ============================================================================

print("\n" + "=" * 80)
print("Step 5: Derive DTC Request Name")
print("=" * 80)

try:
    print(f"🔄 Building DTC request names...")
    print(f"   Format: '<Customer> <SeasonCode> <Brand>'")
    print(f"   Example: 'KTB SS26 Wrangler'")
    
    # Derive DTC request name: "<Customer> <SeasonCode> <Brand>"
    df_with_request_name = df_with_season.withColumn(
        "dtc_request_name",
        concat_ws(" ", lit(customer_code), col("season_code"), col("brands"))
    )
    
    # Show unique request names
    print(f"\n   Unique DTC request names:")
    df_with_request_name.select("dtc_request_name") \
        .distinct() \
        .orderBy("dtc_request_name") \
        .show(20, truncate=False)
    
    unique_requests = df_with_request_name.select("dtc_request_name").distinct().count()
    print(f"\n   Total unique requests: {unique_requests}")
    
except Exception as e:
    print(f"❌ Failed to derive request name: {e}")
    raise

# COMMAND ----------

# ============================================================================
# CELL 7: Map Fields to DTC Columns
# ============================================================================

print("\n" + "=" * 80)
print("Step 6: Map Fields to DTC Columns")
print("=" * 80)

try:
    print(f"🔄 Mapping BeProduct fields to DTC column names...")
    
    # Field mapping: BeProduct column → DTC column
    # Per requirements document (lines 44-45)
    FIELD_MAPPING = {
        # Compulsory fields
        "lf_style_number": "LF Style#",
        "brands": "Brand",
        # Already derived: season_code, dtc_request_name
        
        # Interested fields
        "description": "Style Description",
        "product_status": "Product Status",
        "product_category": "Class",
        "product_sub_category": "Sub Class",
        "division": "Division",
        "garment_finish": "Garment Finish",
        "techpack_stage": "Tech Pack Stage",
        
        # Denormalized fields
        "color": "Color / Wash",
        "fabric_group": "Fabric Group",
        "placement": "Placement",
        
        # Image (deferred to separate notebook)
        "front_image_url": "Style Image URL",  # Not pushed yet
        
        # Metadata (keep for internal tracking)
        "team": "team_code",
        "customer_style_number": "customer_style_number_plm",
        "lot_code": "lot_code",
        "parent_vendor": "parent_vendor",
        "factory": "factory",
    }
    
    # Create staging DataFrame with mapped columns
    df_staging = df_with_request_name.select(
        # Composite key
        col("dtc_request_name"),
        col("lf_style_number"),
        col("color"),
        col("colorway_id"),          # carried for Phase 2 DTC -> BeProduct Lot# pushback
        col("fabric_group"),
        col("placement"),
        
        # DTC columns
        col("brands"),
        col("season"),
        col("year"),
        col("season_code"),
        col("description"),
        col("product_status"),
        col("product_category"),
        col("product_sub_category"),
        col("division"),
        col("garment_finish"),
        col("techpack_stage"),
        col("front_image_url"),
        
        # Metadata
        col("team"),
        col("customer_style_number"),
        col("lot_code"),
        col("parent_vendor"),
        col("factory"),
        col("folder_name"),
        col("id").alias("beproduct_style_id"),
        col("modified_at").alias("beproduct_modified_at"),
        
        # Add sync metadata
        current_timestamp().alias("transformed_at"),
        current_date().alias("transform_date"),
        lit("pending").alias("sync_status"),
    )
    
    staging_count = df_staging.count()
    print(f"✅ Created staging DataFrame: {staging_count} rows")
    
    # Show sample
    print(f"\n   Sample staging data:")
    df_staging.select(
        "dtc_request_name", "lf_style_number", "color", 
        "fabric_group", "placement", "brands", "sync_status"
    ).show(5, truncate=50)
    
except Exception as e:
    print(f"❌ Failed to map fields: {e}")
    raise

# COMMAND ----------

# ============================================================================
# CELL 8: Data Validation
# ============================================================================

print("\n" + "=" * 80)
print("Step 7: Data Quality Validation")
print("=" * 80)

try:
    print(f"🔍 Running data quality checks...")
    
    validations = []
    
    # 1. Check required fields
    print(f"\n1️⃣ Checking required fields...")
    required_fields = ["lf_style_number", "season_code", "brands", "color"]
    
    for field in required_fields:
        null_count = df_staging.where(col(field).isNull()).count()
        if null_count > 0:
            validations.append({
                "rule": f"Required field: {field}",
                "status": "FAIL",
                "count": null_count
            })
            print(f"   ❌ {field}: {null_count} null values")
        else:
            print(f"   ✅ {field}: no nulls")
    
    # 2. Check season code mapping
    print(f"\n2️⃣ Checking season code mapping...")
    unmapped = df_staging.where(col("season_code").isNull()).count()
    if unmapped > 0:
        validations.append({
            "rule": "Season code mapping",
            "status": "FAIL",
            "count": unmapped
        })
        print(f"   ❌ {unmapped} rows without season code")
    else:
        print(f"   ✅ All rows have season code")
    
    # 3. Check DTC request name format
    print(f"\n3️⃣ Checking DTC request name format...")
    # Format: "<Customer> <SSYY> <Brand>"
    invalid_names = df_staging.where(
        ~col("dtc_request_name").rlike("^[A-Z]+ [A-Z]{2}[0-9]{2} .+$")
    ).count()
    if invalid_names > 0:
        validations.append({
            "rule": "DTC request name format",
            "status": "WARN",
            "count": invalid_names
        })
        print(f"   ⚠️  {invalid_names} rows with invalid request name format")
    else:
        print(f"   ✅ All request names valid")
    
    # 4. Check for duplicate composite keys
    print(f"\n4️⃣ Checking for duplicate keys...")
    duplicates = df_staging.groupBy(
        "dtc_request_name", "lf_style_number", "color", "fabric_group"
    ).count().where(col("count") > 1).count()
    
    if duplicates > 0:
        validations.append({
            "rule": "Unique composite key",
            "status": "FAIL",
            "count": duplicates
        })
        print(f"   ❌ {duplicates} duplicate keys found")
    else:
        print(f"   ✅ All keys unique")
    
    # Summary
    print(f"\n{'='*60}")
    print(f"Validation Summary:")
    print(f"{'='*60}")
    
    if not validations:
        print(f"✅ All validations passed!")
    else:
        for v in validations:
            status_emoji = "❌" if v["status"] == "FAIL" else "⚠️"
            print(f"{status_emoji} {v['rule']}: {v['count']} issues")
        
        # Fail if critical issues
        critical_fails = [v for v in validations if v["status"] == "FAIL"]
        if critical_fails:
            print(f"\n❌ {len(critical_fails)} critical validation failures")
            print(f"   Fix these issues before continuing")
            raise ValueError(f"Data quality validation failed: {len(critical_fails)} critical issues")
    
except Exception as e:
    print(f"❌ Validation failed: {e}")
    raise

# COMMAND ----------

# ============================================================================
# CELL 9: Write to Staging Table
# ============================================================================

print("\n" + "=" * 80)
print("Step 8: Write to Staging Table")
print("=" * 80)

try:
    print(f"💾 Writing to {staging_table_full}...")
    
    # Write to staging table
    df_staging.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(staging_table_full)
    
    print(f"✅ Write complete:")
    print(f"   Table: {staging_table_full}")
    print(f"   Rows: {staging_count}")
    
    # Verify write
    verify_count = spark.table(staging_table_full).count()
    print(f"   Verified: {verify_count} rows in table")
    
except Exception as e:
    print(f"❌ Failed to write staging table: {e}")
    raise

# COMMAND ----------

# ============================================================================
# Final Summary
# ============================================================================

print("\n" + "=" * 80)
print("✅ DENORMALIZATION COMPLETE")
print("=" * 80)

print(f"\nTransformation Summary:")
print(f"  Input styles: {source_count}")
print(f"  Styles with colors: {styles_with_colors}")
print(f"  After color explosion: {exploded_count} rows")
print(f"  After adding fabric row: {denorm_count} rows")
print(f"  Final staging rows: {staging_count}")
print(f"  Unique DTC requests: {unique_requests}")
print(f"\nStaging table: {staging_table_full}")
print(f"\nNext steps:")
print(f"  1. Run dtc_request_manager.py to ensure DTC requests exist")
print(f"  2. Run beproduct_to_dtc_push.py to sync to DTC")

# COMMAND ----------

# Final verification query
print("\n" + "=" * 80)
print("Final Verification")
print("=" * 80)

spark.sql(f"""
SELECT 
    dtc_request_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT lf_style_number) as unique_styles,
    COUNT(DISTINCT color) as unique_colors
FROM {staging_table_full}
GROUP BY dtc_request_name
ORDER BY dtc_request_name
""").show(truncate=False)

print("\n✅ Transform complete!")
