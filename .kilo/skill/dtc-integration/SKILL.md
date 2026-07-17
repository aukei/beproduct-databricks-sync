# DTC Integration Skill

Guide for connecting to DTC (Data Collaboration Tool) and reading worksheet data.

> ⚠️ **Partially superseded.** The DTC **connector** usage below is current, but
> the snapshot / change-detection / `dtc_master_chart_uat` change-log examples
> describe a **removed** pipeline. The current model pulls the `WIP_ITS_USE` view
> of registry-discovered requests into `lft.beproduct.dtc_wip_<customer>` and syncs
> via Phase 1 (BeProduct→DTC, incl. request **create** + **share**), Phase 2
> (DTC→BeProduct), and Phase 3 (image upload). Authoritative docs:
> `docs/DTC_GUIDE.md`, `docs/ARCHITECTURE.md`, `docs/PHASE1_WORKFLOW.md`,
> `docs/PHASE2_WORKFLOW.md`, `docs/PHASE3_WORKFLOW.md`, and `AGENTS.md` — not the
> change-tracking snippets in this file.
>
> **Current DTC write contracts** (validated, see `AGENTS.md`): upsert
> `PATCH /v1/sheets/{sheetId}/views/{viewId}` (204); create `POST /v1/sheets`
> (201; `requestReference` + non-empty `requestDescription` + array fields);
> share `POST /v1/requests/{id}/shares/{userEmail}` and `.../shares/usergroups/{group}`
> (201); image `POST /v1/sheets/{sheetId}/views/{viewId}/images?rowindex=..&columnname=Style Image`
> (multipart, file part `file`; webp rejected → transcode to PNG).

## When to Use This Skill

Use this skill when you need to:
- Connect to DTC API (UAT or Production environment)
- Fetch DTC requests and their metadata
- Read worksheets/sheets from DTC
- Extract data from specific views
- Parse DTC request names for business logic
- Convert DTC sheet data to Pandas or Spark DataFrames
- Implement change tracking for DTC data
- Push updates back to DTC sheets

## DTC API Overview

DTC provides a REST API for accessing worksheet data with the following key concepts:
- **Request**: A DTC request contains metadata and links to sheets
- **Sheet**: Contains the actual tabular data
- **View**: Different perspectives/filters on a sheet (e.g., "WIP_ITS_USE", "Summary")
- **Rows**: Individual data rows with row_id for updates
- **Columns**: Named columns, may contain HTML or special formatting

## Prerequisites

Before connecting to DTC:
1. Obtain DTC API key (UAT or Production)
2. Know the DTC workspace name (e.g., "KTB")
3. Have request IDs or sheet IDs to query
4. Understand the environment (uat or prod)

## Authentication

### API Key Setup

**Environment variables:**
```bash
# For UAT
export DTC_API_KEY_UAT="your-uat-api-key"

# For Production
export DTC_API_KEY_PROD="your-prod-api-key"
```

**In Databricks (recommended):**
```bash
# Create secret scope
databricks secrets create-scope --scope beproduct

# Add DTC API keys
databricks secrets put --scope beproduct --key dtc_api_key_uat
databricks secrets put --scope beproduct --key dtc_api_key_prod
```

**Access in notebook:**
```python
# Get API key from Databricks secrets
api_key = dbutils.secrets.get(scope="beproduct", key="dtc_api_key_uat")
```

## Connecting to DTC

### Initialize DTCConnector

**Basic connection:**
```python
from dtc.python.connectors.dtc import DTCConnector

# UAT environment
dtc = DTCConnector(
    api_key="your-api-key",
    environment="uat",  # or "prod"
    workspace_name="KTB"
)
```

**In Databricks notebook:**
```python
import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

from connectors.dtc import DTCConnector

# Get API key from secrets
api_key = dbutils.secrets.get(scope="beproduct", key="dtc_api_key_uat")

# Initialize connector
dtc = DTCConnector(
    api_key=api_key,
    environment="uat",
    workspace_name="KTB"
)

print("✅ Connected to DTC UAT")
```

**Environment mapping:**
```python
# DTCConnector automatically maps environments to URLs:
# "uat"  -> https://dtc-api.lfuat.net/api
# "prod" -> https://dtc-api.lfapps.net/api
```

## Reading DTC Data

### 1. Get Request Metadata

**Fetch a request by ID:**
```python
# Get request details
request = dtc.get_request("REQ_12345")

print(f"Request Name: {request['name']}")
print(f"Request Reference: {request['requestReference']}")
print(f"Sheet ID: {request['sheetId']}")
print(f"Status: {request['status']}")
print(f"Created: {request['createdAt']}")

# Access nested data
brand = request.get('brand', 'Unknown')
season = request.get('season', 'Unknown')
```

**Parse request name for business logic:**
```python
# In-scope DTC request reference format: "<customer> <seasonCode> <brand>"
#   seasonCode = 2 letters + 2 digits (e.g. FW26); brand = everything after it.
# Authoritative parsing / in-scope test: dtc/python/sync/phase1.py
from sync.phase1 import parse_request_reference, is_in_scope

parsed = parse_request_reference(request['requestReference'])
# Example: "KTB FW26 Wrangler Western"
# Returns: {'customer': 'KTB', 'season_code': 'FW26', 'brand': 'Wrangler Western'}

in_scope = is_in_scope(request['requestReference'], customer="KTB")  # True/False
```

### 2. Get Available Views

**List all views for a request:**
```python
# Get views
views = dtc.get_views(request_id="REQ_12345")

for view in views:
    print(f"View ID: {view['viewId']}")
    print(f"View Name: {view['viewName']}")
    print(f"Default: {view.get('isDefault', False)}")
    print("---")

# Find specific view by name
full_version_view = next(
    (v for v in views if v['viewName'] == 'WIP_ITS_USE'),
    None
)

if full_version_view:
    view_id = full_version_view['viewId']
    print(f"Found 'WIP_ITS_USE' view: {view_id}")
```

### 3. Read Sheet Data

**Get raw sheet data:**
```python
# Fetch sheet data for a specific view
sheet_data = dtc.get_sheet(
    sheet_id="sheet_abc123",
    view_id="view_xyz789"
)

# Access sheet metadata
print(f"Total Rows: {sheet_data['totalRows']}")
print(f"Total Columns: {sheet_data['totalColumns']}")

# Access column definitions
columns = sheet_data['columnDefinitions']
for col in columns:
    print(f"Column: {col['columnName']} (Type: {col['columnType']})")

# Access row data
rows = sheet_data['sheetData']
for row in rows[:5]:  # First 5 rows
    print(f"Row ID: {row['rowId']}")
    print(f"Data: {row['columnValues']}")
```

**Convert to Pandas DataFrame:**
```python
# Convert sheet data to Pandas DataFrame
df = dtc.to_dataframe(
    sheet_id="sheet_abc123",
    view_id="view_xyz789"
)

print(f"DataFrame shape: {df.shape}")
print(f"Columns: {df.columns.tolist()}")
print(df.head())

# DataFrame includes:
# - All data columns from the sheet
# - row_id column (for updates)
# - Normalized column names (HTML tags removed)
```

**Convert to Spark DataFrame (in Databricks):**
```python
# Get Pandas DataFrame first
pandas_df = dtc.to_dataframe(
    sheet_id=request['sheetId'],
    view_id=view_id
)

# Convert to Spark DataFrame
spark_df = spark.createDataFrame(pandas_df)

# Display in Databricks
spark_df.display()

# Or use spark.createDataFrame with schema
from pyspark.sql.types import StructType, StructField, StringType

# DTC data is typically all strings initially
columns = pandas_df.columns.tolist()
schema = StructType([
    StructField(col, StringType(), True) for col in columns
])

spark_df = spark.createDataFrame(pandas_df, schema=schema)
```

## Working with DTC Sheet Data

### Column Normalization

DTC columns may contain HTML or special characters. The connector automatically normalizes them:

```python
# Original DTC column names might be:
# - "Style #"
# - "<b>Brand</b>"
# - "Season Code"
# - "Delivery Date (Target)"

# Normalized to valid Python/Delta column names:
# - "Style_"
# - "Brand"
# - "Season_Code"
# - "Delivery_Date_Target"

# The normalization happens in to_dataframe() method
# Access original names in sheet_data['columnDefinitions']
```

### Handling Row IDs

Every DTC row has a unique `row_id` that's essential for updates:

```python
# DataFrame includes row_id column
df = dtc.to_dataframe(sheet_id="...", view_id="...")

# Row ID is preserved for later updates
print(df[['row_id', 'lf_style', 'Brand']].head())

# Example output:
#     row_id    lf_style       Brand
# 0   row_123   STY001        Wrangler
# 1   row_124   STY002        Lee
```

### Data Type Handling

```python
# DTC returns all data as strings
# Convert data types as needed
from pyspark.sql.functions import col, to_date, to_timestamp

df = spark_df \
    .withColumn("delivery_date", to_date(col("Delivery_Date"), "yyyy-MM-dd")) \
    .withColumn("quantity", col("Quantity").cast("integer")) \
    .withColumn("price", col("Price").cast("decimal(10,2)"))
```

## Complete Workflow: DTC to Delta Lake

### End-to-End Pull Example

```python
# ============================================================================
# Pull DTC data to Delta Lake with change tracking
# ============================================================================

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

from connectors.dtc import DTCConnector
from pyspark.sql.functions import current_timestamp, current_date, lit
from datetime import datetime

# Configuration
DTC_REQUEST_ID = "REQ_12345"
DTC_ENVIRONMENT = "uat"
TARGET_TABLE = "lft.beproduct.dtc_master_chart_uat"

# Step 1: Initialize connector
api_key = dbutils.secrets.get(scope="beproduct", key="dtc_api_key_uat")
dtc = DTCConnector(api_key=api_key, environment=DTC_ENVIRONMENT)

# Step 2: Get request metadata
request = dtc.get_request(DTC_REQUEST_ID)
print(f"📋 Request: {request['requestReference']}")

# Step 3: Parse business logic
parsed = dtc.parse_request_name(request['requestReference'])
brand_from_request = parsed.get('brand', 'Unknown')
print(f"🏷️  Brand (from request): {brand_from_request}")

# Step 4: Get views
views = dtc.get_views(DTC_REQUEST_ID)
full_version_view = next(
    (v for v in views if v['viewName'] == 'WIP_ITS_USE'),
    views[0]  # Fallback to first view
)
view_id = full_version_view['viewId']

# Step 5: Fetch sheet data
pandas_df = dtc.to_dataframe(
    sheet_id=request['sheetId'],
    view_id=view_id
)

# Step 6: Convert to Spark DataFrame
df = spark.createDataFrame(pandas_df)

# Step 7: Apply business logic
# Override Brand column with brand from request name (source of truth)
df = df.withColumn("Brand", lit(brand_from_request))
df = df.withColumn("Brand_modified", lit(True))  # Track modification

# Step 8: Add metadata
extraction_time = datetime.now()
df = df.withColumn("extracted_time", lit(extraction_time))
df = df.withColumn("last_modified", current_timestamp())
df = df.withColumn("sync_date", current_date())

# Step 9: Write to Delta Lake
df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

row_count = df.count()
print(f"✅ Synced {row_count} rows to {TARGET_TABLE}")

# Step 10: Query results
spark.sql(f"""
    SELECT row_id, lf_style, Brand, Brand_modified, last_modified
    FROM {TARGET_TABLE}
    LIMIT 10
""").display()
```

## Change Tracking

### Tracking Modified Fields

```python
# Create change log table for audit trail
from pyspark.sql.functions import explode, array, struct, lit

# Detect changes by comparing old and new values
old_df = spark.table("lft.beproduct.dtc_master_chart_uat")
new_df = # ... new data from DTC

# Create change log entries
change_log = old_df.alias("old").join(
    new_df.alias("new"),
    on="row_id",
    how="inner"
).where(
    (col("old.Brand") != col("new.Brand"))
).select(
    col("new.row_id"),
    col("new.lf_style"),
    lit("brand_overwrite").alias("modification_type"),
    col("old.Brand").alias("old_value"),
    col("new.Brand").alias("new_value"),
    current_timestamp().alias("modified_at"),
    current_date().alias("sync_date")
)

# Append to change log table
change_log.write.format("delta") \
    .mode("append") \
    .saveAsTable("lft.beproduct.dtc_master_chart_uat_change_log")

print(f"📝 Logged {change_log.count()} changes")
```

### Query Change History

```python
# Find all modified rows
modified_rows = spark.sql("""
    SELECT DISTINCT row_id, lf_style, new_value as Brand, modified_at
    FROM lft.beproduct.dtc_master_chart_uat_change_log
    WHERE modification_type = 'brand_overwrite'
      AND sync_date >= current_date() - INTERVAL 7 DAYS
    ORDER BY modified_at DESC
""")

modified_rows.display()

# View change history for specific style
spark.sql("""
    SELECT * FROM lft.beproduct.dtc_master_chart_uat_change_log
    WHERE lf_style = 'STYLE123'
    ORDER BY modified_at DESC
""").display()
```

## Pushing Updates to DTC

### Update Single Row

```python
# Use RestClient directly for PATCH operations
from client.rest_client import RestClient

# Initialize client
client = RestClient(
    base_url="https://dtc-api.lfuat.net/api",
    api_key=api_key,
    timeout=30
)

# Update a single cell
response = client.patch(
    f"/v1/sheets/{sheet_id}/rows/{row_id}",
    json={
        "columnValues": {
            "Brand": "Wrangler"
        }
    }
)

print(f"✅ Updated row {row_id}")
```

### Batch Update Multiple Rows

```python
# Get rows that need to be pushed back to DTC
rows_to_push = spark.sql("""
    SELECT row_id, lf_style, Brand
    FROM lft.beproduct.dtc_master_chart_uat_change_log
    WHERE modification_type = 'brand_overwrite'
      AND sync_date = current_date()
""").collect()

# Push each row
from client.rest_client import RestClient

client = RestClient(
    base_url="https://dtc-api.lfuat.net/api",
    api_key=api_key
)

for row in rows_to_push:
    try:
        client.patch(
            f"/v1/sheets/{sheet_id}/rows/{row.row_id}",
            json={"columnValues": {"Brand": row.Brand}}
        )
        print(f"✅ Updated {row.lf_style}")
    except Exception as e:
        print(f"❌ Failed to update {row.lf_style}: {e}")
```

## Common Patterns

### Pattern 1: Full Sync with Brand Override

```python
def sync_dtc_to_delta(request_id: str, environment: str = "uat"):
    """
    Full DTC sync with brand override business logic.
    
    Business Rule: Brand column = Brand parsed from request name
    """
    # Get API key
    api_key = dbutils.secrets.get(scope="beproduct", key=f"dtc_api_key_{environment}")
    
    # Initialize
    dtc = DTCConnector(api_key=api_key, environment=environment)
    
    # Fetch request
    request = dtc.get_request(request_id)
    parsed = dtc.parse_request_name(request['requestReference'])
    brand = parsed.get('brand', 'Unknown')
    
    # Get WIP_ITS_USE view
    views = dtc.get_views(request_id)
    view = next((v for v in views if v['viewName'] == 'WIP_ITS_USE'), views[0])
    
    # Fetch data
    df = dtc.to_dataframe(sheet_id=request['sheetId'], view_id=view['viewId'])
    spark_df = spark.createDataFrame(df)
    
    # Apply business logic
    spark_df = spark_df \
        .withColumn("Brand", lit(brand)) \
        .withColumn("Brand_modified", lit(True)) \
        .withColumn("extracted_time", lit(datetime.now())) \
        .withColumn("sync_date", current_date())
    
    # Write to Delta
    table_name = f"lft.beproduct.dtc_master_chart_{environment}"
    spark_df.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(table_name)
    
    return spark_df.count()

# Usage
row_count = sync_dtc_to_delta("REQ_12345", "uat")
print(f"✅ Synced {row_count} rows")
```

### Pattern 2: Incremental Sync with Change Detection

```python
from delta.tables import DeltaTable

def incremental_sync_dtc(request_id: str, environment: str = "uat"):
    """
    Incremental sync: only update changed rows.
    """
    # Fetch new data
    api_key = dbutils.secrets.get(scope="beproduct", key=f"dtc_api_key_{environment}")
    dtc = DTCConnector(api_key=api_key, environment=environment)
    
    request = dtc.get_request(request_id)
    views = dtc.get_views(request_id)
    view = next((v for v in views if v['viewName'] == 'WIP_ITS_USE'), views[0])
    
    new_df = dtc.to_dataframe(sheet_id=request['sheetId'], view_id=view['viewId'])
    new_df = spark.createDataFrame(new_df)
    new_df = new_df.withColumn("last_modified", current_timestamp())
    
    # Load existing table
    table_name = f"lft.beproduct.dtc_master_chart_{environment}"
    delta_table = DeltaTable.forName(spark, table_name)
    
    # Merge (upsert)
    delta_table.alias("target").merge(
        new_df.alias("source"),
        "target.row_id = source.row_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    
    print("✅ Incremental sync completed")

# Usage
incremental_sync_dtc("REQ_12345", "uat")
```

### Pattern 3: Multi-Request Sync

```python
def sync_multiple_requests(request_ids: list, environment: str = "uat"):
    """
    Sync multiple DTC requests to the same Delta table.
    """
    api_key = dbutils.secrets.get(scope="beproduct", key=f"dtc_api_key_{environment}")
    dtc = DTCConnector(api_key=api_key, environment=environment)
    
    all_dfs = []
    
    for request_id in request_ids:
        print(f"Processing {request_id}...")
        
        request = dtc.get_request(request_id)
        views = dtc.get_views(request_id)
        view = next((v for v in views if v['viewName'] == 'WIP_ITS_USE'), views[0])
        
        df = dtc.to_dataframe(sheet_id=request['sheetId'], view_id=view['viewId'])
        spark_df = spark.createDataFrame(df)
        spark_df = spark_df.withColumn("request_id", lit(request_id))
        
        all_dfs.append(spark_df)
    
    # Union all DataFrames
    from functools import reduce
    from pyspark.sql import DataFrame
    
    combined_df = reduce(DataFrame.union, all_dfs)
    
    # Write to Delta
    combined_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("lft.beproduct.dtc_combined_requests")
    
    return combined_df.count()

# Usage
request_ids = ["REQ_001", "REQ_002", "REQ_003"]
total_rows = sync_multiple_requests(request_ids, "uat")
```

## Troubleshooting

### Common Issues

**Authentication errors:**
```python
# Test API connection
try:
    dtc = DTCConnector(api_key=api_key, environment="uat")
    request = dtc.get_request("REQ_12345")
    print("✅ Connection successful")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    # Check API key validity
    # Verify environment setting
```

**Request not found:**
```python
# Verify request ID exists
try:
    request = dtc.get_request("REQ_12345")
except Exception as e:
    if "404" in str(e):
        print("Request not found. Check request ID.")
    else:
        print(f"Error: {e}")
```

**View not found:**
```python
# List all available views
views = dtc.get_views("REQ_12345")
print("Available views:")
for v in views:
    print(f"  - {v['viewName']} (ID: {v['viewId']})")

# Use exact view name or viewId
```

**Column name issues:**
```python
# DTC columns may have special characters
# Use backticks in Spark SQL
spark.sql("""
    SELECT `Style #`, `<b>Brand</b>`, row_id
    FROM temp_table
""")

# Or use normalized names from to_dataframe()
df = dtc.to_dataframe(...)  # Columns are normalized
```

**Empty data:**
```python
# Check if view has data
sheet_data = dtc.get_sheet(sheet_id, view_id)
if sheet_data['totalRows'] == 0:
    print("⚠️ View has no data")
else:
    print(f"Found {sheet_data['totalRows']} rows")
```

## Best Practices

1. **Use secrets for API keys** - Never hardcode credentials
2. **Parse request names** - Extract brand/season from request reference
3. **Use WIP_ITS_USE view** - For complete data extraction
4. **Track row_id** - Essential for updates/push operations
5. **Add metadata** - Include `extracted_time`, `sync_date`, `batch_id`
6. **Handle column normalization** - DTC columns may have HTML/special chars
7. **Implement change tracking** - Log all modifications for audit
8. **Use mergeSchema** - Handle schema evolution gracefully
9. **Test with limits** - Use `.limit(10)` during development
10. **Monitor API limits** - Be aware of rate limiting

## Reference

### DTCConnector Methods

```python
class DTCConnector:
    def __init__(api_key, environment, workspace_name)
    def get_request(request_id) -> Dict
    def get_views(request_id) -> List[Dict]
    def get_sheet(sheet_id, view_id, filters=None) -> Dict
    def to_dataframe(sheet_id, view_id) -> pd.DataFrame
    
    @staticmethod
    def parse_request_name(request_reference) -> Dict
```

### DTC API Endpoints

- `GET /v1/requests/{request_id}` - Get request details
- `GET /v1/requests/{request_id}/views` - List views
- `GET /v1/sheets/{sheet_id}/views/{view_id}` - Get sheet data
- `PATCH /v1/sheets/{sheet_id}/rows/{row_id}` - Update row

### Environment URLs

- UAT: `https://dtc-api.lfuat.net/api`
- Production: `https://dtc-api.lfapps.net/api`

### Project Files

- Connector: `dtc/python/connectors/dtc.py`
- REST Client: `dtc/python/client/rest_client.py`
- Notebook: `dtc/notebooks/pull_masters_to_delta.py` (+ `00_init_request_registry.py`)
- Documentation: `docs/DTC_GUIDE.md`, `docs/ARCHITECTURE.md`, `docs/PHASE1_WORKFLOW.md`, `docs/PHASE2_WORKFLOW.md`, `docs/PHASE3_WORKFLOW.md`
