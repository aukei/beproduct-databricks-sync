# BeProduct Integration Skill

Guide for connecting to BeProduct API and reading master data (styles, colors, brands, etc.) using SDK and REST API.

> For this project's current BeProduct usage (jobs, field extraction, type-aware
> push-back) and the BeProduct tables on Databricks, follow `docs/BEPRODUCT_GUIDE.md`
> and `docs/ARCHITECTURE.md`; verified schema quirks live in `AGENTS.md`. The SDK
> snippets below remain a valid general reference.

## When to Use This Skill

Use this skill when you need to:
- Connect to BeProduct API using OAuth 2.0
- Authenticate with BeProduct SDK or REST API
- Read STYLE master data records
- Read reference/master data (brands, teams, seasons, colors, etc.)
- Pull dropdown/multiselect field values
- Push updates to STYLE records
- Validate field values against master data
- Work with BeProduct field IDs and mappings

## BeProduct API Overview

BeProduct provides both SDK and REST API for accessing product lifecycle management data:
- **SDK**: Python `beproduct` package (recommended for OAuth)
- **REST API**: Direct HTTP calls (requires manual OAuth token management)
- **Authentication**: OAuth 2.0 with refresh tokens
- **Data Model**: STYLE records with customizable fields organized by folders

### Key Concepts
- **Folder**: Organizational unit for styles (e.g., "KTB", "WMT", "WALMART")
- **STYLE**: Product record with metadata fields
- **Field ID**: Internal identifier for fields (e.g., "style_code", "brands_multi")
- **Master Data**: Valid dropdown/multiselect values (brands, seasons, etc.)
- **Header Data**: Field definitions and metadata
- **Data JSON**: The actual field values for a style record

## Prerequisites

Before connecting to BeProduct:
1. Obtain OAuth credentials:
   - `client_id`
   - `client_secret`
   - `refresh_token`
   - `company_domain`
2. Know the folder name (e.g., "KTB")
3. Understand field IDs used in your BeProduct instance
4. Have network access to BeProduct API

## Authentication

### OAuth 2.0 Flow

BeProduct uses OAuth 2.0 with refresh tokens:
1. Exchange `client_id` + `client_secret` + `refresh_token` for `access_token`
2. Use `access_token` in `Authorization: Bearer {token}` header
3. Access tokens expire (typically 1 hour)
4. Refresh automatically using `refresh_token`

### Setup Credentials

**Environment variables (local development):**
```bash
export BEPRODUCT_CLIENT_ID="your-client-id"
export BEPRODUCT_CLIENT_SECRET="your-client-secret"
export BEPRODUCT_REFRESH_TOKEN="your-refresh-token"
export BEPRODUCT_COMPANY_DOMAIN="your-company"
```

**Databricks secrets (production):**
```bash
# Create secret scope
databricks secrets create-scope --scope beproduct

# Add credentials
databricks secrets put --scope beproduct --key client_id
databricks secrets put --scope beproduct --key client_secret
databricks secrets put --scope beproduct --key refresh_token
databricks secrets put --scope beproduct --key company_domain
```

**Access in Databricks notebook:**
```python
client_id = dbutils.secrets.get(scope="beproduct", key="client_id")
client_secret = dbutils.secrets.get(scope="beproduct", key="client_secret")
refresh_token = dbutils.secrets.get(scope="beproduct", key="refresh_token")
company_domain = dbutils.secrets.get(scope="beproduct", key="company_domain")
```

## Connecting with BeProduct SDK

### Installation

```bash
# Install SDK
pip install beproduct

# Or in Databricks notebook
%pip install beproduct
```

### Initialize SDK

**Basic connection:**
```python
from beproduct.sdk import BeProduct

api = BeProduct(
    client_id="your-client-id",
    client_secret="your-client-secret",
    refresh_token="your-refresh-token",
    company_domain="your-company"
)

print("✅ Connected to BeProduct")
```

**In Databricks notebook:**
```python
import subprocess
import sys

# Install SDK
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "beproduct"])

from beproduct.sdk import BeProduct

# Get credentials from secrets
client_id = dbutils.secrets.get(scope="beproduct", key="client_id")
client_secret = dbutils.secrets.get(scope="beproduct", key="client_secret")
refresh_token = dbutils.secrets.get(scope="beproduct", key="refresh_token")
company_domain = dbutils.secrets.get(scope="beproduct", key="company_domain")

# Initialize API
api = BeProduct(
    client_id=client_id,
    client_secret=client_secret,
    refresh_token=refresh_token,
    company_domain=company_domain
)

print("✅ BeProduct SDK initialized")
```

### Get Access Token

```python
# SDK handles OAuth internally
# To get access token for direct API calls:
access_token = api.oauth2_client.get_access_token()

print(f"Access token length: {len(access_token)}")
# Use this token for direct REST API calls
```

## Reading STYLE Master Data

### Get All Styles in a Folder

**Using SDK:**
```python
from beproduct.sdk import BeProduct

api = BeProduct(
    client_id=client_id,
    client_secret=client_secret,
    refresh_token=refresh_token,
    company_domain=company_domain
)

# Get all styles for folder "KTB"
folder_name = "KTB"
styles = api.get_all_styles(folder_name=folder_name)

print(f"Total styles: {len(styles)}")

# Each style is a dict with:
# - id: Style ID
# - data_json: Field values
# - headerData: Field definitions
# - ... other metadata
```

**Iterate through styles:**
```python
for style in styles:
    style_id = style.get('id')
    data = style.get('data_json', {})
    
    # Extract field values
    style_code = data.get('style_code', '')
    season = data.get('season', '')
    brands = data.get('brands_multi', [])
    
    print(f"Style: {style_code}, Season: {season}, Brands: {brands}")
```

### Get Single Style

**Using SDK:**
```python
# Get specific style by ID
style_id = "style_123456"
style = api.get_style(style_id=style_id, folder_name=folder_name)

# Access data
data = style.get('data_json', {})
style_code = data.get('style_code')
style_name = data.get('style_name')

print(f"Style: {style_code} - {style_name}")
```

### Extract Specific Fields

**Define field mapping:**
```python
# Map BeProduct field IDs to Delta table column names
FIELD_MAPPING = {
    "style_code": "style_code",
    "style_name": "style_name",
    "season": "season_code",
    "year": "year",
    "brands_multi": "brands",
    "team": "team_code",
    "product_category": "category",
    "product_sub_category": "sub_category",
    "division": "division",
    "style_status": "status",
    "created_date": "created_date",
    "modified_date": "last_modified"
}

def extract_fields(style: dict, field_mapping: dict) -> dict:
    """Extract specific fields from style data_json."""
    data = style.get('data_json', {})
    extracted = {}
    
    for beproduct_field, table_column in field_mapping.items():
        value = data.get(beproduct_field)
        extracted[table_column] = value
    
    return extracted

# Usage
for style in styles:
    fields = extract_fields(style, FIELD_MAPPING)
    print(fields)
```

### Convert to DataFrame

**Pandas DataFrame:**
```python
import pandas as pd
from typing import List, Dict

def styles_to_dataframe(styles: List[dict], field_mapping: dict) -> pd.DataFrame:
    """Convert styles list to Pandas DataFrame."""
    records = []
    
    for style in styles:
        data = style.get('data_json', {})
        record = {
            'style_id': style.get('id'),
            'folder_name': style.get('folder_name')
        }
        
        # Extract mapped fields
        for beproduct_field, table_column in field_mapping.items():
            record[table_column] = data.get(beproduct_field)
        
        records.append(record)
    
    return pd.DataFrame(records)

# Usage
df = styles_to_dataframe(styles, FIELD_MAPPING)
print(df.head())
```

**Spark DataFrame (in Databricks):**
```python
from pyspark.sql import Row
from datetime import datetime

def styles_to_spark_df(styles: List[dict], field_mapping: dict):
    """Convert styles to Spark DataFrame."""
    rows = []
    
    for style in styles:
        data = style.get('data_json', {})
        
        # Build row dict
        row_data = {
            'style_id': style.get('id'),
            'folder_name': style.get('folder_name'),
            'extracted_time': datetime.now()
        }
        
        # Add mapped fields
        for beproduct_field, table_column in field_mapping.items():
            value = data.get(beproduct_field)
            
            # Handle list fields (MultiSelect)
            if isinstance(value, list):
                value = ','.join(value) if value else None
            
            row_data[table_column] = value
        
        # Store full JSON for audit
        row_data['data_json_full'] = str(data)
        
        rows.append(Row(**row_data))
    
    # Create DataFrame
    df = spark.createDataFrame(rows)
    return df

# Usage
spark_df = styles_to_spark_df(styles, FIELD_MAPPING)
spark_df.display()
```

## Reading Master Data (Reference Data)

### What is Master Data?

Master Data provides valid values for dropdown and multiselect fields:
- **BRANDS** - Valid brand names
- **TEAM** - Valid team codes
- **SEASON** - Valid season codes (SS, FW, etc.)
- **YEAR** - Valid years
- **PRODUCT STATUS** - Valid status values
- **PRODUCT CATEGORY** - Valid categories
- **DIVISION** - Valid divisions
- **PARENT VENDOR** - Valid vendors
- **FACTORY** - Valid factories

### Why Master Data is Critical

When pushing updates to BeProduct:
- ✅ API call succeeds (HTTP 200) even with invalid values
- ❌ Field is set to blank/null if value is invalid (silent failure)
- 😞 No error message tells you what went wrong

**Always validate against master data before pushing!**

### Get Master Data Using REST API

**Field IDs for master data:**
```python
# Map of master data type → field ID
MASTER_DATA_FIELD_IDS = {
    "brands": "brands_multi",
    "teams": "team",
    "seasons": "season",
    "years": "year",
    "product_status": "style_status",
    "product_category": "product_category",
    "product_sub_category": "product_sub_category",
    "division": "division",
    "techpack_stage": "techpack_stage",
    "parent_vendor": "parent_vendor",
    "factory": "factory"
}
```

**Fetch master data:**
```python
import requests

def get_master_data(
    company_domain: str,
    access_token: str,
    field_id: str
) -> List[dict]:
    """
    Fetch master data for a field.
    
    Args:
        company_domain: Company domain name
        access_token: OAuth access token
        field_id: Field ID (e.g., "brands_multi", "season")
    
    Returns:
        List of master data choices
    """
    url = f"https://{company_domain}.beproduct.com/api/{company_domain}/MasterData/{field_id}"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    choices = data.get('choices', [])
    
    return choices

# Usage
access_token = api.oauth2_client.get_access_token()

# Get brands
brands = get_master_data(
    company_domain=company_domain,
    access_token=access_token,
    field_id="brands_multi"
)

for brand in brands:
    print(f"ID: {brand['id']}, Value: {brand['value']}")
```

**Fetch all master data:**
```python
def fetch_all_master_data(
    company_domain: str,
    access_token: str,
    field_ids: dict
) -> dict:
    """Fetch all master data types."""
    master_data = {}
    
    for data_type, field_id in field_ids.items():
        try:
            print(f"Fetching {data_type}...")
            choices = get_master_data(company_domain, access_token, field_id)
            master_data[data_type] = choices
            print(f"  ✓ {len(choices)} values")
        except Exception as e:
            print(f"  ✗ Failed: {e}")
            master_data[data_type] = []
    
    return master_data

# Usage
all_master_data = fetch_all_master_data(
    company_domain=company_domain,
    access_token=access_token,
    field_ids=MASTER_DATA_FIELD_IDS
)

# Access specific master data
brands = all_master_data['brands']
seasons = all_master_data['seasons']
```

### Store Master Data in Delta Lake

**Convert to Spark DataFrame and save:**
```python
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

def save_master_data_to_delta(
    master_data_type: str,
    choices: List[dict],
    catalog: str,
    schema: str
):
    """Save master data to Delta table."""
    
    # Convert to rows
    rows = []
    for choice in choices:
        rows.append(Row(
            id=choice.get('id'),
            value=choice.get('value'),
            is_active=choice.get('isActive', True),
            display_order=choice.get('displayOrder', 0)
        ))
    
    if not rows:
        print(f"⚠️ No data for {master_data_type}")
        return
    
    # Create DataFrame
    df = spark.createDataFrame(rows)
    
    # Add metadata
    from pyspark.sql.functions import current_timestamp
    df = df.withColumn("last_updated", current_timestamp())
    
    # Table name
    table_name = f"{catalog}.{schema}.beproduct_master_{master_data_type}"
    
    # Write to Delta
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)
    
    print(f"✅ Saved {len(rows)} values to {table_name}")

# Usage: Save all master data
for data_type, choices in all_master_data.items():
    save_master_data_to_delta(
        master_data_type=data_type,
        choices=choices,
        catalog="lft",
        schema="beproduct"
    )
```

**Query master data:**
```sql
-- List all brands
SELECT * FROM lft.beproduct.beproduct_master_brands
WHERE is_active = true
ORDER BY display_order;

-- Validate a brand value
SELECT value FROM lft.beproduct.beproduct_master_brands
WHERE value = 'Wrangler' AND is_active = true;
```

## Pushing Updates to BeProduct

### Update STYLE Record

**Using SDK:**
```python
from beproduct.sdk import BeProduct

api = BeProduct(
    client_id=client_id,
    client_secret=client_secret,
    refresh_token=refresh_token,
    company_domain=company_domain
)

# Prepare update data
style_id = "style_123456"
folder_name = "KTB"

update_data = {
    "season": "SS26",  # Must be valid value from master data
    "style_status": "Development",  # Must be valid value
    "brands_multi": ["Wrangler", "Lee"]  # Must be valid values
}

# Update style
response = api.update_style(
    style_id=style_id,
    folder_name=folder_name,
    data=update_data
)

print("✅ Style updated")
```

### Validate Before Pushing

**Validation helper:**
```python
def validate_field_value(
    field_value: any,
    master_data: List[dict],
    field_name: str
) -> bool:
    """
    Validate field value against master data.
    
    Args:
        field_value: Value to validate (string or list)
        master_data: List of valid choices from master data
        field_name: Field name for error messages
    
    Returns:
        True if valid, raises ValueError if invalid
    """
    valid_values = [choice['value'] for choice in master_data if choice.get('isActive', True)]
    
    # Handle single value
    if isinstance(field_value, str):
        if field_value not in valid_values:
            raise ValueError(
                f"Invalid value '{field_value}' for {field_name}. "
                f"Valid values: {valid_values}"
            )
    
    # Handle multiple values (list)
    elif isinstance(field_value, list):
        for value in field_value:
            if value not in valid_values:
                raise ValueError(
                    f"Invalid value '{value}' for {field_name}. "
                    f"Valid values: {valid_values}"
                )
    
    return True

# Usage
brands_master = all_master_data['brands']
season_master = all_master_data['seasons']

try:
    # Validate before pushing
    validate_field_value("Wrangler", brands_master, "brands")
    validate_field_value("SS26", season_master, "season")
    
    # Push update
    api.update_style(
        style_id=style_id,
        folder_name=folder_name,
        data={"season": "SS26", "brands_multi": ["Wrangler"]}
    )
    print("✅ Update successful")
    
except ValueError as e:
    print(f"❌ Validation failed: {e}")
```

### Batch Update with Validation

```python
def batch_update_styles(
    api: BeProduct,
    updates: List[dict],
    folder_name: str,
    master_data: dict,
    dry_run: bool = True
) -> dict:
    """
    Batch update styles with validation.
    
    Args:
        api: BeProduct SDK instance
        updates: List of dicts with 'style_id' and 'data' keys
        folder_name: Folder name
        master_data: Dict of master data by type
        dry_run: If True, validate only without pushing
    
    Returns:
        Dict with success/failure counts
    """
    results = {"success": 0, "failed": 0, "errors": []}
    
    for update in updates:
        style_id = update['style_id']
        data = update['data']
        
        try:
            # Validate each field
            if 'brands_multi' in data:
                validate_field_value(data['brands_multi'], master_data['brands'], 'brands')
            if 'season' in data:
                validate_field_value(data['season'], master_data['seasons'], 'season')
            if 'style_status' in data:
                validate_field_value(data['style_status'], master_data['product_status'], 'status')
            
            # Push update (if not dry run)
            if not dry_run:
                api.update_style(
                    style_id=style_id,
                    folder_name=folder_name,
                    data=data
                )
            
            results['success'] += 1
            print(f"✅ {style_id}: {'Would update' if dry_run else 'Updated'}")
            
        except Exception as e:
            results['failed'] += 1
            results['errors'].append({
                'style_id': style_id,
                'error': str(e)
            })
            print(f"❌ {style_id}: {e}")
    
    return results

# Usage
updates = [
    {
        "style_id": "style_001",
        "data": {"season": "SS26", "brands_multi": ["Wrangler"]}
    },
    {
        "style_id": "style_002",
        "data": {"season": "FW25", "style_status": "Development"}
    }
]

# Dry run first
results = batch_update_styles(
    api=api,
    updates=updates,
    folder_name="KTB",
    master_data=all_master_data,
    dry_run=True
)

print(f"\nDry run: {results['success']} success, {results['failed']} failed")

# If validation passes, do actual update
if results['failed'] == 0:
    results = batch_update_styles(
        api=api,
        updates=updates,
        folder_name="KTB",
        master_data=all_master_data,
        dry_run=False
    )
```

## Complete Workflows

### Workflow 1: Full STYLE Sync to Delta Lake

```python
# ============================================================================
# Pull all styles from BeProduct folder to Delta Lake
# ============================================================================

import subprocess
import sys
from datetime import datetime

# Install SDK
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "beproduct"])

from beproduct.sdk import BeProduct
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, current_date

# Configuration
FOLDER_NAME = "KTB"
CATALOG = "lft"
SCHEMA = "beproduct"
TABLE_NAME = f"{CATALOG}.{SCHEMA}.ktb_styles"

# Field mapping
FIELD_MAPPING = {
    "style_code": "style_code",
    "style_name": "style_name",
    "season": "season_code",
    "year": "year",
    "brands_multi": "brands",
    "team": "team_code",
    "product_category": "category",
    "division": "division",
    "style_status": "status"
}

# Authenticate
client_id = dbutils.secrets.get(scope="beproduct", key="client_id")
client_secret = dbutils.secrets.get(scope="beproduct", key="client_secret")
refresh_token = dbutils.secrets.get(scope="beproduct", key="refresh_token")
company_domain = dbutils.secrets.get(scope="beproduct", key="company_domain")

api = BeProduct(
    client_id=client_id,
    client_secret=client_secret,
    refresh_token=refresh_token,
    company_domain=company_domain
)

# Fetch all styles
print(f"📥 Fetching styles from folder: {FOLDER_NAME}")
styles = api.get_all_styles(folder_name=FOLDER_NAME)
print(f"✅ Retrieved {len(styles)} styles")

# Convert to Spark DataFrame
rows = []
extraction_time = datetime.now()

for style in styles:
    data = style.get('data_json', {})
    
    row_data = {
        'style_id': style.get('id'),
        'folder_name': FOLDER_NAME,
        'extracted_time': extraction_time
    }
    
    # Extract mapped fields
    for bp_field, table_col in FIELD_MAPPING.items():
        value = data.get(bp_field)
        if isinstance(value, list):
            value = ','.join(value) if value else None
        row_data[table_col] = value
    
    # Store full JSON
    row_data['data_json_full'] = str(data)
    
    rows.append(Row(**row_data))

df = spark.createDataFrame(rows)

# Add metadata
df = df.withColumn("last_modified", current_timestamp())
df = df.withColumn("sync_date", current_date())

# Write to Delta
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TABLE_NAME)

print(f"✅ Synced {df.count()} styles to {TABLE_NAME}")
```

### Workflow 2: Sync All Master Data

```python
# ============================================================================
# Sync all master data from BeProduct to Delta Lake
# ============================================================================

import requests
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

# Master data field IDs
MASTER_DATA_FIELD_IDS = {
    "brands": "brands_multi",
    "teams": "team",
    "seasons": "season",
    "years": "year",
    "product_status": "style_status",
    "product_category": "product_category",
    "division": "division"
}

# Get access token
access_token = api.oauth2_client.get_access_token()

# Fetch each master data type
for data_type, field_id in MASTER_DATA_FIELD_IDS.items():
    print(f"\n{'='*60}")
    print(f"Syncing: {data_type.upper()}")
    print("="*60)
    
    try:
        # Fetch from API
        url = f"https://{company_domain}.beproduct.com/api/{company_domain}/MasterData/{field_id}"
        headers = {"Authorization": f"Bearer {access_token}"}
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        choices = response.json().get('choices', [])
        print(f"📥 Fetched {len(choices)} values")
        
        # Convert to DataFrame
        rows = [
            Row(
                id=c.get('id'),
                value=c.get('value'),
                is_active=c.get('isActive', True),
                display_order=c.get('displayOrder', 0)
            )
            for c in choices
        ]
        
        if rows:
            df = spark.createDataFrame(rows)
            df = df.withColumn("last_updated", current_timestamp())
            
            # Write to Delta
            table_name = f"{CATALOG}.{SCHEMA}.beproduct_master_{data_type}"
            df.write.format("delta") \
                .mode("overwrite") \
                .saveAsTable(table_name)
            
            print(f"✅ Saved to {table_name}")
        else:
            print(f"⚠️ No data to save")
            
    except Exception as e:
        print(f"❌ Failed: {e}")

print("\n" + "="*60)
print("✅ Master data sync complete")
print("="*60)
```

### Workflow 3: Push Changes from Delta to BeProduct

```python
# ============================================================================
# Push modified styles from Delta Lake back to BeProduct
# ============================================================================

from pyspark.sql.functions import col

# Query modified styles
modified_styles = spark.sql("""
    SELECT style_id, season_code, brands, status
    FROM lft.beproduct.ktb_styles
    WHERE last_modified >= current_date() - INTERVAL 1 DAYS
      AND season_code IS NOT NULL
""").collect()

print(f"📤 Found {len(modified_styles)} modified styles to push")

# Fetch master data for validation
access_token = api.oauth2_client.get_access_token()

master_data = {}
for data_type, field_id in {"seasons": "season", "brands": "brands_multi"}.items():
    url = f"https://{company_domain}.beproduct.com/api/{company_domain}/MasterData/{field_id}"
    response = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
    master_data[data_type] = response.json().get('choices', [])

# Push updates
push_results = []

for row in modified_styles:
    style_id = row.style_id
    
    try:
        # Prepare update data
        update_data = {}
        
        if row.season_code:
            # Validate season
            validate_field_value(row.season_code, master_data['seasons'], 'season')
            update_data['season'] = row.season_code
        
        if row.brands:
            # Parse brands (comma-separated)
            brands_list = row.brands.split(',') if row.brands else []
            validate_field_value(brands_list, master_data['brands'], 'brands')
            update_data['brands_multi'] = brands_list
        
        # Push update
        api.update_style(
            style_id=style_id,
            folder_name=FOLDER_NAME,
            data=update_data
        )
        
        push_results.append({
            'style_id': style_id,
            'status': 'success',
            'data': str(update_data)
        })
        print(f"✅ {style_id}: Updated")
        
    except Exception as e:
        push_results.append({
            'style_id': style_id,
            'status': 'failed',
            'error': str(e)
        })
        print(f"❌ {style_id}: {e}")

# Log results to Delta
push_log_rows = [Row(**r) for r in push_results]
push_log_df = spark.createDataFrame(push_log_rows)
push_log_df = push_log_df.withColumn("push_time", current_timestamp())

push_log_df.write.format("delta") \
    .mode("append") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.ktb_styles_push_log")

success_count = sum(1 for r in push_results if r['status'] == 'success')
print(f"\n✅ Push complete: {success_count}/{len(modified_styles)} successful")
```

## Troubleshooting

### Authentication Issues

**Token expired:**
```python
# SDK automatically refreshes tokens
# If manual refresh needed:
try:
    access_token = api.oauth2_client.get_access_token()
except Exception as e:
    print(f"Token refresh failed: {e}")
    # Check refresh_token validity
    # May need to regenerate OAuth credentials
```

**Invalid credentials:**
```python
# Test credentials
try:
    api = BeProduct(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        company_domain=company_domain
    )
    # Try a simple API call
    styles = api.get_all_styles(folder_name="KTB", limit=1)
    print("✅ Credentials valid")
except Exception as e:
    print(f"❌ Credentials invalid: {e}")
```

### Field ID Issues

**Field not found:**
```python
# List all available fields
style = api.get_style(style_id="...", folder_name="KTB")
data = style.get('data_json', {})

print("Available field IDs:")
for field_id in data.keys():
    print(f"  - {field_id}")
```

**Wrong field ID:**
```python
# Field IDs are case-sensitive and instance-specific
# Check headerData for field definitions
header_data = style.get('headerData', {})
fields = header_data.get('fields', [])

for field in fields:
    print(f"Name: {field['name']}, ID: {field['id']}, Type: {field['type']}")
```

### Validation Failures

**Silent failures (field set to null):**
```python
# Always fetch and validate master data before pushing
# Invalid values result in silent failures (field becomes null)

# Good practice:
1. Fetch master data
2. Validate values
3. Push update
4. Verify by fetching style again

# Verify update
updated_style = api.get_style(style_id=style_id, folder_name=folder_name)
updated_data = updated_style.get('data_json', {})

if updated_data.get('season') is None:
    print("⚠️ Season was set to null - likely invalid value")
```

## Best Practices

1. **Always use secrets** - Never hardcode OAuth credentials
2. **Validate before pushing** - Fetch master data and validate all dropdown values
3. **Use dry run mode** - Test validation before actual updates
4. **Store full JSON** - Keep `data_json_full` for audit trail
5. **Add metadata** - Track `extracted_time`, `last_modified`, `sync_date`
6. **Log push operations** - Create push_log table for audit
7. **Handle lists properly** - MultiSelect fields return arrays
8. **Refresh master data daily** - Run master data sync before style sync
9. **Test with single style** - Verify field IDs work before batch operations
10. **Check field definitions** - Field IDs may vary by BeProduct instance

## Reference

### BeProduct SDK Methods

```python
class BeProduct:
    def __init__(client_id, client_secret, refresh_token, company_domain)
    def get_all_styles(folder_name, limit=None) -> List[dict]
    def get_style(style_id, folder_name) -> dict
    def update_style(style_id, folder_name, data) -> dict
    
class OAuth2Client:
    def get_access_token() -> str
```

### REST API Endpoints

- `GET /api/{company}/Styles` - List styles in folder
- `GET /api/{company}/Styles/{style_id}` - Get single style
- `PUT /api/{company}/Styles/{style_id}` - Update style
- `GET /api/{company}/MasterData/{field_id}` - Get master data choices

### Common Field IDs

- `style_code` - Style code (string)
- `style_name` - Style name (string)
- `season` - Season (dropdown)
- `year` - Year (dropdown)
- `brands_multi` - Brands (multiselect)
- `team` - Team (dropdown)
- `style_status` - Status (dropdown)
- `product_category` - Category (dropdown)
- `division` - Division (dropdown)
- `created_date` - Created date (timestamp)
- `modified_date` - Modified date (timestamp)

### Project Files

- SDK notebooks: `beproduct/beproduct_style_sync.py`, `standalone/beproduct_style_push.py`
- Master data: `beproduct/beproduct_master_data_sync.py`
- Documentation: `docs/BEPRODUCT_GUIDE.md`, `docs/ARCHITECTURE.md`, `AGENTS.md`
