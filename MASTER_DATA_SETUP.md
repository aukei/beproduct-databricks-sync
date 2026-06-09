# BeProduct Master Data Sync - Setup & Usage

## Overview

This job pulls **Master Data** (valid dropdown values) from BeProduct and stores them in Databricks. Master Data includes:

- **BRANDS** - Valid brand names
- **TEAM** - Valid team codes
- **SEASON** - Valid season values
- **YEAR** - Valid year values
- **PRODUCT STATUS** - Valid status codes
- **PRODUCT CATEGORY** - Valid category values
- **PRODUCT SUB CATEGORY** - Valid subcategory values
- **DIVISION** - Valid division codes
- **TECHPACK STAGE** - Valid techpack stages
- **GARMENT FINISH** - Valid finish types
- **PARENT VENDOR** - Valid parent vendors
- **FACTORY** - Valid factory codes

### Why Master Data?

When pushing changes back to BeProduct, dropdown/multiselect fields **must use values from Master Data**. If you send an invalid value:
- ✅ API call succeeds (HTTP 200)
- ❌ Field is set to blank/null (silent failure)
- 😞 No error message to tell you what went wrong

By syncing Master Data to Databricks, you can:
1. Validate dropdown values before pushing
2. Create reference tables for data quality checks
3. Build UI dropdowns that match BeProduct's valid values

---

## Setup

### Prerequisites

1. **Secrets configured** - Same BeProduct credentials as pull job (`client_id`, `client_secret`, `refresh_token`, `company_domain`)
2. **Databricks workspace ready** - With catalog and schema access
3. **Verify credentials work** - Test your credentials using the local app first:
   ```bash
   # Local app uses the same SDK approach (see app/beproduct_client.py)
   # Test by running the local Streamlit app:
   streamlit run app/ui/main.py
   
   # If the local app connects successfully, credentials are valid for Databricks too
   ```
4. **Field IDs confirmed** - The notebook uses standard field IDs (brands_multi, team, season, etc.)
   - If your instance uses different field IDs, update `MASTER_DATA_FIELD_IDS` in the notebook before running
   - Field IDs are the `id` properties from the style response `headerData.fields[]` array

### Create the Job

#### Option A: Using Databricks UI (Easiest)

1. **Go to** Workflows → Jobs → **Create job**
2. **Job name:** `BeProduct Master Data - Daily Sync`
3. **Task:**
   - **Task name:** `master_data_sync`
   - **Type:** Notebook
   - **Notebook path:** `/Repos/beproduct-sync/MASTERDATA/beproduct_master_data_sync`
   - **Cluster:** Use same cluster as pull job (or all-purpose cluster)

4. **Parameters:**
   - `catalog` = `lft`
   - `schema` = `beproduct`

5. **Schedule:** Daily at 10am UTC (before pull job at 11am UTC)
   - Cron: `0 0 10 * * ?`

6. **Click Create**

#### Option B: Using Databricks CLI

```bash
databricks jobs create --json '{
  "name": "BeProduct Master Data - Daily Sync",
  "tasks": [{
    "task_key": "master_data_sync",
    "notebook_task": {
      "notebook_path": "/Repos/beproduct-sync/MASTERDATA/beproduct_master_data_sync",
      "base_parameters": {
        "catalog": "lft",
        "schema": "beproduct"
      }
    },
    "job_cluster_config": {
      "spark_version": "14.3.x-scala2.12",
      "node_type_id": "i3.xlarge",
      "num_workers": 1
    },
    "timeout_seconds": 1800
  }],
  "schedule": {
    "quartz_cron_expression": "0 0 10 * * ?",
    "timezone_id": "UTC"
  }
}'
```

---

## Tables Created

The job creates one table for each master data type:

```
lft.beproduct.beproduct_master_brands
lft.beproduct.beproduct_master_teams
lft.beproduct.beproduct_master_seasons
lft.beproduct.beproduct_master_years
lft.beproduct.beproduct_master_product_status
lft.beproduct.beproduct_master_product_category
lft.beproduct.beproduct_master_product_sub_category
lft.beproduct.beproduct_master_division
lft.beproduct.beproduct_master_techpack_stage
lft.beproduct.beproduct_master_garment_finish
lft.beproduct.beproduct_master_parent_vendor
lft.beproduct.beproduct_master_factory
```

Each table has columns:
- **`value`** - The internal identifier/code
- **`label`** - The human-readable name/label
- **`data_json`** - Full JSON response from API (for debugging)
- **`synced_at`** - When this was fetched

---

## Usage

### View Valid Values for a Field

```sql
-- View all valid BRANDS
SELECT value, label 
FROM lft.beproduct.beproduct_master_brands 
ORDER BY label;

-- View all valid TEAMS
SELECT value, label 
FROM lft.beproduct.beproduct_master_teams 
ORDER BY label;

-- View all valid SEASONS
SELECT value, label 
FROM lft.beproduct.beproduct_master_seasons 
ORDER BY label;
```

### Validate Before Pushing

```sql
-- Check if a brand value exists in Master Data
SELECT COUNT(*) as found
FROM lft.beproduct.beproduct_master_brands
WHERE value = 'Nike' OR label = 'Nike';
-- Returns 1 if valid, 0 if invalid
```

### Find Valid Values Dynamically

```sql
-- Show all valid PRODUCT STATUS values
SELECT 
  value,
  label,
  COUNT(*) as count
FROM lft.beproduct.beproduct_master_product_status
GROUP BY value, label
ORDER BY label;
```

### Import into Reference Lookup

```sql
-- Create a lookup table for data quality checks
CREATE OR REPLACE TABLE lft.beproduct.valid_dropdown_values AS
SELECT 'brands' as field_type, value, label FROM lft.beproduct.beproduct_master_brands
UNION ALL
SELECT 'teams' as field_type, value, label FROM lft.beproduct.beproduct_master_teams
UNION ALL
SELECT 'seasons' as field_type, value, label FROM lft.beproduct.beproduct_master_seasons
UNION ALL
SELECT 'years' as field_type, value, label FROM lft.beproduct.beproduct_master_years;

-- Now validate styles against this lookup
SELECT 
  k.id,
  k.lf_style_number,
  k.brands,
  CASE WHEN v.value IS NOT NULL THEN '✓ Valid' ELSE '✗ Invalid' END as brands_status
FROM lft.beproduct.ktb_styles k
LEFT JOIN lft.beproduct.valid_dropdown_values v 
  ON k.brands = v.value AND v.field_type = 'brands'
WHERE k.brands IS NOT NULL;
```

---

## Troubleshooting

### Issue: Job fails with "Endpoint not found (404)"

**Cause:** The field ID mapping in the notebook doesn't match your BeProduct instance's field definitions.

**Solution:**
1. Check if your instance uses different field IDs than the defaults
2. Get the correct field IDs from your BeProduct schema:
   ```sql
   -- Query BeProduct styles to find the actual field IDs
   SELECT DISTINCT id, name FROM your_styles
   WHERE field_id = 'brands_multi' OR name LIKE '%BRAND%'
   LIMIT 20;
   ```
3. Update the `MASTER_DATA_FIELD_IDS` dict in the notebook with correct field IDs
4. Re-run the job

**How to find field IDs:**
The field ID is the `id` property of each field in the `headerData.fields[]` array from a style pull. For example:
```json
{
  "id": "brands_multi",        // This is the field ID to use
  "name": "BRANDS",
  "value": ["Nike", "Adidas"],
  "type": "MultiSelect"
}
```

### Issue: Getting "unauthorized_client" or 401 Unauthorized error

**Cause:** Credentials aren't configured for master data endpoints, or the SDK doesn't have master data methods.

**Solution:**

1. **Test credentials with local app first** (most important!)
   ```bash
   # Start the local app to verify credentials work
   streamlit run app/ui/main.py
   
   # If it connects to BeProduct successfully, credentials are valid
   # If it fails, fix credentials before trying Databricks
   ```

2. **If local app works but Databricks fails:**
   - The master data endpoints may not be available via SDK or require special configuration
   - Contact BeProduct support to ask:
     - Is there a `/MasterData/{fieldId}` REST endpoint available?
     - Do we need special OAuth scopes for master data access?
     - Are there SDK methods like `api.masterdata.get()` available?

3. **Alternative: Manual Master Data** - Create tables from pulled styles (no API call needed):
   ```sql
   -- Extract unique values from pulled styles
   SELECT DISTINCT brands FROM lft.beproduct.ktb_styles 
   WHERE brands IS NOT NULL AND brands != ''
   ORDER BY brands;
   ```

**How the credentials work:**
- **Local app** (`app/beproduct_client.py`): Creates SDK client with `refresh_token`
- **Pull job** (`beproduct_style_sync.py`): Creates SDK client with `refresh_token`
- **Master data job** (this notebook): Also creates SDK client with `refresh_token`

All three use the same BeProduct SDK approach, so if one works, the others should too.

### Issue: Master data tables are empty

**Cause:** The API returned empty results, or the request failed silently.

**Solution:**
1. Check the job logs for errors
2. Verify OAuth token is valid
3. Check that `company_domain` is correct in secrets
4. Verify API endpoints are accessible from your network
5. Verify the endpoints are returning data (not all master data types may be populated in your instance)

### Issue: Values don't match between Databricks and BeProduct

**Cause:** Master data may have changed in BeProduct since last sync, or API returns different format.

**Solution:**
1. Run the master data sync job again to refresh
2. Check `synced_at` column to see when data was last updated
3. Compare `value` vs `label` columns (may need to use different column depending on API format)

---

## Best Practices

### 1. Schedule Before Pull Job

Run master data sync **before** the pull job:
```
Master Data Sync: 10:00 UTC
Pull Job:        11:00 UTC
Push Job:        14:00 UTC (manual or hourly)
```

This ensures push validation rules are always up-to-date.

### 2. Create Validation Rules

Add data quality checks to your pipeline:
```sql
-- Fail if brands are invalid
SELECT *
FROM lft.beproduct.ktb_styles
WHERE brands IS NOT NULL
  AND brands NOT IN (SELECT value FROM lft.beproduct.beproduct_master_brands);
```

### 3. Document Master Data Changes

Track when master data changes:
```sql
-- See when each table was last updated
SELECT 
  'brands' as master_data_type,
  MAX(synced_at) as last_synced
FROM lft.beproduct.beproduct_master_brands
UNION ALL
SELECT 'teams', MAX(synced_at) FROM lft.beproduct.beproduct_master_teams
...
```

### 4. Handle New Master Data

If BeProduct adds new dropdown fields:
1. Contact BeProduct for the API endpoint
2. Add to `MASTER_DATA_ENDPOINTS` dict in notebook
3. Re-run job to fetch new data

---

## API Reference

### Master Data Endpoints

Based on BeProduct API Swagger documentation: https://developers.beproduct.com/swagger/v1/swagger.json

The job calls these BeProduct API endpoints:

**Pattern:** `GET /api/{company}/MasterData/{fieldId}`

| Field | Field ID | Table | Notes |
|-------|----------|-------|-------|
| BRANDS | `brands_multi` | `beproduct_master_brands` | MultiSelect field |
| TEAM | `team` | `beproduct_master_teams` | DropDown field |
| SEASON | `season` | `beproduct_master_seasons` | DropDown field |
| YEAR | `year` | `beproduct_master_years` | DropDown field |
| PRODUCT STATUS | `style_status` | `beproduct_master_product_status` | DropDown field |
| PRODUCT CATEGORY | `product_category` | `beproduct_master_product_category` | DropDown field |
| PRODUCT SUB CATEGORY | `product_sub_category` | `beproduct_master_product_sub_category` | DropDown field |
| DIVISION | `division` | `beproduct_master_division` | DropDown field |
| TECHPACK STAGE | `techpack_stage` | `beproduct_master_techpack_stage` | DropDown field |
| GARMENT FINISH | `garment_finish` | `beproduct_master_garment_finish` | Text field |
| PARENT VENDOR | `parent_vendor` | `beproduct_master_parent_vendor` | PartnerDropDown field |
| FACTORY | `factory` | `beproduct_master_factory` | PartnerDropDown field |

**Base URL:** `https://{company_domain}.beproduct.com/api/{company_domain}/MasterData`

**Example:** To get brands, the full URL is:
```
GET https://hk.beproduct.com/api/hk/MasterData/brands_multi
Authorization: Bearer {access_token}
```

**Authentication:** Bearer token (OAuth Client Credentials)

**Response Format:** Array of objects with `id`, `name`, and other fields

### Response Format

Each endpoint returns an array of objects:
```json
[
  {
    "id": "value-code",
    "name": "Display Label",
    "code": "CODE",
    ...other fields...
  }
]
```

The job normalizes this to:
- `value` = the `id` field (use this when pushing)
- `label` = the `name` field (human-readable)
- `data_json` = full response (for reference)

---

## FAQ

**Q: Do I need to run master data sync every day?**

A: Yes, if master data changes frequently. If it's stable, you can run it weekly. Recommendation: daily at 10am UTC.

**Q: Can I add more master data types?**

A: Yes! Edit the `MASTER_DATA_ENDPOINTS` dict in the notebook to add new endpoints, then re-run the job.

**Q: What if an endpoint doesn't exist?**

A: The job logs a warning and continues. That field's table won't be created. Contact BeProduct to confirm the endpoint path.

**Q: Should I keep old master data or overwrite?**

A: The job uses `mode("overwrite")`, so only the latest data is kept. This is good for staying in sync with BeProduct.

**Q: Can I use master data values when updating Databricks?**

A: Yes! Update your ETL validation to use:
```sql
WHERE brands IN (SELECT value FROM lft.beproduct.beproduct_master_brands)
```

**Q: How do I know when master data was last synced?**

A: Check the `synced_at` column:
```sql
SELECT MAX(synced_at) as last_updated
FROM lft.beproduct.beproduct_master_brands;
```

---

## Next Steps

1. ✅ Upload notebook to Databricks
2. ✅ Create job with schedule (10am UTC)
3. ✅ Run job to verify it works
4. ✅ Query master data tables to verify they're populated
5. ✅ Use master data for validation in push job (future enhancement)
