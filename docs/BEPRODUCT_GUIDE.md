# BeProduct Integration Guide

Complete guide for BeProduct data synchronization with Databricks Delta Lake.

## Overview

This guide covers:
- **STYLE Master Data Sync** - Pull STYLE records with colorways, BOM, and materials
- **Master Data Sync** - Pull reference data (Materials, Colors, Blocks, Directory)  
- **Push to BeProduct** - Bi-directional sync (Delta Lake → BeProduct API)

---

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

### Field Types on Push-Back (MultiSelect vs DropDown)

The push-back job (`beproduct_style_push.py`) shapes each value to match the
BeProduct field **type**, read from the style's `data_json` (`headerData.fields[].type`):

| BeProduct type | Example fields | Stored in Delta | Sent to BeProduct |
|----------------|----------------|-----------------|-------------------|
| **MultiSelect** | `BRANDS`, `CUSTOMER` | single string, e.g. `Wrangler` | one-element array, e.g. `["Wrangler"]` |
| **DropDown** | `PRODUCT STATUS` | single string, e.g. `Proto` | single string, e.g. `Pre-Line` |
| Text / other | `DESCRIPTION`, etc. | string | string |

**Single-value assumption:** `CUSTOMER` and `BRANDS` are multiSelect fields, but
the project team confirms each style always has exactly **one** value selected.
The push therefore always sends a single-element array for these fields. If a
value were ever comma-joined (`"A, B"`), only the first selection is sent.

> ⚠️ Sending a multiSelect value as a bare string (instead of an array) causes
> BeProduct to silently blank the field — this is why type-aware shaping matters.

### Verify Round-Trip: change in Databricks → see in BeProduct

After a Style Sync has populated `lft.beproduct.ktb_styles`:

1. **Edit a value in Databricks** and bump `modified_at` so the change is detected
   (`modified_at > synced_at`):

   ```sql
   -- Brands (MultiSelect): change the single selected brand
   UPDATE lft.beproduct.ktb_styles
   SET brands = 'Lee', modified_at = current_timestamp()
   WHERE lf_style_number = '<LF Style #>';

   -- Product Status (DropDown): e.g. Proto -> Pre-Line
   UPDATE lft.beproduct.ktb_styles
   SET product_status = 'Pre-Line', modified_at = current_timestamp()
   WHERE lf_style_number = '<LF Style #>';
   ```

2. **Dry-run the push** to confirm the payload shape (array for brands, string
   for product_status):

   - Notebook: `beproduct/beproduct_style_push.py`
   - Parameters: `source_table_name=ktb_styles`, `dry_run=true`
   - In the logged `Fields:` the BRANDS field id maps to a **list** `['Lee']`
     while the PRODUCT STATUS field id maps to a **string** `Pre-Line`.

3. **Run the push for real** (`dry_run=false`).

4. **Confirm in BeProduct** the style now shows `Brand = Lee` and
   `Product Status = Pre-Line`. (Both values must exist in the field's Master
   Data list, or BeProduct silently blanks them.)

5. **Optionally re-pull** with `beproduct_style_sync.py` to confirm the new
   values round-trip back into `ktb_styles`.

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


---


# BeProduct STYLE Data Push-Back Job - Setup & Usage

## Overview

This Databricks job syncs **changes from Delta Lake back to BeProduct**. It detects which records have been modified locally (in Databricks) and pushes those changes back to BeProduct.

### Change Detection

The job compares two timestamps:
- **`modified_at`** - When the record was last changed in Databricks (user edited a field)
- **`synced_at`** - When the record was last pulled from BeProduct

If `modified_at > synced_at`, the record was edited locally and should be pushed back.

### Update Strategy

The job updates **all extracted fields** (compulsory + interested) for changed records:

**Compulsory fields:**
- LF Style Number
- Description
- Team
- Season
- Year

**Interested fields:**
- Product Status
- Customer Style Number
- Product Category
- Product Sub Category
- Division
- Brands
- Garment Finish
- Techpack Stage
- Lot Code
- Parent Vendor
- Factory

**Note:** We update all these fields for simplicity and reliability. Trying to detect which specific field changed is complex and error-prone.

---

## Setup

### Prerequisites

1. **Source table exists** - Run the pull job first to create `ktb_styles` (or your table)
2. **BeProduct credentials in secrets** - Same setup as pull job
3. **Table has `modified_at` and `synced_at` columns** - The pull job creates these

### Create the Job

#### Option A: Using Databricks UI (Easiest)

1. **Go to** Workflows → Jobs → **Create job**
2. **Job name:** `BeProduct STYLE Push - KTB`
3. **Task:**
   - **Task name:** `beproduct_style_push`
   - **Type:** Notebook
   - **Notebook path:** `/Repos/beproduct-data-browser/databricks/beproduct_style_push`
   - **Cluster:** Same as pull job

4. **Parameters:**
   - `folder_name` = `KTB`
   - `source_table_name` = `ktb_styles`
   - `catalog` = `main`
   - `schema` = `beproduct`
   - `dry_run` = `false` (or `true` to test first)

5. **Click Create**

#### Option B: Using API

```bash
databricks jobs create --json '{
  "name": "BeProduct STYLE Push - KTB",
  "tasks": [{
    "task_key": "beproduct_style_push",
    "notebook_task": {
      "notebook_path": "/Repos/beproduct-data-browser/databricks/beproduct_style_push",
      "base_parameters": {
        "folder_name": "KTB",
        "source_table_name": "ktb_styles",
        "catalog": "main",
        "schema": "beproduct",
        "dry_run": "false"
      }
    },
    "job_cluster_config": {
      "spark_version": "14.3.x-scala2.12",
      "node_type_id": "i3.xlarge",
      "num_workers": 2
    },
    "timeout_seconds": 1800,
    "max_retries": 0
  }],
  "max_concurrent_runs": 1
}'
```

---

## Usage

### Test with Dry Run

Before pushing actual changes, always test with **dry run mode**:

1. Click **Run now** on the job
2. Override parameter: `dry_run = true`
3. Check the logs to see what **would** be pushed without actually pushing

Example output:
```
DRY RUN MODE - No actual pushes will be made
Pushing 5 records to BeProduct...
  [1/5] DRY RUN: 14524c66-0af8... would push 12 fields
  [2/5] DRY RUN: f55ae693-9e6d... would push 8 fields
```

### Push for Real

Once you're confident with the dry run:

1. Click **Run now**
2. Use default: `dry_run = false`
3. Job will push changes to BeProduct
4. `synced_at` in the Delta table is updated to prevent re-pushing

### Manual Workflow

Typical workflow:

```
1. User edits a record in Databricks/Excel/BI tool
   → modified_at = now
   → synced_at = (old timestamp from last pull)

2. User runs push job with dry_run=true
   → Reviews what will be pushed

3. User runs push job with dry_run=false
   → Changes are pushed to BeProduct
   → synced_at is updated to = now

4. Next time push job runs
   → No changes detected (modified_at == synced_at)
   → Nothing is pushed
```

---

## Parameters

### `folder_name` (Default: `KTB`)

BeProduct folder to push to. Must match the source data's folder.

### `source_table_name` (Default: `ktb_styles`)

Source Delta table containing changes to push.

Must have columns:
- `id` - Record ID
- `modified_at` - Last modification timestamp
- `synced_at` - Last sync timestamp
- `lf_style_number`, `description`, etc. - The extracted fields

### `catalog` (Default: `main`)

Databricks catalog where the source table is located.

### `schema` (Default: `beproduct`)

Databricks schema where the source table is located.

### `dry_run` (Default: `false`)

- `true` - Preview what will be pushed without actually pushing
- `false` - Actually push changes to BeProduct

**Always test with `dry_run=true` first!**

---

## How It Works

### Step 1: Detect Changes

Queries the source table for records where:
```sql
WHERE modified_at > synced_at
  AND modified_at IS NOT NULL
  AND synced_at IS NOT NULL
```

### Step 2: Build Payloads

For each changed record, builds a BeProduct API payload with:
- `header_id` - The style ID
- `fields` - Dict of field_name → value for all extracted fields

### Step 3: Push to BeProduct

Calls the BeProduct SDK:
```python
api.style.attributes_update(
    header_id=style_id,
    fields=fields
)
```

### Step 4: Update Local Timestamp

Updates `synced_at = now()` for pushed records so they won't be re-pushed.

---

## Monitoring & Troubleshooting

### Check What's Changed

```sql
SELECT
    lf_style_number,
    description,
    modified_at,
    synced_at,
    DATEDIFF(MINUTE, synced_at, modified_at) as minutes_since_sync
FROM main.beproduct.ktb_styles
WHERE modified_at > synced_at
ORDER BY modified_at DESC;
```

### View Push History

```sql
-- If you create a push history table:
SELECT * FROM main.beproduct.ktb_styles_push_log
ORDER BY pushed_at DESC
LIMIT 20;
```

### Common Issues

#### Issue: "Records were pushed but synced_at wasn't updated"

**Cause:** The table is read-only or you don't have write permissions

**Solution:**
1. Verify table permissions
2. Manually run SQL to reset:
   ```sql
   UPDATE main.beproduct.ktb_styles
   SET synced_at = CURRENT_TIMESTAMP()
   WHERE id IN ('id1', 'id2', ...)
   ```

#### Issue: Push fails with "Field not found"

**Cause:** Field name in BeProduct schema changed or is different

**Solution:**
1. Run pull job to get latest schema
2. Update field mapping in the push notebook
3. Retry

#### Issue: Dropdown field updated but value not reflected in BeProduct

**Cause:** The field is a dropdown/multiselect with predefined Master Data values. Invalid values are silently rejected.

**Affected fields:**
- BRANDS (MultiSelect)
- PRODUCT STATUS (DropDown)
- TEAM (DropDown)
- SEASON (DropDown)
- YEAR (DropDown)
- PRODUCT CATEGORY (DropDown)
- PRODUCT SUB CATEGORY (DropDown)
- TECHPACK STAGE (DropDown)
- DIVISION (DropDown)
- Others with Master Data constraints

**Solution:**
1. **For BRANDS:** Use exact brand names that exist in Master Data (e.g., "Nike", "Adidas")
2. **For other dropdowns:** Use only values that appear in the original BeProduct data
3. **Verify values:** Run:
   ```sql
   SELECT DISTINCT brands FROM main.beproduct.ktb_styles WHERE brands IS NOT NULL LIMIT 10;
   SELECT DISTINCT product_status FROM main.beproduct.ktb_styles WHERE product_status IS NOT NULL LIMIT 10;
   ```
4. **Check logs:** The push job logs warnings for dropdown fields:
   ```
   WARNING: Field team (TEAM) is DropDown type - ensure value 'INVALID_VALUE' is in valid Master Data list
   ```

**Why this happens:** BeProduct validates dropdown values against Master Data. If the value isn't in the list, it silently rejects the update.

#### Issue: Some records fail to push, others succeed

**Expected behavior** - Job continues and logs failures

**Action:**
1. Check logs for error details
2. Fix data issues in Delta table
3. Re-run push job

---

## Best Practices

1. **Always test with dry_run=true first**
   - Preview changes before pushing
   - Catch data issues early

2. **Run push job frequently**
   - After pull job (e.g., hourly)
   - Reduces chance of conflicts
   - Keeps BeProduct in sync

3. **Monitor push failures**
   - Review logs for errors
   - Fix data issues promptly
   - Don't ignore systematic failures

4. **Backup before large pushes**
   - Export table before pushing many changes
   - Easy to rollback if needed

5. **Document who changed what**
   - Add an `updated_by` column to track changes
   - Helps with auditing and troubleshooting

6. **Use a schedule**
   - Schedule push job to run periodically (e.g., every hour)
   - Keeps Databricks and BeProduct in sync automatically

---

## Workflow Example: Bulk Update

Scenario: Update Season for all 2027 Spring styles from "Spring" to "Spring S2"

### Step 1: Update in Databricks

```sql
UPDATE main.beproduct.ktb_styles
SET season = 'Spring S2',
    modified_at = CURRENT_TIMESTAMP()
WHERE season = 'Spring' AND year = '2027';
```

### Step 2: Dry Run

```
Run push job with:
  folder_name = KTB
  source_table_name = ktb_styles
  dry_run = true
```

Output:
```
Records that would be pushed: 23
Sample payload:
  - SEASON: Spring S2
```

### Step 3: Push

```
Run push job with:
  dry_run = false
```

Output:
```
Records pushed: 23
Records failed: 0
Success rate: 100%
✅ Updated synced_at for 23 records
```

### Step 4: Verify

Check in BeProduct that all 23 styles now have Season = "Spring S2"

---

## FAQ

**Q: What if I don't want to push certain fields?**

A: Edit the `COMPULSORY_FIELDS` and `INTERESTED_FIELDS` dicts in the notebook to exclude fields you don't want to push.

**Q: Can I push only specific records?**

A: Modify the SQL query in Step 2 to add filters:
```sql
WHERE modified_at > synced_at
  AND lf_style_number LIKE 'LFBP-WM1%'  -- Only specific styles
```

**Q: What happens if BeProduct rejects a field?**

A: The push fails for that record, and an error is logged. Other records continue to push. Fix the data and retry.

**Q: Can I schedule the push job to run automatically?**

A: Yes! Add a schedule to the job (e.g., hourly). It will push any changes that occurred since the last run.

**Q: How do I rollback a bad push?**

A: Either:
1. Re-update the fields in BeProduct manually, or
2. Set `modified_at < synced_at` for the affected records in Databricks, pull fresh data with the pull job

**Q: Why did my dropdown field value not get pushed?**

A: Dropdown and MultiSelect fields (BRANDS, TEAM, SEASON, etc.) require values from BeProduct's Master Data. If you set a value that's not in the list, BeProduct silently rejects it.

Solution: Only use values that exist in the original data pulled from BeProduct. Check the pull logs or query the table to see valid values:
```sql
SELECT DISTINCT brands FROM main.beproduct.ktb_styles 
WHERE brands IS NOT NULL 
ORDER BY brands;
```

**Q: How can I find all valid values for a dropdown field?**

A: After pulling fresh data, query the table:
```sql
-- Find all valid BRANDS values from pulled data
SELECT DISTINCT brands FROM main.beproduct.ktb_styles 
WHERE brands IS NOT NULL 
ORDER BY brands;

-- Find all valid TEAM values
SELECT DISTINCT team FROM main.beproduct.ktb_styles 
WHERE team IS NOT NULL 
ORDER BY team;
```

These represent the valid Master Data values for those fields.

---

**Last Updated:** 2026-05-22
