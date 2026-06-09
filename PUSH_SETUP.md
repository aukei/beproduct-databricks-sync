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
