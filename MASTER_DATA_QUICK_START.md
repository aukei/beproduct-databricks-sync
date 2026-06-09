# Master Data Sync - Quick Start

## Get Running in 2-3 Minutes

### 0. Verify Credentials First (optional but recommended)

Test your BeProduct credentials using the local app before deploying to Databricks:

```bash
# The local app (app/ui/main.py) uses the same SDK approach as the notebooks
# If it can connect to BeProduct, your credentials are valid for Databricks
streamlit run app/ui/main.py
```

If the local app fails to connect, fix credentials in `.env` before proceeding.

### 1. Upload Notebook (1 min)

Upload `databricks/beproduct_master_data_sync.py` to your workspace at:
```
/Repos/beproduct-sync/MASTERDATA/beproduct_master_data_sync
```

### 2. Create Job (1 min)

**Workflows → Jobs → Create job**

- **Name:** `BeProduct Master Data - Daily Sync`
- **Notebook:** `/Repos/beproduct-sync/MASTERDATA/beproduct_master_data_sync`
- **Parameters:**
  - `catalog = lft`
  - `schema = beproduct`
- **Schedule:** `0 0 10 * * ?` (10am UTC daily, before pull job)

### 3. Run & Test (immediately)

Click **Run now** → wait ~2 min → check results:

```sql
-- See what brand values are available
SELECT label FROM lft.beproduct.beproduct_master_brands ORDER BY label;

-- See what team values are available  
SELECT label FROM lft.beproduct.beproduct_master_teams ORDER BY label;
```

**✅ Done!** Master data is now synced and available for validation.

---

## Common Queries

### Check Valid Brands
```sql
SELECT label FROM lft.beproduct.beproduct_master_brands ORDER BY label;
```

### Check Valid Teams
```sql
SELECT label FROM lft.beproduct.beproduct_master_teams ORDER BY label;
```

### Check Valid Product Status
```sql
SELECT label FROM lft.beproduct.beproduct_master_product_status ORDER BY label;
```

### Find a Specific Value
```sql
SELECT * FROM lft.beproduct.beproduct_master_brands
WHERE label LIKE '%Nike%';
```

### Validate Brands Before Push
```sql
-- Check if your styles use valid brand values
SELECT DISTINCT brands 
FROM lft.beproduct.ktb_styles
WHERE brands NOT IN (SELECT value FROM lft.beproduct.beproduct_master_brands)
  AND brands IS NOT NULL;
-- Empty result = all brands are valid ✅
-- Non-empty result = found invalid brands ❌
```

---

## What Gets Created

After running, these tables are created:
- `beproduct_master_brands`
- `beproduct_master_teams`
- `beproduct_master_seasons`
- `beproduct_master_years`
- `beproduct_master_product_status`
- `beproduct_master_product_category`
- `beproduct_master_product_sub_category`
- `beproduct_master_division`
- `beproduct_master_techpack_stage`
- `beproduct_master_garment_finish`
- `beproduct_master_parent_vendor`
- `beproduct_master_factory`

Each table has 4 columns:
- `value` - Internal code (use this when pushing to BeProduct)
- `label` - Human-readable name
- `data_json` - Full API response
- `synced_at` - When fetched

---

## Why This Matters

**Problem:** You push a dropdown value that doesn't exist in BeProduct Master Data:
- ✅ API returns success (HTTP 200)
- ❌ Field is set to blank (silent failure)
- 😞 You don't know something went wrong

**Solution:** Validate against master data before pushing:
```sql
-- Only push valid brand values
UPDATE lft.beproduct.ktb_styles
SET brands = 'Nike'  -- Ensure this exists in beproduct_master_brands!
WHERE id = 'xxx';
```

---

## Troubleshooting

**Job fails?** Check the logs for API errors. Common issues:
1. Secrets not configured (same as pull job)
2. Company domain is wrong
3. API endpoint path changed

**Tables are empty?** 
1. Run job again (may be first-time API latency)
2. Check if all master data endpoints are available in your BeProduct instance
3. Some fields may not have master data

**Need help?** See [MASTER_DATA_SETUP.md](MASTER_DATA_SETUP.md) for full documentation.
