# Quick Start - BeProduct STYLE Sync Job

## TL;DR - Get Running in 5 Minutes

### 1. Set Up Secrets (1 min)

In Databricks workspace → User Settings → Secret Keys:

Create scope: `beproduct`

Add keys:
```
client_id = <from-beproduct>
client_secret = <from-beproduct>
refresh_token = <from-beproduct>
company_domain = <your-domain>
```

### 2. Upload Notebook (1 min)

1. Go to **Workspace** → **Repos** (or create `/Repos/beproduct-data-browser/databricks`)
2. Upload `beproduct_style_sync.py`
3. Note the path: `/Repos/beproduct-data-browser/databricks/beproduct_style_sync`

### 3. Create Job (2 min)

Go to **Workflows** → **Jobs** → **Create job**

Fill in:
- **Name:** `BeProduct STYLE Master - KTB Daily Sync`
- **Notebook path:** `/Repos/beproduct-data-browser/databricks/beproduct_style_sync`
- **Parameters:**
  - `folder_name = KTB` (or another folder if needed)
  - `refresh_mode = INCREMENTAL`
  - `catalog = main`
  - `schema = beproduct`
  - `table_name = ktb_styles`
- **Schedule:** Daily at 11:00 UTC (= 7pm HKT)
  - Cron: `0 0 11 * * ?`

### 4. Test (1 min)

Click **Run now** → wait for completion → check results:

```sql
SELECT COUNT(*) FROM main.beproduct.ktb_styles;
```

**Note:** First run will automatically perform a **FULL refresh** (metadata table is created on first run).
After first run, subsequent runs will use **INCREMENTAL mode** by default.

---

## Common Tasks

### Trigger Manual Full Refresh

```bash
# Via Databricks CLI
databricks jobs run-now --job-id <JOB_ID> \
  --notebook-params '{"refresh_mode":"FULL"}'

# Or via UI: Click "Run now" → set refresh_mode=FULL
```

### Check Last Sync Time

Each table has its own metadata table:

```sql
-- KTB styles
SELECT * FROM main.beproduct.ktb_styles_sync_meta;

-- WMT styles (or any other table)
SELECT * FROM main.beproduct.wmt_styles_sync_meta;

-- Pattern: {table_name}_sync_meta
```

### View Styles from Today

```sql
SELECT
    lf_style_number,
    description,
    product_status,
    modified_at
FROM main.beproduct.ktb_styles
WHERE DATE(modified_at) = CURRENT_DATE()
ORDER BY modified_at DESC;
```

### Count by Status

```sql
SELECT product_status, COUNT(*) as count
FROM main.beproduct.ktb_styles
GROUP BY product_status
ORDER BY count DESC;
```

### Access Full JSON Data

```sql
-- Get vendor info (stored in JSON)
SELECT
    lf_style_number,
    get_json_object(data_json, '$.attributes."Parent Vendor"') as parent_vendor,
    get_json_object(data_json, '$.attributes.Factory') as factory
FROM main.beproduct.ktb_styles
LIMIT 5;
```

### Update Secrets

```bash
databricks secrets put-secret beproduct refresh_token --string-value "new_token"
```

### Check Job Status

```bash
databricks jobs list-runs --job-id <JOB_ID> --limit 5
```

---

## Refresh Modes

| Mode | When to Use | Speed | Cost |
|------|------------|-------|------|
| **INCREMENTAL** (default) | Daily scheduled runs | ⚡⚡ Fast | 💰 Low |
| **FULL** | First run, data validation, after issues | ⏱ Slower | 💸 Higher |

---

## Field Mapping

**Compulsory** (always present):
- `lf_style_number` ← LF Style Number
- `description` ← Description
- `team` ← Team
- `season` ← Season
- `year` ← Year

**Interested** (frequently used):
- `product_status`, `customer_style_number`, `product_category`, `product_sub_category`
- `division`, `brands`, `garment_finish`, `techpack_stage`
- `lot_code`, `parent_vendor`, `factory`

All fields + extras → stored in `data_json` as full JSON

---

## Troubleshooting

| Error | Fix |
|-------|-----|
| `Failed to retrieve credentials` | Verify secrets in `beproduct` scope |
| `401 Unauthorized` | Refresh token expired → update via `databricks secrets put-secret` |
| `Table not found` | Check catalog/schema/table names match your parameters |
| `Timeout` | Increase job timeout or use smaller dataset |

See `SETUP.md` for detailed troubleshooting.

---

## Next Steps

- 📊 Set up BI dashboards on top of the Delta table
- 🔔 Add Databricks alerts for job failures
- 📈 Monitor job duration and API rate limits
- 🔐 Rotate credentials periodically

---

For full details, see `SETUP.md`.
