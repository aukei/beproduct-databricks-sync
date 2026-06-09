# Quick Start Guide

Get up and running with BeProduct and DTC data synchronization in minutes.

## Prerequisites

- Databricks workspace with Unity Catalog
- BeProduct API credentials (OAuth)
- DTC API key (UAT or Production)

---

## 🚀 Quick Setup (5 Minutes)

### 1. Configure Databricks Secrets (2 min)

```bash
# Create secret scope
databricks secrets create-scope --scope beproduct

# BeProduct credentials
databricks secrets put --scope beproduct --key client_id
databricks secrets put --scope beproduct --key client_secret
databricks secrets put --scope beproduct --key refresh_token
databricks secrets put --scope beproduct --key company_domain

# DTC credentials
databricks secrets put --scope beproduct --key dtc_api_key_uat
databricks secrets put --scope beproduct --key dtc_api_key_prod
```

### 2. Upload Notebooks (2 min)

```bash
# Install Databricks SDK
pip install databricks-sdk

# Configure .env
export DATABRICKS_HOST="https://adb-XXXXXXXX.azuredatabricks.net"
export DATABRICKS_PAT="dapi..."

# Upload all notebooks
python scripts/upload_notebooks.py
```

### 3. Run First Sync (1 min)

**BeProduct STYLE Sync:**
```
Notebook: /Workspace/Repos/beproduct-sync/beproduct/beproduct_style_sync
Parameters:
  - folder_name: KTB
  - refresh_mode: FULL
```

**DTC Sync:**
```
Notebook: /Workspace/Repos/beproduct-sync/DTC/notebooks/pull_dtc_to_delta
Parameters:
  - dtc_request_id: <your-request-id>
  - dtc_environment: uat
```

---

## 📊 Common Use Cases

### Use Case 1: Sync BeProduct STYLE Data

**Purpose:** Pull STYLE master data for reporting and analysis

**Notebook:** `beproduct/beproduct_style_sync.py`

**Schedule:** Daily at 11:00 AM UTC (7:00 PM HKT)

**Parameters:**
```python
folder_name = "KTB"              # BeProduct folder
refresh_mode = "INCREMENTAL"     # FULL for first run
catalog = "lft"
schema = "beproduct"
table_name = "ktb_styles"
```

**Verify:**
```sql
SELECT COUNT(*) FROM lft.beproduct.ktb_styles;
SELECT * FROM lft.beproduct.ktb_styles LIMIT 10;
```

---

### Use Case 2: Sync BeProduct Master Data

**Purpose:** Pull reference data (Materials, Colors, Blocks)

**Notebook:** `beproduct/beproduct_master_data_sync.py`

**Schedule:** Weekly on Sunday at 2:00 AM UTC

**Parameters:**
```python
refresh_mode = "FULL"            # Always use FULL for master data
catalog = "lft"
schema = "beproduct"
```

**Output Tables:**
- `lft.beproduct.materials`
- `lft.beproduct.colors`
- `lft.beproduct.blocks`
- `lft.beproduct.directory`
- `lft.beproduct.users`

---

### Use Case 3: Pull DTC Request Data

**Purpose:** Monitor DTC WIP Request data

**Notebook:** `dtc/notebooks/pull_dtc_to_delta.py`

**Schedule:** Daily at 2:00 AM UTC

**Parameters:**
```python
dtc_request_id = "req_abc123"    # DTC request ID
dtc_environment = "uat"          # or "prod"
dtc_customer = "KTB"
```

**Output Table:**
- `lft.beproduct.dtc_master_chart_uat` (or `_prod`)

---

### Use Case 4: Push BeProduct Data to DTC

**Purpose:** Automatically populate DTC WIP Requests with BeProduct data

**Notebooks:**
1. Initialize season mapping (one-time)
2. Transform BeProduct → DTC format
3. Create DTC requests/sheets
4. Push to DTC via PATCH API

**Schedule:** Daily at 12:00-1:00 PM UTC (after BeProduct sync)

**See:** `docs/BEPRODUCT_TO_DTC_GUIDE.md` for complete workflow

---

## 🔍 Common Queries

### Check Last Sync Time

```sql
-- BeProduct STYLE sync metadata
SELECT * FROM lft.beproduct.ktb_styles_sync_meta;

-- Check last modified timestamp
SELECT MAX(last_modified) as latest_update,
       MAX(extracted) as last_sync
FROM lft.beproduct.ktb_styles;
```

### View Today's Changes

```sql
SELECT lf_style_number, description, product_status, last_modified
FROM lft.beproduct.ktb_styles
WHERE DATE(last_modified) = CURRENT_DATE()
ORDER BY last_modified DESC;
```

### Count by Product Status

```sql
SELECT product_status, COUNT(*) as count
FROM lft.beproduct.ktb_styles
GROUP BY product_status
ORDER BY count DESC;
```

### Check Colorways and BOM

```sql
SELECT 
    lf_style_number,
    colorways_array,
    colorways_count,
    bom_material_1,
    bom_material_2
FROM lft.beproduct.ktb_styles
WHERE colorways_count > 0
LIMIT 10;
```

---

## 🛠️ Maintenance Tasks

### Force Full Refresh

```bash
# Via Databricks CLI
databricks jobs run-now --job-id <JOB_ID> \
  --notebook-params '{"refresh_mode":"FULL"}'
```

### Update Secrets

```bash
# Update refresh token
databricks secrets put --scope beproduct --key refresh_token

# Update DTC API key
databricks secrets put --scope beproduct --key dtc_api_key_uat
```

### Check Job Status

```bash
# List recent runs
databricks jobs list-runs --job-id <JOB_ID> --limit 5

# Get run details
databricks jobs get-run --run-id <RUN_ID>
```

---

## 🔧 Troubleshooting

| Issue | Solution |
|-------|----------|
| `Failed to retrieve credentials` | Check secrets exist: `databricks secrets list --scope beproduct` |
| `401 Unauthorized` | Refresh token expired, update via secrets |
| `Table not found` | Verify catalog/schema names in parameters |
| `No data returned` | Check folder name spelling (case-sensitive) |
| `Schema mismatch` | Drop table and re-run with FULL refresh |
| `API rate limit` | Reduce sync frequency or use INCREMENTAL mode |

---

## 📚 Next Steps

### Production Setup
1. ✅ Configure all secrets
2. ✅ Upload notebooks
3. ✅ Test each notebook individually
4. ✅ Create scheduled jobs with dependencies
5. ✅ Set up monitoring and alerts

### Learn More
- **BeProduct Sync:** See `docs/BEPRODUCT_GUIDE.md`
- **DTC Sync:** See `docs/DTC_GUIDE.md`
- **Cross-Platform:** See `docs/BEPRODUCT_TO_DTC_GUIDE.md`
- **Architecture:** See `docs/ARCHITECTURE.md`

---

## 🎯 Common Workflows

### Daily Production Workflow

```
11:00 UTC → BeProduct STYLE Sync (enhanced with colorways/BOM)
              ↓
12:00 UTC → Transform (denormalize for DTC)
              ↓
12:30 UTC → DTC Request Manager (auto-create requests)
              ↓
13:00 UTC → Push to DTC (via PATCH API)
```

### Weekly Maintenance

```
Sunday 02:00 UTC → BeProduct Master Data Sync
                    (Materials, Colors, Blocks, Directory)
```

### On-Demand Tasks

- Full refresh: Set `refresh_mode=FULL`
- Test new integration: Use `dry_run=true`
- Investigate issues: Check sync metadata tables

---

For detailed setup instructions, see the appropriate guide in `docs/`.
