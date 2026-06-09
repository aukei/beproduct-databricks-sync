# BeProduct Databricks Sync

Enterprise data synchronization platform for syncing BeProduct and DTC data to Databricks Delta Lake with bi-directional change tracking and cross-platform integration.

## 🎯 Overview

This repository contains Databricks notebooks and Python connectors for:

- **DTC (Data Collaboration Tool)** - Pull DTC worksheets to Delta Lake with change tracking
- **BeProduct** - Bi-directional sync of STYLE master data and reference data
- **BeProduct → DTC** - ✨ **NEW:** Cross-platform sync with denormalization

### Architecture

```
┌─────────────────┐
│   DTC API       │ ──Pull──> Delta Lake ──Push──> DTC API
│   (Worksheets)  │            (Change Log)         (Updates)
└─────────────────┘

┌─────────────────┐
│  BeProduct API  │ <──Pull/Push──> Delta Lake ──Transform──> DTC API
│  (STYLE, Refs)  │    (Extended)     (Denormalize)  (PATCH)
└─────────────────┘
     1 Style                N×M Rows              Flat Rows
  + N Colors              (Staging)              (WIP Requests)
  + M Materials
```

**NEW: BeProduct → DTC Integration**
- Extracts colorways and BOM materials from BeProduct
- Denormalizes to flat DTC structure (Style × Color × BOM)
- Maps season codes and field names
- Pushes to DTC WIP Requests via PATCH API

## 📁 Repository Structure

```
beproduct-databricks-sync/
├── dtc/                              # DTC sync platform
│   ├── notebooks/
│   │   ├── pull_dtc_to_delta.py     # Main sync notebook
│   │   └── 00_init_season_mapping.py
│   ├── python/
│   │   ├── connectors/
│   │   │   └── dtc.py               # DTC API connector
│   │   └── client/
│   │       └── rest_client.py
│   ├── tests/
│   ├── README.md                     # DTC documentation
│   ├── DATA_MODEL.md
│   └── CHANGE_TRACKING_DESIGN.md
│
├── beproduct/                        # BeProduct sync platform
│   ├── beproduct_style_sync.py      # Pull STYLE records
│   ├── beproduct_style_push.py      # Push STYLE changes
│   ├── beproduct_master_data_sync.py # Pull reference data
│   │
│   │ ✨ NEW: BeProduct → DTC Integration
│   ├── beproduct_style_extended_sync.py    # Extended pull (colorways, BOM)
│   ├── beproduct_to_dtc_transform.py       # Denormalization transform
│   ├── dtc_request_manager.py              # DTC request/sheet manager
│   └── beproduct_to_dtc_push.py            # Push to DTC with change detection
│
├── scripts/
│   ├── upload_notebooks.py          # Upload notebooks to workspace
│   └── upload_to_databricks.py      # Upload data (SQLite → Delta)
│
├── .github/
│   └── workflows/                    # CI/CD pipelines
│
├── README.md                         # This file
├── QUICK_START.md
├── QUICK_REFERENCE.md
└── .env.example
```

## 🚀 Quick Start

### Prerequisites

1. **Databricks workspace** with Unity Catalog
2. **DTC API credentials** (UAT or Production)
3. **BeProduct API credentials** (OAuth tokens)

### 1. Configure Secrets

Create a Databricks secret scope:

```bash
databricks secrets create-scope --scope beproduct
```

Add credentials:

```bash
# DTC
databricks secrets put --scope beproduct --key dtc_api_key_uat
databricks secrets put --scope beproduct --key dtc_api_key_prod

# BeProduct
databricks secrets put --scope beproduct --key client_id
databricks secrets put --scope beproduct --key client_secret
databricks secrets put --scope beproduct --key refresh_token
databricks secrets put --scope beproduct --key company_domain
```

### 2. Upload Notebooks

```bash
# Clone the repo
git clone https://github.com/your-org/beproduct-databricks-sync.git
cd beproduct-databricks-sync

# Copy .env.example to .env and fill in values
cp .env.example .env

# Upload notebooks to Databricks workspace
pip install databricks-sdk
python scripts/upload_notebooks.py
```

### 3. Run Notebooks

#### DTC Sync
- **Notebook:** `/Workspace/Repos/beproduct-sync/DTC/notebooks/pull_dtc_to_delta`
- **Schedule:** Daily at 2:00 AM UTC
- **Parameters:**
  - `dtc_request_id`: DTC request to sync
  - `dtc_environment`: uat or prod
  - `beproduct_customer`: KTB, WMT, etc.

#### BeProduct STYLE Sync
- **Notebook:** `/Workspace/Repos/beproduct-sync/STYLE/beproduct_style_sync`
- **Schedule:** Daily at 7pm HKT (11am UTC)
- **Parameters:**
  - `folder_name`: KTB, WMT, WALMART
  - `refresh_mode`: FULL or INCREMENTAL

See [QUICK_START.md](QUICK_START.md) for detailed setup instructions.

## 📊 Features

### DTC Sync Platform

✅ **Pull from DTC**
- Full worksheet extraction with column normalization
- View-based data filtering (Full Version view)
- Document metadata preservation
- Change tracking for bi-directional sync

✅ **Change Tracking**
- Row-level timestamp tracking (`extracted_time`, `last_modified`)
- Change log table with full audit trail
- Identifies rows needing push back to DTC
- Brand business logic enforcement (request name as source of truth)

✅ **Data Quality**
- Automatic column normalization (HTML → Delta Lake compatible)
- Duplicate column detection and resolution
- Schema evolution with `mergeSchema`
- Validation and error handling

📋 **Tables Created:**
- `lft.beproduct.dtc_master_chart_uat` - Main data table
- `lft.beproduct.dtc_master_chart_uat_change_log` - Change audit trail
- `lft.beproduct.dtc_seasoncode_mapping` - Season code lookup

### BeProduct STYLE Sync Platform

✅ **Pull STYLE Records**
- FULL and INCREMENTAL sync modes
- Timestamp-based change detection
- 16 key fields extraction
- Full JSON storage for audit trail

✅ **Push STYLE Changes**
- Detect modified records via timestamp comparison
- Field ID mapping for BeProduct API
- Dry-run mode for safety
- Comprehensive push audit log

✅ **Master Data Sync**
- Reference data (BRANDS, TEAMS, SEASONS, etc.)
- Validation tables for field values
- Shared across all folders

📋 **Tables Created:**
- `lft.beproduct.ktb_styles` - Style data
- `lft.beproduct.ktb_styles_sync_meta` - Sync metadata
- `lft.beproduct.ktb_styles_push_log` - Push audit trail
- `lft.beproduct.beproduct_master_*` - Reference tables

### ✨ NEW: BeProduct → DTC Cross-Platform Sync

✅ **Extract Extended Data**
- Colorways array (`$.colorways[].colorName`)
- BOM materials (`core_main_material`, `Core_main_material2`)
- Material category and content
- Front image URLs

✅ **Denormalization**
- 1 Style → N Colors (explode colorways)
- Each Color → 2 Materials (BOM lines)
- Result: N×2 rows per style (flat structure)

✅ **Season Code Mapping**
- BeProduct (Season + Year) → DTC (SeasonCode)
- Example: Spring 2026 → SS26

✅ **DTC Request Management**
- Auto-create missing DTC requests/sheets
- Format: "<Customer> <SeasonCode> <Brand>"
- Example: "KTB SS26 Wrangler"

✅ **Change Detection & Push**
- Compare staging with current DTC data
- Detect INSERT/UPDATE/DELETE operations
- Timezone-aware comparison (UTC ↔ HKT)
- Push via PATCH API

📋 **Tables Created:**
- `lft.beproduct.ktb_styles_extended` - Extended style data with colorways/BOM
- `lft.beproduct.beproduct_to_dtc_staging` - Denormalized staging (N×M rows)
- `lft.beproduct.dtc_request_mapping` - Request/sheet ID mapping
- `lft.beproduct.beproduct_to_dtc_push_log` - Push audit log
- `lft.beproduct.dtc_current_snapshot_*` - DTC data snapshots

## 🔍 Change Tracking

### Query Modified Rows for DTC Push

```sql
-- Find all rows that need to be pushed back to DTC
SELECT DISTINCT row_id, lf_style, new_value as Brand, modified_at
FROM lft.beproduct.dtc_master_chart_uat_change_log
WHERE modification_type = 'brand_overwrite'
  AND sync_date >= current_date() - INTERVAL 1 DAYS
ORDER BY modified_at DESC;
```

### Query Current Snapshot

```sql
-- Find rows modified in the current snapshot
SELECT row_id, lf_style, Brand, last_modified, extracted_time
FROM lft.beproduct.dtc_master_chart_uat
WHERE Brand_modified = True;
```

### Audit History

```sql
-- View change history for a specific style
SELECT * FROM lft.beproduct.dtc_master_chart_uat_change_log
WHERE lf_style = 'STYLE123'
ORDER BY modified_at DESC;
```

## 🔄 Sync Workflows

### DTC ↔ Delta Lake (Bi-Directional)

**Pull (DTC → Delta Lake):**
1. Notebook pulls data from DTC API
2. Applies business logic (Brand from request name)
3. Tracks changes in change_log table
4. Writes to Delta Lake with timestamps

**Push (Delta Lake → DTC):**
1. Query change_log for modified rows
2. For each row_id, call DTC PATCH API:
   ```
   PATCH /v1/sheets/{sheet_id}/rows/{row_id}
   { "columnValues": { "Brand": "Wrangler" } }
   ```
3. Mark as pushed in push_log table

See [DTC CHANGE_TRACKING_DESIGN.md](dtc/CHANGE_TRACKING_DESIGN.md) for details.

### ✨ BeProduct → DTC (Cross-Platform Integration)

**NEW: End-to-end workflow for syncing BeProduct STYLE data to DTC WIP Requests.**

**Step 1: Extended Pull (11am UTC)**
```python
# beproduct_style_extended_sync.py
BeProduct API → lft.beproduct.ktb_styles_extended
- Extract header fields + colorways + BOM + materials + images
- 1 style = 1 row (colorways as array)
```

**Step 2: Denormalization (12pm UTC)**
```python
# beproduct_to_dtc_transform.py
Extended styles → lft.beproduct.beproduct_to_dtc_staging
- Explode colorways: 1 style → N rows
- Explode BOM: each color → 2 material rows
- Map season codes: Spring 2026 → SS26
- Derive DTC request name: "KTB SS26 Wrangler"
- Result: N×2 rows per style
```

**Step 3: Request Management (12:30pm UTC)**
```python
# dtc_request_manager.py
Staging → lft.beproduct.dtc_request_mapping
- Get unique request names from staging
- Search DTC for existing requests
- Create missing requests/sheets via POST /v1/sheets
- Store request_id / sheet_id mapping
```

**Step 4: Change Detection & Push (1pm UTC)**
```python
# beproduct_to_dtc_push.py
Staging + DTC current → DTC API (PATCH)
- Pull current DTC data for comparison
- Detect INSERT/UPDATE/DELETE operations
- Timezone-aware comparison (UTC ↔ HKT)
- Push via PATCH /v1/sheets/{sheetId}/views/{viewId}
- Log all operations
```

**Complete Guide:** [BEPRODUCT_TO_DTC_GUIDE.md](BEPRODUCT_TO_DTC_GUIDE.md)  
**Implementation Plan:** [.kilo/plans/beproduct-to-dtc-push-integration.md](.kilo/plans/beproduct-to-dtc-push-integration.md)

## 📖 Documentation

- [QUICK_START.md](QUICK_START.md) - Step-by-step setup
- [QUICK_REFERENCE.md](QUICK_REFERENCE.md) - All jobs and parameters
- [dtc/README.md](dtc/README.md) - DTC sync documentation
- [dtc/DATA_MODEL.md](dtc/DATA_MODEL.md) - DTC data model
- [dtc/CHANGE_TRACKING_DESIGN.md](dtc/CHANGE_TRACKING_DESIGN.md) - Change tracking architecture
- [PUSH_SETUP.md](PUSH_SETUP.md) - BeProduct push setup
- [MASTER_DATA_SETUP.md](MASTER_DATA_SETUP.md) - Master data sync

## 🛠️ Development

### Local Testing

```bash
# Set up Python environment
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# Install dependencies
pip install -r requirements.txt

# Run tests
pytest dtc/tests/
```

### Upload Notebooks

```bash
# Install Databricks SDK
pip install databricks-sdk

# Configure .env with DATABRICKS_HOST and DATABRICKS_PAT

# Preview uploads (dry run)
python scripts/upload_notebooks.py --dry-run

# Upload all notebooks
python scripts/upload_notebooks.py

# Upload specific directory
python scripts/upload_notebooks.py --dir beproduct
```

**Note:** `scripts/upload_to_databricks.py` is for uploading DATA (SQLite → Delta tables), while `scripts/upload_notebooks.py` is for uploading NOTEBOOKS to workspace.

## 🔐 Security

- All credentials stored in Databricks secrets
- OAuth 2.0 for BeProduct API
- API keys for DTC (stored in secrets)
- No credentials in code or config files

## 📊 Performance

### Typical Sync Times
- **DTC pull (247 rows × 114 cols):** < 1 second
- **BeProduct FULL sync (50 styles):** 30-60 seconds
- **BeProduct INCREMENTAL (no changes):** 10-15 seconds

### Scaling
- Use larger Databricks clusters for >1000 records
- Adjust batch sizes in push operations
- Enable auto-scaling for variable workloads

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📝 License

[Your License Here]

## 📞 Support

- **Issues:** GitHub Issues
- **Documentation:** [Wiki](wiki)
- **Contact:** [Your Team Email]

---

**Version:** 1.0.0  
**Last Updated:** 2026-06-09  
**Status:** Production Ready
