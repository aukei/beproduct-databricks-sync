# DTC Master Chart Sync Hub

**Status**: ✅ Phase 1 Complete (Pull), 🚀 Phase 2 Complete (Change Tracking)  
**Target Table**: `lft.beproduct.dtc_master_chart_uat`  
**Source**: DTC API (KTB workspace)  
**Schedule**: Daily (configurable)

---

## Overview

This module provides a two-way sync solution for DTC (Data Collaboration Application) requests to Databricks Delta Lake.

**DTC Data Model**:
- **Document**: Schema definition (column structure, field types)
- **Request**: Instance of a Document (contains actual data rows)
- **View**: Column projection defined on a Document (auto-applies to all Requests)
- **Sheet**: Actual data storage for Request (accessed via views)

**Current Scope**:
- ✅ **Phase 1 (Pull)**: DTC → Databricks (read-only)
  - Pull any Request by ID (parameterized)
  - Any environment (uat/prod) via parameter
  - **REQUIRED**: Must use "WIP_ITS_USE" view (all columns, all rows)
    - Other views may hide columns/rows, compromising data integrity
    - Sync fails if "WIP_ITS_USE" not available (prevents partial pulls)
  - Document metadata stored as Delta table properties
- ✅ **Phase 2 (Change Tracking)**: Row-level delta detection + push
  - Snapshot-based change detection (SHA256 hash of data)
  - Change audit trail (INSERT/UPDATE/DELETE tracking)
  - Push infrastructure (PATCH/POST/DELETE to DTC)
  - Environment-aware table naming (uat/prod separate)
- ⏳ **Phase 3**: Conflict resolution & approval workflows

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DTC MASTER CHART SYNC                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  DTC API (UAT)                                                  │
│  ├─ Request: KTB FW26 Wrangler                                 │
│  │  └─ 14 views (WIP_ITS_USE, Vendor 1-3, etc.)             │
│  │     └─ 247 rows × 114 columns                              │
│  │                                                              │
│  ↓ (DTCConnector)                                              │
│                                                                 │
│  Databricks Workspace                                           │
│  └─ Table: lft.beproduct.dtc_master_chart_uat                 │
│     ├─ 247 rows                                                │
│     ├─ 114 columns + metadata (sync_timestamp, sync_date)     │
│     └─ Updated daily (configurable)                            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Prerequisites & Requirements

### DTC Configuration

**CRITICAL**: Each DTC Request must have a **"WIP_ITS_USE" view** configured.

- **WIP_ITS_USE view** must contain:
  - ✅ ALL columns (114 data columns, no filtering)
  - ✅ ALL rows (complete dataset, no filtering)
  - ✅ Unfiltered, complete representation of the data
  
- **Other views** (e.g., "Vendor 1", "Internal Use") are allowed but:
  - ❌ May hide columns
  - ❌ May filter rows
  - ❌ Will NOT be used by sync (ignored)

**Sync Behavior**:
- Always pulls from "WIP_ITS_USE" view
- **FAILS** with clear error if "WIP_ITS_USE" not found
- Prevents accidental partial data pulls from other views
- Ensures data integrity and completeness

**Setup**: Work with DTC admin to ensure all synced requests have "WIP_ITS_USE" view configured.

---

## Quick Start

### 1. Local Testing (Already Done ✅)

```bash
python3 test_dtc_connector.py
```

Output:
```
✅ DTCConnector created
✅ Request loaded: KTB FW26 Wrangler
✅ Got 14 views
✅ DataFrame created: 247 rows, 114 columns
✅ ALL TESTS PASSED
```

### 2. Understand the Data Model

**Important**: Read `DATA_MODEL.md` first to understand:
- How DTC organizes data (workspace → document → requests)
- Request naming pattern: `<customer> <seasonCode> <brand>`
- How DTC maps to BeProduct via composite key: `(Brand, SeasonCode, LFStyle#)`
- Customer mapping: DTC "KTB" ↔ BeProduct "KTB" (configurable)
- SeasonCode mapping: DTC "SS28" ↔ BeProduct (Spring, 2028)

### 3. Deploy to Databricks

**Status**: ✅ Deployed to `/Workspace/Repos/beproduct-sync/DTC/`

#### Step 1: Initialize SeasonCode Mapping Table

Create and populate the mapping table (one-time setup):

```bash
databricks runs submit \
  --notebook-task notebook_path=/Workspace/Repos/beproduct-sync/DTC/notebooks/00_init_season_mapping \
  --existing-cluster-id <CLUSTER_ID>
```

**Important**: Edit the notebook to insert actual seasonCode mappings for your environment.

#### Step 2: Verify secrets are configured

The DTC API key has been added to the existing `beproduct` secrets scope:

```bash
# Verify
databricks secrets list-secrets beproduct | grep dtc
# Output: dtc_api_key_uat
```

#### Step 3: Run the pull notebook

The code is deployed at `/Workspace/Repos/beproduct-sync/DTC/notebooks/pull_dtc_to_delta`

```bash
# Via CLI with customer mapping parameters:
databricks runs submit \
  --notebook-task notebook_path=/Workspace/Repos/beproduct-sync/DTC/notebooks/pull_dtc_to_delta \
  --base-parameters '{
    "dtc_workspace_name": "KTB",
    "dtc_request_id": "69f076f0b7247a661226be9a",
    "dtc_environment": "uat",
    "dtc_customer": "KTB",
    "beproduct_customer": "KTB",
    "target_catalog": "lft",
    "target_schema": "beproduct",
    "target_table": "dtc_master_chart_uat",
    "write_mode": "overwrite"
  }' \
  --existing-cluster-id <CLUSTER_ID>

# Monitor
databricks runs get-output --run-id <RUN_ID>
```

**Parameters explained**:
- `dtc_customer`: Customer code in DTC (e.g., "KTB")
- `beproduct_customer`: Corresponding customer in BeProduct (e.g., "KTB")
- Other parameters match the data model structure

#### Step 4: Create a scheduled job

```bash
# Create job configuration
cat > dtc_job_config.json << 'EOF'
{
  "name": "dtc_master_chart_daily_sync",
  "new_cluster": {
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "num_workers": 2,
    "aws_attributes": {
      "availability": "SPOT"
    }
  },
  "notebook_task": {
    "notebook_path": "/Workspace/Repos/beproduct-sync/DTC/notebooks/pull_dtc_to_delta",
    "base_parameters": {
      "dtc_workspace_name": "KTB",
      "dtc_request_id": "69f076f0b7247a661226be9a",
      "dtc_environment": "uat",
      "dtc_customer": "KTB",
      "beproduct_customer": "KTB",
      "target_catalog": "lft",
      "target_schema": "beproduct",
      "target_table": "dtc_master_chart_uat",
      "write_mode": "overwrite"
    }
  },
  "schedule": {
    "quartz_cron_expression": "0 2 * * *",
    "timezone_id": "UTC"
  },
  "timeout_seconds": 3600,
  "max_concurrent_runs": 1
}
EOF

# Create the job
databricks jobs create --json-file dtc_job_config.json

# View jobs
databricks jobs list | grep dtc
```

#### Step 4: Monitor the job

```bash
# List recent runs
databricks jobs list-runs --job-id <JOB_ID> --limit 10

# Get run details
databricks runs get --run-id <RUN_ID>

# View logs
databricks runs get-output --run-id <RUN_ID>
```

---

## File Structure

```
databricks/dtc/
├── python/
│   ├── client/
│   │   ├── __init__.py
│   │   └── rest_client.py           ← Generic HTTP client with auth + retry
│   │
│   ├── connectors/
│   │   ├── __init__.py
│   │   └── dtc.py                   ← DTC-specific pull logic
│
├── notebooks/
│   └── pull_dtc_to_delta.py         ← Main Databricks notebook
│
├── tests/
│   └── test_dtc_connector.py         ← Local test script (already validated ✅)
│
└── README.md (this file)
```

---

## Notebook Parameters

The `pull_dtc_to_delta.py` notebook accepts these parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `dtc_workspace_name` | `KTB` | DTC workspace to access (parameterized, not hardcoded) |
| `dtc_request_id` | `69f076f0b7247a661226be9a` | DTC request ID to sync (parameterized) |
| `dtc_environment` | `uat` | DTC environment: `uat` (UAT) or `prod` (Production) |
| `target_catalog` | `lft` | Databricks catalog |
| `target_schema` | `beproduct` | Databricks schema |
| `target_table` | `dtc_master_chart_uat` | Target Delta table name |
| `write_mode` | `overwrite` | Write mode: `overwrite` (replace), `append` (add rows), or `merge` (upsert) |

**Dynamic Parameter Support**:
- Change `dtc_request_id` to sync a different Request
- Change `dtc_environment` to sync from UAT or Production
- Change `target_table` to write to a different table

**Example - Sync Different Request**:
```python
# Interactive: Widget defaults to 69f076f0b7247a661226be9a (KTB FW26 Wrangler)
# To change, update the widget value before running

# Or via CLI for scheduled job:
databricks jobs create --json-file - << 'EOF'
{
  "notebook_task": {
    "base_parameters": {
      "dtc_request_id": "DIFFERENT_REQUEST_ID",
      "dtc_environment": "prod",
      "target_table": "dtc_other_request_prod"
    }
  }
}
EOF
```

---

## Document Metadata Storage

When syncing a Request, the notebook automatically captures and stores **Document metadata** as Delta table properties:

```sql
-- View Document metadata
SHOW TBLPROPERTIES lft.beproduct.dtc_master_chart_uat;

-- Output includes:
-- document_name        | KTB WIP
-- request_reference    | KTB FW26 Wrangler
-- request_description  | MASTER CHART - FW26 Supplier
-- owner_name           | Kennis Wong
-- owner_email          | kenniswong@lifung.com
-- sheet_id             | 69f076f0b7247a661226be9b
-- created_at           | 2026-04-28T08:59:28.788Z
-- updated_at           | 2026-05-28T07:49:35.444Z
```

**Benefits**:
- Document metadata is immutable (stored as table properties, not row data)
- Track which Document each Request's data came from
- Audit trail: creation and last update timestamps
- Schema lineage: know the original Document structure

---

## Data Specification

### Input (DTC Request)
- **Request ID**: `69f076f0b7247a661226be9a`
- **Request Reference**: `KTB FW26 Wrangler`
- **Views Available**: 14 (WIP_ITS_USE, Vendor 1-3, Factory Allocation, etc.)
- **Rows in WIP_ITS_USE View**: 247
- **Columns**: 114

### Output (Delta Table)
- **Location**: `lft.beproduct.dtc_master_chart_uat`
- **Rows**: 247 (from DTC)
- **Columns**: 114 (DTC fields) + metadata columns
  - `request_id`: DTC request ID (same for all rows)
  - `request_reference`: Request name (same for all rows)
  - `request_description`: Request description
  - `document_name`: Which Document this Request belongs to
  - `request_status`: Request status
  - `request_is_active`: Active flag
  - `updated_at`: When request was last updated in DTC
  - `fetched_at`: When data was pulled to Databricks
  - `sync_timestamp`: When the Databricks sync ran
  - `sync_date`: Date of Databricks sync

**Note**: Request-level metadata (workspace_name, owner_name, owner_email) are stored as Delta **table properties** (not row columns) to avoid "void" type columns.

### Data Types
- Strings: Product descriptions, styles, names, statuses
- Numbers: Prices (FOB), quantities, lead times, months
- Dates: All stored as ISO 8601 UTC strings from DTC
- Nulls: Many sparse fields (75-90% null for some columns)

### Column Name Normalization

DTC field names contain HTML display markup (e.g., `<BR/>`, `</>`) and spaces for readability in the DTC UI.
Delta Lake has strict column naming requirements (alphanumeric, underscores only).

**Automatic normalization** converts column names:

| DTC Name (Original) | Delta Name (Normalized) | Reason |
|---------------------|-------------------------|--------|
| `Product Status` | `Product_Status` | Spaces removed |
| `Proto Sample<BR/>Request Date` | `Proto_SampleRequest_Date` | HTML tags removed |
| `Final<BR/>Inspection - Due` | `FinalInspection_Due` | HTML & dashes removed |
| `FOB Price (USD/yd/) in CW` | `FOB_Price_USD_yd_in_CW` | Parentheses & slashes removed |

**Impact**: 
- ✅ Data is preserved unchanged
- ✅ Queries use normalized names (map visually to DTC display names)
- ✅ No data loss or transformation

**Reference mapping** (optional):
To query using original DTC names, create a mapping table or use column aliases in Spark SQL.

### Environment-Specific URLs

The DTCConnector automatically selects the correct API URL based on the `dtc_environment` parameter:

| Environment | API URL | Use Case |
|-------------|---------|----------|
| `uat` | `https://dtc-api.lfuat.net/api` | Development & testing (default) |
| `prod` | `https://dtc-api.lfapps.net/api` | Production data |

No code changes needed — just pass the environment parameter when initializing the connector or running the notebook.

---

## Example Queries

Once the data is synced to Databricks:

```sql
-- Count rows by product status
SELECT product_status, COUNT(*) as count
FROM lft.beproduct.dtc_master_chart_uat
GROUP BY product_status
ORDER BY count DESC;

-- Find rows with prices > $5
SELECT lf_style, style_description, 
       CAST(`fob_price_(usd/yd/)_in_cw` AS FLOAT) as fob_price
FROM lft.beproduct.dtc_master_chart_uat
WHERE CAST(`fob_price_(usd/yd/)_in_cw` AS FLOAT) > 5
ORDER BY fob_price DESC;

-- Recently synced data
SELECT COUNT(*) as total_rows, 
       MAX(sync_timestamp) as last_sync,
       MIN(sync_timestamp) as first_sync
FROM lft.beproduct.dtc_master_chart_uat;
```

---

## Troubleshooting

### Issue: `ModuleNotFoundError: No module named 'connectors.dtc'`

**Cause**: Python path not set correctly

**Solution**: Verify the deployment location:
```bash
# Check that files exist at the correct location
databricks workspace list /Workspace/Repos/beproduct-sync/DTC/python/connectors/

# Output should show rest_client.py and dtc.py
```

### Issue: `401 Unauthorized` from DTC API

**Cause**: API key invalid or expired

**Solution**:
```bash
# Verify key is set
databricks secrets list-secrets beproduct | grep dtc

# Update if needed
databricks secrets put-secret beproduct dtc_api_key_uat \
  --string-value "NEW_KEY_HERE"
```

### Issue: `Table not found: lft.beproduct.dtc_master_chart_uat`

**Cause**: Table doesn't exist yet

**Solution**: Run notebook with `write_mode=overwrite` to create:
```bash
databricks runs submit \
  --notebook-task notebook_path=/Workspace/Repos/beproduct-sync/DTC/notebooks/pull_dtc_to_delta \
  --existing-cluster-id <CLUSTER_ID>
```

### Issue: `VOID` type columns in Delta table

**Cause**: Request metadata (like `workspace_name`, `owner_email`) were being added as row columns with all null values.
Delta Lake infers these as "void" type, causing `SELECT *` errors.

**Solution**: These columns are now stored as **table properties only** (not row columns):
```sql
-- View metadata
SHOW TBLPROPERTIES lft.beproduct.dtc_master_chart_uat;

-- Workspace name (stored as property)
-- Owner name (stored as property)
-- Owner email (stored as property)
```

**Impact**: Cleaner schema with only data columns + sync metadata columns

### Issue: Timeout after 3600 seconds

**Cause**: 247 rows + 114 columns might take too long

**Solution**: Increase timeout or use a larger cluster:
```json
{
  "timeout_seconds": 7200,
  "new_cluster": {
    "num_workers": 4
  }
}
```

---

## Next Steps (Roadmap)

### Phase 1: Pull (✅ Complete)
- [x] RestClient with auth + retry
- [x] DTCConnector for pulling requests
- [x] Databricks notebook to write to Delta table
- [x] Local testing validated
- [x] Deploy to Databricks workspace (`/Workspace/Repos/beproduct-sync/DTC/`)
- [x] Secrets configured in `beproduct` scope
- [x] Column name normalization for Delta Lake
- [x] Document metadata storage as table properties
- [ ] **Next**: Schedule daily job via Databricks UI

### Phase 2: Bi-directional Sync with Change Tracking (✅ Complete)
**Status**: Implementation Complete! See `PHASE2_WORKFLOW.md` for detailed workflow.

**Strategy**: Snapshot + Change Log pattern
- Create baseline snapshot after each pull
- Detect INSERT/UPDATE/DELETE by comparing snapshots
- Log all changes with columns modified
- Push changes to DTC via PATCH/POST/DELETE
- Handle conflicts (last-write-wins + manual review)

**Completed**:
- **Phase 2a**: Change tracking infrastructure ✅
  - [x] Create `dtc_sync_metadata_{environment}` table (snapshots)
  - [x] Create `dtc_master_chart_changes_{environment}` table (change log)
  - [x] Implement snapshot + detection algorithm
  - [x] SnapshotManager class (SHA256 hash calculation)
  - [x] ChangeDetector class (INSERT/UPDATE/DELETE detection)
  
- **Phase 2b**: Push logic ✅
  - [x] Extend DTCConnector: `update_row()`, `create_row()`, `delete_row()`
  - [x] Notebook `01_create_sync_tables.py` (table initialization)
  - [x] Notebook `02_create_snapshot.py` (baseline snapshot)
  - [x] Notebook `03_detect_changes.py` (change detection)
  - [x] Notebook `04_push_changes.py` (push to DTC)
  - [x] PATCH/POST/DELETE endpoints tested
  - [x] Conflict handling framework (ready for Phase 3)
  
- **Phase 2c**: Monitoring ⏳
  - [ ] Dashboard: Sync status, error rates
  - [ ] Alerts: Conflicts, failures
  - [ ] Audit log: All pushes tracked

### Phase 3: Advanced Sync (Future)
- [ ] Real-time sync (event-driven, not daily batches)
- [ ] Field-level ACLs (some fields read-only)
- [ ] Custom transformations (calculated fields)
- [ ] Incremental updates (MERGE instead of overwrite)

### Phase 4: Multi-App Integration (Future)
- [ ] BeProduct connector
- [ ] Miro connector
- [ ] XTS connector
- [ ] N-to-N conflict resolution across all systems

---

## Support & Questions

**Data Model & Mapping**: See `DATA_MODEL.md` ⭐ **START HERE**
- How DTC organizes data (workspace → document → requests)
- Request naming pattern and parsing
- Customer mapping (DTC ↔ BeProduct)
- SeasonCode mapping (DTC codes → Season/Year)
- Composite key structure for joins
- Implementation checklist

**API Reference**: See `data_samples/DTC_API_FINDINGS.md`

**Architecture & Sync Strategy**: See `.kilo/plans/1779966530296-shiny-comet.md`

**Phase 2 Workflow** (Complete): See `PHASE2_WORKFLOW.md`
- Step-by-step: pull → snapshot → detect → push
- Environment-aware naming (uat/prod separate tables)
- Change lifecycle and status tracking
- Best practices and scheduled job examples

**Change Tracking Design** (Phase 2 Architecture): See `CHANGE_TRACKING_DESIGN.md`
- Snapshot + change log pattern
- How to detect INSERT/UPDATE/DELETE
- Conflict resolution strategy
- Implementation details

**Issues**: Check `Troubleshooting` section above

---

## Files Created

### Phase 1 (Pull) - Complete ✅

| File | Purpose | Status |
|------|---------|--------|
| `python/client/rest_client.py` | Generic HTTP client with auth + retry | ✅ Complete |
| `python/connectors/dtc.py` | DTC API connector, snapshot + metadata | ✅ Complete |
| `notebooks/pull_dtc_to_delta.py` | Main notebook: pull → normalize → write | ✅ Complete |
| `tests/test_dtc_connector.py` | Unit tests (7/7 passing) | ✅ Complete |
| `README.md` | Deployment guide, field normalization, metadata | ✅ Complete |

### Phase 2 (Bi-directional) - Complete ✅

| File | Purpose | Status |
|------|---------|--------|
| `CHANGE_TRACKING_DESIGN.md` | Snapshot + change log architecture | ✅ Design Complete |
| `PHASE2_WORKFLOW.md` | Step-by-step Phase 2 workflow guide | ✅ Complete |
| `python/sync/snapshot.py` | SnapshotManager class (SHA256 hashing) | ✅ Complete |
| `python/sync/change_detection.py` | ChangeDetector class (INSERT/UPDATE/DELETE) | ✅ Complete |
| `python/sync/__init__.py` | Sync module initialization | ✅ Complete |
| `python/connectors/dtc.py` extensions | PATCH/POST/DELETE methods | ✅ Complete |
| `notebooks/01_create_sync_tables.py` | Initialize metadata + change log tables | ✅ Complete |
| `notebooks/02_create_snapshot.py` | Create baseline snapshot after pull | ✅ Complete |
| `notebooks/03_detect_changes.py` | Detect INSERT/UPDATE/DELETE operations | ✅ Complete |
| `notebooks/04_push_changes.py` | Push changes back to DTC | ✅ Complete |

---

**Last Updated**: 2026-05-29  
**Current Status**: ✅ Phase 1 Complete (Pull) + Phase 2 Complete (Change Tracking)  
**Next Phase**: Phase 3 Conflict Resolution & Approval Workflows

# DTC Data Model & Mapping to BeProduct

**Last Updated**: 2026-05-29

---

## DTC Data Organization

### Hierarchy

```
Workspace (e.g., "KTB")
  └─ Document (e.g., "KTB WIP")
      └─ Requests (multiple, one per customer/season/brand)
          ├─ KTB SS28 Wrangler Western
          ├─ KTB SS28 Wrangler Rugged
          ├─ KTB FW27 Wrangler Western
          └─ ...
              └─ Rows (247 rows per request, sheet data)
                  ├─ LFStyle# (unique identifier)
                  ├─ Product columns (114 total)
                  └─ Metadata
```

### Views (Critical Requirement)

**IMPORTANT**: All DTC requests **must have a "WIP_ITS_USE" view** configured.

- **WIP_ITS_USE view**: Contains ALL columns and ALL rows (unfiltered, complete data)
- **Other views**: May hide specific columns or filter rows (used for specific reporting needs)

**Sync Rule**: Always pull from "WIP_ITS_USE" view to ensure data integrity and completeness.

The sync process will **FAIL** if "WIP_ITS_USE" is not available, preventing partial data pulls.

---

### Request Naming Convention

Format: `<customer> <seasonCode> <brand>`

**Examples**:
- `KTB SS28 Wrangler Western` → customer=KTB, seasonCode=SS28, brand="Wrangler Western"
- `KTB FW27 Wrangler Rugged` → customer=KTB, seasonCode=FW27, brand="Wrangler Rugged"
- `KTB SS28 Lee Regular` → customer=KTB, seasonCode=SS28, brand="Lee Regular"

**Parsing**:
1. Split by space
2. First token = customer (e.g., "KTB")
3. Second token = seasonCode (e.g., "SS28")
4. Rest = brand (e.g., "Wrangler Western")

---

## Row Identity in DTC

Each row is uniquely identified by: **`(Brand, SeasonCode, LFStyle#)`**

- **Brand**: Extracted from request name (e.g., "Wrangler Western")
- **SeasonCode**: Extracted from request name (e.g., "SS28")
- **LFStyle#**: Column in DTC data (already in the 114 columns pulled)

**Note**: DTC internal `rowId` (UUID) is used for API operations (PATCH/DELETE), but the composite key is used for data reconciliation with BeProduct.

---

## Mapping to BeProduct

### Customer Mapping

BeProduct and DTC use **different customer codes** for the same entity:

| BeProduct Customer | DTC Customer |
|--------------------|--------------|
| KTB | KTB |
| (other examples) | (to be provided) |

**Strategy**: Pass as notebook parameters
- `beproduct_customer`: Customer code in BeProduct tables
- `dtc_customer`: Customer code in DTC requests

This allows single notebook to work for any customer mapping.

### Composite Key for Joins

To join DTC with BeProduct:

```sql
-- DTC data
SELECT * FROM lft.beproduct.dtc_master_chart_uat
WHERE brand = 'Wrangler Western'
  AND season_code = 'SS28'
  AND lf_style_number = 'ABC123'

-- Join with BeProduct (example)
JOIN lft.beproduct.products bp ON
  bp.brand = dtc.brand
  AND bp.season = dtc.season_beproduct  -- mapped from season_code
  AND bp.year = dtc.year_beproduct      -- mapped from season_code
  AND bp.lf_style_number = dtc.lf_style_number
  AND bp.customer = @beproduct_customer
```

---

## SeasonCode Mapping

DTC and BeProduct identify a season differently and must be reconciled:

- **DTC** uses 2 values: `(Customer, SeasonCode)` — e.g. `(KTB, SS28)`, `(KTB, FW26)`
- **BeProduct** uses 3 values: `(Customer, Season, Year)` — e.g. `(KTB, Spring, 2028)`, `(KTB, Fall, 2026)`

A DTC `SeasonCode` is a **prefix + year**: `SS28` = prefix `SS` + year `28`.
Only the **prefix** is stored in the lookup table; the **year** part comes from
the BeProduct `year` (last 2 digits).

```
DTC SeasonCode = DTCCODE + last 2 digits (YY) of the BeProduct Year
  SPRING + 2028  ->  "SS28"      FALL + 2027  ->  "FW27"
```

### Mapping Table Structure

The real table is `lft.beproduct.dtc_seasoncode_mapping` (note: **no** underscore
between `season` and `code`). Created by `dtc/notebooks/00_init_season_mapping.py`:

```sql
CREATE TABLE IF NOT EXISTS lft.beproduct.dtc_seasoncode_mapping (
  CUSTOMER STRING NOT NULL,  -- BeProduct customer code, e.g. "KTB"
  SEASON   STRING NOT NULL,  -- BeProduct season name,   e.g. "SPRING", "FALL"
  DTCCODE  STRING NOT NULL   -- DTC season code prefix,  e.g. "SS", "FW"
)
USING DELTA
```

### Example Mappings (prefix only — no year)

| CUSTOMER | SEASON | DTCCODE | Example derivation |
|----------|--------|---------|--------------------|
| KTB | SPRING | SS | `SPRING` + `2028` -> `SS28` |
| KTB | FALL | FW | `FALL` + `2027` -> `FW27` |

**Notes**:
- The **prefix** (SS/FW/...) is **not algorithmic** and may differ between
  customers, so it **must** come from this lookup table.
- The **year** part **is** algorithmic: last 2 digits of the BeProduct year.
- Join is case-insensitive on `CUSTOMER` / `SEASON`; the styles `year` field is a
  STRING and may be `"N/A"` (such rows stay unmapped).
- Forward (BeProduct -> DTC): `beproduct/beproduct_to_dtc_transform.py`.
  Reverse (DTC -> BeProduct): `dtc/notebooks/pull_dtc_to_delta.py`. Same table.

---

## DTC Data Table Structure

### Current: `lft.beproduct.dtc_master_chart_uat`

After implementing this clarification, table will include:

**Extraction Columns** (from request name):
- `dtc_customer`: Customer code from DTC (e.g., "KTB")
- `season_code`: Season code from request name (e.g., "SS28")
- `brand`: Brand from request name (e.g., "Wrangler Western")

**Mapping Columns** (joined from mapping table):
- `beproduct_season`: Mapped season (e.g., "Spring")
- `beproduct_year`: Mapped year (e.g., 2028)

**Original DTC Columns**:
- `lf_style_number`: Unique identifier for product style (from column in DTC)
- [110 other product columns from DTC]

**Metadata Columns**:
- `row_id`: DTC internal row UUID (for API operations)
- `request_id`: DTC request ID
- `request_reference`: Request name (for reference)
- `document_name`: Document name
- `request_status`: Status
- `request_is_active`: Active flag
- `updated_at`: Last update time in DTC
- `fetched_at`: When pulled from DTC API
- `sync_timestamp`: When written to Databricks
- `sync_date`: Date of sync

### Primary Keys

**For DTC Operations**:
- `row_id` — Used for PATCH/DELETE in push

**For BeProduct Joins**:
- Composite: `(dtc_customer, brand, season_code, lf_style_number)`
- Maps to BeProduct: `(customer, brand, season, year, lf_style_number)`

### Table Properties

Store non-varying metadata as table properties:
```sql
SHOW TBLPROPERTIES lft.beproduct.dtc_master_chart_uat;

-- Properties:
-- workspace_name | KTB
-- document_name | KTB WIP
-- dtc_customer | KTB
-- owner_name | ...
-- owner_email | ...
```

---

## Notebook Parameters

All extraction parameters should be parameterized:

| Parameter | Example | Purpose |
|-----------|---------|---------|
| `dtc_workspace_name` | `KTB` | DTC workspace to access |
| `dtc_request_id` | `69f076f0b7247a661226be9a` | Which request to pull |
| `dtc_environment` | `uat` | Environment (uat/prod) |
| `dtc_customer` | `KTB` | Customer code in DTC |
| `beproduct_customer` | `KTB` | Customer code in BeProduct |
| `target_catalog` | `lft` | Databricks catalog |
| `target_schema` | `beproduct` | Databricks schema |
| `target_table` | `dtc_master_chart_uat` | Target table name |

---

## Change Detection & Push

### Composite Key for Change Tracking

Changes are tracked by composite key: `(dtc_customer, brand, season_code, lf_style_number)`

When detecting changes:
1. Group rows by composite key
2. Compare current vs snapshot using all columns for that key
3. Log INSERT/UPDATE/DELETE by key

When pushing:
1. Use DTC `row_id` for PATCH/DELETE operations
2. Include composite key columns in INSERT payload

### Example Change Log Entry

```json
{
  "change_id": "uuid-123",
  "request_id": "69f076f0b7247a661226be9a",
  "row_id": "e25849e3-f160-4617-b123-9d7c810599cf",
  "composite_key": {
    "dtc_customer": "KTB",
    "brand": "Wrangler Western",
    "season_code": "SS28",
    "lf_style_number": "WW001"
  },
  "operation": "UPDATE",
  "columns_changed": {
    "FOB_Price_USD": {
      "old_value": "3.07",
      "new_value": "2.99"
    }
  },
  "status": "pending"
}
```

---

## Implementation Checklist

- [ ] Update DTCConnector to extract (dtc_customer, season_code, brand) from request name
- [ ] Update pull notebook to pass customer mapping parameters
- [ ] Create seasonCode mapping table: `dtc_seasoncode_mapping`
- [ ] Update pull notebook to join and populate (beproduct_season, beproduct_year)
- [ ] Update change detection to use composite key
- [ ] Update change log schema to include composite_key field
- [ ] Update push to use composite key for validation
- [ ] Document mapping table initialization
- [ ] Add sample mappings for KTB → KTB

---

## Reference

- **Request Name Parsing**: Implemented in `DTCConnector.get_document_metadata()` extension
- **SeasonCode Mapping**: Query `dtc_seasoncode_mapping` table
- **Customer Mapping**: Passed as notebook parameters

