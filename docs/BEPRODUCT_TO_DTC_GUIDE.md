# BeProduct to DTC Sync - Complete Guide

**Status:** ✅ Implementation Complete  
**Version:** 1.0.0  
**Last Updated:** 2026-06-09

---

## Overview

This guide documents the BeProduct → DTC integration that syncs STYLE master data from BeProduct to DTC WIP Requests with full denormalization.

### Architecture

```
BeProduct (Normalized)              Databricks (Transform)           DTC (Denormalized)
┌────────────────────┐              ┌─────────────────────┐          ┌──────────────────┐
│ 1 Style            │              │ Extended Pull       │          │ N Flat Rows      │
│ ├─ Header Fields   │───Pull──────▶│ ├─ Colorways Array │          │                  │
│ ├─ N Colorways     │              │ └─ Images          │──────────▶│ Each row =       │
│ └─ 1 Fabric Row    │              │                     │          │ (Style × Color)  │
└────────────────────┘              │                     │          └──────────────────┘
                                    │ Denormalization     │
                                    │ ├─ Explode Colors   │
                                    │ ├─ Add Fabric Row   │
                                    │ ├─ Map Season Code  │
                                    │ └─ Map Fields       │
                                    └─────────────────────┘
```

### Data Flow

```
1. beproduct_style_sync.py               → lft.beproduct.ktb_styles
   - Pull from BeProduct with colorways, BOM, materials, images
   - 1 style = 1 row (with colorways as array)
   - Enhanced with change tracking: last_modified, extracted

2. beproduct_to_dtc_transform.py         → lft.beproduct.beproduct_to_dtc_staging
   - Explode colorways: 1 style → N rows
   - Add 1 hardcoded fabric row per (style × color)
   - Result: N rows per style

3. dtc_request_manager.py                → lft.beproduct.dtc_request_mapping
   - Ensure all DTC requests exist
   - Create missing requests/sheets

4. beproduct_to_dtc_push.py              → DTC API (PATCH)
   - Detect changes (INSERT/UPDATE/DELETE)
   - Push to DTC via PATCH API
   - Log results
```

---

## Notebooks

### 1. BeProduct STYLE Sync (Enhanced)

**File:** `beproduct/beproduct_style_sync.py`

**Purpose:** Extract BeProduct STYLE data with ALL fields for reporting and DTC integration.

**Features:**
- ✅ Standard STYLE fields (LF Style Number, Season, Year, etc.)
- ✅ Colorways array extraction (`$.colorways[].colorName`)
- ✅ BOM material fields (`core_main_material`, `Core_main_material2`)
- ✅ Material category and content
- ✅ Front image URL (`frontImage.origin`)
- ✅ Change tracking: `last_modified` (from source), `extracted` (at pull time)

**Schedule:** Daily at 11am UTC (existing job)

**Parameters:**
```python
folder_name = "KTB"              # BeProduct folder
refresh_mode = "FULL"            # or "INCREMENTAL"
catalog = "lft"
schema = "beproduct"
table_name = "ktb_styles"        # Single unified table
```

**Output Table:** `lft.beproduct.ktb_styles` (single source of truth)

**Schema:**
```sql
CREATE TABLE lft.beproduct.ktb_styles (
    id STRING,
    folder_name STRING,
    synced_at TIMESTAMP,              -- Legacy: extraction timestamp
    created_at TIMESTAMP,
    modified_at TIMESTAMP,            -- Legacy: from BeProduct modifiedAt
    
    -- Change tracking (NEW)
    last_modified TIMESTAMP,          -- From source system (modifiedAt)
    extracted TIMESTAMP,              -- When we pulled it (extraction time)
    
    -- Standard fields
    lf_style_number STRING,
    season STRING,
    year STRING,
    brands STRING,
    description STRING,
    team STRING,
    product_status STRING,
    customer_style_number STRING,
    product_category STRING,
    product_sub_category STRING,
    division STRING,
    garment_finish STRING,
    techpack_stage STRING,
    lot_code STRING,
    parent_vendor STRING,
    factory STRING,
    
    -- NEW: Extended fields
    colorways_array ARRAY<STRING>,          -- Array of color names
    colorways_count STRING,
    bom_material_1 STRING,                   -- Main Fabric material
    bom_material_2 STRING,                   -- Secondary Fabric material
    main_material_category STRING,
    main_material_content STRING,
    front_image_url STRING,
    
    data_json STRING                         -- Full JSON for audit
)
```

**Example Data:**
```
style_id: STY001
lf_style_number: LF001
season: Spring
year: 2026
brands: Wrangler
colorways_array: ["Dark Wash", "Light Wash"]
colorways_count: 2
bom_material_1: DENIM-001
bom_material_2: COTTON-002
```

---

### 2. Denormalization Transform

**File:** `beproduct/beproduct_to_dtc_transform.py`

**Purpose:** Transform normalized BeProduct data to flat denormalized structure for DTC.

**Process:**
1. **Explode Colorways:** 1 style → N rows (one per color)
2. **Add Fabric Row:** Each (style × color) → 1 hardcoded row
   (`fabric_group` = "MAIN MATERIAL CONTENT", `placement` = `main_material_content`)
3. **Map Season Code:** BeProduct (Season + Year) → DTC SeasonCode (SS26, FW27)
4. **Derive Request Name:** Build DTC request name: "<Customer> <SeasonCode> <Brand>"
5. **Map Fields:** Map BeProduct columns to DTC column names

**Schedule:** Daily at 12pm UTC (after extended pull at 11am)

**Parameters:**
```python
catalog = "lft"
schema = "beproduct"
source_table = "ktb_styles"
staging_table = "beproduct_to_dtc_staging"
folder_name = "KTB"
customer_code = "KTB"           # DTC customer code
```

**Input:** `lft.beproduct.ktb_styles` (unified table with colorways/BOM)  
**Output:** `lft.beproduct.beproduct_to_dtc_staging`

**Transformation Example:**
```
INPUT (1 style):
  lf_style_number: LF001
  colorways_array: ["Dark Wash", "Light Wash"]
  bom_material_1: DENIM-001
  bom_material_2: COTTON-002

OUTPUT (4 rows):
  Row 1: LF001 | Dark Wash  | Main Fabric | DENIM-001
  Row 2: LF001 | Dark Wash  | Fabric      | COTTON-002
  Row 3: LF001 | Light Wash | Main Fabric | DENIM-001
  Row 4: LF001 | Light Wash | Fabric      | COTTON-002
```

**Staging Table Schema:**
```sql
CREATE TABLE lft.beproduct.beproduct_to_dtc_staging (
    -- Composite key
    dtc_request_name STRING,           -- "KTB SS26 Wrangler"
    lf_style_number STRING,
    color_name STRING,
    fabric_group STRING,               -- hardcoded "MAIN MATERIAL CONTENT"
    
    -- DTC columns (BeProduct names, mapped during push)
    brands STRING,
    season STRING,
    year STRING,
    season_code STRING,                -- "SS26", "FW27"
    description STRING,
    product_status STRING,
    product_category STRING,
    product_sub_category STRING,
    division STRING,
    garment_finish STRING,
    techpack_stage STRING,
    mill_fabric_article STRING,
    front_image_url STRING,
    
    -- Metadata
    team STRING,
    customer_style_number STRING,
    lot_code STRING,
    parent_vendor STRING,
    factory STRING,
    folder_name STRING,
    beproduct_style_id STRING,
    beproduct_modified_at TIMESTAMP,
    
    -- Sync tracking
    transformed_at TIMESTAMP,
    transform_date DATE,
    sync_status STRING,                -- "pending", "pushed", "failed"
    pushed_at TIMESTAMP
)
```

**Season Code Mapping:**

Uses table: `lft.beproduct.dtc_seasoncode_mapping`

DTC and BeProduct identify a season differently:

- **DTC** uses 2 values: `(Customer, SeasonCode)` — e.g. `(KTB, SS28)`, `(KTB, FW26)`
- **BeProduct** uses 3 values: `(Customer, Season, Year)` — e.g. `(KTB, Spring, 2028)`, `(KTB, Fall, 2026)`

The mapping table stores only the **prefix** relationship (no year):

```sql
CUSTOMER | SEASON | DTCCODE
---------|--------|--------
KTB      | SPRING | SS
KTB      | FALL   | FW
```

The full DTC SeasonCode is derived at runtime:

```
DTC SeasonCode = DTCCODE + last 2 digits (YY) of the BeProduct Year

  SPRING + 2028  ->  "SS" + "28"  ->  "SS28"
  FALL   + 2027  ->  "FW" + "27"  ->  "FW27"
```

Notes:
- The styles `year` field is a STRING (e.g. `"2026"`) and may be `"N/A"`; such rows stay unmapped (NULL `season_code`) and are reported.
- The join is case-insensitive on CUSTOMER/SEASON (`Spring` matches `SPRING`).
- Reverse direction (DTC -> BeProduct) in `dtc/notebooks/pull_dtc_to_delta.py` reads the **same** table: it splits the DTC `season_code` into prefix + year and looks up `SEASON` via `DTCCODE`.

Created by: `dtc/notebooks/00_init_season_mapping.py`

**DTC Request Name Format:**
```
"<Customer> <SeasonCode> <Brand>"

Examples:
  - "KTB SS26 Wrangler"
  - "KTB FW27 Lee"
  - "KTB SS28 Wrangler Western"
```

---

### 3. DTC Request/Sheet Manager

**File:** `beproduct/dtc_request_manager.py`

**Purpose:** Ensure all DTC requests exist before pushing data.

**Process:**
1. Get unique request names from staging
2. Search DTC for existing requests
3. Create missing requests/sheets
4. Store request/sheet ID mapping

**Schedule:** Daily at 12:30pm UTC (after transform at 12pm)

**Parameters:**
```python
catalog = "lft"
schema = "beproduct"
staging_table = "beproduct_to_dtc_staging"
dtc_environment = "uat"
dtc_workspace = "KTB"
dtc_document = "KTB WIP"
dry_run = "false"
```

**Output Table:** `lft.beproduct.dtc_request_mapping`

**Schema:**
```sql
CREATE TABLE lft.beproduct.dtc_request_mapping (
    dtc_request_name STRING PRIMARY KEY,  -- "KTB SS26 Wrangler"
    request_id STRING,
    sheet_id STRING,
    workspace_name STRING,
    document_name STRING,
    environment STRING,
    status STRING,                         -- "exists", "created"
    last_updated_at TIMESTAMP
)
```

**API Used:**
- `POST /v1/sheets` - Create new request/sheet
- `GET /v1/requests` - Search existing requests

**Dry Run Mode:**
```bash
# Test without creating requests
dry_run = "true"
```

---

### 4. Change Detection & Push

**File:** `beproduct/beproduct_to_dtc_push.py`

**Purpose:** Detect changes and push BeProduct data to DTC.

**Process:**
1. Load staging data (BeProduct denormalized)
2. Pull current DTC data for comparison
3. Join and detect changes (INSERT/UPDATE/DELETE)
4. Validate data before push
5. Push to DTC via PATCH API
6. Log results and update sync status

**Schedule:** Daily at 1pm UTC (after request manager at 12:30pm)

**Parameters:**
```python
catalog = "lft"
schema = "beproduct"
staging_table = "beproduct_to_dtc_staging"
dtc_environment = "uat"
dry_run = "false"
batch_size = "100"
```

**Operations:**

**INSERT (New Rows):**
```python
# Per requirements (line 84-85): Use PATCH API with new rowIndex
max_row_index = connector.get_max_row_index(sheet_id, view_id)
new_row_index = max_row_index + 1

connector.patch_row(
    sheet_id=sheet_id,
    view_id=view_id,
    column_values=payload,
    row_index=new_row_index
)
```

**UPDATE (Existing Rows):**
```python
# Per requirements (line 80-82): Use PATCH API with rowId
# Include ALL fields, even unchanged ones
connector.patch_row(
    sheet_id=sheet_id,
    view_id=view_id,
    column_values=merged_payload,
    row_id=row_id
)
```

**DELETE (Mark as "Drop"):**
```python
# Per requirements (line 110): Mark Product Status = "Drop"
# Don't actually delete rows
payload["Product Status"] = "Drop"

connector.patch_row(
    sheet_id=sheet_id,
    view_id=view_id,
    column_values=payload,
    row_id=row_id
)
```

**Field Mapping:**
```python
COLUMN_MAPPING = {
    # Staging column → DTC column name
    "lf_style_number": "LF Style#",
    "brands": "Brand",
    "description": "Style Description",
    "product_status": "Product Status",
    "product_category": "Class",
    "product_sub_category": "Sub Class",
    "division": "Division",
    "garment_finish": "Garment Finish",
    "techpack_stage": "Tech Pack Stage",
    "color_name": "Color / Wash",
    "fabric_group": "Fabric Group",
    "mill_fabric_article": "Mill Fabric Article #",
}
```

**Timezone Handling:**
```python
# BeProduct: UTC timestamps
# DTC: HKT (UTC+8) timestamps
# All comparisons done in UTC

from pyspark.sql.functions import to_utc_timestamp, from_utc_timestamp

# Convert DTC timestamps to UTC for comparison
df = df.withColumn(
    "dtc_updated_at_utc",
    to_utc_timestamp(col("dtc_updated_at"), "Asia/Hong_Kong")
)

# Compare in UTC
where(col("beproduct_modified_at") > col("dtc_updated_at_utc"))
```

**Push Log Table:** `lft.beproduct.beproduct_to_dtc_push_log`

**Schema:**
```sql
CREATE TABLE lft.beproduct.beproduct_to_dtc_push_log (
    push_time TIMESTAMP,
    dtc_request_name STRING,
    operation STRING,                  -- "INSERT", "UPDATE", "DELETE"
    lf_style_number STRING,
    color_name STRING,
    fabric_group STRING,
    status STRING,                     -- "success", "failed"
    error_message STRING,
    payload STRING,                    -- JSON payload sent to DTC
    dry_run BOOLEAN
)
```

---

## Deployment

### Prerequisites

1. **Databricks Secrets:**
```bash
databricks secrets create-scope --scope beproduct

# DTC API keys
databricks secrets put --scope beproduct --key dtc_api_key_uat
databricks secrets put --scope beproduct --key dtc_api_key_prod

# BeProduct OAuth
databricks secrets put --scope beproduct --key client_id
databricks secrets put --scope beproduct --key client_secret
databricks secrets put --scope beproduct --key refresh_token
databricks secrets put --scope beproduct --key company_domain
```

2. **Season Code Mapping:**
```python
# Run once to initialize mapping table
/Workspace/Repos/beproduct-sync/DTC/notebooks/00_init_season_mapping.py

# Then add prefix mappings (CUSTOMER, SEASON, DTCCODE) -- no year here.
# Full SeasonCode is derived as DTCCODE + last 2 digits of the style's year.
INSERT INTO lft.beproduct.dtc_seasoncode_mapping (CUSTOMER, SEASON, DTCCODE) VALUES
  ('KTB', 'SPRING', 'SS'),
  ('KTB', 'FALL', 'FW');
```

3. **Upload Notebooks:**

**Option A: Using Python Script (Recommended)**
```bash
# Install Databricks SDK
pip install databricks-sdk

# Configure .env file (one-time setup)
cp .env.example .env
# Edit .env and add your credentials:
#   DATABRICKS_HOST=https://adb-XXXXXXXX.azuredatabricks.net
#   DATABRICKS_PAT=dapi...

# Preview uploads (dry run)
python scripts/upload_notebooks.py --dry-run

# Upload all notebooks (automatically reads .env)
python scripts/upload_notebooks.py

# Upload specific directory only
python scripts/upload_notebooks.py --dir beproduct
```

**Option B: Using Databricks CLI**
```bash
# Install and configure CLI
pip install databricks-cli
databricks configure --token

# Upload BeProduct notebooks
databricks workspace import_dir \
  ./beproduct \
  /Workspace/Repos/beproduct-sync/beproduct \
  --overwrite

# Upload DTC notebooks
databricks workspace import_dir \
  ./dtc/notebooks \
  /Workspace/Repos/beproduct-sync/DTC/notebooks \
  --overwrite
```

### Job Configuration

**Job 1: BeProduct STYLE Sync (Enhanced)**
- **Notebook:** `/Workspace/Repos/beproduct-sync/beproduct/beproduct_style_sync`
- **Schedule:** Daily at 11am UTC
- **Cluster:** Single-node (Standard_DS3_v2)
- **Parameters:**
  - `folder_name`: KTB
  - `refresh_mode`: INCREMENTAL

**Job 2: Denormalization Transform**
- **Notebook:** `/Workspace/Repos/beproduct-sync/beproduct/beproduct_to_dtc_transform`
- **Schedule:** Daily at 12pm UTC (depends on Job 1)
- **Cluster:** Single-node
- **Parameters:**
  - `customer_code`: KTB

**Job 3: DTC Request Manager**
- **Notebook:** `/Workspace/Repos/beproduct-sync/beproduct/dtc_request_manager`
- **Schedule:** Daily at 12:30pm UTC (depends on Job 2)
- **Cluster:** Single-node
- **Parameters:**
  - `dtc_environment`: uat
  - `dry_run`: false

**Job 4: BeProduct to DTC Push**
- **Notebook:** `/Workspace/Repos/beproduct-sync/beproduct/beproduct_to_dtc_push`
- **Schedule:** Daily at 1pm UTC (depends on Job 3)
- **Cluster:** Single-node
- **Parameters:**
  - `dtc_environment`: uat
  - `dry_run`: false
  - `batch_size`: 100

### Job Dependencies

```
┌─────────────────────────┐
│ 11:00 UTC               │
│ Extended Pull           │
│                         │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ 12:00 UTC               │
│ Denormalization         │
│                         │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ 12:30 UTC               │
│ Request Manager         │
│                         │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ 13:00 UTC               │
│ Push to DTC             │
│                         │
└─────────────────────────┘
```

---

## Monitoring

### Key Metrics

**Check sync status:**
```sql
SELECT 
    sync_status,
    COUNT(*) as row_count,
    COUNT(DISTINCT dtc_request_name) as unique_requests
FROM lft.beproduct.beproduct_to_dtc_staging
GROUP BY sync_status;
```

**Check push results:**
```sql
SELECT 
    DATE(push_time) as push_date,
    operation,
    status,
    COUNT(*) as count
FROM lft.beproduct.beproduct_to_dtc_push_log
GROUP BY DATE(push_time), operation, status
ORDER BY push_date DESC, operation;
```

**Check recent errors:**
```sql
SELECT 
    push_time,
    dtc_request_name,
    operation,
    lf_style_number,
    error_message
FROM lft.beproduct.beproduct_to_dtc_push_log
WHERE status = 'failed'
  AND push_time >= current_date() - INTERVAL 7 DAYS
ORDER BY push_time DESC;
```

### Alerts

Set up alerts for:
- No data synced for 24 hours
- Error rate > 10%
- Job duration > 1 hour
- Failed job runs

---

## Troubleshooting

### Common Issues

**1. Season code not mapped**
```
Error: unmapped rows without season code

Fix:
- Check lft.beproduct.dtc_seasoncode_mapping
- Add missing (CUSTOMER, SEASON, DTCCODE) prefix rows
- Also check the style's `year` field is a real year, not "N/A"
- Re-run transform
```

**2. DTC request not found**
```
Error: Request "KTB SS26 Wrangler" not found

Fix:
- Run dtc_request_manager.py to create missing requests
- Check dtc_request_mapping table
```

**3. Field validation failure**
```
Error: Required field 'lf_style_number' has null values

Fix:
- Check BeProduct data quality
- Review field extraction in extended pull
- Verify field IDs are correct
```

**4. DTC API errors**
```
Error: 403 Forbidden or 401 Unauthorized

Fix:
- Check API key validity
- Verify secret scope configuration
- Test API key manually with curl
```

**5. Colorways array empty**
```
Warning: Styles with no colorways

This is expected for some styles.
- Styles without colorways are skipped
- Review BeProduct data to confirm
```

### Recovery Procedures

**Retry failed pushes:**
```sql
-- Reset failed rows to pending
UPDATE lft.beproduct.beproduct_to_dtc_staging
SET sync_status = 'pending',
    pushed_at = NULL
WHERE sync_status = 'failed';

-- Re-run push notebook
```

**Full resync:**
```sql
-- Reset all to pending
UPDATE lft.beproduct.beproduct_to_dtc_staging
SET sync_status = 'pending',
    pushed_at = NULL;

-- Run push with FULL mode
```

---

## Testing

### Unit Tests

**Test colorway explosion:**
```python
# Input: 1 style with 2 colors
# Expected: 2 rows

assert exploded_df.count() == 2
```

**Test fabric row (no BOM explosion):**
```python
# Input: 1 (style × color)
# Expected: 1 hardcoded fabric row
#   fabric_group = "MAIN MATERIAL CONTENT", placement = main_material_content

assert fabric_df.count() == 1
```

**Test season code mapping:**
```python
# Mapping row: (CUSTOMER=KTB, SEASON=SPRING, DTCCODE=SS)
# Input: Season="Spring", Year="2026"
# Expected: SeasonCode = DTCCODE + last2(year) = "SS" + "26" = "SS26"

assert derive_season_code("Spring", "2026") == "SS26"   # FALL/2027 -> "FW27"
```

### Integration Tests

**End-to-end test:**
```python
# 1. Create test data in BeProduct
# 2. Run full pipeline
# 3. Verify data in DTC
# 4. Check field values match
```

---

## Future Enhancements

### Phase 2: Image Sync (Deferred)

**File:** `beproduct/beproduct_to_dtc_images.py`

**Purpose:** Upload BeProduct front images to DTC.

**Process:**
1. Query rows with front_image_url but no DTC image
2. Download image binary from BeProduct CDN
3. Upload to DTC via multipart/form-data
4. API: `POST /v1/sheets/{sheetId}/views/{viewId}/images`

**Status:** ⏳ Deferred per requirements (line 112)

### Improvements

- [ ] Implement full UPDATE/DELETE logic (requires DTC column mapping confirmation)
- [ ] Add parallel processing for large datasets
- [ ] Implement retry logic with exponential backoff
- [ ] Add data quality validations
- [ ] Create monitoring dashboard
- [ ] Implement conflict resolution
- [ ] Add approval workflows

---

## Contact & Support

**Repository:** https://github.com/your-org/beproduct-databricks-sync  
**Documentation:** This file + implementation plan  
**Issues:** GitHub Issues

---

**Document Version:** 1.0.0  
**Implementation Status:** ✅ Complete (Core functionality)  
**Production Ready:** ⚠️ Requires testing and DTC column mapping confirmation  
**Last Updated:** 2026-06-09
