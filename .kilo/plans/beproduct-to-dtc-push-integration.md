# BeProduct to DTC Push Integration - Implementation Plan

**Created:** 2026-06-09  
**Status:** Ready for Implementation  
**Complexity:** High - Cross-platform integration with denormalization

---

## Executive Summary

This plan addresses the Phase 1 requirement to sync BeProduct STYLE data to DTC WIP Requests with the following key challenges:

1. **Denormalization**: Transform BeProduct's normalized data (1 Style → N Colors → M BOM lines) into DTC's flat structure (N×M rows per style)
2. **Field Mapping**: Map updated BeProduct field list to DTC column names
3. **Cross-Platform Push**: Implement BeProduct → DTC workflow (currently only intra-platform sync exists)
4. **Change Detection**: Timestamp-based comparison with timezone handling
5. **Request/Sheet Management**: Create new DTC requests/sheets as needed, update existing rows
6. **Image Sync**: Deferred to separate notebook (per requirements)

---

## Current State Analysis

### ✅ What's Implemented

| Component | Status | Files |
|-----------|--------|-------|
| BeProduct → Delta Lake | ✅ Complete | `beproduct/beproduct_style_sync.py` |
| BeProduct ← Delta Lake | ✅ Complete | `beproduct/beproduct_style_push.py` |
| DTC → Delta Lake | ✅ Complete | `dtc/notebooks/pull_dtc_to_delta.py` |
| DTC ← Delta Lake | ✅ Complete | `dtc/notebooks/04_push_changes.py` |
| DTCConnector API | ✅ Complete | `dtc/python/connectors/dtc.py` |
| Season Code Mapping | ✅ Complete | `dtc/notebooks/00_init_season_mapping.py` |
| Master Data Sync | ✅ Complete | `beproduct/beproduct_master_data_sync.py` |

### ❌ What's Missing (Critical Gaps)

| Gap | Priority | Impact |
|-----|----------|--------|
| **BeProduct → DTC push workflow** | P0 | Core requirement not implemented |
| **Colorway extraction & denormalization** | P0 | 1 style → N colors = N DTC rows |
| **BOM data extraction & denormalization** | P0 | Each (style × color) → M materials = M DTC rows |
| **Field mapping updates** | P0 | New fields required, current mapping incomplete |
| **Style × Color × BOM join logic** | P0 | Cartesian product needed for denormalization |
| **DTC Request creation API** | P1 | For new season/brand combinations |
| **Image sync** | P2 | Explicitly deferred per requirements (line 112) |
| **Material fields extraction** | P0 | `core_main_material`, `Core_main_material2` not extracted |

---

## Requirements Deep Dive

### Data Structure Requirements

**BeProduct Structure (Normalized):**
```
1 STYLE record
  ├─ Header fields (LF Style #, Season, Brand, etc.)
  ├─ N Colorways ($.colorways[])
  │   └─ colorName (string)
  └─ 2 BOM lines (hardcoded per requirements)
      ├─ Line 1: ("Main Fabric", $.headerData[id="core_main_material"].value)
      └─ Line 2: ("Fabric", $.headerData[id="Core_main_material2"].value)
```

**DTC Structure (Denormalized - Flat):**
```
N × M DTC rows where:
  N = number of colorways
  M = 2 (hardcoded BOM lines per style)
  
Each row = 1 (Style × Color × Material) combination
```

**Example:**
```
BeProduct:
- Style: STY001, Season: SS26, Brand: Wrangler
  - Colorways: ["Dark Wash", "Light Wash"]
  - BOM: [core_main_material="DENIM-001", Core_main_material2="COTTON-002"]

DTC (4 rows):
Row 1: STY001 | Dark Wash  | Main Fabric | DENIM-001
Row 2: STY001 | Dark Wash  | Fabric      | COTTON-002
Row 3: STY001 | Light Wash | Main Fabric | DENIM-001
Row 4: STY001 | Light Wash | Fabric      | COTTON-002
```

### Field Mapping (BeProduct → DTC)

**Updated Field Requirements:**

| BeProduct Field | Field ID | DTC Column | Notes |
|----------------|----------|------------|-------|
| **LF Style Number** | `lf_style_number` | `LF Style#` | Compulsory, unique identifier |
| **SEASON** | `season` | (derived) | Compulsory, mapped via dtc_season_code_mapping |
| **YEAR** | `year` | (derived) | Compulsory, used for season mapping |
| **BRANDS** | `brands` | `Brand` | Compulsory, can be multi-select |
| **DESCRIPTION** | `description` | `Style Description` | Interested field |
| **TEAM** | `team` | (metadata) | Interested field |
| **PRODUCT STATUS** | `product_status` | `Product Status` | Interested field |
| **CUSTOMER STYLE NUMBER** | `customer_style_number` | (metadata) | Interested field |
| **PRODUCT CATEGORY** | `product_category` | `Class` | Interested field |
| **PRODUCT SUB CATEGORY** | `product_sub_category` | `Sub Class` | Interested field |
| **Division** | `division` | `Division` | Interested field |
| **GARMENT FINISH** | `garment_finish` | `Garment Finish` | Interested field |
| **TECHPACK STAGE** | `techpack_stage` | `Tech Pack Stage` | Interested field |
| **Lot Code** | `lot_code` | (metadata) | Interested field |
| **PARENT VENDOR** | `parent_vendor` | (metadata) | Interested field |
| **FACTORY** | `factory` | (metadata) | Interested field |
| **Main Material Category** | `main_material_category` | (derived) | New field, not in current extraction |
| **Main Material Content** | `main_material_content` | (derived) | New field, not in current extraction |
| --- | --- | --- | --- |
| **colorways[].colorName** | (array) | `Color / Wash` | From colorways array, denormalized |
| **frontImage.origin** | `front_image` | `Style Image` | Image URL, sync deferred |
| **core_main_material** | (headerData) | `Mill Fabric Article #` | BOM Line 1 |
| **Core_main_material2** | (headerData) | `Mill Fabric Article #` | BOM Line 2 |
| (hardcoded) | N/A | `Fabric Group` | "Main Fabric" or "Fabric" |
| (hardcoded) | N/A | `Placement` | From material placement logic |

**Current Implementation Gap:**
```python
# Current beproduct_style_sync.py extracts only 16 fields:
COMPULSORY_FIELDS = {
    "LF Style Number": "lf_style_number",
    "DESCRIPTION": "description",
    "TEAM": "team",
    "SEASON": "season",
    "YEAR": "year",
}

INTERESTED_FIELDS = {
    "PRODUCT STATUS": "product_status",
    "CUSTOMER STYLE NUMBER / PLM #": "customer_style_number",
    "PRODUCT CATEGORY": "product_category",
    "PRODUCT SUB CATEGORY": "product_sub_category",
    "Division": "division",
    "BRANDS": "brands",
    "GARMENT FINISH": "garment_finish",
    "TECHPACK STAGE": "techpack_stage",
    "Lot Code": "lot_code",
    "PARENT VENDOR": "parent_vendor",
    "FACTORY": "factory",
}

# ❌ MISSING:
# - colorways[] array extraction
# - core_main_material field
# - Core_main_material2 field
# - Main Material Category
# - Main Material Content
# - frontImage.origin
```

### DTC Request Naming Convention

Per requirements (lines 73-79):
```
Format: "<Customer> <SeasonCode> <Brand>"
Examples:
  - "KTB SS26 Wrangler"
  - "KTB FW27 Lee"

Where:
  - Customer = BeProduct Folder Name (e.g., "KTB" for KTB)
  - SeasonCode = SSYY format (e.g., SS26, FW27)
    - Mapped via lft.beproduct.dtc_season_code_mapping table
    - BeProduct Season="Spring" + Year=2026 → DTC SeasonCode="SS26"
  - Brand = BeProduct BRANDS field value
```

### Timezone Handling

**Critical requirement (line 34):**
```
- BeProduct: Full timestamp with timezone (UTC)
- DTC output: Timestamp as UTC (on retrieve)
- DTC input: Timestamp in user profile timezone (+0800 HKT)

Change Detection:
  BeProduct_Style_modifiedAt (UTC) > DTC_row_Updated_at (convert to UTC)
```

---

## Implementation Strategy

### Phase 1A: Extend BeProduct Pull (Data Extraction)

**Goal:** Extract all required fields including colorways, BOM, materials, images

**New notebook:** `beproduct/beproduct_style_extended_sync.py`

**Changes:**
1. **Add colorways extraction:**
   ```python
   # Extract from $.colorways[] array
   colorways = style_data.get("colorways", [])
   color_names = [cw.get("colorName") for cw in colorways if cw.get("colorName")]
   ```

2. **Add BOM fields extraction:**
   ```python
   # Extract material fields from headerData
   bom_fields = {
       "core_main_material": "bom_material_1",
       "Core_main_material2": "bom_material_2",
       "main_material_category": "main_material_category",
       "main_material_content": "main_material_content"
   }
   ```

3. **Add image field:**
   ```python
   # Extract frontImage URL
   front_image_url = style_data.get("headerData", {}).get("frontImage", {}).get("origin")
   ```

4. **Store in intermediate table:**
   ```
   Table: lft.beproduct.ktb_styles_extended
   Schema:
     - style_id (PK)
     - folder_name
     - [all existing fields from beproduct_style_sync.py]
     - colorways_array (array<string>)  -- Array of color names
     - bom_material_1 (string)
     - bom_material_2 (string)
     - main_material_category (string)
     - main_material_content (string)
     - front_image_url (string)
     - extracted_time (timestamp)
   ```

### Phase 1B: Denormalization Transform

**Goal:** Explode Style × Color × BOM into flat rows

**New notebook:** `beproduct/beproduct_to_dtc_transform.py`

**Process:**
1. **Read extended styles:**
   ```sql
   SELECT * FROM lft.beproduct.ktb_styles_extended
   WHERE last_modified >= [last_sync_time]
   ```

2. **Explode colorways:**
   ```python
   from pyspark.sql.functions import explode, col
   
   # Explode colorways array
   df_with_colors = df.withColumn("color_name", explode(col("colorways_array")))
   # Now: 1 row per (style × color)
   ```

3. **Create BOM rows:**
   ```python
   # Create 2 rows per (style × color):
   # Row 1: Main Fabric + bom_material_1
   # Row 2: Fabric + bom_material_2
   
   bom_line_1 = df_with_colors.withColumn("fabric_group", lit("Main Fabric")) \
                               .withColumn("mill_fabric_article", col("bom_material_1"))
   
   bom_line_2 = df_with_colors.withColumn("fabric_group", lit("Fabric")) \
                               .withColumn("mill_fabric_article", col("bom_material_2"))
   
   df_denormalized = bom_line_1.union(bom_line_2)
   # Now: 2 rows per (style × color) = final denormalized structure
   ```

4. **Add DTC metadata:**
   ```python
   # Map BeProduct Season + Year → DTC SeasonCode
   df_with_season = df_denormalized.join(
       spark.table("lft.beproduct.dtc_season_code_mapping"),
       on=[
           col("folder_name") == col("dtc_customer"),
           col("season") == col("beproduct_season"),
           col("year") == col("beproduct_year")
       ],
       how="left"
   )
   
   # Derive DTC Request Name: <Customer> <SeasonCode> <Brand>
   df_with_season = df_with_season.withColumn(
       "dtc_request_name",
       concat_ws(" ", col("folder_name"), col("season_code"), col("brands"))
   )
   ```

5. **Write to staging table:**
   ```
   Table: lft.beproduct.beproduct_to_dtc_staging
   Schema:
     - style_id
     - lf_style_number (unique key within request)
     - color_name (part of composite key)
     - fabric_group (part of composite key)
     - dtc_request_name (derived: "KTB SS26 Wrangler")
     - season_code (DTC format: "SS26")
     - [all mapped DTC columns]
     - beproduct_modified_at (UTC timestamp)
     - sync_status (pending/pushed/failed)
   
   PK: (dtc_request_name, lf_style_number, color_name, fabric_group)
   ```

### Phase 1C: DTC Request/Sheet Management

**Goal:** Ensure target DTC Request/Sheet exists before push

**New notebook:** `beproduct/dtc_request_manager.py`

**Process:**
1. **Get unique request names from staging:**
   ```sql
   SELECT DISTINCT dtc_request_name
   FROM lft.beproduct.beproduct_to_dtc_staging
   WHERE sync_status = 'pending'
   ```

2. **Check if DTC Request exists:**
   ```python
   # Use DTCConnector to search by workspace + document + request name
   # Workspace: "KTB" (customer)
   # Document: "KTB WIP"
   # Request Name: "KTB SS26 Wrangler"
   
   existing_requests = connector.search_requests(
       workspace_name="KTB",
       document_name="KTB WIP"
   )
   
   request_map = {
       req["requestReference"]: req["requestId"]
       for req in existing_requests
   }
   ```

3. **Create missing requests:**
   ```python
   # Per requirements (lines 93-97):
   # POST /v1/sheets - creates both Request and Sheet
   # Returns: request_id, sheet_id
   
   for request_name in new_request_names:
       response = connector.create_sheet(
           workspace_name="KTB",
           document_name="KTB WIP",
           request_name=request_name,
           # ... other metadata
       )
       request_id = response["requestId"]
       sheet_id = response["sheetId"]
       
       # Store mapping
       request_map[request_name] = request_id
   ```

4. **Store Request/Sheet mapping:**
   ```
   Table: lft.beproduct.dtc_request_mapping
   Schema:
     - dtc_request_name (PK)
     - request_id
     - sheet_id
     - workspace_name
     - document_name
     - created_at
     - last_synced_at
   ```

### Phase 1D: Change Detection & Push

**Goal:** Detect changes and push to DTC using PATCH API

**New notebook:** `beproduct/beproduct_to_dtc_push.py`

**Process:**

1. **Fetch existing DTC data for comparison:**
   ```python
   # For each request in staging:
   # 1. Pull current DTC sheet data
   # 2. Store as snapshot for comparison
   
   for request_name, request_id in request_map.items():
       df_dtc_current = connector.to_dataframe(
           request_id=request_id,
           view_id="Full Version"
       )
       
       # Store in comparison table
       df_dtc_current.write.saveAsTable(
           f"lft.beproduct.dtc_current_snapshot_{env}"
       )
   ```

2. **Join staging with current DTC data:**
   ```python
   # Match on composite key:
   # (lf_style_number, color_name, fabric_group)
   
   staging = spark.table("lft.beproduct.beproduct_to_dtc_staging")
   dtc_current = spark.table("lft.beproduct.dtc_current_snapshot_uat")
   
   comparison = staging.alias("bp").join(
       dtc_current.alias("dtc"),
       on=[
           col("bp.lf_style_number") == col("dtc.lf_style"),
           col("bp.color_name") == col("dtc.color_wash"),
           col("bp.fabric_group") == col("dtc.fabric_group")
       ],
       how="full_outer"
   )
   ```

3. **Detect operation type:**
   ```python
   # INSERT: Row in BP but not in DTC
   inserts = comparison.where(col("dtc.row_id").isNull())
   
   # UPDATE: Row in both, BP modified_at > DTC updated_at (handle timezone!)
   updates = comparison.where(
       col("dtc.row_id").isNotNull() &
       (col("bp.beproduct_modified_at") > col("dtc.updated_at_utc"))
   )
   
   # DELETE (mark as "Drop"): Row in DTC but not in BP
   # Per requirements (line 110): Mark Product Status = "Drop", don't DELETE
   deletes = comparison.where(col("bp.style_id").isNull())
   ```

4. **Prepare DTC payloads:**
   ```python
   # Map staging columns to DTC column names
   COLUMN_MAPPING = {
       "lf_style_number": "LF Style#",
       "description": "Style Description",
       "product_status": "Product Status",
       "product_category": "Class",
       "product_sub_category": "Sub Class",
       "division": "Division",
       "brands": "Brand",
       "color_name": "Color / Wash",
       "garment_finish": "Garment Finish",
       "techpack_stage": "Tech Pack Stage",
       "fabric_group": "Fabric Group",
       "mill_fabric_article": "Mill Fabric Article #",
       # ... all other fields
   }
   
   def prepare_payload(row, operation):
       """Prepare DTC PATCH payload."""
       payload = {}
       for bp_col, dtc_col in COLUMN_MAPPING.items():
           value = row[bp_col]
           if value is not None:  # Only include non-null values
               payload[dtc_col] = value
       
       # For DELETEs, override Product Status
       if operation == "DELETE":
           payload["Product Status"] = "Drop"
       
       return payload
   ```

5. **Push to DTC:**
   ```python
   # Per requirements (lines 80-85):
   # - Existing rows: PATCH with rowId
   # - New rows: PATCH with new rowIndex (max + 1)
   
   results = {"success": 0, "failed": 0, "errors": []}
   
   # INSERTs
   for row in inserts.collect():
       try:
           # Get max rowIndex for this sheet
           max_row_index = dtc_current \
               .where(col("request_id") == row.request_id) \
               .agg({"row_index": "max"}) \
               .collect()[0][0]
           
           new_row_index = (max_row_index or 0) + 1
           
           payload = prepare_payload(row, "INSERT")
           
           # PATCH API: POST or PATCH?
           # Per requirements (line 84): Use PATCH API for new rows too
           response = connector.client.patch(
               f"/v1/sheets/{row.sheet_id}/views/{view_id}",
               json={
                   "rowIndex": new_row_index,
                   "columnValues": payload
               }
           )
           
           results["success"] += 1
           
       except Exception as e:
           results["failed"] += 1
           results["errors"].append({
               "operation": "INSERT",
               "style": row.lf_style_number,
               "error": str(e)
           })
   
   # UPDATEs
   for row in updates.collect():
       try:
           payload = prepare_payload(row, "UPDATE")
           
           # Per requirements (line 81): Include ALL fields, even unchanged
           # Need to merge with existing row data
           existing_payload = get_existing_row_data(row.row_id)
           merged_payload = {**existing_payload, **payload}
           
           response = connector.client.patch(
               f"/v1/sheets/{row.sheet_id}/views/{view_id}",
               json={
                   "rowId": row.row_id,  # Use rowId for existing rows
                   "columnValues": merged_payload
               }
           )
           
           results["success"] += 1
           
       except Exception as e:
           results["failed"] += 1
           results["errors"].append({
               "operation": "UPDATE",
               "style": row.lf_style_number,
               "error": str(e)
           })
   
   # "DELETEs" (mark as Drop)
   for row in deletes.collect():
       try:
           payload = get_existing_row_data(row.row_id)
           payload["Product Status"] = "Drop"
           
           response = connector.client.patch(
               f"/v1/sheets/{row.sheet_id}/views/{view_id}",
               json={
                   "rowId": row.row_id,
                   "columnValues": payload
               }
           )
           
           results["success"] += 1
           
       except Exception as e:
           results["failed"] += 1
   ```

6. **Update sync status:**
   ```python
   # Update staging table with push results
   for result_row in results:
       spark.sql(f"""
       UPDATE lft.beproduct.beproduct_to_dtc_staging
       SET sync_status = 'pushed',
           pushed_at = current_timestamp()
       WHERE style_id = '{result_row.style_id}'
         AND color_name = '{result_row.color_name}'
         AND fabric_group = '{result_row.fabric_group}'
       """)
   ```

7. **Log push audit:**
   ```
   Table: lft.beproduct.beproduct_to_dtc_push_log
   Schema:
     - push_id (auto-increment)
     - push_time (timestamp)
     - dtc_request_name
     - operation (INSERT/UPDATE/DELETE)
     - style_id
     - lf_style_number
     - color_name
     - fabric_group
     - status (success/failed)
     - error_message
     - payload (JSON string)
   ```

### Phase 1E: Image Sync (Deferred)

**Goal:** Upload BeProduct frontImage to DTC Style Image column

**New notebook:** `beproduct/beproduct_to_dtc_images.py`

**Status:** ⏳ **DEFERRED** per requirements (line 112: "separate add/update image into dedicated notebook so we can tackle it later")

**Placeholder workflow:**
```python
# For each row that needs image:
# 1. Check if DTC.Style_Image is blank
# 2. If blank:
#    a. Download binary from BeProduct CDN (frontImage.origin URL)
#    b. Upload to DTC via multipart/form-data
#    c. API: POST /v1/sheets/{sheetId}/views/{viewId}/images
#           ?rowindex={number}&columnname=Style_Image

# Per requirements (lines 114-117):
def upload_image_to_dtc(sheet_id, view_id, row_index, image_url):
    """Upload image to DTC."""
    # 1. Download from BeProduct CDN
    response = requests.get(image_url, timeout=30)
    image_binary = response.content
    
    # 2. Upload to DTC
    files = {"file": ("image.jpg", image_binary, "image/jpeg")}
    
    connector.client.post(
        f"/v1/sheets/{sheet_id}/views/{view_id}/images",
        params={
            "rowindex": row_index,
            "columnname": "Style Image"
        },
        files=files
    )
```

---

## Implementation Order & Dependencies

### Step 1: Update BeProduct Pull (2-3 days)
**Files to modify:**
- `beproduct/beproduct_style_sync.py` → Extend field extraction

**Tasks:**
1. Add colorways array extraction
2. Add BOM material fields (core_main_material, Core_main_material2)
3. Add material category/content fields
4. Add frontImage URL extraction
5. Update schema to include array<string> for colorways
6. Test with sample BeProduct data

**Validation:**
```sql
-- Check colorways extraction
SELECT style_id, lf_style_number, colorways_array, 
       bom_material_1, bom_material_2
FROM lft.beproduct.ktb_styles_extended
LIMIT 10;

-- Verify array is populated
SELECT style_id, 
       size(colorways_array) as color_count
FROM lft.beproduct.ktb_styles_extended
WHERE size(colorways_array) > 0;
```

### Step 2: Create Denormalization Transform (3-4 days)
**Files to create:**
- `beproduct/beproduct_to_dtc_transform.py`

**Tasks:**
1. Implement colorway explosion (1 style → N rows)
2. Implement BOM explosion (each color → 2 material rows)
3. Join with season code mapping
4. Derive DTC request name
5. Map all fields to DTC column names
6. Create staging table
7. Test denormalization logic

**Validation:**
```sql
-- Check denormalization
SELECT lf_style_number, color_name, fabric_group, 
       dtc_request_name, mill_fabric_article
FROM lft.beproduct.beproduct_to_dtc_staging
WHERE lf_style_number = 'TEST001';

-- Expected: 1 style × 2 colors × 2 materials = 4 rows

-- Verify request name format
SELECT DISTINCT dtc_request_name
FROM lft.beproduct.beproduct_to_dtc_staging;
-- Expected: "KTB SS26 Wrangler", "KTB FW27 Lee", etc.
```

### Step 3: Implement Request Manager (2-3 days)
**Files to create:**
- `beproduct/dtc_request_manager.py`
- Update `dtc/python/connectors/dtc.py` to add `create_sheet()` method

**Tasks:**
1. Add DTCConnector.create_sheet() method
2. Implement request search logic
3. Create missing requests/sheets
4. Store request/sheet mapping table
5. Handle errors (duplicate requests, API failures)
6. Test with UAT environment

**Validation:**
```sql
-- Check request mapping
SELECT * FROM lft.beproduct.dtc_request_mapping;

-- Verify all staging requests have mapping
SELECT DISTINCT s.dtc_request_name
FROM lft.beproduct.beproduct_to_dtc_staging s
LEFT JOIN lft.beproduct.dtc_request_mapping m
  ON s.dtc_request_name = m.dtc_request_name
WHERE m.request_id IS NULL;
-- Expected: 0 rows (all mapped)
```

### Step 4: Implement Change Detection (3-4 days)
**Files to create:**
- `beproduct/beproduct_to_dtc_push.py`

**Tasks:**
1. Pull current DTC data for comparison
2. Implement 3-way join (staging ⟕ DTC current)
3. Classify operations (INSERT/UPDATE/DELETE)
4. Handle timezone conversion (UTC ↔ HKT)
5. Test change detection logic
6. Add dry-run mode for validation

**Validation:**
```sql
-- Check change detection
SELECT operation, COUNT(*) as count
FROM lft.beproduct.beproduct_to_dtc_changes
GROUP BY operation;

-- Verify timestamp logic
SELECT bp.lf_style_number,
       bp.beproduct_modified_at as bp_time_utc,
       dtc.updated_at as dtc_time_hkt,
       dtc.updated_at_utc,
       CASE WHEN bp.beproduct_modified_at > dtc.updated_at_utc 
            THEN 'UPDATE' ELSE 'NO_CHANGE' END as decision
FROM staging bp
JOIN dtc_current dtc ON ...
LIMIT 10;
```

### Step 5: Implement DTC Push (4-5 days)
**Files to modify:**
- `beproduct/beproduct_to_dtc_push.py` (continue from Step 4)
- Update `dtc/python/connectors/dtc.py` with helper methods

**Tasks:**
1. Implement payload preparation with field mapping
2. Handle INSERT operations (new rowIndex assignment)
3. Handle UPDATE operations (merge with existing data)
4. Handle DELETE operations (mark as "Drop")
5. Add retry logic for API failures
6. Update sync status in staging table
7. Create push audit log
8. Test with small batch in UAT
9. Test with full dataset in UAT
10. Add error handling and rollback

**Validation:**
```sql
-- Check push results
SELECT status, COUNT(*) as count
FROM lft.beproduct.beproduct_to_dtc_push_log
WHERE push_time >= current_date()
GROUP BY status;

-- Verify data in DTC (manual check via DTC UI)
-- Compare staging vs DTC for sample rows

-- Check sync status
SELECT sync_status, COUNT(*) as count
FROM lft.beproduct.beproduct_to_dtc_staging
GROUP BY sync_status;
```

### Step 6: End-to-End Testing (3-4 days)
**Tasks:**
1. Create test dataset in BeProduct UAT
2. Run full pipeline end-to-end
3. Verify data in DTC UAT
4. Test edge cases:
   - Style with 1 color
   - Style with multiple colors
   - Style with missing BOM fields
   - New season/brand (request creation)
   - Updates to existing rows
   - Deleted styles (marked as "Drop")
5. Performance testing (100+ styles)
6. Error handling testing (API failures, invalid data)

### Step 7: Production Deployment (1-2 days)
**Tasks:**
1. Schedule Databricks jobs:
   - Job 1: BeProduct Pull (extended) - Daily 11am UTC
   - Job 2: Denormalization Transform - Daily 12pm UTC (after Job 1)
   - Job 3: Request Manager - Daily 12:30pm UTC (after Job 2)
   - Job 4: Change Detection & Push - Daily 1pm UTC (after Job 3)
2. Configure secrets for PROD environment
3. Update season code mapping for production
4. Set up monitoring and alerts
5. Document operational procedures

### Step 8: Image Sync (Future - Deferred)
**Tasks:**
1. Implement image download from BeProduct CDN
2. Implement image upload to DTC (multipart/form-data)
3. Add image sync status tracking
4. Test with sample images
5. Schedule as separate job

**Estimated time:** 2-3 days (when prioritized)

---

## Technical Considerations

### 1. Performance Optimization

**Denormalization at scale:**
```python
# Use Spark's native explode for colorways
# Avoid collecting to driver - use DataFrame operations

# Instead of:
colors = row.colorways_array  # ❌ Collect to driver
for color in colors:
    create_row(color)

# Do:
df.withColumn("color", explode("colorways_array"))  # ✅ Distributed
```

**Batch DTC API calls:**
```python
# Instead of: 1 API call per row (slow)
# Do: Batch multiple rows in single PATCH request (if supported)

# Check DTC API docs for bulk update support
# If not available, use threading for parallel PATCH calls
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(push_row, row) for row in rows]
```

### 2. Error Handling

**API Failures:**
```python
# Implement exponential backoff retry
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def push_to_dtc(row):
    response = connector.patch(...)
    return response

# Log all failures for manual review
# Don't fail entire job - continue with other rows
```

**Data Validation:**
```python
# Validate before push
def validate_row(row):
    errors = []
    
    # Required fields
    if not row.lf_style_number:
        errors.append("Missing LF Style Number")
    
    if not row.season_code:
        errors.append("Season code mapping not found")
    
    # Field length limits (check DTC column constraints)
    if len(row.description or "") > 500:
        errors.append("Description exceeds 500 chars")
    
    return errors

# Skip invalid rows, log errors
```

### 3. Timezone Handling

**UTC ↔ HKT Conversion:**
```python
from pyspark.sql.functions import from_utc_timestamp, to_utc_timestamp

# Convert DTC timestamps (HKT) to UTC for comparison
df = df.withColumn(
    "dtc_updated_at_utc",
    to_utc_timestamp(col("dtc_updated_at"), "Asia/Hong_Kong")
)

# Convert UTC to HKT when pushing to DTC
df = df.withColumn(
    "last_modified_hkt",
    from_utc_timestamp(col("beproduct_modified_at"), "Asia/Hong_Kong")
)

# Always compare in UTC!
where(col("beproduct_modified_at") > col("dtc_updated_at_utc"))
```

### 4. Idempotency

**Ensure reruns are safe:**
```python
# Use upsert logic in staging table
staging.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"folder_name = '{folder}'") \
    .saveAsTable("staging_table")

# Track push status to avoid duplicate pushes
# Only push rows with sync_status = 'pending'
```

### 5. Monitoring

**Add observability:**
```python
# Log metrics after each run
metrics = {
    "run_id": str(uuid.uuid4()),
    "run_time": datetime.now(),
    "styles_processed": style_count,
    "rows_generated": denormalized_count,
    "inserts": insert_count,
    "updates": update_count,
    "deletes": delete_count,
    "errors": error_count,
    "duration_seconds": duration
}

# Write to metrics table
spark.createDataFrame([metrics]).write \
    .mode("append") \
    .saveAsTable("lft.beproduct.sync_metrics")

# Set up alerts for:
# - High error rate (> 10%)
# - No data synced for 24h
# - Long duration (> 1 hour)
```

---

## API Extensions Needed

### DTCConnector Methods to Add

```python
# In dtc/python/connectors/dtc.py

def create_sheet(
    self,
    workspace_name: str,
    document_name: str,
    request_name: str,
    request_description: str = "",
    **kwargs
) -> Dict[str, str]:
    """
    Create new DTC Request and Sheet.
    
    POST /v1/sheets
    
    Returns:
        {
            "requestId": "...",
            "sheetId": "..."
        }
    """
    payload = {
        "workspaceName": workspace_name,
        "documentName": document_name,
        "requestName": request_name,
        "requestDescription": request_description,
        **kwargs
    }
    
    response = self.client.post("/v1/sheets", json=payload)
    return response


def search_requests(
    self,
    workspace_name: str,
    document_name: str = None
) -> List[Dict]:
    """
    Search for requests in a workspace.
    
    GET /v1/requests?workspace={name}&document={name}
    
    Returns:
        List of request dicts with requestId, requestReference, etc.
    """
    params = {"workspace": workspace_name}
    if document_name:
        params["document"] = document_name
    
    response = self.client.get("/v1/requests", params=params)
    return response.get("data", [])


def get_max_row_index(self, sheet_id: str, view_id: str) -> int:
    """
    Get maximum rowIndex for a sheet.
    
    Returns:
        Max rowIndex (int), or 0 if sheet is empty
    """
    sheet = self.get_sheet(sheet_id, view_id)
    rows = sheet.get("sheetData", [])
    
    if not rows:
        return 0
    
    return max(row.get("rowIndex", 0) for row in rows)


def patch_row(
    self,
    sheet_id: str,
    view_id: str,
    column_values: Dict[str, Any],
    row_id: str = None,
    row_index: int = None
) -> Dict:
    """
    Update existing row or create new row.
    
    PATCH /v1/sheets/{sheetId}/views/{viewId}
    
    Args:
        sheet_id: DTC sheet ID
        view_id: DTC view ID
        column_values: Dict of {columnName: value}
        row_id: For updating existing row
        row_index: For creating new row (if row_id not provided)
    
    Returns:
        Response dict
    """
    if not row_id and not row_index:
        raise ValueError("Must provide either row_id or row_index")
    
    payload = {"columnValues": column_values}
    
    if row_id:
        payload["rowId"] = row_id
    elif row_index:
        payload["rowIndex"] = row_index
    
    response = self.client.patch(
        f"/v1/sheets/{sheet_id}/views/{view_id}",
        json=payload
    )
    
    return response
```

---

## Data Quality & Validation

### Pre-Push Validations

```python
def validate_staging_data(df):
    """Validate staging data before push."""
    
    validations = []
    
    # 1. Check required fields
    required_fields = ["lf_style_number", "season_code", "brands", "color_name"]
    for field in required_fields:
        null_count = df.where(col(field).isNull()).count()
        if null_count > 0:
            validations.append({
                "rule": f"Required field: {field}",
                "status": "FAIL",
                "count": null_count,
                "action": "Skip rows with null values"
            })
    
    # 2. Check season code mapping exists
    unmapped = df.where(col("season_code").isNull()).count()
    if unmapped > 0:
        validations.append({
            "rule": "Season code mapping",
            "status": "FAIL",
            "count": unmapped,
            "action": "Update dtc_season_code_mapping table"
        })
    
    # 3. Check DTC request name format
    # Should match: "<Customer> <SSYY> <Brand>"
    invalid_names = df.where(
        ~col("dtc_request_name").rlike("^[A-Z]+ [A-Z]{2}[0-9]{2} .+$")
    ).count()
    if invalid_names > 0:
        validations.append({
            "rule": "DTC request name format",
            "status": "FAIL",
            "count": invalid_names,
            "action": "Fix brand or season data"
        })
    
    # 4. Check for duplicate composite keys
    duplicates = df.groupBy(
        "dtc_request_name", "lf_style_number", "color_name", "fabric_group"
    ).count().where(col("count") > 1).count()
    if duplicates > 0:
        validations.append({
            "rule": "Unique composite key",
            "status": "FAIL",
            "count": duplicates,
            "action": "Investigate duplicate rows"
        })
    
    # 5. Check field lengths (example)
    long_descriptions = df.where(length(col("description")) > 500).count()
    if long_descriptions > 0:
        validations.append({
            "rule": "Description length <= 500",
            "status": "WARN",
            "count": long_descriptions,
            "action": "Truncate or log warning"
        })
    
    # Print validation report
    print("\n=== Data Quality Validation ===")
    for v in validations:
        status_emoji = "❌" if v["status"] == "FAIL" else "⚠️"
        print(f"{status_emoji} {v['rule']}: {v['count']} rows")
        print(f"   Action: {v['action']}")
    
    # Fail if any critical validations failed
    critical_fails = [v for v in validations if v["status"] == "FAIL"]
    if critical_fails:
        raise ValueError(f"Data quality validation failed: {len(critical_fails)} critical issues")
    
    return validations
```

### Post-Push Verification

```python
def verify_push_results(staging_df, dtc_df):
    """Verify pushed data matches staging."""
    
    # Join on composite key
    comparison = staging_df.alias("stg").join(
        dtc_df.alias("dtc"),
        on=[
            col("stg.lf_style_number") == col("dtc.lf_style"),
            col("stg.color_name") == col("dtc.color_wash"),
            col("stg.fabric_group") == col("dtc.fabric_group")
        ],
        how="inner"
    )
    
    # Check field mismatches
    mismatches = []
    
    for stg_col, dtc_col in COLUMN_MAPPING.items():
        mismatch_count = comparison.where(
            col(f"stg.{stg_col}") != col(f"dtc.{dtc_col}")
        ).count()
        
        if mismatch_count > 0:
            mismatches.append({
                "field": stg_col,
                "mismatches": mismatch_count
            })
    
    if mismatches:
        print("\n⚠️ Field mismatches detected:")
        for m in mismatches:
            print(f"  - {m['field']}: {m['mismatches']} rows")
    else:
        print("\n✅ All fields match between staging and DTC")
    
    return mismatches
```

---

## Testing Strategy

### Unit Tests

```python
# tests/test_denormalization.py

def test_colorway_explosion():
    """Test that 1 style with N colors becomes N rows."""
    input_data = [{
        "style_id": "STY001",
        "lf_style_number": "LF001",
        "colorways_array": ["Dark Wash", "Light Wash"]
    }]
    
    df = spark.createDataFrame(input_data)
    result = explode_colorways(df)
    
    assert result.count() == 2
    assert result.where(col("color_name") == "Dark Wash").count() == 1
    assert result.where(col("color_name") == "Light Wash").count() == 1


def test_bom_explosion():
    """Test that each color becomes 2 material rows."""
    input_data = [{
        "style_id": "STY001",
        "color_name": "Dark Wash",
        "bom_material_1": "DENIM-001",
        "bom_material_2": "COTTON-002"
    }]
    
    df = spark.createDataFrame(input_data)
    result = explode_bom(df)
    
    assert result.count() == 2
    assert result.where(col("fabric_group") == "Main Fabric").count() == 1
    assert result.where(col("fabric_group") == "Fabric").count() == 1


def test_season_code_mapping():
    """Test season code derivation."""
    # Season="Spring", Year=2026 → SeasonCode="SS26"
    assert derive_season_code("Spring", 2026) == "SS26"
    assert derive_season_code("Fall", 2027) == "FW27"
    
    # Invalid cases
    with pytest.raises(ValueError):
        derive_season_code("Summer", 2026)  # Not in mapping


def test_request_name_format():
    """Test DTC request name format."""
    name = build_request_name("KTB", "SS26", "Wrangler")
    assert name == "KTB SS26 Wrangler"
    
    # Multi-word brand
    name = build_request_name("KTB", "FW27", "Lee Western")
    assert name == "KTB FW27 Lee Western"
```

### Integration Tests

```python
# tests/test_integration.py

def test_end_to_end_flow():
    """Test complete BeProduct → DTC flow."""
    
    # 1. Create test data in BeProduct staging
    test_styles = create_test_styles(count=5)
    
    # 2. Run denormalization
    staging_df = run_denormalization(test_styles)
    
    # Expected: 5 styles × 2 colors × 2 materials = 20 rows
    assert staging_df.count() == 20
    
    # 3. Run push (dry-run mode)
    results = run_push(staging_df, dry_run=True)
    
    # Should detect 20 INSERTs
    assert results["inserts"] == 20
    assert results["updates"] == 0
    assert results["deletes"] == 0
    
    # 4. Run actual push (to UAT)
    results = run_push(staging_df, dry_run=False)
    
    assert results["success"] == 20
    assert results["failed"] == 0
    
    # 5. Verify data in DTC
    dtc_df = pull_from_dtc(request_id=test_request_id)
    assert dtc_df.count() == 20
    
    # 6. Verify field values match
    mismatches = verify_push_results(staging_df, dtc_df)
    assert len(mismatches) == 0


def test_update_flow():
    """Test update detection and push."""
    
    # 1. Create and push initial data
    initial_df = create_test_styles(count=2)
    run_push(initial_df)
    
    # 2. Modify styles in BeProduct
    modified_df = modify_styles(initial_df, fields=["description", "product_status"])
    
    # 3. Run change detection
    changes = detect_changes(modified_df)
    
    # Should detect 4 UPDATEs (2 styles × 2 colors × 2 materials)
    assert changes.where(col("operation") == "UPDATE").count() == 4
    
    # 4. Push updates
    results = run_push(modified_df)
    assert results["updates"] == 4
    
    # 5. Verify updates in DTC
    dtc_df = pull_from_dtc(request_id=test_request_id)
    assert verify_field_updated(dtc_df, "description", modified_df)
```

### Edge Case Tests

```python
def test_style_with_no_colorways():
    """Test style with empty colorways array."""
    style = {
        "style_id": "STY001",
        "colorways_array": []  # Empty
    }
    
    # Should log warning and skip style
    result = explode_colorways([style])
    assert result.count() == 0


def test_missing_bom_material():
    """Test style with null BOM material."""
    style = {
        "style_id": "STY001",
        "bom_material_1": "DENIM-001",
        "bom_material_2": None  # Missing
    }
    
    result = explode_bom([style])
    
    # Should still create 2 rows, one with null material
    assert result.count() == 2
    assert result.where(col("mill_fabric_article").isNull()).count() == 1


def test_unmapped_season():
    """Test style with season not in mapping table."""
    style = {
        "season": "Winter",
        "year": 2026
    }
    
    # Should raise error or set season_code to NULL
    result = derive_season_code(style)
    assert result["season_code"] is None
    # Validation should catch this before push


def test_duplicate_push():
    """Test that pushing same data twice doesn't create duplicates."""
    df = create_test_styles(count=1)
    
    # Push once
    run_push(df)
    dtc_count_1 = pull_from_dtc().count()
    
    # Push again (without changes)
    run_push(df)
    dtc_count_2 = pull_from_dtc().count()
    
    # Should be same count (no duplicates)
    assert dtc_count_1 == dtc_count_2
```

---

## Rollback & Recovery

### Rollback Procedures

```python
def rollback_push(push_id: str):
    """Rollback a failed push operation."""
    
    # 1. Get all rows pushed in this batch
    push_log = spark.sql(f"""
        SELECT * FROM lft.beproduct.beproduct_to_dtc_push_log
        WHERE push_id = '{push_id}'
          AND status = 'success'
    """)
    
    # 2. For each INSERT, delete from DTC
    inserts = push_log.where(col("operation") == "INSERT")
    for row in inserts.collect():
        connector.delete_row(row.sheet_id, row.row_id)
    
    # 3. For each UPDATE, restore previous values
    updates = push_log.where(col("operation") == "UPDATE")
    for row in updates.collect():
        # Get previous snapshot
        previous = get_previous_snapshot(row.row_id)
        connector.patch_row(
            row.sheet_id,
            row.view_id,
            previous.column_values,
            row_id=row.row_id
        )
    
    # 4. Update staging table
    spark.sql(f"""
        UPDATE lft.beproduct.beproduct_to_dtc_staging
        SET sync_status = 'rollback',
            rolled_back_at = current_timestamp()
        WHERE push_id = '{push_id}'
    """)
    
    print(f"✅ Rolled back push {push_id}")
```

### Recovery from Failures

```python
def retry_failed_pushes():
    """Retry rows that failed to push."""
    
    # Get failed rows
    failed = spark.sql("""
        SELECT * FROM lft.beproduct.beproduct_to_dtc_staging
        WHERE sync_status = 'failed'
          AND retry_count < 3
    """)
    
    print(f"Retrying {failed.count()} failed rows...")
    
    for row in failed.collect():
        try:
            # Retry push
            push_row(row)
            
            # Update status
            spark.sql(f"""
                UPDATE lft.beproduct.beproduct_to_dtc_staging
                SET sync_status = 'pushed',
                    retry_count = retry_count + 1,
                    pushed_at = current_timestamp()
                WHERE row_key = '{row.row_key}'
            """)
            
        except Exception as e:
            # Log error and increment retry count
            spark.sql(f"""
                UPDATE lft.beproduct.beproduct_to_dtc_staging
                SET retry_count = retry_count + 1,
                    last_error = '{str(e)}'
                WHERE row_key = '{row.row_key}'
            """)
```

---

## Documentation Updates

### Files to Update

1. **README.md** - Add BeProduct → DTC section
2. **QUICK_START.md** - Add new job setup instructions
3. **QUICK_REFERENCE.md** - Add new notebook parameters
4. **New: BEPRODUCT_TO_DTC_GUIDE.md** - Complete workflow documentation

### New Documentation Sections

```markdown
# BeProduct → DTC Sync Guide

## Overview
Syncs BeProduct STYLE data to DTC WIP Requests with denormalization.

## Architecture
BeProduct (1 Style) 
  → Transform (N Colors × M Materials)
  → DTC (N×M flat rows)

## Notebooks
1. beproduct_style_extended_sync.py - Pull with colorways/BOM
2. beproduct_to_dtc_transform.py - Denormalization
3. dtc_request_manager.py - Request/sheet creation
4. beproduct_to_dtc_push.py - Change detection & push

## Scheduling
- Run daily after BeProduct master data sync
- Dependencies: Master data → Extended pull → Transform → Push

## Monitoring
- Check lft.beproduct.sync_metrics table
- Alert if error_rate > 10%
- Review push logs for failures
```

---

## Success Criteria

### Phase 1A Complete
- ✅ BeProduct pull extracts colorways array
- ✅ BeProduct pull extracts BOM material fields
- ✅ All 16+ fields mapped correctly
- ✅ Data stored in `ktb_styles_extended` table
- ✅ No data loss compared to current pull

### Phase 1B Complete
- ✅ Denormalization produces correct row count (N colors × 2 materials)
- ✅ All fields mapped to DTC column names
- ✅ Season code mapping works for all seasons
- ✅ DTC request names formatted correctly
- ✅ Staging table populated with valid data

### Phase 1C Complete
- ✅ Request manager finds existing DTC requests
- ✅ Request manager creates missing requests/sheets
- ✅ All staging rows have valid request_id/sheet_id mapping
- ✅ No duplicate requests created

### Phase 1D Complete
- ✅ Change detection correctly identifies INSERTs/UPDATEs/DELETEs
- ✅ Timezone conversion works correctly
- ✅ Dry-run mode validates without pushing
- ✅ No false positives/negatives in change detection

### Phase 1E Complete
- ✅ Push successfully creates new rows in DTC
- ✅ Push successfully updates existing rows in DTC
- ✅ "Deleted" styles marked as "Drop" in DTC
- ✅ All fields match between staging and DTC
- ✅ Push audit log captures all operations
- ✅ Error handling prevents data corruption

### End-to-End Success
- ✅ 100 test styles pushed to UAT successfully
- ✅ Field values verified in DTC UI
- ✅ No duplicate rows created
- ✅ Updates correctly overwrite previous values
- ✅ Performance acceptable (<1 hour for 1000 styles)
- ✅ Error rate <5%
- ✅ Production deployment successful
- ✅ Scheduled jobs running daily without manual intervention

---

## Open Questions for User

1. **Field IDs Confirmation:**
   - Are `core_main_material` and `Core_main_material2` the correct field IDs in your BeProduct instance?
   - What are the exact field IDs for "Main Material Category" and "Main Material Content"?

2. **BOM Structure:**
   - Is the hardcoded 2-line BOM structure (Main Fabric + Fabric) correct for all styles?
   - Are there styles with more or fewer BOM lines?

3. **Colorways:**
   - Can a style have 0 colorways? How should we handle this?
   - Is `colorways[].colorName` the only field needed, or do we need other colorway data?

4. **DTC Column Names:**
   - What are the exact DTC column names (case-sensitive)?
   - Are there any columns in the mapping that need adjustment?

5. **Season Code Mapping:**
   - Do you have the complete season code mapping table data?
   - How should we handle seasons not in the mapping table?

6. **Request Creation:**
   - Who should be set as the owner when creating new DTC requests?
   - Any other metadata required for new requests?

7. **Change Detection:**
   - What should be the cutoff for "modified"? (e.g., sync only if modified in last 7 days?)
   - Should we sync all historical data or only recent changes?

8. **Error Handling:**
   - What error rate is acceptable for production? (e.g., <5%?)
   - Should we halt on critical errors or continue with warnings?

9. **Performance:**
   - What's the typical number of styles to sync per day?
   - What's the acceptable runtime for the job?

10. **Image Sync Priority:**
    - When should we implement image sync? (currently deferred)
    - Is image sync a blocker for Phase 1 deployment?

---

## Next Steps

1. **User Review** - Review this plan and answer open questions
2. **Approval** - Approve plan or request changes
3. **Implementation Start** - Begin with Step 1 (Update BeProduct Pull)
4. **Iterative Development** - Complete steps 1-5 with testing
5. **UAT Deployment** - Deploy to UAT for full testing
6. **Production Go-Live** - Schedule and deploy to production

**Estimated Total Time:** 15-20 days (3-4 weeks)

---

**Plan Status:** ✅ Ready for Review  
**Last Updated:** 2026-06-09  
**Author:** Kilo AI Assistant
