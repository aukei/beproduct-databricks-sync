# DTC Data Model & Mapping to BeProduct

**Last Updated**: 2026-05-29

---

## DTC Data Organization

### Hierarchy

```
Workspace (e.g., "Kontoor")
  └─ Document (e.g., "KON WIP")
      └─ Requests (multiple, one per customer/season/brand)
          ├─ KON SS28 Wrangler Western
          ├─ KON SS28 Wrangler Rugged
          ├─ KON FW27 Wrangler Western
          └─ ...
              └─ Rows (247 rows per request, sheet data)
                  ├─ LFStyle# (unique identifier)
                  ├─ Product columns (114 total)
                  └─ Metadata
```

### Views (Critical Requirement)

**IMPORTANT**: All DTC requests **must have a "Full Version" view** configured.

- **Full Version view**: Contains ALL columns and ALL rows (unfiltered, complete data)
- **Other views**: May hide specific columns or filter rows (used for specific reporting needs)

**Sync Rule**: Always pull from "Full Version" view to ensure data integrity and completeness.

The sync process will **FAIL** if "Full Version" is not available, preventing partial data pulls.

---

### Request Naming Convention

Format: `<customer> <seasonCode> <brand>`

**Examples**:
- `KON SS28 Wrangler Western` → customer=KON, seasonCode=SS28, brand="Wrangler Western"
- `KON FW27 Wrangler Rugged` → customer=KON, seasonCode=FW27, brand="Wrangler Rugged"
- `KON SS28 Lee Regular` → customer=KON, seasonCode=SS28, brand="Lee Regular"

**Parsing**:
1. Split by space
2. First token = customer (e.g., "KON")
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
| KTB | KON |
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

DTC uses encoded seasonCode (e.g., "SS28") that must be mapped to BeProduct (Season, Year).

### Mapping Table Structure

Create `lft.beproduct.dtc_season_code_mapping`:

```sql
CREATE TABLE IF NOT EXISTS lft.beproduct.dtc_season_code_mapping (
  dtc_customer STRING,          -- e.g., "KON"
  season_code STRING,           -- e.g., "SS28"
  beproduct_season STRING,      -- e.g., "Spring"
  beproduct_year INT,           -- e.g., 2028
  description STRING,           -- e.g., "Spring 2028"
  created_date TIMESTAMP,
  PRIMARY KEY (dtc_customer, season_code)
)
USING DELTA
```

### Example Mappings

| DTC Customer | Season Code | BeProduct Season | BeProduct Year | Notes |
|--------------|------------|-----------------|----------------|-------|
| KON | SS28 | Spring | 2028 | Spring Summer 2028 |
| KON | FW27 | Fall | 2027 | Fall Winter 2027 |
| KON | SS26 | Spring | 2026 | Spring Summer 2026 |
| KTB | SS28 | Spring | 2028 | Customer-specific mapping |

**Notes**:
- Codes are **arbitrary and differ between customers**
- Same code (e.g., "SS28") may mean different season for different customers
- Mapping is **not algorithmic** (cannot infer from code alone)
- **Must use lookup table** with fallback handling

---

## DTC Data Table Structure

### Current: `lft.beproduct.dtc_master_chart_uat`

After implementing this clarification, table will include:

**Extraction Columns** (from request name):
- `dtc_customer`: Customer code from DTC (e.g., "KON")
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
-- workspace_name | Kontoor
-- document_name | KON WIP
-- dtc_customer | KON
-- owner_name | ...
-- owner_email | ...
```

---

## Notebook Parameters

All extraction parameters should be parameterized:

| Parameter | Example | Purpose |
|-----------|---------|---------|
| `dtc_workspace_name` | `Kontoor` | DTC workspace to access |
| `dtc_request_id` | `69f076f0b7247a661226be9a` | Which request to pull |
| `dtc_environment` | `uat` | Environment (uat/prod) |
| `dtc_customer` | `KON` | Customer code in DTC |
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
    "dtc_customer": "KON",
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
- [ ] Create seasonCode mapping table: `dtc_season_code_mapping`
- [ ] Update pull notebook to join and populate (beproduct_season, beproduct_year)
- [ ] Update change detection to use composite key
- [ ] Update change log schema to include composite_key field
- [ ] Update push to use composite key for validation
- [ ] Document mapping table initialization
- [ ] Add sample mappings for KON → KTB

---

## Reference

- **Request Name Parsing**: Implemented in `DTCConnector.get_document_metadata()` extension
- **SeasonCode Mapping**: Query `dtc_season_code_mapping` table
- **Customer Mapping**: Passed as notebook parameters

