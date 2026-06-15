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
Only the **prefix** is stored in the lookup table; the **year** part is
derived from / contributes the last 2 digits of the BeProduct `year`.

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

