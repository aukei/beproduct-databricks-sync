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
SELECT * FROM lft.beproduct.dtc_wip_ktb
WHERE brands = 'Wrangler Western'
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
  BPSEASON STRING NOT NULL,  -- BeProduct season name,   e.g. "SPRING", "FALL"
  DTCCODE  STRING NOT NULL   -- DTC season code prefix,  e.g. "SS", "FW"
)
USING DELTA
```
(`BPSEASON` was renamed from `SEASON` to avoid a case-insensitive collision with
the styles table's `season` column during the join.)

### Example Mappings (prefix only — no year)

| CUSTOMER | BPSEASON | DTCCODE | Example derivation |
|----------|--------|---------|--------------------|
| KTB | SPRING | SS | `SPRING` + `2028` -> `SS28` |
| KTB | FALL | FW | `FALL` + `2027` -> `FW27` |

**Notes**:
- The **prefix** (SS/FW/...) is **not algorithmic** and may differ between
  customers, so it **must** come from this lookup table.
- The **year** part **is** algorithmic: last 2 digits of the BeProduct year.
- Join is case-insensitive on `CUSTOMER` / `BPSEASON`; the styles `year` field is a
  STRING and may be `"N/A"` (such rows stay unmapped).
- Forward (BeProduct -> DTC): `beproduct/beproduct_to_dtc_transform.py`.
  Reverse (DTC -> BeProduct): `dtc/notebooks/pull_dtc_to_delta.py`. Same table.

---

## DTC Data Table Structure

### Pull target: `lft.beproduct.dtc_wip_<customer>`

`dtc/notebooks/pull_requests_to_delta.py` writes **one row per DTC sheet row**
across all in-scope requests for a customer into a single Delta table — e.g.
`lft.beproduct.dtc_wip_ktb` (customer lowercased). The DataFrame is built from an
**explicit `StructType`** (`FIXED_FIELDS` in the notebook), *not* inferred, so a
request with few rows / an all-NULL column does not trip Spark's
`CANNOT_DETERMINE_TYPE` error.

**Fixed columns:**

| Column | Type | Source / purpose |
|--------|------|------------------|
| `customer` | STRING | `customer` widget |
| `workspace_name` | STRING | `dtc_workspace` widget |
| `document_name` | STRING | registry `document_name` |
| `request_id` | STRING | DTC request ID |
| `request_reference` | STRING | request name, e.g. `KTB FW28 Wrangler` |
| `season_code` | STRING | registry (parsed from request name) |
| `brands` | STRING | registry (parsed from request name) |
| `row_id` | STRING | DTC `rowId` — key for UPDATE (PATCH) |
| `row_index` | LONG | DTC `rowIndex` — key for INSERT / DELETE |
| `lf_style_number` | STRING | normalized `LF Style#` (match key) |
| `color_wash` | STRING | normalized `Color / Wash` (match key) |
| `extracted_at` | TIMESTAMP | when pulled from the DTC API |
| `data_json` | STRING | full DTC row as JSON (full fidelity) |

**Dynamic columns:** every DTC view column is also flattened to
`col_<normalized_name>` as **STRING** (e.g. `Product Status` → `col_product_status`,
value stringified). Empty view columns may be absent for a given request; the
notebook aligns the union across requests by name and casts any missing column to
STRING. Untyped full fidelity always remains in `data_json`.

### Keys

**For DTC operations** (push): `row_id` → UPDATE via PATCH; `row_index` →
INSERT / DELETE (PATCH cannot mix the two — separate batches).

**In-request match key:** `(lf_style_number, color_wash)` — season & brand are
fixed per request, so they don't vary within it.

**Cross-request identity:** `(customer, season_code, brands, lf_style_number,
color_wash)`.

### Table Properties

The table carries **no `TBLPROPERTIES`** — per-request metadata lives in the row
columns above and in the control table `lft.beproduct.dtc_request_registry`
(`last_extracted`, `row_count`, etc.).

---

## Notebook Parameters

`pull_requests_to_delta.py` widgets:

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `dtc_environment` | `uat` | Environment (uat/prod); selects the `dtc_api_key_<env>` secret |
| `customer` | `KTB` | Customer; also the table suffix `dtc_wip_<customer>` |
| `dtc_workspace` | `KTB` | DTC workspace name |
| `catalog` | `lft` | Databricks catalog |
| `schema` | `beproduct` | Databricks schema |
| `write_mode` | `overwrite` | `overwrite` \| `append` |

The target table name is **derived** (`dtc_wip_<customer>`), not a parameter.
Request discovery is registry-driven (`dtc_request_registry`), so there is no
single `request_id` parameter here.

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

