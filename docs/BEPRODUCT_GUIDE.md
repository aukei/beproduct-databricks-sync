# BeProduct Component Guide

Everything the jobs need about the **BeProduct** side: the SDK/API surface used,
and the BeProduct-related tables on Databricks (`lft.beproduct`).

> Cross-platform flow & full data model: `ARCHITECTURE.md`. Field-mapping SSOT:
> `beproduct_style_interested_fields.txt`. Verified schema quirks: `../AGENTS.md`.

---

## 1. BeProduct model

Parent/child JSON PLM. A **STYLE** header links to **Colorways**, **Size**, and
**BOM** detail. One environment; data is partitioned by **Folder** (the customer,
e.g. `KTB`). A style is uniquely identified by `Customer (folder) + Brand +
LF Style# + Season`. One style has one colorway record whose `colorways[]` array
holds N color names → N DTC rows downstream.

Header fields live under `headerData.fields[]` (each `{id, value, type}`); the
front image under `headerData.frontImage.origin` (a CDN URL).

---

## 2. Connectivity (SDK + OAuth)

Jobs use the **`beproduct` Python SDK** (installed via `pip` in the notebook),
authenticated with OAuth client-credentials from the `beproduct` secret scope:

```python
from beproduct.sdk import BeProduct
api = BeProduct(
    client_id      = dbutils.secrets.get("beproduct", "client_id"),
    client_secret  = dbutils.secrets.get("beproduct", "client_secret"),
    refresh_token  = dbutils.secrets.get("beproduct", "refresh_token"),
    company_domain = dbutils.secrets.get("beproduct", "company_domain"),
)
```

- SDK docs: <https://python.beproduct.com/> · API Swagger:
  <https://developers.beproduct.com/swagger/v1/swagger.json>
- Match BeProduct fields by **`fieldId`**, not display name (names are
  inconsistently cased / have trailing spaces).

---

## 3. SDK / API surface used by the jobs

| Call | Used by | Purpose |
|------|---------|---------|
| `api.style.attributes_list(filters=…)` | `beproduct_style_sync.py` | iterate styles in a folder (pull) |
| `api.style.attributes_get(header_id)` | `05_push_dtc_to_beproduct.py` | read current values live for an accurate NOOP diff (Phase 2) |
| `api.style.attributes_update(header_id, fields={…}, colorways=[{"id":…,"fields":{…}}])` | `beproduct_style_push.py`, `05_push_dtc_to_beproduct.py` | write header and/or colorway fields back |
| `GET /api/{company}/MasterData/{fieldId}` | `beproduct_master_data_sync.py` | pull valid dropdown/multiselect values |

### Push-back is type-aware (MultiSelect vs DropDown)

Each value is shaped to the field's **type** (read from `headerData.fields[].type`):

| Type | Example | Stored in Delta | Sent to BeProduct |
|------|---------|-----------------|-------------------|
| MultiSelect | `BRANDS`, `CUSTOMER` | single string `Wrangler` | one-element array `["Wrangler"]` |
| DropDown | `PRODUCT STATUS` | string `Proto` | string `Pre-Line` |
| Text / other | `DESCRIPTION` | string | string |

> Sending a MultiSelect value as a bare string makes BeProduct **silently blank**
> the field — type-aware shaping matters. MultiSelect values are also read as
> `value[0]` (plain-string list), not `[0].value`; guard the empty list.
> Dropdown/MultiSelect writes must use values that exist in that field's Master
> Data, or BeProduct silently blanks them.

### Colorway-level writes (Phase 2 `Lot#`)

`Lot Code` is a **colorway** field, fieldId **`drawing_number_walmart`** (the id is
misleading; the header no longer defines it). Colorway writes need the colorway
**id**, so the transform carries `colorway_id` into staging and pushback groups one
`attributes_update` per style with a `colorways=[{"id":…, "fields":{…}}]` list.

---

## 4. Notebooks (BeProduct side)

| Notebook | Does | Writes |
|----------|------|--------|
| `beproduct/beproduct_style_sync.py` | Pull styles for a folder (FULL/INCREMENTAL); extract header fields, colorways, front image. | `ktb_styles` |
| `beproduct/beproduct_master_data_sync.py` | Pull valid values for 12 dropdown/multiselect fields. | `beproduct_master_*` |
| `standalone/beproduct_style_push.py` | Generic Delta → BeProduct push-back of locally edited rows (`modified_at > synced_at`), type-aware. | BeProduct |

`standalone/beproduct_style_push.py` is a standalone bi-directional helper (not
part of the DTC daily pipeline; see `standalone/README.md`). The DTC-driven
pushback is Phase 2 (`dtc/notebooks/05_push_dtc_to_beproduct.py`, see
`PHASE2_WORKFLOW.md`).

---

## 5. BeProduct data model on ADB

All under `lft.beproduct`.

### `ktb_styles` — 1 row per style

Extracted header fields (fieldId → column): `lf_style_number` (`header_number`),
`description`, `team`, `season`, `year`, `product_status` (`style_status`),
`customer_style_number`, `product_category`, `product_sub_category`, `division`,
`brands` (`brands_multi`), `garment_finish`, `techpack_stage`, `lot_code`,
`parent_vendor`, `factory`.

Plus:
- `id` — BeProduct style/header id
- `colorways_array` (ARRAY<STRING>), `colorways_count`
- `colorways_json` — `[{colorway_id, color_name, color_number}, …]` (carries the
  colorway **id** needed for Phase 2 colorway writes)
- `front_image_url` — `headerData.frontImage.origin`
- `data_json` — full raw record (full fidelity)
- change tracking: `modified_at`/`last_modified`, `synced_at`/`extracted`,
  `created_at`

`refresh_mode` = `FULL` (all styles) or `INCREMENTAL` (changed only). Field lists
are `COMPULSORY_FIELDS` / `INTERESTED_FIELDS` in the notebook; keep them aligned
with the SSOT field file.

### `beproduct_master_*` — 1 row per valid value (12 tables)

`beproduct_master_brands`, `_teams`, `_seasons`, `_years`, `_product_status`,
`_product_category`, `_product_sub_category`, `_division`, `_techpack_stage`,
`_garment_finish`, `_parent_vendor`, `_factory`. Columns: `value` (id, use when
pushing), `label` (display), `data_json`, `synced_at`. `mode("overwrite")` each run.

Validate before pushing:
```sql
WHERE brands IN (SELECT value FROM lft.beproduct.beproduct_master_brands)
```

---

## 6. Troubleshooting

| Issue | Fix |
|-------|-----|
| `401 / unauthorized_client` | Refresh token expired → update `refresh_token` secret. |
| Master data endpoint 404 | Confirm the `fieldId` / endpoint path; the job logs a warning and skips. |
| Pushed field silently blanked | MultiSelect sent as string, or value not in that field's Master Data. Use type-aware shaping + a valid value. |
| Tables empty | Folder name is case-sensitive; verify `folder_name`. |
| Colorway write didn't land | Ensure `colorway_id` is present (from `colorways_json`) and use the `colorways=[{"id":…}]` form. |
