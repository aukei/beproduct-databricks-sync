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
| `api.style.attributes_list(filters=…)` | `p1p7_beproduct_style_sync.py` | iterate styles in a folder (pull) |
| `api.style.attributes_get(header_id)` | `p2_push_dtc_to_beproduct.py` | read current values live for an accurate NOOP diff (Phase 2) |
| `api.style.attributes_update(header_id, fields={…}, colorways=[{"id":…,"fields":{…}}])` | `beproduct_style_push.py`, `p2_push_dtc_to_beproduct.py` | write header and/or colorway fields back |
| `api.style.app_list(header_id)` | `00_init_style_app_registry.py` | list a folder's applications (ids are folder-constant) |
| `api.style.app_get(header_id, app_id)` | `p1p7_beproduct_style_sync.py` | read one application's content (sample-app submit status) |
| `api.raw_api.get("MasterData/{fieldId}")` | `p5utl_beproduct_master_data_sync.py` | pull valid dropdown/multiselect choices (token refresh auto-handled) |
| `api.raw_api.post("MasterData/{fieldId}/Update", body=…)` | `p5utl_beproduct_master_data_sync.py` | push choice changes back (add / deactivate / rename) |
| `api.directory.directory_list()` | `p5utl_beproduct_master_data_sync.py` | paginated iterator over all directory companies |
| `api.directory.directory_contact_list(header_id=<uuid>)` | `p5utl_beproduct_master_data_sync.py` | contacts for one company |
| `api.directory.directory_add(fields=…)` | `p5utl_beproduct_master_data_sync.py` | create a new directory company |
| `api.directory.directory_contact_add(header_id, fields=…)` | `p5utl_beproduct_master_data_sync.py` | add a contact to a company |
| `api.raw_api.post("Directory/Update/{id}", body=…)` | `p5utl_beproduct_master_data_sync.py` | update existing company (SDK has no Update) |
| `api.raw_api.post("Directory/{dId}/Contact/{cId}/Update", body=…)` | `p5utl_beproduct_master_data_sync.py` | update existing contact (SDK has no Update) |

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

## 3a. Style Applications (sample submit data)

Besides attributes, a BeProduct style has **applications** ("pages"): Tech Pack, BOM,
Artboards, and **sample requests** (Proto / PreLine / SMS / Fit / PP / TOP). The sync
enriches each style with the full **submit data** of its 6 sample apps.

**Key behaviours (live-validated 2026-06-19, see `../AGENTS.md`):**

- **App IDs are constant per FOLDER, not per style** — every style in `KTB` shares
  the same 6 sample-app IDs. We cache them once with `00_init_style_app_registry`
  rather than calling `app_list` per style. (https://python.beproduct.com/075-apps/)
- **App changes are INVISIBLE to the style.** An app has its OWN `modifiedAt`,
  independent of `style.modifiedAt`; nothing in the style payload hints an app
  changed. `app.modifiedAt == "0001-01-01T00:00:00"` means the app exists but has no
  data. ⇒ the only way to read sample status is one `app_get` per (style × app), and
  the daily JOB must run Step 1 in **FULL** (INCREMENTAL would miss app-only edits).
- `app_get` for a `SampleRequestMulti` returns `data.submits[].sizes[]` each with
  `submitStatus`, `submitStatusDate`, `dueDate`, `receivedDate`, `fitDate`, plus
  `data.poms[]` (measurements). A sample app **explodes** like colorways/BOM
  (N submit rounds × M sizes), so we do NOT collapse it to a single status.
  ~1.5 s/call API latency; the sync parallelises with `app_max_workers`.
- BeProduct's "2 calls/sec" is a **minimum throughput SLA, not a cap** — 10 workers
  sustained ~7 calls/sec with no throttling.

**The 6 JSON-array columns added to `ktb_styles`** — for each app `<prefix>` ∈
`{proto, preline, sms, fit, pp, top}_sample`: a single column `<prefix>_json`
holding the full list of submit×size records
(`submit_id/name, size_id/size, is_sample_size, submit_status, submit_status_date,
due_date, received_date, fit_date`); `'[]'` when the style has no data for that app.
Stored RAW (POMs excluded) — **flattening/selection is delegated to the Step-2
transform** (same pattern as `colorways_json`), so no presentation format is
pre-baked here.

SSOT for the title→prefix map: the `SAMPLE_APPS` dict, defined identically in
`p1p7_beproduct_style_sync.py` and `00_init_style_app_registry.py`.

> DTC push of these fields is a **future step** — they currently land only in
> `ktb_styles`; the transform → staging → Phase 1 wiring is not done yet.

---

## 4. Notebooks (BeProduct side)

| Notebook | Does | Writes |
|----------|------|--------|
| `beproduct/00_init_style_app_registry.py` | Cache a folder's application IDs (run on-demand when app setup changes). | `beproduct_style_app_registry` |
| `beproduct/p1p7_beproduct_style_sync.py` | Pull styles for a folder (FULL/INCREMENTAL); extract header fields, colorways, front image; enrich with 6 sample-app submit arrays. | `ktb_styles` |
| `beproduct/p0_xts_master_to_directory_upsert.py` | **Phase 0** (live in the daily DAG as the first step, 2026-08-31). Reads `dtc_xts_master_ktb` (DTC "XTS Master" pull), dedupes by `(name, partner_type)`, MERGEs into `beproduct_directory` (`COALESCE` on every field so a NULL from XTS never destroys real data). See `PHASE0_WORKFLOW.md`. | `beproduct_directory` |
| `beproduct/p5utl_beproduct_master_data_sync.py` | Pull or push-back MasterData dropdown choices and Directory records/contacts. Four modes: `PULL_ONLY` (default), `PUSH_MASTER_DATA`, `PUSH_DIRECTORY`, `PUSH_ALL`. `dry_run=true` previews push changes without writing. **`PUSH_DIRECTORY` mode is the live daily `phase0_push` task** (Phase 0's 3rd step); other modes remain admin-only/not in the DAG. | `beproduct_master_*` (11 tables), `beproduct_directory`, `beproduct_directory_contacts` |
| `standalone/beproduct_style_push.py` | Generic Delta → BeProduct push-back of locally edited rows (`modified_at > synced_at`), type-aware. | BeProduct |

`beproduct_directory` now has TWO populators: `p0_xts_master_to_directory_upsert.py`
(Phase 0, DTC-sourced, `(name, partner_type)` match key) and
`p5utl_beproduct_master_data_sync.py` (`PULL_ONLY`, the original full
BeProduct-side pull). Both write the same table; Phase 0's upsert never
deactivates rows absent from its (much smaller) XTS Master source.

`standalone/beproduct_style_push.py` is a standalone bi-directional helper (not
part of the DTC daily pipeline; see `standalone/README.md`). The DTC-driven
pushback is Phase 2 (`dtc/notebooks/p2_push_dtc_to_beproduct.py`, see
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
- **sample-app data (6 JSON cols)**: `{proto,preline,sms,fit,pp,top}_sample_json`
  (each a JSON array of submit×size records; `'[]'` when no data). See §3a.

`refresh_mode` = `FULL` (all styles) or `INCREMENTAL` (changed only). The daily job
uses **FULL** (sample-app changes don't bump `style.modifiedAt`). Field lists
are `COMPULSORY_FIELDS` / `INTERESTED_FIELDS` in the notebook; keep them aligned
with the SSOT field file. Sample apps: `SAMPLE_APPS` dict.

### `beproduct_style_app_registry` — 1 row per (folder × application)

Cache of folder-constant application IDs, written by `00_init_style_app_registry`.
Columns: `folder_name`, `app_id`, `app_title`, `app_type`, `is_sample` (bool),
`column_prefix` (e.g. `proto_sample`, null unless sample), `registered_at`. The sync
reads `WHERE is_sample = true` to know which apps to fetch. Re-run the init notebook
only when the folder's app setup changes.

### `beproduct_master_*` — 1 row per valid choice (11 tables)

`beproduct_master_brands`, `_teams`, `_seasons`, `_years`, `_product_status`,
`_product_category`, `_product_sub_category`, `_division`, `_techpack_stage`,
`_parent_vendor`, `_factory`. (`garment_finish` excluded — free-text field, no choices.)

Columns: `field_id` (BeProduct fieldId, e.g. `brands_multi`), `value` (the choice
display string — **use this when pushing to BeProduct**), `code` (short code),
`active` (false = deactivated in BeProduct), `data_json`, `synced_at`.
Full refresh (`DROP + overwrite`) each pull run.

Validate before pushing:
```sql
WHERE brands IN (SELECT value FROM lft.beproduct.beproduct_master_brands)
```

To push choice changes back from the Delta table to BeProduct, run
`p5utl_beproduct_master_data_sync` with `mode=PUSH_MASTER_DATA`. The push is PATCH-style:
only rows present in the table are sent; absent rows are left as-is.
Optional admin columns: `update_value` (rename a choice), `delete_choice` (remove it).

### `beproduct_directory` — 1 row per vendor / factory / partner

Columns: `id` (BeProduct UUID — null = new record to add on next push),
`directory_id` (human-readable code), `name`, `partner_type` (VENDOR/FACTORY/…;
**cannot be changed after creation**), `address`, `country`, `state`, `zip`, `city`,
`phone`, `fax`, `website`, `notes`, `active`, `data_json`, `synced_at`.

**Match key is `name` + `partner_type` TOGETHER** (confirmed by the project
team, corrected 2026-08-28) — NOT `id`/`directory_id` alone and NOT `name`
alone; the same `name` legitimately recurs under a different `partner_type`
(e.g. the same company as both a Supplier and a Factory) as two separate,
valid records. Populated by both `p5utl_beproduct_master_data_sync.py`
(`PULL_ONLY`, full BeProduct-side pull) and Phase 0's
`p0_xts_master_to_directory_upsert.py` (DTC "XTS Master"-sourced, upsert
only — never deactivates rows the smaller XTS source doesn't cover).

### `beproduct_directory_contacts` — 1 row per contact

Columns: `directory_id` (parent company UUID), `contact_id` (null = new contact),
`email`, `first_name`, `last_name`, `title`, `mobile_phone`, `work_phone`, `role`,
`active`, `data_json`, `synced_at`. Email/firstName/lastName cannot be changed for
fully-registered BeProduct users.

---

## 6. Troubleshooting

| Issue | Fix |
|-------|-----|
| `401 / unauthorized_client` | Refresh token expired → update `refresh_token` secret. |
| Master data endpoint 404 | Confirm the `fieldId` / endpoint path; the notebook logs a warning and skips that field. |
| Pushed field silently blanked | MultiSelect sent as string, or value not in that field's Master Data. Use type-aware shaping + a valid value from `beproduct_master_*`. |
| Tables empty | Folder name is case-sensitive; verify `folder_name`. |
| Colorway write didn't land | Ensure `colorway_id` is present (from `colorways_json`) and use the `colorways=[{"id":…}]` form. |
| Sample-app columns all null | Run `00_init_style_app_registry` for the folder; confirm the 6 sample apps exist (`is_sample=true`). Sync falls back to `app_list` discovery + warns if the registry is missing. |
| Sample status stale / not updating | Daily job must be FULL — app edits don't bump `style.modifiedAt`, so INCREMENTAL won't re-fetch unchanged styles' apps. |
| Directory push — `partnerType` not updated | API restriction: `partnerType` is immutable after creation. Only set it on new records (null `id`). |
| Directory push — contact fields not updated | `email`/`firstName`/`lastName` cannot be changed for fully-registered users (API restriction). |
| MasterData push PATCH semantics | Only choices present in the Delta table are sent. To deactivate a choice, set `active=false`. To delete permanently, add an `delete_choice=true` column. Choices absent from the table are left unchanged in BeProduct. |
