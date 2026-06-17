# Agent guide — BeProduct ⇄ DTC sync

Read this first. It is the durable memory for this repo so each new change can build
on prior **verified** discoveries instead of re-deriving them.

## What this repo does

Bi-directional sync between **BeProduct** (style PLM) and **DTC** ("Data Collab"
sheets), staged through **Databricks/Delta**.

- **Phase 1 — BeProduct → DTC** (`dtc/PHASE1_WORKFLOW.md`): push BeProduct-owned
  style fields into the matching DTC request (upsert).
- **Phase 2 — DTC → BeProduct** (`dtc/PHASE2_WORKFLOW.md`): push DTC-owned fields
  back into the BeProduct style.

Each field syncs **one way only** (no loops). Direction table below.

## Single source of truth (SSOT) for field mapping

**`docs/beproduct_style_interested_fields.txt`** — DTC column ⇄ BeProduct field,
fieldId, JSONPath, sync direction. Always update it first, then the code constants.

### Where a field mapping lives in code (edit ALL that apply, together)

| Direction | Constant | File |
|-----------|----------|------|
| BeProduct → DTC | `FIELD_MAPPING` | `dtc/python/sync/phase1.py` |
| DTC → BeProduct (header) | `REVERSE_HEADER_FIELDS` | `dtc/python/sync/phase2.py` |
| DTC → BeProduct (colorway) | `REVERSE_COLORWAY_FIELDS` | `dtc/python/sync/phase2.py` |
| DTC → BeProduct (no target yet) | `UNSUPPORTED_FIELDS` | `dtc/python/sync/phase2.py` |
| BeProduct extraction (raw → master) | `COMPULSORY_FIELDS` / `INTERESTED_FIELDS` | `beproduct/beproduct_style_sync.py` |
| BeProduct → DTC transform (denormalize) | `FIELD_MAPPING` + staging `select` | `beproduct/beproduct_to_dtc_transform.py` |

Then update unit tests: `dtc/tests/test_phase1.py`, `dtc/tests/test_phase2.py`.

### Current direction partition

- **BeProduct → DTC**: Product Status, Style Description, Class, Sub Class, Division,
  Brand, Garment Finish, Tech Pack Stage, Fabric Group, Placement.
- **DTC → BeProduct**: Legacy Code (`customer_style_number`), Main Vendor (Sampling)
  (`parent_vendor`), Main Factory (Sampling) (`factory`) [header];
  Lot# (`drawing_number_walmart`) [colorway]; Main Factory Customer ID (no target → skipped).
- **Keys** (match, not overwritten): LF Style# (`header_number`), Color/Wash (`colorName`).
- **Excluded**: Style Image.

## Ground rules / invariants

1. **Validate live before/while coding.** Local creds in `.env`
   (`BEPRODUCT_*`, `DATABRICKS_*`); on Databricks use secret scope `beproduct`
   (`client_id`, `client_secret`, `refresh_token`, `company_domain`,
   `dtc_api_key_<env>`). There is a sacrificial in-scope DTC request for reversible
   write tests: `KTB FW26 Wrangler` (UAT request `6a26581854e92e7acd8fa71b`).
2. **Match BeProduct fields by `fieldId`, not display name** (names are inconsistently
   cased / have trailing spaces).
3. **One field, one direction.** Never add a field to both `phase1.FIELD_MAPPING` and
   `phase2.REVERSE_*`.
4. **Notebooks can't run locally** (Spark/dbutils). Put deterministic logic in
   `dtc/python/sync/phase1.py` / `phase2.py` (pure-Python, unit-tested); keep
   notebooks as thin Spark/IO wrappers.
5. **In-request match key = (LF Style#, Color/Wash)**; season+brand are fixed per
   request. Brand is one-per-request and agrees with the request name.

## Verified discoveries log (append-dated; do not delete)

**DTC API (validated 2026-06-17):**
- Sheet upsert: `PATCH /v1/sheets/{sheetId}/views/{viewId}` body
  `{"sheetData":[{...,"rowId"|"rowIndex":..}]}` → 204. A single PATCH **cannot mix**
  rowId (update) and rowIndex (insert) — send separate batches.
- Row delete EXISTS: `DELETE /v1/sheets/{sheetId}/views/{viewId}/rows` body
  `{"rowIndexes":[...]}` → 204 (keys off rowIndex, not rowId).
- Request listing works via `GET /v1/requests` with `workspaceName`+`filters` in the
  **request body** (not query params; query param → 400 "Invalid workspaceName").
- Registry refresh is the shared `sync.registry.refresh` (used by
  `00_init_request_registry`, `pull_requests_to_delta`, `dtc_request_manager`):
  `search_requests(workspace, document_name=document, filters={"requestIsActive":"Y"})`
  lists **active** requests (server-side filter — DTC dev confirmed inactive requests
  400 on get-by-id; field is `requestIsActive`). A client-side `is_active_item` guard
  remains as cheap insurance. It then
  **pre-filters on the listed `requestReference` so only ACTIVE + IN-SCOPE names are
  read/registered** by-id (`get_request`/`get_views`). Inactive and
  out-of-scope/foreign requests are skipped entirely — NOT enriched, NOT registered —
  which is why `get_request` is never called on them (they were the HTTP 400 noise).
  Explicit
  `request_ids` are read by-id without the pre-filter. Upsert `mode=merge` preserves
  `last_extracted`/`last_pushed`/`row_count`; `replace` wipes them. After a full
  auto-discover (non-empty listing) it **reconciles**: registry rows in the scanned
  scope (`environment`+`customer`+`document`) absent from the scan are **marked**
  `request_is_active='N'`/`in_scope=false` (mark, not delete — sync state survives,
  re-discovery flips them back). So the registry doesn't keep stale inactive/
  out-of-scope rows. Skipped for explicit `request_ids` and empty listings.
- Allowed columns must come from the **view definition** (`GET /v1/views/{viewId}` →
  178 dynamicFields), NOT from sheet cells (empty columns don't appear in `sheetData`,
  which previously caused false "missing column" findings). `WIP_ITS_USE` column
  `Division` was once `Division?` (the `?` was removed).

**BeProduct schema (KTB folder, validated 2026-06-17 via `folder_schema` /
`folder_colorway_schema`):**
- `BRANDS` (`brands_multi`) and `CUSTOMER` (`sold_to_customer`) are MultiSelect
  **lists of plain strings** → read `value[0]` (NOT `[0].value`); guard empty list.
- `Lot Code` is a **colorway** field, fieldId **`drawing_number_walmart`** (the id is
  misleading). Header no longer defines it; ignore any legacy
  `headerData.fields[id="drawing_number_walmart"]`. Filled by DTC → BeProduct.
- Pushback uses `api.style.attributes_update(header_id, fields={fieldId:val},
  colorways=[{"id":colorway_id,"fields":{fieldId:val}}])` — colorway writes need the
  colorway **id**, so the transform carries `colorway_id` into staging
  (`colorways_json` from `beproduct_style_sync`).

## Decisions on record

- **Phase 1 now CREATES missing in-scope DTC requests (2026-06-17, reverses prior
  scope).** The earlier rule "Phase 1 does NOT create requests; the project team
  pre-creates them" is superseded. `dtc_request_manager` creates missing **in-scope**
  requests (`connector.create_sheet` → `POST /v1/sheets`) in `dtc_document`, then
  re-scans + resolves. Guardrails: only in-scope names (`<customer> <seasonCode>
  <brand>`; brand-less names → `NOT_IN_SCOPE`, never created); gated by `dry_run`
  (default true = preview only). The registry scan is the shared
  `sync.registry.refresh` (discover → enrich → merge), invoked automatically by
  `pull_requests_to_delta` and `dtc_request_manager` (default `refresh_registry=true`)
  and standalone by `00_init_request_registry`. NOTE: `create_sheet` is a live,
  not-easily-reversible write that still needs a UAT validation run; there is no
  delete-request in the connector.
- **Legacy change-tracking pipeline removed (2026-06-17).** The old single-table
  `dtc_master_chart_uat` snapshot/change-log flow (`pull_dtc_to_delta`,
  `01_create_sync_tables`, `02_create_snapshot`, `03_detect_changes`,
  `04_push_changes`, `sync/snapshot.py`, `sync/change_detection.py`,
  `CHANGE_TRACKING_DESIGN.md`) was deleted from the repo and the Databricks
  workspace. Current model: registry-driven pull of the `WIP_ITS_USE` view into
  `dtc_wip_<customer>` + Phase 1/2. The season mapping is forward-only (Phase 2
  doesn't reverse-map season). `.kilo/skill/dtc-integration/SKILL.md` still contains
  disclaimed legacy examples behind a "superseded" banner.
- Moved-key orphan (style's key changed → row belongs to a different request): mark the
  stale DTC row `Product Status = "(removed)"` (invalid BeProduct value, user signal);
  do not delete. Only mark keys that moved to another request.
- `Main Factory Customer ID` has no BeProduct target → skipped + logged until a
  fieldId is provided.
- DTC blanks do not clear BeProduct unless `push_blanks=true`.

## Commands

```bash
python3 dtc/tests/test_phase1.py        # Phase 1 core unit tests
python3 dtc/tests/test_phase2.py        # Phase 2 core unit tests
python3 dtc/tests/test_phase1_live.py   # live reversible DTC write test (needs UAT)
python scripts/upload_notebooks.py --dry-run   # preview Databricks notebook upload
python scripts/upload_notebooks.py             # deploy notebooks to Databricks
```
