# Agent guide — BeProduct ⇄ DTC sync

Read this first. It is the durable memory for this repo so each new change can build
on prior **verified** discoveries instead of re-deriving them.

## What this repo does

Bi-directional sync between **BeProduct** (style PLM) and **DTC** ("Data Collab"
sheets), staged through **Databricks/Delta**.

- **Phase 1 — BeProduct → DTC** (`docs/PHASE1_WORKFLOW.md`): push BeProduct-owned
  style fields into the matching DTC request (upsert); create + share missing
  in-scope requests.
- **Phase 2 — DTC → BeProduct** (`docs/PHASE2_WORKFLOW.md`): push DTC-owned fields
  back into the BeProduct style.
- **Phase 3 — BeProduct → DTC image** (`docs/PHASE3_WORKFLOW.md`): upload the front
  image into the DTC "Style Image" cell (binary, separate step).
- **Phase 7 — BeProduct → DTC sample history**: push BeProduct sample-app submit
  history (all 6 apps: Proto/PreLine/SMS/Fit/PP/TOP) into the matching DTC status
  columns as JSON lists of `[submit_name, submitStatus, submitStatusDate]`.
- **Phase 8a — DTC FABRIC → Delta**: pull `"KTB FABRIC"` document sheets (Adoption=Y
  rows only) into `lft.beproduct.dtc_fabric_ktb` + `dtc_fabric_registry`. Runs as an
  independent parallel task in the DAG (`pull_fabric_dtc`, gated by `run_phase8a`).
- **Phase 8b** (planned): upsert adopted fabric rows into BeProduct Material Master.

Each field syncs **one way only** (no loops). Direction table below.
Components, data flow, and the full ADB data model: `docs/ARCHITECTURE.md`.
The whole pipeline runs as a **multi-task Databricks job** (`BeProduct_DTC_sync_dag`,
job 294837488757511), defined in `scripts/deploy_job.py`. The old single-notebook
orchestrator `beproduct/orchestrate_sync.py` is retired (kept as a manual fallback).

## Single source of truth (SSOT) for field mapping

**`docs/beproduct_style_interested_fields.txt`** — DTC column ⇄ BeProduct field,
fieldId, JSONPath, sync direction. Always update it first, then the code constants.

### Where a field mapping lives in code (edit ALL that apply, together)

| Direction | Constant | File |
|-----------|----------|------|
| BeProduct → DTC | `FIELD_MAPPING` | `dtc/python/sync/phase1.py` |
| BeProduct → DTC (image only) | `STYLE_IMAGE_COL` + `compute_image_uploads` | `dtc/python/sync/phase3.py` |
| DTC → BeProduct (header) | `REVERSE_HEADER_FIELDS` | `dtc/python/sync/phase2.py` |
| DTC → BeProduct (colorway) | `REVERSE_COLORWAY_FIELDS` | `dtc/python/sync/phase2.py` |
| DTC → BeProduct (no target yet) | `UNSUPPORTED_FIELDS` | `dtc/python/sync/phase2.py` |
| BeProduct extraction (raw → master) | `COMPULSORY_FIELDS` / `INTERESTED_FIELDS` | `beproduct/beproduct_style_sync.py` |
| BeProduct sample-app status (title → column prefix) | `SAMPLE_APPS` | `beproduct/beproduct_style_sync.py` + `beproduct/00_init_style_app_registry.py` |
| BeProduct sample submits → DTC (Phase 7) | `SAMPLE_SUBMIT_FIELDS` + `format_sample_field` | `dtc/python/sync/samples.py` (+ `phase1.FIELD_MAPPING`, transform staging) |
| BeProduct → DTC transform (denormalize) | `FIELD_MAPPING` + staging `select` | `beproduct/beproduct_to_dtc_transform.py` |

Then update unit tests: `dtc/tests/test_phase1.py`, `dtc/tests/test_phase2.py`,
`dtc/tests/test_phase3.py`, `dtc/tests/test_samples.py`.

### Current direction partition

- **BeProduct → DTC**: Product Status, Style Description, Class, Sub Class, Division,
  Brand, Garment Finish, Tech Pack Stage, Fabric Group, Placement, Gender (pending DTC col);
  BP Style# (new match key, pending DTC col), LF Style# (optional), Legacy Code (optional);
  Supplier (default-fill "Supplier" when blank; pending DTC col).
  **Filter**: styles with Product Status = "Finalized" are excluded from staging/DTC sync.
- **BeProduct → DTC (Phase 7, sample submit history)**: All 6 apps now mapped.
  Proto → "Proto Sample - Sample Status", PreLine → "Pre-line Sample - Status",
  SMS → "SMS - Sample Status", Fit → "1st Fit Sample Approval Status",
  PP → "2nd Fit Sample Approval Status", TOP → "TOP Sample Approval Status".
  Each value is a JSON list of `[submit_name, submitStatus, submitStatusDate]`
  (first size per submit). All 6 DTC columns confirmed in the 198-field view
  (2026-07-07). Note: "Pre-line Sample - Status" uses lowercase 'l' and dash.
- **DTC → BeProduct**: Main Vendor (Sampling) (`parent_vendor`), Main Factory
  (Sampling) (`factory`) [header]; Lot# (`drawing_number_walmart`) [colorway];
  Main Factory Customer ID (no target → skipped).
  [REMOVED Phase 6]: "Legacy Code" DTC→BP. "Customer Style#" DTC column not created.
- **Keys** (match, not overwritten): BP Style# (`header_number`), Color/Wash (`colorName`)
  [in-request]; [Customer, BP Style# (header_number), SeasonCode, Brand (brand_hk)]
  [composite/request-routing].
- **BeProduct → DTC, image only (Phase 3)**: Style Image (`front_image_url` →
  DTC "Style Image"). Binary, so it does NOT ride the Phase 1 sheetData PATCH;
  uploaded by `beproduct_to_dtc_images` via the multipart images endpoint, only
  when the DTC cell is blank and BeProduct has a valid `front_image_url`. Still
  one-directional (never read back to BeProduct, never in `phase2.REVERSE_*`).

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
5. **In-request match key = (BP Style#, Color/Wash)**; season+brand are fixed per
   request. Brand is one-per-request and agrees with the request name. (Phase 6:
   was (LF Style#, Color/Wash). Composite key uses brand_hk, not brands_multi.)

## Verified discoveries log (append-dated; do not delete)

**BeProduct Directory (validated 2026-06-23):**
- `api.directory.directory_list()` returns 3852 records (vendors, factories, mills).
  `api.directory.directory_contact_list(header_id)` returns 0 contacts for ALL records —
  confirmed by the org: no BeProduct user accounts have been attached to any
  mill/factory/supplier. `beproduct_directory_contacts` therefore stays empty and
  `fetch_contacts` widget defaults to `false` (skips the extra ~3800 API calls).
  Do NOT assume contacts exist when reasoning about Directory data.
- `beproduct_master_parent_vendor` and `beproduct_master_factory` each contain 3852
  choices — same count as Directory records (they are the flattened choice lists
  derived from the same partner database).
- Directory pull throughput: ~2 records/sec via `directory_list()`, ~30 min for 3852
  records. Pull is `PULL_ONLY` mode only. `PUSH_DIRECTORY` skips the pull and uses
  change detection (`modified_at > extracted_at`) to push only pending rows.

**DTC API (validated 2026-06-17):**
- Sheet upsert: `PATCH /v1/sheets/{sheetId}/views/{viewId}` body
  `{"sheetData":[{...,"rowId"|"rowIndex":..}]}` → 204. A single PATCH **cannot mix**
  rowId (update) and rowIndex (insert) — send separate batches.
- Row delete EXISTS: `DELETE /v1/sheets/{sheetId}/views/{viewId}/rows` body
  `{"rowIndexes":[...]}` → 204 (keys off rowIndex, not rowId).
- Cell image upload (Phase 3, **LIVE-VALIDATED 2026-06-17**, 41 uploads OK):
  `POST /v1/sheets/{sheetId}/views/{viewId}/images?rowindex={int}&columnname=Style Image`
  with the image bytes as a **multipart/form-data** file part (binary). Operates
  on an EXISTING row (keys off lowercase `rowindex`), so it must run AFTER Phase 1
  creates the rows. The JSON sheetData PATCH cannot set images. **Confirmed**: the
  multipart file PART NAME is **`file`**, `columnname="Style Image"` and the
  `rowindex` query param all work (jpg + png uploaded successfully). Connector:
  `DTCConnector.upload_row_image` → `RestClient.post_multipart` (the only client
  method that sends `files=`/`params=` and does NOT force `application/json`).
  **DTC REJECTS `webp` with HTTP 400** (3/3 webp rows failed; jpg/png accepted).
  `beproduct_to_dtc_images` now classifies via `phase3.classify_image_type` and
  **transcodes webp/gif/bmp/tiff → PNG (Pillow) before upload**; jpg/png upload
  as-is; vector/unknown (svg, pdf, ...) are skipped with `unsupported_type`.
  Separately, a subset of BeProduct CDN `frontImage.origin` URLs return **HTTP 403**
  on download (Azure SAS auth — "signature" error) even within the SAS validity
  window; affects only some files (36 png succeeded, 53 png failed), so it is a
  per-file BeProduct CDN/SAS issue, NOT a Phase 3 code defect. Still open.
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

**BeProduct schema Phase 6 structural changes (validated live 2026-06-26):**
- `header_number` field **renamed** in BeProduct from "LF Style Number" → "BP Style Number".
  Field_id and data values **unchanged**. This field is the intended new match key for DTC
  (target DTC column "BP Style#"). Staging column renamed `lf_style_number` → `bp_style_number`.
- New field `lf_style_number` (field_id: `lf_style_number`, display name "LF STYLE NUMBER"):
  completely separate from `header_number`. Currently `None` for all checked styles.
  Will be populated by upstream integration. Pushed to DTC's **existing** "LF Style#" column.
- New field `brand_hk` (field_id: `brand_hk`, display name "Brand"): single-value string.
  Used as the Brand key in the composite key `[Customer, BP Style#, SeasonCode, Brand]`.
  Replaces `brands_multi` for request-routing and scope checks. `brands_multi` ("BRANDS")
  is retained in ktb_styles as non-key metadata.
- Ignore field_id `brand` (display name "BRAND") — legacy, not the composite key Brand.
- Composite key target: `[Customer, BP Style# (header_number), SeasonCode, Brand (brand_hk)]`
- In-request match key target: `("BP Style#", "Color / Wash")` (was `("LF Style#", ...)`).
- **No new "Legacy Code" BeProduct field.** The existing `customer_style_number` field
  (CUSTOMER STYLE NUMBER / PLM #) is what DTC calls "Legacy Code". It will be populated by
  upstream integration and flows BP→DTC (optional, sent when non-blank).
  Previously "Legacy Code" was DTC→BP; that role now belongs to the new DTC "Customer Style#".
- `customer_style_number` will also be populated by upstream. Both `lf_style_number` and
  `customer_style_number` are upstream-filled; the sync reads them from BP and writes to DTC.

**DTC WIP view columns — status (confirmed by scanning all 75 active KTB requests, 2026-07-02):**
  Columns **NOT YET IN VIEW** (DTC admin must add; sync code is ready but dormant):
  - `"BP Style#"` — **BLOCKER**: MATCH_KEY_COLS is already `("BP Style#", ...)` in code; sync will
    produce `missing_bp_style` exceptions for all rows until DTC creates this column and the existing
    "LF Style#" data is migrated to "BP Style#" by the DTC admin.
  - `"Gender"` — Phase 1 mapping ready in `FIELD_MAPPING`; dormant until DTC adds column.
  - `"Supplier"` — Phase 1 `DEFAULT_FILL_COLS` logic ready; dormant until DTC adds column.
  Columns **DECIDED NOT TO CREATE**:
  - `"Customer Style#"` — removed from `phase2.REVERSE_HEADER_FIELDS` (2026-07-02). No DTC→BP
    path for `customer_style_number`. "Legacy Code" (BP→DTC only) is the only direction.
  Columns **EXIST IN VIEW** (confirmed with data in ≥7 of 75 KTB requests, just blank on fresh ones):
  - `"Legacy Code"` (9 rows), `"Garment Finish"` (69), `"Tech Pack Stage"` (141),
    `"Main Vendor (Sampling)"` (46), `"BP Style#"` (0 — in view def, all blank),
    `"Gender"` (0 — in view def, all blank), `"Supplier"` (2 rows).
    All confirmed via `GET /v1/views/69f04983501f3d9cf4fc379c` → **194 dynamicFields**
    (2026-07-03, with proxy). The earlier view ID `6a3907f6df772fd797ee5b7c` is a
    different document ("XTS Master", 8 fields) — do not use for KTB WIP.
    `allowed_cols` in push notebook now **UNIONs** data-scan result with `FALLBACK_COLS` to avoid
    silently dropping these columns on fresh sheets where they're blank.

**BeProduct Style Applications (sample status, validated 2026-06-19):**
- A style has **applications** (`api.style.app_list(header_id)` →
  `[{id,title,type}]`; `api.style.app_get(header_id, app_id)` → content). The 6
  SAMPLE apps are Proto / PreLine / SMS / Fit / PP / TOP Sample (type
  `SampleRequestMulti`). KTB-folder sample app IDs: Proto `a765845f-…`,
  PreLine `8979ea71-…`, SMS `91094294-…`, Fit `a5a51c66-…`, PP `e5b7564d-…`,
  TOP `ca05cf47-…` (verified identical across styles).
- **App IDs are constant per FOLDER, not per style** (confirmed across two styles,
  23/23 ids matched). Cache them once via `00_init_style_app_registry` →
  `beproduct_style_app_registry`; do NOT call `app_list` per style.
- **`app.modifiedAt` is INDEPENDENT of `style.modifiedAt`.** Editing a sample app
  does NOT bump the style; editing attributes does NOT bump the app. Nothing in the
  style payload hints an app changed. So there is **no incremental shortcut** — the
  only way to read sample status is one `app_get` per (style × app). `app.modifiedAt
  == "0001-01-01T00:00:00"` = app exists, no data.
- `app_get` (`SampleRequestMulti`) → `data.submits[].sizes[]` each with
  `submitStatus`, `submitStatusDate`, `dueDate`, `receivedDate`, `fitDate`; plus
  `data.poms[]` (measurements). A sample app **explodes** like colorways/BOM
  (N submit rounds × M sizes) — do NOT collapse it to one status.
- `beproduct_style_sync` enriches `ktb_styles` with **6 JSON-array columns**:
  `{proto,preline,sms,fit,pp,top}_sample_json` — each the full list of submit×size
  records (`submit_id/name, size_id/size, is_sample_size, submit_status,
  submit_status_date, due_date, received_date, fit_date`), `'[]'` when no data.
  Stored RAW (POMs excluded); flattening/selection is **delegated to the Step-2
  transform** (mirrors `colorways_json`) so we don't pre-bake an unspecified format.
  One write — enrichment runs in parallel (`app_max_workers`, default 10) BEFORE the
  single DataFrame build. SSOT title→prefix map = `SAMPLE_APPS` (duplicated in the
  sync + init notebooks; keep aligned).
- **Daily job runs Step 1 in FULL** (set in `scripts/deploy_job.py`) precisely
  because app edits don't bump `style.modifiedAt`; INCREMENTAL would miss them.
  INCREMENTAL stays for ad-hoc developer runs via the ADB portal.
- BeProduct "2 calls/sec" is a **minimum-throughput SLA, not a cap** — 10 workers
  sustained ~7 calls/sec, no throttling. ~1.5 s genuine latency per `app_get`.
  146 KTB styles × 6 apps = 876 calls ≈ ~120 s at 10 workers (the full-scan cost
  added to Step 1). Only ~15 of 146 styles currently hold any sample data.
- Good verification candidates (rich sample data): `Iris - Test- Top-111` (Proto
  Approved + Fit Requested + TOP Approved), `Boy  Short Sleeve Tee` (Proto+PP),
  `LFBP-1WTP0003` (Proto+SMS), `HOODED-K263` (Proto, 27 POMs).
- DTC push of sample status is **wired for all 6 apps (Phase 7)**: Proto/PreLine/SMS/Fit/PP/TOP
  → DTC status columns via `sync.samples.format_sample_field` (transform UDF) +
  `phase1.FIELD_MAPPING`. Each DTC cell gets a JSON list of `[submit_name,
  submitStatus, submitStatusDate]` (first size per submit). All 6 DTC columns confirmed in
  the 198-field view (2026-07-07). Note: "Pre-line Sample - Status" uses lowercase 'l' and dash.

**DTC FABRIC document (KTB, validated 2026-07-16):**
- Document "KTB FABRIC", Workspace "KTB". **39 active requests** in two patterns:
  - DEV sheets:  `"KTB <season> <brand> - DEV"` — master development sheet per brand
  - Mill sheets: `"KTB <season> <brand>-<MILLCODE>"` — mill-specific allocation sheets
- **WIP_ITS_USE view**: id `6a0ac943fedfa0ca7ff2bf48`, **119 dynamicFields**.
  Same view name as WIP document; different per-request view ID.
- **Adoption (Y/N)**: filter field. 0 adopted rows in UAT as of 2026-07-16 (UAT data
  not yet populated). Phase 8a code filters to Adoption=Y at pull time; logic is ready.
- **Key staging columns** (DTC field → Delta column):
  `"ITS_Key"` → `its_key` (system key; proxy for future "LF MATERIAL ID"),
  `"Mill Fabric Article #"` → `mill_fabric_code`, `"Mill Name"` → `mill_name`,
  `"Material Class"` → `material_class`, `"Fabric Type"` → `fabric_type`,
  `"Fabric Content"` → `fabric_content` (MATERIAL DESCRIPTION proxy),
  `"KB Fabric Code (SAP Code)"` → `kb_fabric_code`.
- **"LF MATERIAL ID" and "MATERIAL DESCRIPTION"** NOT yet in the view — DTC admin
  must add before Phase 8b can map to BeProduct Material Master.
- **Phase 8a Delta tables**: `dtc_fabric_<customer>` (Adoption=Y rows) +
  `dtc_fabric_registry` (request metadata, same structure as dtc_request_registry).
- **Job task**: `pull_fabric_dtc` gated by `run_phase8a=true`; runs in parallel with
  the WIP chain after `wait_cluster`. Notebook: `dtc/notebooks/pull_fabric_to_delta.py`.
- Do NOT use view ID `6a0ac943fedfa0ca7ff2bf48` for WIP document operations —
  that ID belongs to FABRIC only.

## Decisions on record

- **`create_sheet` payload fix (2026-06-17): `requestName` → `requestReference`.**
  The original `POST /v1/sheets` payload used the key `requestName`; the DTC API
  uses `requestReference` everywhere (GET responses, search filters, `get_request`
  return values). Sending the wrong key caused 400 on all create attempts. Fixed in
  `DTCConnector.create_sheet` (`dtc/python/connectors/dtc.py`). Also added
  `RestClient._log_error_body` which logs the API response body on any HTTP error so
  future 4xx failures are self-diagnosable without a debugger.
  Status: code fix applied; UAT validation still required.
- **Phase 3 image sync is a separate post-Phase-1 step (2026-06-17).** Style Image
  is binary and cannot ride the Phase 1 JSON sheetData PATCH, and its endpoint
  targets an EXISTING row by `rowindex`, so it cannot run at insert time. New
  notebook `beproduct/beproduct_to_dtc_images.py` runs AFTER `beproduct_to_dtc_push`:
  it re-reads each in-scope sheet live (freshest rowIndex + Style Image state),
  and for every row whose cell is blank AND whose BeProduct staging row has a
  valid `front_image_url`, downloads the CDN image and POSTs it to the multipart
  images endpoint (`columnname="Style Image"`). Pure decision logic in
  `dtc/python/sync/phase3.py` (`compute_image_uploads`, unit-tested in
  `test_phase3.py`); idempotent (already-imaged rows skipped). The orchestrator
  (`orchestrate_sync.py`) runs it as Step 8, gated by `run_phase3`, preceded by a
  Step 7 re-pull of `dtc_wip_<customer>` so the table reflects Phase 1 inserts.
  **Validated live 2026-06-17** (UAT full run): 41 uploads succeeded; open
  follow-ups are DTC rejecting `webp` (400) and a subset of BeProduct CDN URLs
  failing download with 403 (SAS auth) — both data/CDN-side, not core-logic bugs.
  webp is now auto-transcoded to PNG (`phase3.classify_image_type` + Pillow).
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
  not-easily-reversible write; there is no delete-request in the connector.
  **`POST /v1/sheets` body shape VALIDATED LIVE 2026-06-18 (HTTP 201)** after the
  first prod attempt 400'd on all 4 requests. The correct body is
  `{workspaceName, documentName, requestReference (NOT requestName),
  requestDescription (MUST be non-empty), viewName,
  requestAssigneeSharingViewNames:[], sheetData:[]}`. Omitting the two arrays
  crashes the server with 400 "Cannot read properties of undefined (reading
  'map')"; empty arrays are accepted. Optional: `requestStatusName`
  (e.g. "Factory Allocation"), `requestAssigneeEmail`. Success response nests ids
  under `data` with a CAPITAL-S `SheetId`: `{"data":{"requestId":..,"SheetId":..}}`
  — `create_sheet` now normalises this to flat `{requestId, sheetId, raw}`.
- **Request sharing VALIDATED LIVE 2026-06-18 (HTTP 201).** A freshly created
  request grants FULL rights to its CREATOR (the API identity) ONLY; the data is
  invisible to the team until SHARED. `POST /v1/requests/{requestId}/shares/{userEmail}`
  and `POST /v1/requests/{requestId}/shares/usergroups/{userGroupName}` (path
  segments URL-encoded — group names have spaces), body
  `{"viewNames":[...],"message":"...","sendEmail":"Y"|"N"}` → 201 (empty body).
  Read current shares via `GET .../shares` and `.../shares/usergroups` (used for
  idempotency). Connector: `share_request_with_user`, `share_request_with_usergroup`,
  `get_request_shares`, `get_request_share_usergroups`. Project policy: share ALL
  views with `aiagentwip@lifung.com` (AI Agent WIP) and the **Full Version** view
  with the **Fabric Group** user group. Applied automatically by
  `dtc_request_manager` at create time (`share_on_create=true`, `send_email=N`)
  and backfillable via the idempotent `beproduct/dtc_share_requests` notebook.
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
  fieldId is provided. Live schema check (2026-06-18): `factory_id_no` is a
  read-only LabelText formula (`[factory]`); `customer_supplier_id` is a DropDown
  with only 2 predefined choices — neither is a suitable writable target.
- DTC blanks do not clear BeProduct unless `push_blanks=true`.
- `customer_style_number` / DTC "Legacy Code" direction: **CHANGED in Phase 6
  (2026-06-26)** to **BeProduct → DTC** (optional; populated from BP's
  `customer_style_number` when non-blank). Previously "Legacy Code" was DTC→BP;
  that role now belongs to the new DTC column **"Customer Style#"**. There is no
  loop: "Legacy Code" (BP→DTC) and "Customer Style#" (DTC→BP) are different DTC
  columns. **Note:** There is NO new BeProduct "Legacy Code" field — the existing
  `customer_style_number` (CUSTOMER STYLE NUMBER / PLM #) is the source. Both
  `lf_style_number` and `customer_style_number` will be populated by upstream
  integration before flowing to DTC. **DTC "Customer Style#" column is NOT being
  created (decision 2026-07-02)**. Removed from `phase2.REVERSE_HEADER_FIELDS`.
- BeProduct `header_number` for style `127-WM2FF-K26` has a leading space
  (`' 127-WM2FF-K26'`). `phase1.norm()` strips it for matching so the sync
  functions correctly; the cosmetic issue should be fixed in BeProduct directly.

## Pipeline performance (validated 2026-06-19)

Full history in `docs/PERFORMANCE.md`. Key facts for agents:

**Current production job:** `BeProduct_DTC_sync_dag` (job 294837488757511),
8 first-class tasks on one shared single-node non-Photon cluster. Defined in
`scripts/deploy_job.py`; deploy with `python scripts/deploy_job.py`. The old
single-notebook orchestrator (job 22324120218492, `orchestrate_sync.py`) is retired.

**Parallel DAG:** Steps 1→2 (BeProduct chain) run in parallel with Step 3 (DTC
pull), converging at Step 4. Steps 1-2 ≈ 110 s; Step 3 ≈ 84 s — they overlap.

**Current typical execution (single-node cold start ~290 s):**

| Step | Task | Typical exec |
|------|------|-------------|
| 1 | `bp_style_sync` (BeProduct API → ktb_styles) | ~75–130 s |
| 2 | `transform` (ktb_styles → staging) | ~30–50 s |
| **3** | **`pull_dtc`** (DTC API → dtc_wip + registry) | **~84 s** |
| 4 | `request_manager` | ~13 s |
| 5 | `phase1_push` (BeProduct → DTC upsert) | ~14–60 s |
| 6 | `phase2_push` (DTC → BeProduct) | ~20–30 s |
| 7 | `repull_dtc` (targeted: only INSERT'd requests) | ~17 s full / ~5 s targeted |
| 8 | `phase3_images` (front image upload) | ~25–130 s |
| **Total execution** | | **~320–450 s** |
| **Total wall (cold)** | | **~610–740 s (~10–12 min)** |

**What was actually slow and why (intra-step cell-level profiling, 2026-06-19):**
The original hypothesis (serial `get_sheet` HTTP calls = 396 s) was wrong. The real
costs in `pull_requests_to_delta` were 100% Spark overhead:
- Cell 5: 66 per-request DataFrames → `reduce(unionByName)` → Delta overwrite +
  redundant `count()` = **277 s** for 422 rows. Fixed: one flat list → one DF → one
  write → `len()`. Now **8.8 s**.
- Cell 6: 66 serial `spark.sql("UPDATE registry …")` = **179 s**. Fixed: single
  batched `MERGE INTO` from a temp view. Now **5.5–8 s**.
- DTC API (registry refresh + all `get_sheet`s combined) = **~24 s** and was never
  the bottleneck. Opt A (parallel `get_sheet`) therefore gave nothing.

**Per-step log access (no hidden child runs):** Because the pipeline is now a
multi-task job, each step's logs are at `runs/get → tasks[].run_id`. Export any
step directly with `runs/export?run_id=<task_run_id>`. No WORKFLOW_RUN hunting.

**INCREMENTAL mode upsert bug fixed (2026-06-18):** `beproduct_style_sync.py`
was using `mode="append"` for INCREMENTAL writes, causing duplicates when the
BeProduct `FolderModifiedAt` filter (folder-scoped, not style-scoped) returned
styles that were already in `ktb_styles`. Fixed to `DeltaTable.merge` (keyed on
BeProduct style `id`) so INCREMENTAL correctly upserts — matched rows UPDATE,
new rows INSERT, unrelated rows are untouched. FULL mode remains `overwrite`.
NOTE: the `FolderModifiedAt` filter is folder-scoped (any change in the KTB folder
re-qualifies all styles), so INCREMENTAL is NOT reliably faster than FULL for Step 1
— leave `refresh_mode=INCREMENTAL` (default) but do not expect it to save time.

**BeProduct SDK install (~10 s/run):** `beproduct_style_sync.py` installs the SDK
via `subprocess.check_call(pip install)` on every run. Isolated in its own cell so
the cost is measured. To eliminate, bake `beproduct` into the cluster init script.

**Remaining opportunity:** parallelize `registry.refresh()` inner loop
(`registry.py:308`, serial `get_request_scope` by-id reads) — now ~40 s, the last
remaining lever in Step 3. Same `ThreadPoolExecutor(max_workers=4)` pattern.
Pre-warmed cluster pool also eliminates the ~290 s cold start.

## Commands

```bash
python3 dtc/tests/test_phase1.py        # Phase 1 core unit tests
python3 dtc/tests/test_phase2.py        # Phase 2 core unit tests
python3 dtc/tests/test_phase3.py        # Phase 3 image-upload core unit tests
python3 dtc/tests/test_samples.py       # Phase 7 sample formatter unit tests
python3 dtc/tests/test_phase1_live.py   # live reversible DTC write test (needs UAT)
python scripts/check_dtc_view.py        # DTC WIP_ITS_USE column readiness check
python scripts/upload_notebooks.py --dry-run   # preview Databricks notebook upload
python scripts/upload_notebooks.py             # deploy notebooks to Databricks
python scripts/deploy_job.py --dry-run         # preview multi-task job definition
python scripts/deploy_job.py                   # create BeProduct_DTC_sync_dag job
python scripts/deploy_job.py --reset-existing <JOB_ID>  # update existing job in place
```

Run `beproduct/00_init_style_app_registry` (ADB) once per folder, and again whenever
the folder's BeProduct application setup changes, to refresh the cached sample-app IDs.
