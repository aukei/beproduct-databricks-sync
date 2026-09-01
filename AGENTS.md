# Agent guide — BeProduct ⇄ DTC sync

Read this first. It is the durable memory for this repo so each new change can build
on prior **verified** discoveries instead of re-deriving them.

## What this repo does

Bi-directional sync between **BeProduct** (style PLM) and **DTC** ("Data Collab"
sheets), staged through **Databricks/Delta**.

- **Phase 0 — DTC XTS Master → BeProduct Directory**: logically precedes Style/
  Material/Costing sync. Pulls DTC workspace "KTB", document "XTS Master"
  (requests "XTS Supplier Master"/"XTS Factory Master" — "XTS Mill Master" is
  intentionally OUT OF SCOPE for now, see decision below) into
  `lft.beproduct.dtc_xts_master_ktb` (`dtc/notebooks/p0_pull_xts_master_to_delta.py`),
  then upserts into `lft.beproduct.beproduct_directory`
  (`beproduct/p0_xts_master_to_directory_upsert.py`), matched by **`name` +
  `partner_type` together** (BeProduct's real Directory key — NOT `id`/
  `directory_id`, and NOT `name` alone). **Wired into the daily DAG**
  (2026-08-31) as the FIRST step, gated by `run_phase0` (default `true`):
  `wait_cluster → gate_phase0 → phase0_pull → phase0_upsert → phase0_push`
  (`p5utl_beproduct_master_data_sync` mode=`PUSH_DIRECTORY`, which pushes only
  rows where `id IS NULL OR extracted_at IS NULL OR modified_at > extracted_at`).
  Every Style/Material/Costing task then waits on `phase0_push`
  (`run_if=ALL_DONE`, so disabling `run_phase0` doesn't deadlock them).
- **Phase 1 — BeProduct → DTC** (`docs/PHASE1_WORKFLOW.md`): push BeProduct-owned
  style fields into the matching DTC request (upsert); create + share missing
  in-scope requests.
- **Phase 2 — DTC → BeProduct** (`docs/PHASE2_WORKFLOW.md`): push DTC-owned fields
  back into the BeProduct style.
- **Phase 3 — BeProduct → DTC image** (`docs/PHASE3_WORKFLOW.md`): upload the front
  image into the DTC "Style Image" cell (binary, separate step).
- **Phase 7 — BeProduct → DTC sample history**: push BeProduct sample-app submit
  history (all 6 apps: Proto/PreLine/SMS/Fit/PP/TOP) into the matching DTC status
  columns as one quoted, comma-separated line PER submit: `"submit_name",
  "submitStatus","submitStatusDate"` — NOT a JSON array (no `[` `]` at all),
  multiple submits stacked on separate newline-separated lines (confirmed
  2026-08-28; `phase1.norm()` preserves embedded newlines so the multi-line
  structure survives to the actual DTC push).
  Proto → "Proto Sample - Sample Status", PreLine → "Pre-line Sample - Status",
  SMS → "SMS - Sample Status", Fit → "2nd Fit Sample Approval Status",
  PP → "PP Sample Submission Approval Status", TOP → "TOP Sample Approval Status".
  All 6 DTC columns confirmed in the 204-field view
  (2026-08-28; Fit/PP destinations changed from the original 2026-07-07 mapping
  after a DTC WIP doc restructure — see Verified discoveries log).
- **Phase 8a/8b — RETIRED (2026-09-01).** DTC FABRIC → Delta → BeProduct
  Material Master (`pull_fabric_dtc` / planned Material Master upsert) is
  confirmed by the project team to be replaced by a separate "MaterialLib"
  application. Removed entirely from the deployed DAG (`gate_phase8a` +
  `pull_fabric_dtc` tasks, `run_phase8a`/`include_test_sheets`/
  `fabric_document` job parameters — not just gated off). The notebook
  `dtc/notebooks/p8a_pull_fabric_to_delta.py` is left in the repo as a
  historical/manual-fallback artifact; its output tables
  `dtc_fabric_<customer>` / `dtc_fabric_registry` were DROPPED from Delta
  (2026-09-01, owner-confirmed) rather than left empty/stale. See
  decisions log.
- **Phase 9a — DTC LinePlan + Costing Chart**: pull `"KTB LinePlan"` document into
  `dtc_lineplan_ktb`; join WIP × LinePlan on `"Lineplan Ref #"`; transpose 4
  vendor/factory slots → `lft.beproduct.costing_chart`. Runs as two parallel tasks
  (`pull_lineplan_dtc` → `p9a_build_costing_chart`, gated by `run_phase9a`). The
  `p9a_build_costing_chart` task also depends on `pull_dtc` (WIP data).
- **Phase 9b — NT Orbit Duty Tools**: for `costing_chart` rows missing
  `hts_code` / `duty_rate_us` / `duty_rate_ca` / `duty_rate_mx` / `tariff_rate`,
  calls the NT Orbit Duty Tools 3rd-party API
  (https://orbitduty.neotangent.com/API-DOCS/, `POST /api/v1/calculate/single/`)
  — up to one call per row per still-blank market (US/CA/MX). Each call is
  ~30s and `costing_chart` is FULLY OVERWRITTEN by every Phase 9a run, so
  in-run dedup alone isn't enough — a PERSISTENT cross-run cache table
  (`duty_cache_table`, default `lft.beproduct.nt_orbit_duty_cache`, never
  wiped) keyed on `(product_description, origin_country, import_country)` is
  checked FIRST; a hit within `cache_ttl_days` (default 180 — tariff policy
  does change over time) skips the API call entirely, and only genuinely new
  or stale combinations reach NT Orbit. Fills the gaps on `costing_chart`
  (write-once), and optionally (`push_to_wip=true`) PATCHes the HTS/Duty
  values back onto the live DTC WIP per-slot columns (Tariff Rate has no WIP
  column yet — see decisions log). Auth is Microsoft Entra ID delegated
  OAuth2 (refresh_token → access_token), NOT the DTC x-api-key scheme — see
  `dtc/python/client/entra_auth.py` and the one-time setup CLI
  `scripts/nt_orbit_oauth_setup.py`. Entra rotates the refresh_token on
  (most) uses; the notebook auto-persists the rotated value to
  `lft.beproduct.nt_orbit_oauth_state` every run and prefers it over the
  static secret next time — so, like BeProduct's OAuth, this is "seed once,
  then fully automatic" (no manual secret-scope updates needed) as long as
  the job keeps running at least every ~90 days, which it does (3x/day).
  Runs as job task `fill_duty_rates`, gated by `run_phase9b` (**live in the
  DAG as of 2026-09-01**: `run_phase9b=true`, `push_duty_to_wip=true` on the
  deployed job `BeProduct_DTC_sync_dag` — both were `false` during earlier
  development), after `build_costing_chart`. The `p9b_fill_duty_rates.py`
  notebook's OWN standalone widget default for `dry_run` is still `"true"`
  (safe for ad-hoc/interactive runs from the ADB portal without job
  parameters); the deployed job's `dry_run` parameter is `false` (shared
  across every phase, same as Phase 1/2/3), so scheduled runs push for real.
  Costing chart table name is a job parameter `costing_chart_table` (default
  `lft.beproduct.costing_chart`; testing override
  `lft.beproduct.costing_chart_kei`); cache table/TTL are also job
  parameters (`duty_cache_table` / `duty_cache_ttl_days`). Notebook:
  `dtc/notebooks/p9b_fill_duty_rates.py`; pure logic + tests:
  `dtc/python/sync/duty.py` / `dtc/tests/test_duty.py`.

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
| BeProduct extraction (raw → master) | `COMPULSORY_FIELDS` / `INTERESTED_FIELDS` | `beproduct/p1p7_beproduct_style_sync.py` |
| BeProduct sample-app status (title → column prefix) | `SAMPLE_APPS` | `beproduct/p1p7_beproduct_style_sync.py` + `beproduct/00_init_style_app_registry.py` |
| BeProduct sample submits → DTC (Phase 7) | `SAMPLE_SUBMIT_FIELDS` + `format_sample_field` | `dtc/python/sync/samples.py` (+ `phase1.FIELD_MAPPING`, transform staging) |
| BeProduct → DTC transform (denormalize) | `FIELD_MAPPING` + staging `select` | `beproduct/p1p7_beproduct_to_dtc_transform.py` |

Then update unit tests: `dtc/tests/test_phase1.py`, `dtc/tests/test_phase2.py`,
`dtc/tests/test_phase3.py`, `dtc/tests/test_samples.py`.

### Current direction partition

- **BeProduct → DTC**: Product Status, Style Description, Class, Sub Class, Division,
  Brand, Garment Finish, Tech Pack Stage, Fabric Group, Placement, Gender (pending DTC col);
  BP Style# (new match key, pending DTC col), LF Style# (optional), Legacy Code (optional);
  Supplier (default-fill "Supplier" when blank; pending DTC col).
  **Filter**: styles with Product Status = "Finalized" are excluded from staging/DTC sync.
- **BeProduct → DTC (Phase 7, sample submit history)**: All 6 apps mapped.
  Proto → "Proto Sample - Sample Status", PreLine → "Pre-line Sample - Status",
  SMS → "SMS - Sample Status", Fit → "2nd Fit Sample Approval Status",
  PP → "PP Sample Submission Approval Status", TOP → "TOP Sample Approval Status".
  Each value is one quoted, comma-separated line PER submit: `"submit_name",
  "submitStatus","submitStatusDate"` — NOT a JSON array (no `[` `]` at all),
  multiple submits stacked on separate newline-separated lines (confirmed
  2026-08-28; `phase1.norm()` preserves embedded newlines so the multi-line
  structure survives to the actual DTC push — superseded a same-day flat-JSON-
  array iteration, itself a fix of the original nested array-of-arrays that
  always showed doubled `[[`/`]]` for the common single-submit case). All 6
  DTC columns confirmed in the 204-field view (2026-08-28; Fit/PP destinations
  changed from the original 2026-07-07 mapping after a DTC WIP doc restructure
  — was Fit → "1st Fit Sample Approval Status", PP → "2nd Fit Sample Approval
  Status"). Note: "Pre-line Sample - Status" uses lowercase 'l' and dash.
  Requested PP destination "PP Sample Approval Status" does not exist live —
  "PP Sample Submission Approval Status" was the only plausible match
  (confirmed via `get_view_definition`) and has since been **confirmed
  correct by the project team** (2026-08-28).
- **DTC → BeProduct**: Main Vendor (Sampling) (`parent_vendor`), Main Factory
  (Sampling) (`factory`) [header]; Lot# (`drawing_number_walmart`) [colorway];
  Main Factory Customer ID (no target → skipped).
  [REMOVED Phase 6]: "Legacy Code" DTC→BP. "Customer Style#" DTC column not created.
- **Keys** (match, not overwritten): BP Style# (`header_number`), Color/Wash (`colorName`)
  [in-request]; [Customer, BP Style# (header_number), SeasonCode, Brand (brand_hk)]
  [composite/request-routing].
- **BeProduct → DTC, image only (Phase 3)**: Style Image (`front_image_url` →
  DTC "Style Image"). Binary, so it does NOT ride the Phase 1 sheetData PATCH;
  uploaded by `p3_beproduct_to_dtc_images` via the multipart images endpoint, only
  when the DTC cell is blank and BeProduct has a valid `front_image_url`. Still
  one-directional (never read back to BeProduct, never in `phase2.REVERSE_*`).

## Ground rules / invariants

1. **Validate live before/while coding.** Local creds in `.env`
   (`BEPRODUCT_*`, `DATABRICKS_*`, `NT_ORBIT_*`); on Databricks use secret scope
   `beproduct` (`client_id`, `client_secret`, `refresh_token`, `company_domain`,
   `dtc_api_key_<env>`, and Phase 9b's `nt_orbit_tenant_id`, `nt_orbit_client_id`,
   `nt_orbit_client_secret`, `nt_orbit_refresh_token` — seeded once via
   `scripts/nt_orbit_oauth_setup.py`; rotated refresh tokens are then persisted
   to the Delta control table `lft.beproduct.nt_orbit_oauth_state`, NOT back
   into the secret scope, since `dbutils.secrets` is read-only). **Update
   2026-08-31: Phase 9b now uses a DEDICATED confidential app registration**
   (client_id `d270069e-20cc-4e63-ba38-156fb0ee9296`, tenant_id
   `c4d8a220-a9ec-4572-8c77-ab36a3ecdbae` — supersedes the earlier "reuse an
   existing shared client_id, no secret" assumption below, and also
   supersedes an earlier client_id `c486611e-9bfc-49d5-8930-7d1943884b03`
   whose redirect_uri was registered under the "Mobile and desktop
   applications" platform and hit AADSTS700025 — see decisions log), so
   `nt_orbit_client_secret` IS set and used. The Entra login is still a
   DELEGATED-USER sign-in (auchunkei@lifung.com), never client-credentials
   (app-only) — NT Orbit authorizes the signed-in person, not the app. Real
   secret VALUES must only ever live in the untracked local `.env` and the
   Databricks secret scope — never in `.env.example` or any other tracked
   file. There is a sacrificial in-scope DTC request for reversible write
   tests: `KTB FW26 Wrangler` (UAT request `6a26581854e92e7acd8fa71b`).
   Because this is now a dedicated app registration, its redirect URI IS
   ours to register, so `python scripts/nt_orbit_oauth_setup.py --flow authcode
   --redirect-uri http://localhost:8765/callback` (register that same URI on
   the app in the Entra portal first) is the recommended one-time login —
   fully scripted, no manual copy/paste. `--flow manual` (Postman's
   `https://oauth.pstmn.io/v1/browser-callback`, no redirect-URI registration
   needed) and `--flow devicecode` (RFC 8628) remain as no-portal-access
   fallbacks; all three end in the same `access_token`/`refresh_token` pair.
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

**DTC "XTS Master" document (Phase 0, live-validated 2026-08-28):**
- Workspace "KTB", document "XTS Master" has exactly the 3 requested requests
  (`"XTS Supplier Master"`, `"XTS Factory Master"`, `"XTS Mill Master"`), plus 3
  `"(BACKUP)"`/differently-named siblings (`"XTS (BACKUP) Supplier Master"`,
  `"XTS (BACKUP) Factory Master"`, `"KTB (BACKUP) Mill Master"`) — all
  `requestIsActive='Y'`. **Scope decision (2026-08-28): only Supplier and
  Factory are pulled** — `"XTS Mill Master"` is intentionally out of scope for
  now (see next bullet for why), and the `(BACKUP)` siblings are deliberately
  excluded regardless. The Phase 0 pull matches **exact** `requestReference`
  only (`sync.xts_master.XTS_REQUESTS`).
- All 6 requests share the SAME 4 views (`Supplier`, `Mill`, `Factory`,
  `WIP_ITS_USE` — identical view IDs across all 6 requests in the document,
  confirmed via `get_views`), even though only `Supplier`/`Factory` are used.
  The `WIP_ITS_USE` view here (id `6a3907f6df772fd797ee5b7c`) is the same one
  already flagged elsewhere in this doc as belonging to a *different*
  document than KTB WIP — do not use it for anything; Phase 0 never touches
  it. The `Mill` view (id `6a3b351185ceba6dca6712e5`) has NO code column at
  all, and in UAT its 8 rows are 100% brand-config rows (`Type="Fabric
  Brand"`, same 8 brand names as Supplier's brand rows below) — there is
  currently NO real Mill company data in this environment, which is why it
  was excluded from scope rather than pulled empty.
- **The document is NOT a rich vendor-master sheet.** `GET /v1/views/{id}`
  (authoritative field list, not sample rows) confirms the in-scope views:
  - `"Supplier"` view: `Supplier Name`, `Supplier Code`, `Customer Vendor ID`,
    `Type`, plus 9 request-sharing/access-config columns (`Brand Views`,
    `Group Users Name`, `Agent Alert Recipient (...)`, etc.) — NO
    address/state/zip/city/phone/fax/website/notes at all.
  - `"Factory"` view: only 4 fields — `Factory Name`, `Factory Code`,
    `Customer Factory ID`, `Production Country`. No `Type` column, no
    sharing/config columns, no address/phone/etc. either.
  - None of address/state/zip/city/phone/fax/website/notes exist ANYWHERE in
    either in-scope view — they are always `NULL` for every XTS-sourced
    `beproduct_directory` row until DTC adds them.
- **CRITICAL data-quality finding**: the `Supplier` sheet is polluted with
  BRAND-level access-sharing config rows interleaved with real company rows,
  distinguishable ONLY by the `Type` column: `Type="Brand"` (8/42 rows in
  UAT, e.g. "Wrangler", "Blue Bell", "Slam Jam" — brand names, not companies,
  always blank code) vs `Type="Supplier"` for real rows (34/42 rows, each
  with a real code). `sync.xts_master.EXCLUDE_TYPE_VALUES` / `is_brand_row()`
  filters these out. Partner type is derived from WHICH REQUEST/view a row
  came from, never from the `Type` cell's literal value.
- **Directory match key correction (2026-08-28, same day, supersedes the
  initial hypothesis below)**: BeProduct's Directory record is keyed by
  **`name` + `partner_type` TOGETHER**, not `name` alone as first assumed.
  This means the SAME name legitimately recurring under a DIFFERENT partner
  type (see next bullet) is simply two separate, valid Directory records —
  NOT a collision requiring a pick. `sync.xts_master.find_duplicate_keys` /
  `dedupe_by_key` operate on the `(name, partner_type)` pair; a true
  collision only exists if that exact pair repeats (e.g. a duplicate row
  within one sheet).
- 19 of the 34 real Supplier rows share the exact same `name` AND code as a
  Factory row (e.g. `"SUPPLIER ASPGAR"` / code `ASPGAR` appears in both
  sheets) — the same physical entity apparently acts as both a sourcing
  supplier and a production factory. Under the corrected (name, partner_type)
  key model this is expected and unproblematic (both records are created);
  it was initially (incorrectly) treated as a same-name collision needing a
  SUPPLIER-wins tie-break before the key model was clarified the same day.

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
  `p3_beproduct_to_dtc_images` now classifies via `phase3.classify_image_type` and
  **transcodes webp/gif/bmp/tiff → PNG (Pillow) before upload**; jpg/png upload
  as-is; vector/unknown (svg, pdf, ...) are skipped with `unsupported_type`.
  Separately, a subset of BeProduct CDN `frontImage.origin` URLs return **HTTP 403**
  on download (Azure SAS auth — "signature" error) even within the SAS validity
  window; affects only some files (36 png succeeded, 53 png failed), so it is a
  per-file BeProduct CDN/SAS issue, NOT a Phase 3 code defect. Still open.
- Request listing works via `GET /v1/requests` with `workspaceName`+`filters` in the
  **request body** (not query params; query param → 400 "Invalid workspaceName").
- Registry refresh is the shared `sync.registry.refresh` (used by
  `00_init_request_registry`, `p1_pull_masters_to_delta`, `p1_dtc_request_manager`):
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
  (`colorways_json` from `p1p7_beproduct_style_sync`).

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
- `p1p7_beproduct_style_sync` enriches `ktb_styles` with **6 JSON-array columns**:
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
  submitStatus, submitStatusDate]` (first size per submit). All 6 DTC columns confirmed in the
  198-field view (2026-07-07). Note: "Pre-line Sample - Status" uses lowercase 'l' and dash.
  **Superseded 2026-08-28**: the DTC WIP doc was restructured (198 → 204 fields) and Fit/PP
  now target different columns — see the "Current direction partition" section above for the
  current mapping (Fit → "2nd Fit Sample Approval Status", PP → "PP Sample Submission Approval
  Status") and `sync/samples.py`'s module docstring for full detail.
- **Phase 7 DTC WIP restructure (live-validated 2026-08-28)**: `get_view_definition` on the
  KTB WIP_ITS_USE view (id `69f04983501f3d9cf4fc379c`) now returns **204 fields** (up from
  198 on 2026-07-03). Both `"1st Fit Sample Approval Status"` and `"2nd Fit Sample Approval
  Status"` still exist side-by-side; only WHICH ONE Phase 7 pushes Fit to changed (now `2nd`,
  was `1st`). The old PP target `"2nd Fit Sample Approval Status"` was reassigned away from PP
  (now used by Fit) — PP's new field is `"PP Sample Submission Approval Status"` (confirmed
  present). The literally-requested name `"PP Sample Approval Status"` does **not** exist in
  the view at all — flagged, not silently guessed past this one plausible match; **confirmed
  correct by the project team** the same day.
- **`format_sample_field()` output format changed 2026-08-28 (two iterations same day)**:
  first from a nested array-of-arrays (`[["name","status","date"]]` — always doubled
  `[[`/`]]` for the very common single-submit case) to a single flat JSON array
  (`["name","status","date", ...]`); then per follow-up request, to a plain
  **non-JSON** format: one quoted, comma-separated line PER submit
  (`"name","status","date"`), multiple submits stacked on separate `\n`-separated
  lines — no `[` `]` brackets at all in either single- or multi-submit cases.
  **Critical companion fix**: `phase1.build_target_payload()` stores `phase1.norm(value)`
  as the actual pushed payload value (not the raw pre-norm value), and `norm()`'s
  original regex (`\s+` → single space) would have silently collapsed the new format's
  intentional `\n` separators into a single space before the value ever reached DTC —
  found and fixed the same day by changing the regex to `[^\S\n]+` (collapses all
  whitespace EXCEPT newlines). No other current field legitimately contains embedded
  newlines, so this is a no-op for every other column.

**DTC FABRIC document (KTB, validated 2026-07-16) — Phase 8a/8b RETIRED 2026-09-01,
kept below for historical reference only (see decisions log):**
- Document "KTB FABRIC", Workspace "KTB". **39 active requests** in two patterns:
  - DEV sheets:  `"KTB <season> <brand> - DEV"` — master development sheet per brand
  - Mill sheets: `"KTB <season> <brand>-<MILLCODE>"` — mill-specific allocation sheets
- **WIP_ITS_USE view**: id `6a0ac943fedfa0ca7ff2bf48`, **120 dynamicFields**
  (updated 2026-07-17 — "LF Material ID" added).
  Same view name as WIP document; different per-request view ID.
- **Adoption (Y/N)**: filter field. 0 adopted rows in UAT as of 2026-07-16 (UAT data
  not yet populated). Phase 8a code filters to Adoption=Y at pull time; logic is ready.
- **Key staging columns** (DTC field → Delta column):
  `"LF Material ID"` → `lf_material_id` (BP Material Master primary key; confirmed 2026-07-17),
  `"ITS_Key"` → `its_key` (DTC system row key),
  `"Mill Fabric Article #"` → `mill_fabric_code`, `"Mill Name"` → `mill_name`,
  `"Material Class"` → `material_class`, `"Fabric Type"` → `fabric_type`,
  `"Fabric Content"` → `fabric_content` (→ BP Material Description),
  `"KB Fabric Code (SAP Code)"` → `kb_fabric_code`.
- **Sheet-type switch** (`include_test_sheets` widget, default `false`):
  `false` = PROD sheets only (clean `<customer> <season> <brand>`, no suffix);
  `true`  = include DEV + MILL sheets (needed in UAT where all current sheets have suffixes).
- **Phase 8a Delta tables**: `dtc_fabric_<customer>` (Adoption=Y rows) +
  `dtc_fabric_registry` (request metadata, same structure as dtc_request_registry).
- **Job task**: `pull_fabric_dtc` gated by `run_phase8a=true`; runs in parallel with
  the WIP chain after `wait_cluster`. Notebook: `dtc/notebooks/p8a_pull_fabric_to_delta.py`.
- **SSOT field mapping**: `docs/beproduct_material_interested_fields.txt`
- Do NOT use view ID `6a0ac943fedfa0ca7ff2bf48` for WIP document operations —
  that ID belongs to FABRIC only.

**DTC LinePlan document (KTB, validated 2026-07-17):**
- Document "KTB LinePlan", Workspace "KTB". **1 active request** in UAT:
  `"FA HO 27 MENS WESTERN TOPS LINEPLAN - FA27"` (396 rows). No standard
  `<customer> <season> <brand>` naming — LinePlan uses its own naming convention.
- **No "LINEPLAN_ITS_USE" view** — only `"Full"` (id `69f0788555010bb745140ac4`,
  30 dynamicFields). `p9a_pull_lineplan_to_delta` tries `"LINEPLAN_ITS_USE"` first then
  falls back to `"Full"`.
- **Key LinePlan fields** (exact DTC column names — all UPPERCASE):
  `"Lineplan Ref #"` → `lineplan_ref` (join key to WIP `"Lineplan Ref #"`),
  `"PROJECTED VOLUME (season)"` → `projected_volume` (Order Qty in costing),
  `"TARGET SAP w/ Tariff impact"` → `target_ldp` (lowercase 'i'; Target LDP),
  `"TARGET FOB"` → `target_fob`,
  `"INTERNAL/ SOURCED"` → `internal_sourced` (→ Costing Supplier Type).
- **WIP join field** is plain `"Lineplan Ref #"` (no `(GC)` suffix in actual
  view — spec description used `(GC)` as annotation, not literal column name).
- **WIP Tariff Rate fields do NOT exist**: `"Main Factory Tariff rate"`,
  `"Factory 1 - Tariff rate"` etc. are all absent from WIP. Tariff Rate column
  in costing_chart is NULL placeholder, to be filled by Phase 9b (NT Orbit).
- **Phase 9a Delta tables**: `dtc_lineplan_ktb` + `dtc_lineplan_registry`.
  `costing_chart` = fully overwritten join result (Style × Color × Vendor/Factory).

**BeProduct `attributes_list` folder scoping bug (found + fixed live, 2026-08-27):**
- The BeProduct SDK's `api.style.attributes_list(folder_id: str = "", ...)` takes
  a `folder_id` kwarg. Passing `folderId=` (wrong casing) is silently absorbed
  into `**kwargs`/`raw_api.post(**kwargs)` and never applied — the call falls
  back to `folder_id=""` and returns EVERY style across the WHOLE account, not
  just the intended folder. Confirmed live: an ad-hoc "list styles in TEST KTB"
  query using `folderId=` returned all 104 account-wide styles (99 SANDBOX +
  Apparel + KTB + TEST KTB combined) instead of the 8 actually in TEST KTB;
  fixed by switching to `folder_id=`, verified against both `TEST KTB` (8
  styles, matches the DTC web portal) and `KTB` (158 styles, all confirmed
  `folder.name == 'KTB'`).
- `beproduct/p1p7_beproduct_style_sync.py` (the daily Phase 1/7 style sync) and
  `beproduct/00_init_style_app_registry.py`'s fallback API scan had the SAME
  class of bug in a different shape: neither passed `folder_id` AT ALL to
  `attributes_list()` — they fetched the **entire account** every run and
  post-filtered client-side by `style.folder.name == folder_name`. This gave
  correct RESULTS (name equality is unambiguous today) but was needlessly slow
  and would silently break if two folders were ever named identically. Fixed
  (2026-08-27): both notebooks now resolve `folder_id` once via
  `api.style.folders()` (raising if the name is missing or ambiguous) and pass
  it to `attributes_list(folder_id=...)` so the API itself scopes the result
  server-side; the old client-side name check is kept only as a defense-in-depth
  sanity assertion (should now always be a no-op).

## Decisions on record

- **LinePlan request naming — no filter, human-enforced uniqueness (project
  team decision, 2026-09-01).** The project team will maintain MULTIPLE DTC
  LinePlan requests with NO specific naming convention for now (this
  includes `(BACKUP)`-named requests — unlike Phase 0/XTS Master, which
  explicitly excludes those). `p9a_pull_lineplan_to_delta.py` therefore has
  and keeps NO name-pattern filter; it pulls every active request in the
  document regardless of name. Uniqueness of `"Lineplan Ref #"` ACROSS ALL
  requests is a HUMAN-enforced invariant (human-in-the-loop), not something
  this pipeline validates or enforces. `p9a_build_costing_chart.py`'s
  LinePlan aggregation step (`F.first(ignorenulls=True)` per ref) adds a
  best-effort conflict DETECTOR that prints a warning (does not block) when
  a ref's `order_quantity`/`target_ldp`/`target_fob` actually disagree
  across rows/requests, so violations of the human-enforced invariant are at
  least surfaced rather than silently resolved to an arbitrary value.
- **`costing_chart` is a FULL OVERWRITE every `build_costing_chart` run, not
  an upsert (clarified 2026-09-01).** If a style initially has only "Main"
  factory assigned and a later run adds Factory 2/3, the later run does NOT
  duplicate the Main row — the entire table is rebuilt from current WIP +
  LinePlan state on every run via `.mode("overwrite")`. Manually truncating
  `costing_chart` before a run is therefore redundant (the overwrite already
  clears prior content) but harmless. This is also why Phase 9b's
  `nt_orbit_duty_cache` must be a SEPARATE, never-overwritten table (see
  Phase 9b entry above) — any HTS/Duty/Tariff values Phase 9b fills into
  `costing_chart` itself are wiped by the next Phase 9a rebuild.
- **`factory_slot` renamed to `supplier_type` in `costing_chart` — RESOLVED
  (2026-09-01), traced back to the original spec.** Initial implementation
  wrongly mapped `costing_chart.supplier_type` to LinePlan's
  `"INTERNAL/ SOURCED"` field. Checking `implement_prompts.txt`'s original
  Phase 9a spec: `"Supplier Type - Generated from Master Chart data"` — i.e.
  ONE flag, GENERATED from WIP structure (which of the 4 vendor/factory
  column-pairs a row came from — `"Main"|"1"|"2"|"3"`), NOT the LinePlan
  business classification. Fixed: `p9a_build_costing_chart.py`'s `factory_slot`
  column is renamed to `supplier_type` (same values, same derivation — no
  functional change to Phase 9b's WIP-push routing, which still keys on this
  same value, just reads it from the renamed column); LinePlan's
  `"INTERNAL/ SOURCED"` is still captured into `dtc_lineplan_ktb.internal_sourced`
  (harmless raw data) but is no longer joined into `costing_chart` at all.
  `dtc/python/sync/duty.py`'s `build_wip_patch_fields()` / `WIP_HTS_COL` /
  `WIP_DUTY_COL` are UNCHANGED (they only ever took a slot-value string
  positionally, independent of the caller's column name) — only the caller
  in `p9b_fill_duty_rates.py` and `COSTING_KEY` were updated to read
  `supplier_type` instead of `factory_slot`.
- **Phase 8a/8b retired (2026-09-01), confirmed by the project team.** DTC
  FABRIC → Delta → BeProduct Material Master is superseded by a separate
  "MaterialLib" application. Removed from `scripts/deploy_job.py`'s DAG
  entirely — the `gate_phase8a` condition task, the `pull_fabric_dtc` task,
  and the `run_phase8a` / `include_test_sheets` / `fabric_document` job
  parameters are all gone from the deployed job (`BeProduct_DTC_sync_dag`,
  reset 2026-09-01: task count 23 → 21). Verified nothing else in the DAG
  depended on `pull_fabric_dtc`/`gate_phase8a`, so removal was a clean,
  self-contained deletion — Phase 9a/9b and the WIP chain were unaffected.
  Left in place (NOT deleted) as historical/manual-fallback artifacts:
  `dtc/notebooks/p8a_pull_fabric_to_delta.py`, and the SSOT doc
  `docs/beproduct_material_interested_fields.txt` (marked superseded).
  Docs updated: `README.md`, `docs/ARCHITECTURE.md`, `docs/DTC_GUIDE.md`,
  `docs/DIAGRAM.md`, `QUICK_START.md`.
- **`lft.beproduct` table cleanup (2026-09-01), owner-confirmed.** Live-schema
  audit (via `databricks-sql-connector`, not just code grep) found 6 drop
  candidates beyond the already-known Phase 8a tables; owner triaged each:
  - **Dropped**: `dtc_fabric_ktb` (0 rows), `dtc_fabric_registry` (81 rows —
    Phase 8a outputs, retired same day, see above), `ktb_styles_push_log`
    (5 rows, last written 2026-05-26, zero code references anywhere —
    superseded by `beproduct_to_dtc_sync_log`, which is what's actually
    written today).
  - **Kept, NOT dropped**: `wmt_styles` / `wmt_styles_sync_meta` (75 + 21
    rows, last written 2026-06-17, zero current code references) — owner
    confirmed these are an intentional artifact demonstrating the pipeline
    generalizes beyond `KTB` to another customer folder (`WMT`); leave as-is,
    do not treat as orphaned/stale in future audits. `costing_chart_kei`
    (job param `costing_chart_table` test override) — owner's own active
    Phase 9b DAG-testing table; **`costing_chart` itself must stay stable**
    since it has real downstream readers, which is exactly why the test
    override table exists — keep both.
  - Already gone before this audit (per the 2026-06-17 decision above,
    confirmed still absent): `dtc_master_chart_uat`,
    `dtc_master_chart_uat_change_log`. Confirmed never created:
    `beproduct_directory_contacts` (0 contacts org-wide, `fetch_contacts`
    defaults `false`).
- **NT Orbit / Entra token endpoint AADSTS700025 (live-validated 2026-08-31).**
  Registering `http://localhost:8765/callback` under the app's "Mobile and
  desktop applications" platform in the Entra portal makes Entra treat EVERY
  token request using that redirect_uri as a public client — it rejects
  `client_secret` with `AADSTS700025` ("Client is public so neither
  'client_assertion' nor 'client_secret' should be presented"), even though
  the app registration (`c486611e-9bfc-49d5-8930-7d1943884b03`) has a secret
  configured. This is determined by the redirect_uri's PLATFORM TYPE, not by
  whether the app has a secret. Fix: register the redirect_uri under "Web"
  instead. `entra_auth.exchange_code_for_tokens`/`refresh_access_token` also
  auto-detect this specific error (`error_codes` contains `700025`) and
  retry once without the secret, so a misconfigured platform type degrades
  to a public-client exchange instead of hard-failing the login (unit-tested
  in `dtc/tests/test_entra_auth.py` with a mocked 401 response).
  **Resolved 2026-08-31**: rather than fix the platform type on
  `c486611e-...`'s existing redirect_uri, a fresh dedicated app registration
  (client_id `d270069e-20cc-4e63-ba38-156fb0ee9296`) was created with
  `http://localhost:8765/callback` registered correctly under "Web" from the
  start. `--flow authcode` against it completed the one-time login
  successfully (health check confirmed `auchunkei@lifung.com` is authorized
  against NT Orbit). `c486611e-...` is abandoned/unused going forward —
  `d270069e-...` is the live Phase 9b client_id.
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
  notebook `beproduct/p3_beproduct_to_dtc_images.py` runs AFTER `p1p7_beproduct_to_dtc_push`:
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
  pre-creates them" is superseded. `p1_dtc_request_manager` creates missing **in-scope**
  requests (`connector.create_sheet` → `POST /v1/sheets`) in `dtc_document`, then
  re-scans + resolves. Guardrails: only in-scope names (`<customer> <seasonCode>
  <brand>`; brand-less names → `NOT_IN_SCOPE`, never created); gated by `dry_run`
  (default true = preview only). The registry scan is the shared
  `sync.registry.refresh` (discover → enrich → merge), invoked automatically by
  `p1_pull_masters_to_delta` and `p1_dtc_request_manager` (default `refresh_registry=true`)
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
  with the **Kontoor Project Team** user group (**changed 2026-08-27**, was
  "Fabric Group" — do not confuse with the unrelated DTC data column also named
  "Fabric Group" in `phase1.FIELD_MAPPING`). Applied automatically by
  `p1_dtc_request_manager` at create time (`share_on_create=true`, `send_email=N`)
  and backfillable via the idempotent `beproduct/p1utl_dtc_share_requests` notebook.
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
costs in `p1_pull_masters_to_delta` were 100% Spark overhead:
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

**INCREMENTAL mode upsert bug fixed (2026-06-18):** `p1p7_beproduct_style_sync.py`
was using `mode="append"` for INCREMENTAL writes, causing duplicates when the
BeProduct `FolderModifiedAt` filter (folder-scoped, not style-scoped) returned
styles that were already in `ktb_styles`. Fixed to `DeltaTable.merge` (keyed on
BeProduct style `id`) so INCREMENTAL correctly upserts — matched rows UPDATE,
new rows INSERT, unrelated rows are untouched. FULL mode remains `overwrite`.
NOTE: the `FolderModifiedAt` filter is folder-scoped (any change in the KTB folder
re-qualifies all styles), so INCREMENTAL is NOT reliably faster than FULL for Step 1
— leave `refresh_mode=INCREMENTAL` (default) but do not expect it to save time.

**BeProduct SDK install (~10 s/run):** `p1p7_beproduct_style_sync.py` installs the SDK
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
python3 dtc/tests/test_registry.py      # request-registry pure-function unit tests
python3 dtc/tests/test_xts_master.py    # Phase 0 XTS Master pure-function unit tests
python3 dtc/tests/test_duty.py          # Phase 9b NT Orbit Duty Tools pure-function unit tests (fixture-based)
python3 dtc/tests/test_entra_auth.py    # Phase 9b Entra OAuth2 URL-building / callback-parsing pure-function unit tests
python3 dtc/tests/test_phase1_live.py   # live reversible DTC write test (needs UAT)
python3 dtc/tests/test_nt_orbit_live.py # LIVE Entra OAuth2 + real NT Orbit API call (needs NT_ORBIT_* env + RUN_NT_ORBIT_LIVE_TEST=true; skips cleanly otherwise)
python scripts/nt_orbit_oauth_setup.py  # ONE-TIME Entra ID interactive login for NT Orbit (Phase 9b)
python scripts/check_dtc_view.py        # DTC WIP_ITS_USE column readiness check
python scripts/upload_notebooks.py --dry-run   # preview Databricks notebook upload
python scripts/upload_notebooks.py             # deploy notebooks to Databricks
python scripts/deploy_job.py --dry-run         # preview multi-task job definition
python scripts/deploy_job.py                   # create BeProduct_DTC_sync_dag job
python scripts/deploy_job.py --reset-existing <JOB_ID>  # update existing job in place
```

Run `beproduct/00_init_style_app_registry` (ADB) once per folder, and again whenever
the folder's BeProduct application setup changes, to refresh the cached sample-app IDs.
