# Phase 1: BeProduct → DTC Sync

**Status**: Implemented ✅ (core unit-tested + live-verified on UAT)

Phase 1 pushes **BeProduct-owned** style fields into the matching DTC request
("upsert"). It never writes back to BeProduct (that is `PHASE2_WORKFLOW.md`) and
never uploads the Style Image (that is `PHASE3_WORKFLOW.md`). Missing **in-scope**
requests are **created** (and shared) by `dtc_request_manager` — see
"Missing-request creation" below.

> The original braindump spec for this phase is preserved in
> `implement_prompts.txt` (section "# Phase 1") for reference.
> Data model for every table named here: `docs/ARCHITECTURE.md`.

---

## Scope & conventions (DTC admin guidelines)

| Concept    | Value                                              |
|------------|----------------------------------------------------|
| Customer   | `KTB` (user param)                                 |
| Workspace  | `${customer}` = `KTB`                              |
| Document   | `"${customer} WIP"` = `KTB WIP`                    |
| View       | `WIP_ITS_USE` (user param)                          |
| In-scope request name | `"${customer} ${DTC seasoncode} ${brands}"`, e.g. `KTB FW26 Wrangler` |

Other requests of the same Document with different naming conventions are **out of
scope** and ignored (e.g. developer `KON …` requests). One brand per request,
agreeing with the request name (project guarantee).

---

## Fields pushed BeProduct → DTC

Only BeProduct-owned columns are pushed (`Style Image` excluded). DTC-owned columns
(Legacy Code, Lot#, Main Vendor/Factory (Sampling), Main Factory Customer ID) are
**not** pushed here — they flow the other way in Phase 2. Authoritative mapping:
`docs/beproduct_style_interested_fields.txt`; code: `phase1.FIELD_MAPPING`.

| DTC column        | BeProduct source (fieldId)            |
|-------------------|---------------------------------------|
| Product Status    | header `style_status`                 |
| Style Description | header `header_name`                  |
| Class / Sub Class | header `product_category` / `product_sub_category` |
| Division          | header `division_hk`                  |
| Brand (key-like)  | header `brands_multi`[0]              |
| Garment Finish    | header `garment_finish`               |
| Tech Pack Stage   | header `techpack_stage`               |
| Fabric Group / Placement | header `core_main_material`    |
| LF Style# (key)   | header `header_number`                |
| Color / Wash (key)| colorway `colorName`                  |

---

## Flow

```
0. Build/refresh request registry        dtc/notebooks/00_init_request_registry.py
                                         → lft.beproduct.dtc_request_registry
1. Pull in-scope DTC requests → Delta   dtc/notebooks/pull_requests_to_delta.py
                                         → lft.beproduct.dtc_wip_<customer>
2. Ensure BeProduct style sync is fresh  beproduct/beproduct_style_sync.py
3. Transform / denormalize               beproduct/beproduct_to_dtc_transform.py
                                         → lft.beproduct.beproduct_to_dtc_staging
4. Resolve / create requests             beproduct/dtc_request_manager.py
                                         → lft.beproduct.dtc_request_mapping
5. Upsert + push BeProduct → DTC         beproduct/beproduct_to_dtc_push.py
```

The registry scan (`sync.registry.refresh`) is **shared** and runs automatically
inside `pull_requests_to_delta` and `dtc_request_manager` (both default
`refresh_registry=true`), so the registry mirrors the workspace+document at sync
time. `00_init_request_registry.py` is the same scan as a standalone notebook —
useful for the first build or targeted `request_ids`, but no longer a mandatory
step 0 for routine runs. The table is created once and **upserted** thereafter
(`mode=merge`), so the scan never loses sync state.

---

## Request discovery (registry-driven)

Discovery is backed by the **control table**
`lft.beproduct.dtc_request_registry`
`[request_id, view_id, customer, season_code, brands, sheet_id, request_reference,
in_scope, request_is_active, row_count, last_extracted, last_pushed, msgs, ...]`,
populated by `00_init_request_registry.py`.

- **Auto-discovery (default):** the shared `sync.registry.refresh` lists every
  request in the workspace+document via `DTCConnector.search_requests` (`GET
  /v1/requests` with `workspaceName`+`filters` in the **body**), then enriches each
  by-id (`get_request` + `get_views`). Called automatically by
  `pull_requests_to_delta` and `dtc_request_manager`, and standalone by
  `00_init_request_registry`.
- **Manual override:** pass `request_ids` (comma-separated) to `00_init_request_registry`
  to register only those.

In-scope = reference parses as `<customer> <seasonCode> <brand>` AND the customer
token matches (e.g. `KTB …` in, `KON …` out). During **auto-discovery** the scan
pre-filters on the listed `requestReference` and **only reads/registers in-scope
requests** — out-of-scope/foreign requests are skipped entirely (no by-id
`get_request`, so no HTTP 400 and no registry rows). A request/view may be empty
(0 rows). (Explicit `request_ids` are read by-id without the reference pre-filter.)

### Missing-request creation (`dtc_request_manager`)

When a pending staging request name has no in-scope registry entry, the resolver
will **create** the DTC request/sheet (`POST /v1/sheets` via
`connector.create_sheet`) in `dtc_document`, then re-scan + resolve it:

- Only **in-scope** names are created. A name that doesn't parse as
  `<customer> <seasonCode> <brand>` (e.g. a brand-less `KTB SS26`) is logged as
  `NOT_IN_SCOPE` and never created.
- Creation is gated by **`dry_run`** (default `true`): dry-run logs `CREATE_REQUEST`
  with status `dry_run` and creates nothing; set `dry_run=false` to create.
- The start-of-run scan means requests that already exist in DTC are registered
  first, so they resolve instead of being re-created.
- **Sharing (gated by `share_on_create`, default true):** a freshly created request
  is visible only to its creator (the API identity). Immediately after a successful
  create, `dtc_request_manager` shares it: **all views → `aiagentwip@lifung.com`**
  (AI Agent WIP) and the **Full Version** view → the **Fabric Group** user group.
  Share events are logged to `beproduct_to_dtc_sync_log` (stage `share`).
  Already-created requests can be (re-)shared idempotently with the standalone
  `beproduct/dtc_share_requests.py` notebook.

Validated `POST /v1/sheets` body (HTTP 201): `requestReference` (NOT `requestName`),
non-empty `requestDescription`, `viewName`, and `requestAssigneeSharingViewNames`/
`sheetData` present as arrays (empty `[]` accepted).

**Refresh semantics (`mode=merge`, default):** upsert keyed on
`(environment, request_id)`. Matched rows refresh metadata / `request_is_active` /
`in_scope` but **preserve** `last_extracted`, `last_pushed`, `row_count`; new
requests are inserted with null sync-state. `mode=replace` overwrites the whole
table (wipes sync state) — avoid for routine runs.

**Reconciliation:** because the scan now only reads ACTIVE + IN-SCOPE requests, a
request that later goes inactive / is renamed out of scope would otherwise keep a
stale `request_is_active='Y'` / `in_scope=true` row. So after a full auto-discover
(non-empty listing), refresh **marks** any registry row in the scanned scope
(`environment`+`customer`+`document`) absent from the scan as
`request_is_active='N'`, `in_scope=false` (`reconciled_inactive` in the summary).
It's a **mark, not a delete** — sync state survives and a later scan that
re-discovers the request flips it back via merge. Reconciliation is skipped for
explicit `request_ids` (partial) and for empty listings (treated as a failed scan).

---

## Match key, rowIndex & upsert

- **In-request key**: `(LF Style#, Color / Wash)`. Season & brand are fixed per
  request, so they don't vary within it; the denormalized colorway is what
  distinguishes rows.
- **UPDATE**: matched row → PATCH changed non-key fields by `rowId`; original
  `rowIndex` preserved.
- **INSERT**: new row → key + mapped fields, `rowIndex = max(rowIndex)+1` within the
  request (sparse-aware; partition = season+brand).
- Updates and inserts are pushed as **separate** PATCH batches (the API rejects a
  mixed rowId/rowIndex body).
- **Delta push**: only staging rows with `beproduct_modified_at > registry.last_pushed`
  are considered; `compute_upsert` also emits NOOP for rows whose mapped values
  already match.

Core: `dtc/python/sync/phase1.py` (`compute_upsert`, `build_target_payload`,
`update_sheet_data` / `insert_sheet_data`).

---

## Moved-key handling (shared with Phase 2)

When a BeProduct **key field** (LF Style#, brand, season) changes, the row's request
changes. The new request gets an INSERT; the stale row left in the OLD request is
flagged with DTC `Product Status = "(removed)"` (an invalid BeProduct value that
signals the DTC user). Not deleted; only rows whose key now lives under a different
request are marked. Core: `phase1.compute_orphan_marks`, wired in
`beproduct_to_dtc_push.py`.

---

## Missing requests, exceptions & logging

- Missing **in-scope** requests are **created** by `dtc_request_manager.py`
  (`dry_run=false`) and then resolve normally. Requests that are **out-of-scope /
  inactive / lack WIP_ITS_USE** are logged as errors and excluded from the push.
- Per-row results/exceptions (scope mismatch, dup keys, missing rowId, PATCH
  failures, orphan marks) are written to `lft.beproduct.beproduct_to_dtc_sync_log`.
- `beproduct_to_dtc_push.py` supports `dry_run=true` (compute + log, no PATCH) and
  updates `registry.last_pushed` + staging `sync_status` on real runs.

---

## DTC write contract (validated live)

- Upsert: `PATCH /v1/sheets/{sheetId}/views/{viewId}` body `{"sheetData":[{...,"rowId"|"rowIndex":..}]}` → 204
- Delete: `DELETE /v1/sheets/{sheetId}/views/{viewId}/rows` body `{"rowIndexes":[...]}` → 204
- Create request/sheet: `POST /v1/sheets` (body shape above) → 201; response nests ids
  under `data` with a capital-S `SheetId`.
- Share request: `POST /v1/requests/{requestId}/shares/{userEmail}` and
  `.../shares/usergroups/{userGroupName}` body `{"viewNames":[...],"message":"...","sendEmail":"Y|N"}` → 201.
- Allowed columns come from the **view definition** (`GET /v1/views/{viewId}`,
  `DTCConnector.get_view_column_names`); payloads are filtered to it so a PATCH never
  trips `'<col>' is not found in the mapping`.

Full endpoint detail + the BeProduct/DTC data model live in `docs/ARCHITECTURE.md`
and `docs/DTC_GUIDE.md`.

---

## Tests

| Test | Scope |
|------|-------|
| `dtc/tests/test_phase1.py`      | upsert / payload / partition / orphan marks (unit) |
| `dtc/tests/test_phase1_live.py` | live reversible insert + update + DELETE cleanup |
