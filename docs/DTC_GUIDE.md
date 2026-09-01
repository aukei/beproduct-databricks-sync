# DTC Component Guide

Everything the jobs need about the **DTC** ("Data Collab") side: the API surface
used, the connector, and the DTC-related tables on Databricks (`lft.beproduct`).

> Cross-platform flow & full data model: `ARCHITECTURE.md`. Phase workflows:
> `PHASE1_WORKFLOW.md` (BeProduct → DTC), `PHASE2_WORKFLOW.md` (DTC → BeProduct),
> `PHASE3_WORKFLOW.md` (images). Field-mapping SSOT:
> `beproduct_style_interested_fields.txt`. Verified API behaviour: `../AGENTS.md`.

---

## 1. DTC model

`Workspace → Document → Request → Sheet → View`. A **Request** (e.g.
`KTB FW26 Wrangler`) instantiates a **Document** (`KTB WIP`); **Views** are column
projections on the Document. Sync only ever reads the **`WIP_ITS_USE`** view
(complete data). Requests registered with any other view are skipped + logged.

**In-scope rule:** a reference must parse as `<customer> <seasonCode> <brand>`
(`seasonCode` = 2 letters + 2 digits, e.g. `FW26`) **and** the customer token must
match the configured `customer`. `KTB FW26 Wrangler` is in scope for `KTB`;
`KON FW26 Wrangler` is not. Logic: `dtc/python/sync/phase1.py`
(`parse_request_reference`, `is_in_scope`).

---

## 2. Connectivity

- **Base URLs:** UAT `https://dtc-api.lfuat.net/api`, PROD `https://dtc-api.lfapps.net/api`.
- **Auth:** `x-api-key` header. Key from the `beproduct` secret scope, selected by
  environment: `dtc_api_key_uat` / `dtc_api_key_prod`.
- **Client:** `dtc/python/client/rest_client.py` — `requests.Session` with retry
  (429/5xx) and a `post_multipart` for binary image upload. **Connector:**
  `dtc/python/connectors/dtc.py` (`DTCConnector`).

```python
api_key = dbutils.secrets.get(scope="beproduct", key=f"dtc_api_key_{environment}")
connector = DTCConnector(api_key=api_key, environment=environment, workspace_name="KTB")
```

---

## 3. DTC API surface used (all validated live)

| Purpose | Endpoint | Notes |
|---------|----------|-------|
| List requests | `GET /v1/requests` | `workspaceName` + `filters` in the **JSON body** (not query params). Server-side `requestIsActive:"Y"` filter. |
| Get request | `GET /v1/requests/{id}` | by-id; inactive requests 400 on get-by-id. |
| Get views | `GET /v1/requests/{id}/views` | resolve the `WIP_ITS_USE` `viewId`. |
| View definition | `GET /v1/views/{viewId}` | `dynamicFields[].fieldName` = authoritative column list. **NOTE:** returns 403 for some view IDs with the sync API key (e.g. the wrong view id `6a3907f6df772fd797ee5b7c` is "XTS Master"). Correct KTB WIP view id: `69f04983501f3d9cf4fc379c` (198 fields). `allowed_cols` in push notebook UNIONs data-scan with FALLBACK_COLS. |
| Get sheet rows | `GET /v1/sheets/{sheetId}/views/{viewId}` | returns `sheetData[]` with `rowId`/`rowIndex` + columns. |
| **Upsert rows** | `PATCH /v1/sheets/{sheetId}/views/{viewId}` | body `{"sheetData":[{…,"rowId"\|"rowIndex":…}]}` → **204**. A single PATCH **cannot mix** `rowId` (update) and `rowIndex` (insert) — separate batches. |
| Delete rows | `DELETE /v1/sheets/{sheetId}/views/{viewId}/rows` | body `{"rowIndexes":[…]}` → 204 (keys off `rowIndex`). |
| **Create request/sheet** | `POST /v1/sheets` | → **201**. Body must use `requestReference` (NOT `requestName`), a **non-empty** `requestDescription`, `viewName`, and `requestAssigneeSharingViewNames`/`sheetData` as **arrays** (empty `[]` ok). Response nests ids under `data` with a capital-S `SheetId`. |
| **Share (user)** | `POST /v1/requests/{requestId}/shares/{userEmail}` | body `{"viewNames":[…],"message":"…","sendEmail":"Y\|N"}` → 201. |
| **Share (group)** | `POST /v1/requests/{requestId}/shares/usergroups/{userGroupName}` | path segment URL-encoded (group names have spaces). |
| Read shares | `GET …/shares`, `GET …/shares/usergroups` | used for idempotency. |
| **Image upload** | `POST /v1/sheets/{sheetId}/views/{viewId}/images?rowindex={int}&columnname=Style Image` | `multipart/form-data`, file part named `file`. DTC **rejects webp (400)** → transcode to PNG first. |

Connector methods: `search_requests`, `get_request`, `get_views`,
`get_view_definition`/`get_view_column_names`, `get_sheet`, `patch_rows`
(+`create_row`/`update_row`), `delete_rows`, `create_sheet`,
`share_request_with_user`/`share_request_with_usergroup`/`get_request_shares`/
`get_request_share_usergroups`, `upload_row_image`.

---

## 4. Notebooks (DTC side)

| Notebook | Does | Writes |
|----------|------|--------|
| `dtc/notebooks/00_init_request_registry.py` | Standalone WIP registry build/refresh (first build or targeted `request_ids`). | `dtc_request_registry` |
| `dtc/notebooks/00_init_season_mapping.py` | Seed the season-code prefix table. | `dtc_seasoncode_mapping` |
| `dtc/notebooks/p1_pull_masters_to_delta.py` | Refresh WIP registry; pull each in-scope active request's `WIP_ITS_USE` view (Steps 3 + 7). | `dtc_wip_<customer>` |
| `dtc/notebooks/p8a_pull_fabric_to_delta.py` | ⚠️ **RETIRED 2026-09-01** (superseded by MaterialLib) — kept as manual-fallback only, no longer scheduled. | `dtc_fabric_<customer>`, `dtc_fabric_registry` |
| `dtc/notebooks/p9a_pull_lineplan_to_delta.py` | Phase 9a: pull KTB LinePlan (LINEPLAN_ITS_USE → Full fallback). | `dtc_lineplan_<customer>`, `dtc_lineplan_registry` |
| `dtc/notebooks/p9a_build_costing_chart.py` | Phase 9a: join WIP × LinePlan on "Lineplan Ref #"; transpose 4 vendor/factory slots. | `costing_chart` (full overwrite) |
| `dtc/notebooks/p9b_fill_duty_rates.py` | Phase 9b: NT Orbit Duty Tools HTS/Duty/Tariff fill, with a persistent cross-run cache. | `costing_chart`, `nt_orbit_duty_cache`, `nt_orbit_oauth_state` (+ optional DTC WIP push) |
| `dtc/notebooks/p2_push_dtc_to_beproduct.py` | Phase 2 pushback of DTC-owned fields (Vendor, Factory, Lot#). | BeProduct (+ `dtc_to_beproduct_sync_log`) |

`beproduct/p1_dtc_request_manager.py` (BeProduct-side, but DTC-writing) resolves /
**creates** / **shares** WIP requests and writes `dtc_request_mapping`.

### Registry scan (shared)

`sync.registry.refresh` = discover (`search_requests`) → enrich by-id → upsert
(`mode=merge`, preserving `last_extracted`/`last_pushed`/`row_count`). It runs
automatically inside `p1_pull_masters_to_delta` and `p1_dtc_request_manager` (both
default `refresh_registry=true`), so the registry mirrors the workspace+document
each run; `00_init_request_registry.py` is the same scan standalone. After a full
auto-discover, in-scope rows absent from the scan are **marked** inactive
(`request_is_active='N'`, `in_scope=false`) — a mark, not a delete.

### Missing-request creation & sharing

`p1_dtc_request_manager.py` **creates** missing **in-scope** requests
(`POST /v1/sheets`) in `dtc_document`, then re-scans + resolves. Gated by `dry_run`
(default `true` = preview). Newly created requests are **shared** (gated by
`share_on_create`): all views → `aiagentwip@lifung.com`, Full Version → the
`Kontoor Project Team` user group. Backfill existing requests with
`beproduct/p1utl_dtc_share_requests.py`. Names that don't parse are logged `NOT_IN_SCOPE`.

Names only resolve against **active + in-scope** registry rows, so a name whose
previous target request went **inactive** (hidden = deleted) falls through to
"missing" and is recreated under the same name on the next run (same
missing → create path). Conversely, if 2+ requests are concurrently active with
the identical name (DTC permits this — it IDs requests only by `requestId`), the
name is flagged `DUPLICATE_ACTIVE_NAME` and never resolved/created for, since we
cannot safely pick one.

---

## 5. DTC data model on ADB

All under `lft.beproduct`.

### `dtc_request_registry` — control table (1 row / request)

`environment`, `request_id`, `view_id`, `customer`, `season_code`, `brands`,
`sheet_id`, `request_reference`, `document_name`, `in_scope`, `request_is_active`,
`row_count`, `last_extracted`, `last_pushed`, `msgs`. Upserted on
`(environment, request_id)`.

### `dtc_wip_<customer>` — pulled sheet rows (1 row / DTC row)

Built from an **explicit schema** (so all-NULL columns don't trip
`CANNOT_DETERMINE_TYPE`); e.g. `dtc_wip_ktb`.

**Fixed columns:** `customer`, `workspace_name`, `document_name`, `request_id`,
`request_reference`, `season_code`, `brands`, `row_id` (STRING), `row_index`
(LONG), `bp_style_number` (Phase 6 match key), `lf_style_number`, `color_wash`,
`extracted_at` (TIMESTAMP), `data_json` (full row JSON).

- **Operation keys:** `row_id` → UPDATE (PATCH); `row_index` → INSERT/DELETE.
- **In-request match key:** `(BP Style#, Color / Wash)` (Phase 6; was `LF Style#`).
- **Cross-request identity:** `(customer, season_code, brand, bp_style_number, color_wash)`.

### `dtc_fabric_<customer>` — ⚠️ RETIRED and DROPPED (Phase 8a, 2026-09-01)

Superseded by a separate "MaterialLib" application, per project team
confirmation. No longer pulled/written, and `dtc_fabric_ktb` /
`dtc_fabric_registry` were DROPPED from Delta the same day (owner-confirmed
— zero downstream readers). Kept below for historical reference only. Prior
shape: `lf_material_id`, `its_key`, `mill_fabric_code`,
`mill_name`, `material_class`, `fabric_type`, `fabric_content` (→ BP Material
Description), `kb_fabric_code`, `adoption`, `season_code`, `brand`,
`sheet_type` (PROD/DEV/MILL), `mill_code`, `data_json`. View: FABRIC
`WIP_ITS_USE` (id `6a0ac943fedfa0ca7ff2bf48`, 120 fields).

### `dtc_lineplan_<customer>` — Phase 9a LinePlan rows

`lineplan_ref`, `projected_volume`, `target_ldp`, `target_fob`, `internal_sourced`,
`gender`, `category`, `product_line`, `region`, `season_launched`, `data_json`.
View: "Full" (id `69f0788555010bb745140ac4`, 30 fields). Exact DTC field names
(all UPPERCASE): `"PROJECTED VOLUME (season)"`, `"TARGET SAP w/ Tariff impact"`.

### `costing_chart` — Phase 9a Style × Color × Vendor/Factory

Key: `[customer, bp_style_no, color_name, lineplan_ref, factory_slot, supplier, factory]`.
Slots: Main / 1 / 2 / 3. HTS/Duty columns per slot; `tariff_rate = NULL` (Phase 9b).
Full overwrite each run.

### `dtc_request_mapping` — resolved requests (overwritten each run)

`environment`, `dtc_request_name`, `request_id`, `sheet_id`, `view_id`,
`season_code`, `brands`, `resolved_at`. Consumed by the push and image notebooks.

### `dtc_seasoncode_mapping` — `(CUSTOMER, BPSEASON, DTCCODE)`

Season-code **prefix** only; the year is algorithmic (last 2 digits of BeProduct
year). Forward-only (BeProduct → DTC), applied in `p1p7_beproduct_to_dtc_transform.py`.

### Sync logs

`beproduct_to_dtc_sync_log` (stages `resolve`/`create`/`share`/`push`/`images`) and
`dtc_to_beproduct_sync_log` (Phase 2).

`p1_pull_masters_to_delta.py` parameters: `dtc_environment` (uat|prod), `customer`
(also the table suffix), `dtc_workspace`, `dtc_document`, `catalog`/`schema`,
`write_mode` (overwrite|append), `refresh_registry`.

---

## 6. Troubleshooting

| Issue | Fix |
|-------|-----|
| `ModuleNotFoundError: connectors.dtc` | Deploy modules: `python scripts/upload_notebooks.py --modules-only`. |
| `401 Unauthorized` | `dtc_api_key_<env>` secret missing/expired. |
| `TABLE_OR_VIEW_NOT_FOUND … dtc_request_registry` | Run `00_init_request_registry.py` (or any notebook with `refresh_registry=true`) first. |
| `NO_IN_SCOPE_REQUESTS` | Registry has no in-scope active request for that customer/env — check `request_reference` parsing and `request_is_active`. |
| `400 … create` | Use the validated `POST /v1/sheets` body (§3): `requestReference`, non-empty description, array fields. |
| `400` on image upload | Source is webp — transcoded to PNG by Phase 3 (`classify_image_type`). |
| `CANNOT_DETERMINE_TYPE` | The pull builds an explicit schema; ensure no all-NULL column is created without a declared type. |
