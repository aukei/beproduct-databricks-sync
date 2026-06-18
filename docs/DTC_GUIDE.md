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
| View definition | `GET /v1/views/{viewId}` | `dynamicFields[].fieldName` = the **authoritative** allowed-column list (empty columns don't appear in sheet data). |
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
| `dtc/notebooks/00_init_request_registry.py` | Standalone registry build/refresh (first build or targeted `request_ids`). | `dtc_request_registry` |
| `dtc/notebooks/00_init_season_mapping.py` | Seed the season-code prefix table. | `dtc_seasoncode_mapping` |
| `dtc/notebooks/pull_requests_to_delta.py` | Refresh registry, then pull each in-scope active request's `WIP_ITS_USE` view. | `dtc_wip_<customer>` |
| `dtc/notebooks/05_push_dtc_to_beproduct.py` | Phase 2 pushback of DTC-owned fields. | BeProduct (+ `dtc_to_beproduct_sync_log`) |

`beproduct/dtc_request_manager.py` (BeProduct-side, but DTC-writing) resolves /
**creates** / **shares** requests and writes `dtc_request_mapping`.

### Registry scan (shared)

`sync.registry.refresh` = discover (`search_requests`) → enrich by-id → upsert
(`mode=merge`, preserving `last_extracted`/`last_pushed`/`row_count`). It runs
automatically inside `pull_requests_to_delta` and `dtc_request_manager` (both
default `refresh_registry=true`), so the registry mirrors the workspace+document
each run; `00_init_request_registry.py` is the same scan standalone. After a full
auto-discover, in-scope rows absent from the scan are **marked** inactive
(`request_is_active='N'`, `in_scope=false`) — a mark, not a delete.

### Missing-request creation & sharing

`dtc_request_manager.py` **creates** missing **in-scope** requests
(`POST /v1/sheets`) in `dtc_document`, then re-scans + resolves. Gated by `dry_run`
(default `true` = preview). Newly created requests are **shared** (gated by
`share_on_create`): all views → `aiagentwip@lifung.com`, Full Version → the
`Fabric Group` user group. Backfill existing requests with
`beproduct/dtc_share_requests.py`. Names that don't parse are logged `NOT_IN_SCOPE`.

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
(LONG), `lf_style_number`, `color_wash`, `extracted_at` (TIMESTAMP), `data_json`.
**Dynamic columns:** every view column flattened to `col_<normalized_name>`
(STRING; may be absent when empty for a request). Full fidelity always in
`data_json`.

- **Operation keys:** `row_id` → UPDATE (PATCH); `row_index` → INSERT/DELETE.
- **In-request match key:** `(lf_style_number, color_wash)`.
- **Cross-request identity:** `(customer, season_code, brands, lf_style_number, color_wash)`.

### `dtc_request_mapping` — resolved requests (overwritten each run)

`environment`, `dtc_request_name`, `request_id`, `sheet_id`, `view_id`,
`season_code`, `brands`, `resolved_at`. Consumed by the push and image notebooks.

### `dtc_seasoncode_mapping` — `(CUSTOMER, BPSEASON, DTCCODE)`

Season-code **prefix** only; the year is algorithmic (last 2 digits of BeProduct
year). Forward-only (BeProduct → DTC), applied in `beproduct_to_dtc_transform.py`.

### Sync logs

`beproduct_to_dtc_sync_log` (stages `resolve`/`create`/`share`/`push`/`images`) and
`dtc_to_beproduct_sync_log` (Phase 2).

`pull_requests_to_delta.py` parameters: `dtc_environment` (uat|prod), `customer`
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
