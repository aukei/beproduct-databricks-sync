# DTC Component Guide

Everything the jobs need about the **DTC** ("Data Collab") side: the API surface
used, the connector, and the DTC-related tables on Databricks (`lft.beproduct`).

> Systems & data model: [ARCHITECTURE.md](ARCHITECTURE.md). What runs and in
> what order, with every gate: [PIPELINE.md](PIPELINE.md). Field directions,
> match keys and the WIP PATCH allow-list: [SYNC_CONTRACT.md](SYNC_CONTRACT.md).
> Diagnosing a symptom: [TROUBLESHOOTING.md](TROUBLESHOOTING.md). Verified API
> behaviour: [../AGENTS.md](../AGENTS.md).

---

## 1. DTC model

`Workspace → Document → Request → Sheet → View`. A **Request** (e.g.
`KTB FW26 Wrangler`) instantiates a **Document** (`KTB WIP`); **Views** are column
projections on the Document. Sync only ever reads and writes through the
**`WIP_ITS_USE`** view (complete data). Requests registered with any other view
are skipped + logged. Users typically save through the **Full** view — which
matters for lookup fields (below).

**In-scope rule** (`phase1.is_in_scope`): parses as `<customer> <seasonCode>
<brand>`, customer token matches, and the name contains no
cancel / backup / archive / delete word and no `-SUPPLIER`. Full rule:
[SYNC_CONTRACT.md](SYNC_CONTRACT.md) → Scope.

**Locking.** Any successful write moves the request's server-side `last_read`;
every browser session loaded earlier is refused on save and must reload. Reads
are free. The scope is the whole request.

**Lookup / formula fields** are computed by DTC and **only materialized if the
column is on the view the user saved through**. A lookup hidden in "Full" (e.g.
`Factory Production Country for …`, `Main Factory Customer ID`) stays NULL in
storage while the UI shows a value, so the API — and this pipeline — see NULL.
Formula and `contact`-type (image) columns reject writes.

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
| View definition | `GET /v1/views/{viewId}` | `dynamicFields[]` (`fieldName`, `type`, `formula`) = the **authoritative** column list (205 fields on `WIP_ITS_USE`). Can 403 intermittently at the gateway; `wip_push` unions a static fallback. `sheetData` only contains populated cells, so never infer "column missing" from sheet data or `dtc_wip_ktb`. View id `6a3907f6df772fd797ee5b7c` belongs to "XTS Master", not KTB WIP. |
| Get sheet rows | `GET /v1/sheets/{sheetId}/views/{viewId}` | returns `sheetData[]` with `rowId`/`rowIndex` + populated columns. |
| **Update rows** | `PATCH /v1/sheets/{sheetId}/views/{viewId}` | body `{"sheetData":[{…,"rowId":…}]}` → **204 = stored**. |
| **Append rows** | `POST /v1/sheets/{sheetId}/views/{viewId}/rows` | body `{"sheetData":[{…}]}` with **no** `rowId`/`rowIndex` (either → 400) → **201** `{"rows":[{rowId,rowIndex}…]}` in send order. Used for every v2 insert. |
| Delete rows | `DELETE /v1/sheets/{sheetId}/views/{viewId}/rows` | body `{"rowIndexes":[…]}` → 204, but removes **at most ~11 rows per call** (silently) and renumbers the rest — loop re-read + delete. |
| **Create request/sheet** | `POST /v1/sheets` | → **201**. Body must use `requestReference` (NOT `requestName`), a **non-empty** `requestDescription`, `viewName`, and `requestAssigneeSharingViewNames`/`sheetData` as **arrays** (empty `[]` ok). Response nests ids under `data` with a capital-S `SheetId`. |
| **Share (user)** | `POST /v1/requests/{requestId}/shares/{userEmail}` | body `{"viewNames":[…],"message":"…","sendEmail":"Y\|N"}` → 201. |
| **Share (group)** | `POST /v1/requests/{requestId}/shares/usergroups/{userGroupName}` | path segment URL-encoded (group names have spaces). |
| Read shares | `GET …/shares`, `GET …/shares/usergroups` | used for idempotency. |
| **Image upload** | `POST /v1/sheets/{sheetId}/views/{viewId}/images?rowid={uuid}&columnname=Style Image` | `multipart/form-data`, file part named `file`. Parameter is lowercase **`rowid`** (camelCase is ignored). Never use `rowindex`: a non-existent index returns 201 and **creates a row**. DTC stores **jpg/png only** (webp → 400). Phase 3 transcodes webp/gif/bmp/tiff to PNG and rasterises page 1 of a PDF-compatible `.ai`/PDF. |

Connector methods: `search_requests`, `get_request`, `get_views`,
`get_view_definition`/`get_view_column_names`, `get_sheet`, `patch_rows`,
`append_rows`, `delete_rows`, `create_sheet`,
`share_request_with_user`/`share_request_with_usergroup`/`get_request_shares`/
`get_request_share_usergroups`, `upload_row_image`.

---

## 4. Notebooks (DTC side)

| Notebook | Stage | Does | Writes |
|----------|-------|------|--------|
| `p0_pull_xts_master_to_delta.py` | 00 | Pull XTS Supplier/Factory Master | `dtc_xts_master_ktb` |
| `p1_pull_masters_to_delta.py` | 10 | Refresh WIP registry; pull each in-scope active request's `WIP_ITS_USE` view | `dtc_wip_<customer>`, `dtc_request_registry` |
| `p9a_pull_lineplan_to_delta.py` | 10 | Pull every active KTB LinePlan request (no name filter; `Full` view) | `dtc_lineplan_<customer>`, `dtc_lineplan_registry` |
| `v2_pull_bom_segments.py` | 20b | Lakebase techpack BOM (serverless-only source) | `bom_segments` |
| `p9a_build_costing_chart.py` | 30 | Intent overlay + LinePlan INNER join + 4-slot transpose + cache fill | `costing_chart` (full overwrite) |
| `v2_wip_push.py` | 40 | **The single DTC write window** (style + material + duty) | DTC WIP, `beproduct_to_dtc_sync_log` |
| `p2_push_dtc_to_beproduct.py` | 50 | DTC-owned fields → BeProduct (Vendor, Factory, Customer Factory ID, COO, Lot#) | BeProduct, `dtc_to_beproduct_sync_log` |
| `v2_push_customer_code.py` | 55 | DTC customer code → BeProduct material master (**disabled**) | BeProduct |
| `p9b1_compute_duty_rates.py` | duty_compute job | NT Orbit lookups with persistent cache; zero DTC contact | `costing_chart`, `nt_orbit_duty_cache`, `nt_orbit_oauth_state` |
| `00_init_request_registry.py` | on demand | Standalone WIP registry build/refresh | `dtc_request_registry` |
| `00_init_season_mapping.py` | on demand | Seed the season-code prefix table | `dtc_seasoncode_mapping` |
| `v2_inspect_requests.py` / `v2_set_dtc_cell.py` | on demand | Read-only request inventory / set one cell safely | — / one DTC cell |

v1-only, not scheduled: `p10_pull_bom_and_enrich.py`, `p9b2_push_duty_to_wip.py`,
`p9b_fill_duty_rates.py`, `p8a_pull_fabric_to_delta.py` (retired).

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

A **start-of-run snapshot**: a DTC save made after `pull_master_dtc` is seen by
the next run. `data_json` holds only populated cells. `row_id` is the stable
locator; `row_index` is kept for deterministic tie-breaks only. Keys:
[SYNC_CONTRACT.md](SYNC_CONTRACT.md) → Keys.

`dtc_fabric_<customer>` / `dtc_fabric_registry` — retired Phase 8a tables, DROPPED 2026-09-01.

### `dtc_lineplan_<customer>` — LinePlan rows (Stage 10)

`lineplan_ref`, `projected_volume`, `target_ldp`, `target_fob`, `internal_sourced`,
`gender`, `category`, `product_line`, `region`, `season_launched`, `data_json`.
View: "Full" (id `69f0788555010bb745140ac4`, 30 fields). Exact DTC field names
(all UPPERCASE): `"PROJECTED VOLUME (season)"`, `"TARGET SAP w/ Tariff impact"`.

### `costing_chart` — Stage 30

Style × colour × material × vendor slot, Main Fabric rows only. Columns, key and
gates: [PIPELINE.md](PIPELINE.md) → Stage 30 and
[ARCHITECTURE.md](ARCHITECTURE.md) § 5. A WIP row with no matching
`LinePlan ref#` is dropped (INNER join), not surfaced with nulls.

### `dtc_request_mapping` — resolved requests (overwritten each run)

`environment`, `dtc_request_name`, `request_id`, `sheet_id`, `view_id`,
`season_code`, `brands`, `resolved_at`. Consumed by the push and image notebooks.

### `dtc_seasoncode_mapping` — `(CUSTOMER, BPSEASON, DTCCODE)`

Season-code **prefix** only; the year is algorithmic (last 2 digits of BeProduct
year). Forward-only (BeProduct → DTC), applied in `p1p7_beproduct_to_dtc_transform.py`.

### Sync logs

`beproduct_to_dtc_sync_log` (stages `resolve`/`create`/`share`/`registry_audit`/`wip_push`/`images`) and
`dtc_to_beproduct_sync_log` (Stage 50). Query recipes: TROUBLESHOOTING.md § 0.4.

`p1_pull_masters_to_delta.py` parameters: `dtc_environment` (uat|prod), `customer`
(also the table suffix), `dtc_workspace`, `dtc_document`, `catalog`/`schema`,
`write_mode` (overwrite|append), `refresh_registry`.

---

## 6. Troubleshooting

Symptom runbooks and job-level errors: [TROUBLESHOOTING.md](TROUBLESHOOTING.md).
DTC-specific extras:

| Issue | Fix |
|-------|-----|
| `TABLE_OR_VIEW_NOT_FOUND … dtc_request_registry` | Run `00_init_request_registry.py` (or any notebook with `refresh_registry=true`) first. |
| `400 … create` | Use the validated `POST /v1/sheets` body (§3): `requestReference`, non-empty description, array fields. |
| `CANNOT_DETERMINE_TYPE` | The pull builds an explicit schema; ensure no all-NULL column is created without a declared type. |
| A value visible in DTC is NULL via the API | Lookup/formula column not on the view the user saved through (§1). |
