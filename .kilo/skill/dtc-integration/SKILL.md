# DTC Integration Skill

How to read from and write to DTC (Data Collab sheets) through this repo's
connector, `dtc/python/connectors/dtc.py` (`DTCConnector`).

> **Authoritative docs win over this file:** `docs/PIPELINE.md` (what runs, every
> gate), `docs/SYNC_CONTRACT.md` (field directions, keys, PATCH allow-list),
> `docs/TROUBLESHOOTING.md`, then `docs/DTC_GUIDE.md` and `AGENTS.md` (verified
> discoveries, dated). Rewritten 2026-10-06. The old snapshot / change-log
> examples (`dtc_master_chart_uat`) described a pipeline removed on 2026-06-17
> and are gone.

## When to use this skill

- Reading DTC requests, views, view definitions or sheet rows.
- Writing to a DTC sheet: **update** rows, **append (add) rows**, upload a cell
  image, delete rows.
- Creating or sharing a DTC request.
- Debugging a DTC write that "succeeded" but did not do what you expected.

## Concepts

| Thing | Meaning |
|---|---|
| **Workspace / document** | e.g. workspace `KTB`, documents `KTB WIP`, `KTB LinePlan`, `XTS Master` |
| **Request** | one sheet's worth of work inside a document, named by `requestReference` (e.g. `KTB SS28 Collaborations`). Has a `sheetId` |
| **View** | a column subset of the sheet. The pipeline uses `WIP_ITS_USE`; users mostly work in `Full` |
| **Row** | `rowId` (UUID, **stable**) + `rowIndex` (int, **shifts** on every insert/delete/re-order) + cells keyed by column **display name** |

Address rows by **`rowId`** wherever the API allows it. `rowIndex` is
unsafe as an address (see the image and delete gotchas below).

## Connect

```python
import sys
sys.path.append(MODULE_PATH)   # the `module_path` job parameter; v2 root:
                               # /Workspace/Repos/beproduct-sync-v2/DTC/python
from connectors.dtc import DTCConnector

api_key = dbutils.secrets.get(scope="beproduct", key="dtc_api_key_uat")  # or _prod
dtc = DTCConnector(api_key=api_key, environment="uat", workspace_name="KTB")
# uat  -> https://dtc-api.lfuat.net/api
# prod -> https://dtc-api.lfapps.net/api
```

Auth is an `x-api-key` header, added by `client/rest_client.py`. Calls from a
local machine to `dtc-api.lfuat.net` must go through the proxy (environment
only, never in tracked files). A gateway 403 seen locally without the proxy
is the local network being refused, not a DTC finding.

## Read

```python
# List ACTIVE requests in a document (workspaceName + filters go in the BODY)
reqs = dtc.search_requests("KTB", document_name="KTB WIP",
                           filters={"requestIsActive": "Y"})

req   = dtc.get_request(request_id)          # -> sheetId, requestReference, ...
views = dtc.get_views(request_id)            # -> [{viewId, viewName}, ...]
sheet = dtc.get_sheet(req["sheetId"], view_id)
rows  = sheet["sheetData"]                   # [{rowId, rowIndex, "<col>": value, ...}]

# Which columns EXIST: always the view definition, never sheetData
cols  = dtc.get_view_column_names(view_id)   # GET /v1/views/{viewId} -> dynamicFields
vdef  = dtc.get_view_definition(view_id)     # type, formula, lookup, allowInsertRow ...

df, meta = dtc.pull_request_to_dataframe(request_id, view_id)  # pandas + doc metadata
```

- `sheetData` omits empty cells, so a blank column is invisible there. Use the
  view definition to check whether a column exists.
- In-scope request names and parsing: `sync.phase1.parse_request_reference()` /
  `is_in_scope()`. Excluded: `-SUPPLIER` requests and any name containing
  cancel / backup / archiv / delet.
- `dtc_request_registry` can hold several rows with the same
  `request_reference`. Always filter `request_is_active='Y' AND in_scope` and
  refuse on ambiguity.

## Write

DTC write contracts, all live-validated (dates in `AGENTS.md`):

### Update rows: `patch_rows`

```python
dtc.patch_rows(sheet_id, view_id, [
    {"rowId": "6a55…", "Sub Class": "Jacket", "Product Status": "Active"},
])
# PATCH /v1/sheets/{sheetId}/views/{viewId}  body {"sheetData":[…]}  -> 204
```

- **204 means stored** (proven 2026-09-15; an earlier "204 but not stored"
  claim was retracted).
- **Lean bodies only:** send just the changed fields, all from the PATCH
  allow-list in `docs/SYNC_CONTRACT.md` (AGENTS ground rule #6). Merge every
  change for one row into one object; a duplicated `rowId` in one body gets
  400 `Duplicate rowId found`.
- A body cannot mix `rowId` and `rowIndex` keys. Use PATCH for updates only.

### Add rows: `append_rows` (server-assigned locators, since 2026-09-23)

```python
assigned = dtc.append_rows(sheet_id, view_id, [
    {"BP Style#": "KTB-00040", "Color / Wash": "Black", "Fabric Group": "NO TPM BOM"},
    {"BP Style#": "KTB-00040", "Color / Wash": "Indigo", "Fabric Group": "NO TPM BOM"},
])
# POST /v1/sheets/{sheetId}/views/{viewId}/rows  body {"sheetData":[…]}
# -> 201 {"rows":[{"rowId":…,"rowIndex":…}, …]}   IN SEND ORDER
for sent, loc in zip(rows_sent, assigned):
    ...  # the new row's real rowId, without re-reading the sheet
```

- **Use this for every new row. Do NOT compute a `rowIndex`.** The old insert
  path (`patch_rows` / `create_row` / `patch_row(row_index=…)` from
  `get_max_row_index() + 1`) is v1-only. Any concurrent insert, delete or
  re-order after your read made the computed index wrong.
- **Never send `rowId` / `rowIndex` / `rowStatus`**. The connector raises
  `ValueError`, and DTC rejects the whole request. When copying a live row
  forward (the BOM fan-out), strip them first with
  `bom.INSERT_EXCLUDE_COLS` / `bom.build_insert_row_payload()`. That also
  drops image (`contact`) and `formula` columns, which DTC refuses to accept
  as data.
- The view must allow inserts (`allowInsertRow="Y"`), and **every mandatory
  field must be supplied**. `WIP_ITS_USE` has zero mandatory fields today;
  re-check if the view changes.
- Image cells cannot be set here. Append first, then upload against the
  returned `rowId`.
- Row limit: 3000 per request. If an append would exceed it, nothing is saved.
- v2 order inside the one write window: PATCH (updates), then POST (inserts),
  back to back (`dtc/notebooks/v2_wip_push.py`).

### Upload a cell image: `upload_row_image`

```python
dtc.upload_row_image(sheet_id, view_id, row_id=row_id, image_bytes=png,
                     column_name="Style Image", filename="x.png",
                     content_type="image/png")
# POST …/images?rowid={rowId}&columnname=Style Image   multipart, part "file"
```

- **Always pass `row_id`.** A stale or non-existent `rowindex` returns **201
  and CREATES a row** holding the image. A bad `rowid` fails loudly (400).
- The query param is lowercase `rowid`. CamelCase `rowId` is silently ignored.
- DTC stores jpg/png only. Phase 3 transcodes webp/gif/bmp/tiff to PNG, and
  rasterises page 1 of a PDF-compatible `.ai`/PDF (pypdfium2). It skips SVG
  and PostScript-only `.ai`.
- `Style Image` can never be written through `sheetData` (400).

### Delete rows: `delete_rows`

```python
dtc.delete_rows(sheet_id, view_id, [row_index, ...])
# DELETE …/rows  body {"rowIndexes":[…]}  -> 204
```

- Keyed by `rowIndex` (there is no delete-by-rowId).
- **A single-row delete is exact.** A bulk delete processes **at most ~11 rows
  per call**, renumbers the survivors, and still returns 204. To empty a sheet,
  LOOP: re-read, delete the indexes it reports, repeat until empty.
- Nothing in the pipeline deletes rows.

### Create and share a request

```python
r = dtc.create_sheet("KTB", "KTB WIP", "KTB SS28 Wrangler Western",
                     request_description="Created by sync")   # MUST be non-empty
# POST /v1/sheets -> 201, normalised to {requestId, sheetId, raw}
dtc.share_request_with_user(r["requestId"], "aiagentwip@lifung.com", view_names, send_email="N")
dtc.share_request_with_usergroup(r["requestId"], "Kontoor Project Team", ["Full Version"])
```

A new request is visible only to the API identity until it is shared. There
is no delete-request in the connector, so `create_sheet` is effectively
irreversible.

## Gotchas that have cost real time

- **Lookup/formula fields are only materialised if they are on the view the
  user SAVES through.** A cell can be NULL in storage while looking correct in
  the UI. Example: the `Factory Production Country for …` lookups. Writing a
  lookup field is accepted (204) and silently ignored.
- `isReadOnly` is unreliable. Use `type == "contact"` (images) and a truthy
  `formula` to decide what cannot be written (`bom.compute_non_writable_cols()`).
- DTC renames columns silently (e.g. `Lineplan Ref #` became `LinePlan ref#`
  on 2026-09-28). Read keys through a fallback list
  (`sync/lineplan.LINEPLAN_REF_COLS`), and treat zero rows from non-empty
  input as an alert.
- `GET` can return the **same rowId twice** (seen on a LinePlan sheet,
  2026-09-29). If a PATCH 400s with `Duplicate rowId found`, check the live
  `sheetData` before anything else.
- DTC-hosted image URLs (`/api/v1/images/*`) need `x-api-key`, and from
  Databricks they currently get 403 from DTC's gateway (source-IP allow-list).
  Uploads are unaffected.
- **Locking:** every WRITE moves the request's server-side `last_read` and can
  silently discard an open user's save. Reads are free. So: zero diff means
  zero write, and all writes happen in one window per run.

## Reference

### `DTCConnector` methods

| Method | Endpoint |
|---|---|
| `search_requests(workspace, document_name, filters)` | `GET /v1/requests` (body) |
| `get_request(id)` / `get_views(id)` / `get_request_scope(id)` | `GET /v1/requests/{id}[/views]` |
| `get_sheet(sheet_id, view_id)` | `GET /v1/sheets/{s}/views/{v}` |
| `get_view_definition(view_id)` / `get_view_column_names(view_id)` | `GET /v1/views/{v}` |
| `pull_request_to_dataframe(request_id, view_id)` | read helper, returns `(DataFrame, metadata)` |
| `patch_rows(s, v, sheet_data)` | `PATCH /v1/sheets/{s}/views/{v}`: **update** |
| `append_rows(s, v, sheet_data)` | `POST /v1/sheets/{s}/views/{v}/rows`: **add rows** |
| `upload_row_image(s, v, row_id=…, image_bytes=…)` | `POST /v1/sheets/{s}/views/{v}/images` |
| `delete_rows(s, v, row_indexes)` / `delete_row` | `DELETE /v1/sheets/{s}/views/{v}/rows` |
| `create_sheet(...)` | `POST /v1/sheets` |
| `share_request_with_user` / `share_request_with_usergroup` | `POST /v1/requests/{id}/shares/...` |
| `get_request_shares` / `get_request_share_usergroups` | `GET /v1/requests/{id}/shares[/usergroups]` |
| `create_row`, `patch_row(row_index=…)`, `get_max_row_index` | **v1 legacy insert path. Do not use for new code** |

### Project files

- Connector: `dtc/python/connectors/dtc.py`; REST client: `dtc/python/client/rest_client.py`
- Write planner (allow-list, zero-diff-zero-write): `dtc/python/sync/wip_plan.py`
- The write window: `dtc/notebooks/v2_wip_push.py`; images: `beproduct/p3_beproduct_to_dtc_images.py`
- Pull: `dtc/notebooks/p1_pull_masters_to_delta.py` (+ `sync/registry.py`)
- One-cell utility: `dtc/notebooks/v2_set_dtc_cell.py`. Run notebooks ad hoc
  with `scripts/run_v2_task.py`, which creates a dev-tagged job.
