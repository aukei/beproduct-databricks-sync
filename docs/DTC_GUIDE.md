# DTC Sync Guide

How DTC ("Data Collab") WIP requests are pulled to Databricks Delta and how
BeProduct data is pushed to / from them.

> **Source of truth:** field mapping lives in
> `docs/beproduct_style_interested_fields.txt`; the data model lives in
> `dtc/DATA_MODEL.md`; the end-to-end flows live in `dtc/PHASE1_WORKFLOW.md`
> (BeProduct → DTC) and `dtc/PHASE2_WORKFLOW.md` (DTC → BeProduct). Verified API
> behaviour is logged in `AGENTS.md`.

---

## Overview

DTC organizes data as **Workspace → Document → Request → Sheet → View**. A
**Request** (e.g. `KTB FW26 Wrangler`) is an instance of a **Document**
(e.g. `KTB WIP`); **Views** are column projections on the Document.

Phase 1 only ever reads the **`WIP_ITS_USE`** view — the canonical column
projection used for sync. Requests whose registered view is anything else are
skipped and logged.

Discovery is **registry-driven**: `dtc_request_registry` lists the in-scope
requests and their resolved `WIP_ITS_USE` `view_id`. See "Request registry" below.

---

## Prerequisites

Secrets in the `beproduct` scope:

```bash
databricks secrets put --scope beproduct --key dtc_api_key_uat
databricks secrets put --scope beproduct --key dtc_api_key_prod
```

The notebook picks `dtc_api_key_<environment>` automatically.

---

## Request naming & in-scope rule

A request reference must parse as `<customer> <seasonCode> <brand>`, where
`seasonCode` is 2 letters + 2 digits (e.g. `FW26`). A request is **in scope** when
it parses cleanly **and** its customer token matches the configured `customer`
(e.g. `KTB FW26 Wrangler` is in scope for `KTB`; `KON FW26 Wrangler` is not).

Parsing and the in-scope test live in `dtc/python/sync/phase1.py`
(`parse_request_reference`, `is_in_scope`).

---

## Registry scan (shared)

The registry is refreshed by the shared helper `sync.registry.refresh` — discover
(`search_requests`) → enrich by-id → upsert (`mode=merge`, preserving
`last_extracted` / `last_pushed` / `row_count`). It runs **automatically** inside
`pull_requests_to_delta` and `dtc_request_manager` (both default
`refresh_registry=true`), so the registry mirrors the workspace+document each run.
`00_init_request_registry.py` is the same scan as a standalone notebook (first build
or targeted `request_ids`); it's optional for routine runs.

## Workflow: pull DTC requests → Delta

**`dtc/notebooks/pull_requests_to_delta.py`** scans+refreshes the registry, then
pulls the `WIP_ITS_USE` view of every in-scope, active request into one table per
customer: **`lft.beproduct.dtc_wip_<customer>`** (e.g. `dtc_wip_ktb`), updating each
request's `last_extracted` / `row_count`.

Parameters: `dtc_environment` (uat|prod), `customer` (KTB), `dtc_workspace` (KTB),
`dtc_document` (KTB WIP), `catalog`/`schema` (lft/beproduct), `write_mode`
(overwrite|append), `refresh_registry` (true).

### Output table

`lft.beproduct.dtc_wip_<customer>` — one row per DTC sheet row, built from an
explicit schema (so all-NULL columns don't trip `CANNOT_DETERMINE_TYPE`). Full
column/type list and keys: see **`dtc/DATA_MODEL.md` → "DTC Data Table Structure"**.

---

## SeasonCode mapping

`lft.beproduct.dtc_seasoncode_mapping` `[CUSTOMER, BPSEASON, DTCCODE]` maps a
BeProduct season name to the DTC season-code **prefix** (the year is algorithmic:
last 2 digits of the BeProduct year). Applied **forward-only** (BeProduct → DTC) in
`beproduct/beproduct_to_dtc_transform.py`; created by
`dtc/notebooks/00_init_season_mapping.py`. Season is a fixed per-request key, so the
DTC → BeProduct direction never reverse-maps it.

---

## Push directions

- **BeProduct → DTC (Phase 1):** `beproduct/beproduct_to_dtc_transform.py` →
  `dtc_request_manager.py` → `beproduct_to_dtc_push.py`. See `dtc/PHASE1_WORKFLOW.md`.
- **DTC → BeProduct (Phase 2):** `dtc/notebooks/05_push_dtc_to_beproduct.py`. See
  `dtc/PHASE2_WORKFLOW.md`.

### Missing-request creation

`dtc_request_manager.py` **creates** missing **in-scope** DTC requests
(`POST /v1/sheets`) in `dtc_document`, then re-scans + resolves them. Gated by
`dry_run` (default `true` = preview/log only; set `false` to create). Names that
don't parse as `<customer> <seasonCode> <brand>` are logged `NOT_IN_SCOPE` and never
created. Creation/skip events are written to `beproduct_to_dtc_sync_log`
(stage `create`).

---

## DTC API reference (validated)

- **Sheet upsert:** `PATCH /v1/sheets/{sheetId}/views/{viewId}` body
  `{"sheetData":[{...,"rowId"|"rowIndex":..}]}` → 204. A single PATCH **cannot mix**
  `rowId` (update) and `rowIndex` (insert) — send separate batches.
- **Row delete:** `DELETE /v1/sheets/{sheetId}/views/{viewId}/rows` body
  `{"rowIndexes":[...]}` → 204 (keys off `rowIndex`).
- **Request listing:** `GET /v1/requests` with `workspaceName` + `filters` in the
  **JSON body** (not query params).
- **Allowed columns:** from the view definition `GET /v1/views/{viewId}`
  (`dynamicFields`), not from sheet cells (empty columns don't appear in `sheetData`).

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `ModuleNotFoundError: No module named 'connectors.dtc'` | Modules must be deployed to `/Workspace/Repos/beproduct-sync/DTC/python` (`python scripts/upload_notebooks.py --modules-only`). |
| `401 Unauthorized` | `dtc_api_key_<env>` secret missing/expired. |
| `TABLE_OR_VIEW_NOT_FOUND … dtc_request_registry` | Run `00_init_request_registry.py` first. |
| `NO_IN_SCOPE_REQUESTS` | Registry has no in-scope, active request for that customer/env — check `request_reference` parsing and `request_is_active`. |
| `CANNOT_DETERMINE_TYPE` | Fixed: pull builds an explicit schema. If reintroduced, ensure no all-NULL column is created without a declared type. |

---

## Related docs

- `dtc/DATA_MODEL.md` — tables, keys, `dtc_wip` schema, season mapping.
- `dtc/PHASE1_WORKFLOW.md` / `dtc/PHASE2_WORKFLOW.md` — full flows.
- `docs/BEPRODUCT_TO_DTC_GUIDE.md` — cross-platform integration detail.
- `AGENTS.md` — verified discoveries & invariants.
