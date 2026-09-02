# Phase 10: BOM enrichment from externally-processed techpack data

**Status:** Implemented ✅ — **wired into the daily DAG** (2026-09-02), gated
by `run_phase10` (default `false` until UAT-validated live; **live-validated
dry-run 2026-09-02**: 8 matched styles, 17 updates + 4 inserts planned, 0
errors). Placed BEFORE `build_costing_chart` (Phase 9a), not after — the
point is to get up-to-date material names into `costing_chart`'s
`fabric_content` (part of `product_description`) before Phase 9b calls NT
Orbit for duty classification:

```
                    ┌─► gate_phase10 ─► fill_bom_data ─► repull_dtc_bom ─┐
phase0_push ────────┤     (run_if=ALL_DONE)                              ├─► build_costing_chart ─► gate_phase9b ─► fill_duty_rates
                    └─► gate_phase9a ─► pull_lineplan_dtc ───────────────┘
```

Notebook: `dtc/notebooks/p10_pull_bom_and_enrich.py` (runs on **serverless
compute** — see "Critical constraint" below). Pure logic + tests:
`dtc/python/sync/bom.py` / `dtc/tests/test_bom.py`.

---

## Why this phase exists

BOM (Bill of Materials) data is not available from the BeProduct API at all
— it's produced by a separate techpack-extraction pipeline and landed in:

```
alb_tpm_uat.public.customer_teckpack_style_log   (UAT)
alb_tpm_prd.public.customer_teckpack_style_log   (PRD)
```

Both catalogs are live-confirmed directly reachable from this workspace's
Unity Catalog metastore (`SHOW CATALOGS` lists them) via `spark.table(...)`
— **but see the critical constraint below.**

## Critical constraint — serverless compute required

`alb_tpm_uat`/`alb_tpm_prd` are **Lakebase databases** registered in Unity
Catalog, not plain Delta-backed catalogs. The classic shared job cluster
(`Standard_D4s_v3`, used by every other task in `BeProduct_DTC_sync_dag`)
fails outright with:

```
UnauthorizedAccessException: ... requires serverless compute. A Lakebase
database registered in Unity Catalog can only be queried from a serverless
SQL warehouse or serverless general compute; Pro and Classic SQL warehouses
are not supported.
```

Live-discovered 2026-09-02 — an earlier local validation via a SQL warehouse
succeeded (that warehouse happens to be serverless), which masked this until
the actual job task was run. **Fixed**: `scripts/deploy_job.py`'s `nb_task()`
helper takes a `serverless=True` flag (omits `job_cluster_key` entirely,
which runs the task on serverless compute) — set ONLY for `fill_bom_data`;
every other task in the job stays on the shared classic cluster. Serverless
compute reads ordinary Unity Catalog Delta tables (`ktb_styles`,
`dtc_wip_ktb`) just fine too, so no other code needed to change.

## Join

```sql
ktb_styles.bp_style_number = customer_teckpack_style_log.style_no
AND (ktb_styles.season || ' - ' || ktb_styles.year) = customer_teckpack_style_log.style_season
```

INNER JOIN only — a BeProduct style with no matching BOM row is simply not
processed (not an error). Live-validated against the same KONTOOR/Wrangler
test data used throughout Phase 9a/9b: `KTB-00016`..`KTB-00023` all appear
in `customer_teckpack_style_log` with `style_season="Spring - 2028"`,
matching `ktb_styles.season="Spring"` + `.year="2028"`.

`style_season`'s literal format varies WILDLY by customer in this
multi-tenant table (`"SS26"`, `"SS 2027"`, `"FH 2026"`, `"Spring - 2028"`,
...) — the notebook pre-filters `customer_name = bom_customer_name` (job
param, default `"KONTOOR"`) purely as a scoping/performance optimization;
the join keys alone are already customer-correct without it.

`customer_teckpack_style_log` can carry multiple rows per (style_no,
style_season) over time (`current_version` column, re-extracted techpacks)
— the notebook keeps only the highest `current_version` (tie-broken by the
most recent `timestamp_lf_captured`) per key. Not live-validated against a
real multi-version example (none exists yet in the KONTOOR test data).

## BOM JSON parsing

`bom_unified` is a JSON string shaped like:

```json
[{"part": "BOM", "details": [
    {"bom_detail_name": "Main Fabric", "material_name": "...", "material_no": "...", "placement": "...", "..." : "..."},
    {"bom_detail_name": "Fabric", "..." : "..."},
    {"bom_detail_name": "Stitch/Seam", "..." : "..."},
    {"bom_detail_name": "Trim", "..." : "..."},
    {"bom_detail_name": "Label", "..." : "..."}
], "column_header": [...]}]
```

Only two `bom_detail_name` values matter: **"Main Fabric"** (exactly ONE per
style, by construction) and **"Fabric"** (zero or more — corrected
2026-09-02; see decisions log, `AGENTS.md`. Live-confirmed across all 16
KONTOOR rows: "Body" never appears at all; "Fabric" genuinely does, in 3/16
styles — `KTB-00020`, `KTB-00023`, `CB-S28003`, `s234160`). `"Stitch/Seam"`,
`"Trim"`, `"Label"` are live-confirmed present and NOT used.

## Enrichment decision logic

1. A style's WIP rows are enriched **only if NONE of them already carry real
   `Fabric Group` data** — a single already-enriched row (`Fabric Group` !=
   the DTC placeholder `"MAIN MATERIAL CONTENT"`) short-circuits the WHOLE
   style to a no-op. Phase 10 never partially re-enriches a style some of
   whose colorway rows already have real data.
2. Otherwise, every currently-placeholder row for that style gets `Fabric
   Group` / `Placement` / `Mill Fabric Article #` set from the "Main Fabric"
   segment.
3. For **each** "Fabric" segment found (0 or more), each such row is ALSO
   duplicated into a brand-new row carrying THAT "Fabric" segment's values
   instead — i.e. an N-colorway style with 1 "Main Fabric" + M "Fabric"
   segments produces N UPDATEs + N×M INSERTs.
4. **`Fabric Group` is set to the segment's own `bom_detail_name`** (literally
   `"Main Fabric"` or `"Fabric"`) — corrected 2026-09-02, NOT `material_name`.
   `Placement` / `Mill Fabric Article #` are unaffected by this correction —
   still `placement` / `material_no`.
5. A BOM row with neither segment (e.g. only `"Trim"`/`"Label"`/`"Stitch/Seam"`)
   is a no-op for that style.

Raw DTC WIP field names used for the PATCH (live-confirmed via
`GET /v1/views/{WIP_ITS_USE view id}` `dynamicFields`, 2026-09-02 — NOT the
Delta `col_*` normalized names):

| Module field | Raw DTC field |
|---|---|
| `fabric_group` | `Fabric Group` |
| `placement` | `Placement` |
| `mill_fabric_article` | `Mill Fabric Article #` |

## Push mechanics

UPDATEs are sent as `sheetData` PATCH objects keyed by `rowId` (existing
rows); INSERTs are sent keyed by `rowIndex` (new rows, built by copying the
FULL original row's fields from `data_json` and overriding just the 3 BOM
fields) — matches the established "cannot mix `rowId` and `rowIndex` in one
PATCH call" contract (`DTCConnector.patch_rows`; see Phase 9b's own
`repull_dtc_bom`/PATCH-batching notes and `AGENTS.md`'s `create_sheet` note).

This notebook never mutates the local Delta `dtc_wip_ktb` table directly —
it pushes to the LIVE DTC sheet only. `repull_dtc_bom` (a full
`p1_pull_masters_to_delta` re-pull, `run_if=ALL_DONE` on `fill_bom_data`, no
gate of its own) runs immediately after so `build_costing_chart` sees the
enrichment; it executes unconditionally so a disabled/skipped/failed Phase
10 never blocks Phase 9a.

## Parameters

| Widget / job param | Default | Notes |
|---|---|---|
| `run_phase10` | `false` | Job-level gate; default off until fully UAT-validated live end-to-end. |
| `bom_catalog` | `alb_tpm_uat` | **NOT** derived from `dtc_environment` — the PRD suffix is `_prd`, not `_prod`. |
| `bom_customer_name` | `KONTOOR` | Scoping/perf pre-filter only; the join keys alone are already customer-correct. |
| `folder_name` | `TEST KTB` | Shared with `bp_style_sync`; scopes which BeProduct styles to consider. |
| `dry_run` | `true` (notebook default) / `false` (deployed job) | Same convention as every other phase. |

## Tests

`dtc/tests/test_bom.py` — pure-Python, unit tests all `dtc/python/sync/bom.py`
decision logic (JSON parsing, no-op/update/duplicate fan-out, field mapping)
against both the exact spec sample and real KTB-00016/KTB-00023 BOM payloads.
No Spark/network required.
