# Phase 10: BOM enrichment from externally-processed techpack data

**Status:** Implemented ✅ — **wired into the daily DAG** (2026-09-02), gated
by `run_phase10` (default `false` until UAT-validated live; **live-validated
dry-run 2026-09-02**: 8 matched styles, 17 updates + 4 inserts planned, 0
errors). Placed BEFORE `build_costing_chart` (Phase 9a), not after — the
point is to get up-to-date material names into `costing_chart`'s
`fabric_content` (part of `product_description`) before Phase 9b calls NT
Orbit for duty classification:

```
phase1_push ─► repull_dtc ─┬─► fill_bom_data ─► repull_dtc_bom ─┐
                           │                                    ├─► build_costing_chart ─► gate_phase9b ─► fill_duty_rates
                           └─► gate_phase3[true] ─► phase3_images       gate_phase9a ─► pull_lineplan_dtc ┘
```

**Depends on `repull_dtc`, NOT `pull_master_dtc` directly** (changed
2026-09-02, owner clarification of intended lineage): Phase 1 completes the
style×color WIP master chart first (Phase 3 image upload is optional, not a
blocker); Phase 10 then needs to read that COMPLETE post-Phase-1 style×color
state to fill in the material dimension. `pull_master_dtc`'s snapshot is
taken BEFORE `phase1_push` runs, so it can be missing style×color rows
`phase1_push` just created in DTC this same run. `repull_dtc` is what makes
those rows visible in Delta, and is now a SHARED, unconditional prerequisite
for both `phase3_images` and `fill_bom_data` — it no longer depends on
`gate_phase3`, so Phase 10 is never held hostage to whether images (`run_phase3`)
are wanted; only `phase3_images` itself still checks `run_phase3`.

**IMPORTANT — `run_phase10` (and, as of 2026-09-02, `run_phase1`) is checked
INSIDE the notebook, NOT via a DAG-level `gate_phase10`/`gate_phase1`
condition task** (unlike most other phases). Live-discovered 2026-09-02:
gating `fill_bom_data`'s *scheduling* via a condition task made it become
`EXCLUDED` (not merely `SKIPPED`) whenever `run_phase10=false` — and
Databricks propagates `EXCLUDED` to every downstream dependent
UNCONDITIONALLY, ignoring `run_if` entirely. Since `repull_dtc_bom` →
`build_costing_chart` → `gate_phase9b` → `fill_duty_rates` all transitively
depend on `fill_bom_data`, this silently excluded the ENTIRE Phase 9a/9b
chain on every scheduled run while `run_phase10` defaulted to `false`
(confirmed live, 2026-09-02 15:57 HKT). Fixed: `fill_bom_data` always runs
(`depends=[dep("repull_dtc")]`, no condition) and reads `run_phase10` as a
plain widget, calling `dbutils.notebook.exit(...)` immediately as a genuine
SUCCESS no-op when disabled (matching the `dry_run` pattern used everywhere
else in this repo) — so nothing downstream is ever excluded. The same fix
was applied to `phase1_push`/`gate_phase1` once `fill_bom_data` started
depending (transitively, via `repull_dtc`) on `phase1_push` too — see
`AGENTS.md`'s decisions log.

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

## Enrichment decision logic — UPSERT semantics (REVISED 2026-09-03)

Rewritten from an earlier all-or-nothing design (a single already-enriched
row used to short-circuit the WHOLE style to a permanent no-op) to genuine
per-row upsert semantics — see `AGENTS.md`'s decisions log for the full
history of why.

1. **Match key**: a BOM segment matches an existing WIP row by the PAIR
   `(Fabric Group, Mill Fabric Article #)` together. `Placement` is
   deliberately excluded from the key — it's the one field expected to still
   legitimately drift for an otherwise-unchanged assignment.
2. Per existing row, per run:
   - If its current `(Fabric Group, Mill Fabric Article #)` matches a
     CURRENT BOM segment exactly: upsert `Placement` ONLY, and only if it
     actually changed.
   - Else if the row is still un-enriched (blank or the placeholder
     `"MAIN MATERIAL CONTENT"`): apply the "Main Fabric" segment's FULL
     field set (first-time enrichment — unchanged from the original design).
   - Else (the row carries some OTHER real value not in the current BOM
     data — e.g. a "Fabric" segment that's since disappeared, or hand-edited
     DTC data): leave it COMPLETELY UNTOUCHED. Phase 10 NEVER reverts or
     blanks existing DTC data just because this run's BOM snapshot no
     longer contains a matching segment.
3. If the style's `bom_unified` is entirely missing/blank this run, or its
   "Main Fabric" segment itself is absent: take ZERO actions for the WHOLE
   style — never revert.
4. For **each** "Fabric" segment (0 or more) whose `(Fabric Group, Mill
   Fabric Article #)` key is NOT already represented by ANY existing row for
   this style: it's genuinely new — duplicate every existing row once per
   such segment (unchanged fan-out shape: N colorway rows × each new segment
   produces N new INSERTs).
5. **`Fabric Group` is set to the segment's own `bom_detail_name`** (literally
   `"Main Fabric"` or `"Fabric"`) — corrected 2026-09-02, NOT `material_name`.
   `Placement` / `Mill Fabric Article #` map from `placement` / `material_no`.
   **`Content` (added 2026-09-03) maps from `material_name`** — see next
   section for why Phase 10 writes this itself.
6. A BOM row with neither segment (e.g. only `"Trim"`/`"Label"`/`"Stitch/Seam"`)
   is equivalent to "no Main Fabric" above — zero actions, never revert.

## `Content` — written directly by Phase 10 (added 2026-09-03)

"Content" is normally a DTC WIP column populated by a DTC-internal trigger
that polls "Mill Fabric Article #" — but that trigger's timing/conditions
in UAT are unreliable (live-confirmed: every KTB test row stayed blank for
days after Mill Fabric Article # was set), which was blocking Phase 9a's
completeness filter entirely (see `docs/costing_interested_fields.txt`).
**Fixed at the source**: Phase 10 now writes `Content` itself, from the
SAME BOM segment's `material_name` — removing the dependency on DTC's own
trigger for this column. This is an intentional, accepted dual-write (DTC's
trigger may still also write the same cell independently) per explicit
owner instruction — unlike the earlier Phase 1/Phase 10 `Fabric Group`
conflict, which was an unintentional bug (see `AGENTS.md` decisions log).
"Fabric Type" remains solely DTC-trigger-populated (traceability only, not
part of the filter/key — see costing_interested_fields.txt).

Raw DTC WIP field names used for the PATCH (live-confirmed via
`GET /v1/views/{WIP_ITS_USE view id}` `dynamicFields`, 2026-09-02 — NOT the
Delta `col_*` normalized names):

| Module field | Raw DTC field |
|---|---|
| `fabric_group` | `Fabric Group` |
| `placement` | `Placement` |
| `mill_fabric_article` | `Mill Fabric Article #` |
| `content` | `Content` (added 2026-09-03) |

## Push mechanics

UPDATEs are sent as `sheetData` PATCH objects keyed by `rowId` (existing
rows, carrying ONLY the fields actually being upserted — see AGENTS.md
Ground rule #6); INSERTs are sent keyed by `rowIndex` (new rows, built by
copying the original row's fields from `data_json` via
`bom.build_insert_row_payload`, MINUS identity fields and any column DTC
marks non-writable — `type=="contact"` or a truthy `formula`, see
`bom.compute_non_writable_cols` — then overriding the 4 BOM fields: Fabric
Group / Placement / Mill Fabric Article # / Content) — matches the
established "cannot mix `rowId` and `rowIndex` in one PATCH call" contract
(`DTCConnector.patch_rows`; see Phase 9b's own `repull_dtc_bom`/
PATCH-batching notes and `AGENTS.md`'s `create_sheet` note).

This notebook never mutates the local Delta `dtc_wip_ktb` table directly —
it pushes to the LIVE DTC sheet only. `repull_dtc_bom` (a full
`p1_pull_masters_to_delta` re-pull, `run_if=ALL_DONE` on `fill_bom_data`, no
gate of its own) runs immediately after so `build_costing_chart` sees the
enrichment; it executes unconditionally so a disabled/skipped/failed Phase
10 never blocks Phase 9a.

## Parameters

| Widget / job param | Default | Notes |
|---|---|---|
| `run_phase10` | `false` | Checked INSIDE the notebook (NOT a DAG-level gate — see above); default off until fully UAT-validated live end-to-end. |
| `bom_catalog` | `alb_tpm_uat` | **NOT** derived from `dtc_environment` — the PRD suffix is `_prd`, not `_prod`. |
| `bom_customer_name` | `KONTOOR` | Scoping/perf pre-filter only; the join keys alone are already customer-correct. |
| `folder_name` | `TEST KTB` | Shared with `bp_style_sync`; scopes which BeProduct styles to consider. |
| `dry_run` | `true` (notebook default) / `false` (deployed job) | Same convention as every other phase. |

## Tests

`dtc/tests/test_bom.py` — pure-Python, unit tests all `dtc/python/sync/bom.py`
decision logic (JSON parsing, no-op/update/duplicate fan-out, field mapping)
against both the exact spec sample and real KTB-00016/KTB-00023 BOM payloads.
No Spark/network required.
