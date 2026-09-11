# Phase 10: BOM enrichment from externally-processed techpack data

**Status:** Implemented ✅ — **wired into the daily DAG** (2026-09-02), gated
by `run_phase10` (**flipped to `true` 2026-09-03** after extensive live
validation; deployed job default is `true`). Placed BEFORE
`build_costing_chart` (Phase 9a), not after — the point is to get up-to-date
material data into `costing_chart`'s `fabric_content` (part of
`product_description`) before Phase 9b calls NT Orbit for duty
classification:

```
phase1_push ─► repull_dtc ─┬─► fill_bom_data ─► repull_dtc_bom ─┐
                           │                                    ├─► build_costing_chart ─► gate_phase9b ─► fill_duty_rates
                           └─► gate_phase3[true] ─► phase3_images       gate_phase9a ─► pull_lineplan_dtc ┘
```

**Depends on `repull_dtc`, NOT `pull_master_dtc` directly** (owner
clarification): Phase 1 completes the style×color WIP master chart first
(Phase 3 image upload is optional, not a blocker); Phase 10 then needs to
read that COMPLETE post-Phase-1 style×color state to fill in the material
dimension. `repull_dtc` is what makes Phase 1's newly-created rows visible
in Delta, and is a SHARED, unconditional prerequisite for both
`phase3_images` and `fill_bom_data`.

**`run_phase10` (and `run_phase1`) is checked INSIDE the notebook, NOT via a
DAG-level condition task** (unlike most other phases) — a condition-task
gate would make `fill_bom_data` become `EXCLUDED` (not merely `SKIPPED`)
whenever disabled, and Databricks propagates `EXCLUDED` to every downstream
dependent UNCONDITIONALLY, ignoring `run_if` entirely; since
`repull_dtc_bom → build_costing_chart → fill_duty_rates` all transitively
depend on `fill_bom_data`, that would silently exclude the ENTIRE Phase
9a/9b chain. Instead `fill_bom_data` always runs and reads `run_phase10` as
a plain widget, exiting as a genuine SUCCESS no-op when disabled.

Notebook: `dtc/notebooks/p10_pull_bom_and_enrich.py` (runs on **serverless
compute** — see "Critical constraint" below). Pure logic + tests:
`dtc/python/sync/bom.py` / `dtc/tests/test_bom.py`.

---

## Why this phase exists

BOM (Bill of Materials) data is not available from the BeProduct API at all
— it's produced by a separate techpack-extraction pipeline.

## Source (CHANGED 2026-09-09, "2nd revision" — supersedes everything below
this section that references `bom_unified`)

The BOM developer pushed back on ever adding new fields to
`customer_teckpack_style_latest` again — new fields (including the ones
this phase needs) are added ONLY to the raw
`customer_teckpack_style_log.custom_fields` JSON column. This is now a
**two-hop join**:

```
alb_tpm_uat.public.customer_teckpack_style_latest    (UAT, resolves latest_techpack_style_log_id per style)
alb_tpm_uat.public.customer_teckpack_style_log        (UAT, custom_fields -- the ACTUAL BOM data)
alb_tpm_prd.public.customer_teckpack_style_latest    (PRD)
alb_tpm_prd.public.customer_teckpack_style_log        (PRD)
```

`customer_teckpack_style_latest` is STILL used, but ONLY to resolve, per
style, which specific log row is current
(`latest_techpack_style_log_id` — a real FK onto
`customer_teckpack_style_log.teckpack_style_log_id`). The actual BOM
segment data now comes from that log row's `custom_fields` column instead
of the "latest" table's own `bom_unified` column — **this overrides
`bom_unified` entirely, no fallback.**

## Critical constraint — serverless compute required

`alb_tpm_uat`/`alb_tpm_prd` are **Lakebase databases** registered in Unity
Catalog, not plain Delta-backed catalogs. The classic shared job cluster
fails outright with:

```
UnauthorizedAccessException: ... requires serverless compute. A Lakebase
database registered in Unity Catalog can only be queried from a serverless
SQL warehouse or serverless general compute; Pro and Classic SQL warehouses
are not supported.
```

**Fixed**: `scripts/deploy_job.py`'s `nb_task()` helper takes a
`serverless=True` flag — set ONLY for `fill_bom_data`; every other task in
the job stays on the shared classic cluster. Serverless compute reads
ordinary Unity Catalog Delta tables (`ktb_styles`, `dtc_wip_ktb`) just fine
too, so no other code needed to change. (This also means an ad-hoc debug
session against a CLASSIC interactive cluster — e.g. via the Databricks
Command Execution API — cannot run this notebook; only a serverless
context/cluster can.)

## Join

```sql
ktb_styles.bp_style_number = customer_teckpack_style_latest.style_no
AND (ktb_styles.season || ' - ' || ktb_styles.year) = customer_teckpack_style_latest.style_season
-- then, to fetch the actual BOM data:
customer_teckpack_style_log.teckpack_style_log_id = customer_teckpack_style_latest.latest_techpack_style_log_id
```

INNER JOIN throughout (both hops) — a BeProduct style with no matching
`customer_teckpack_style_latest` row, or whose `latest_techpack_style_log_id`
has no live BOM data in `custom_fields`, is simply not processed this run
(not an error). `style_season`'s literal format varies WILDLY by customer
in this multi-tenant table — the notebook pre-filters
`customer_name = bom_customer_name` (job param, default `"KONTOOR"`) purely
as a scoping/performance optimization; the join keys alone are already
customer-correct without it.

`customer_teckpack_style_latest` pre-resolves the multi-version-per-style
history the old `customer_teckpack_style_log`-only approach required this
notebook to dedupe itself; a defensive `dropDuplicates` on
`(style_no, customer_department, style_season)` remains as a near-zero-cost
safety net.

## BOM JSON parsing

`custom_fields` (on the LOG table) is a JSON string/object shaped like:

```json
{
  "xts_data": {
    "ACTION_CODE": "NEW_TECHPACK",
    "TECH_PACK_EXTRACTION": {
      "Table": [
        {"Seq": 1, "Type": "POM", "...": "..."},
        {"Seq": 2, "Type": "BOM",
         "ColumnHeader": ["**BomHeader", "**MaterialCategory", "**MaterialCode",
             "**MaterialType", "**MaterialDescription", "**Quantity",
             "**MaterialContent", "**MaterialConstruction", "**MaterialCuttableWidth",
             "**Placement", "...", "**SupplierRefNo", "...",
             {"Colorway": [...]}, {"Color": [...]}, "..."],
         "Data": [
           ["SLEEVELESS SHIRT", "Main Fabric", "LF-BD26-000002--SH", "Sheeting",
            "WV-0003", "", "Cotton 100%", "", "", "BODICE", "...", "WV-0003", "..."],
           ["SLEEVELESS SHIRT", "Fabric", "LF-BD26-000004--PN", "Poplin",
            "WV-0061", "", "Cotton 100%", "", "", "HEM", "...", "WV-0061", "..."]
         ]},
        {"Seq": 3, "Type": "Colorway", "...": "..."}
      ]
    },
    "DATE": ""
  }
}
```

**Path**: `custom_fields -> xts_data -> TECH_PACK_EXTRACTION -> Table[] ->
(entries where Type == "BOM") -> ColumnHeader (defines column order) + Data
(list of row-arrays)`. `ColumnHeader` can ALSO contain DICT entries (e.g.
`{"Colorway": [...]}`) for per-colorway-column breakdowns this phase does
not use — a plain-string column-name lookup simply never matches those, no
special-casing needed. If more than one `Type == "BOM"` table entry exists,
ALL of their `Data` rows are concatenated.

Only two `**MaterialCategory` values matter: **"Main Fabric"** (exactly ONE
per style, by construction) and **"Fabric"** (zero or more). Live-confirmed
2026-09-09/10: all 16 KTB/KONTOOR test styles have this structure populated,
14/16 have a real "Main Fabric" segment (`KTB-00016`/`KTB-00021` are the
two exceptions).

## Enrichment decision logic — UPSERT semantics (revised 2026-09-03, source
revised again 2026-09-09, 3 more fixes 2026-09-10)

1. **Match key**: a BOM segment matches an existing WIP row by the PAIR
   `(Fabric Group, Mill Fabric Article #)` together. `Placement`/`Content`
   are deliberately excluded from the key — they're the fields expected to
   still legitimately drift for an otherwise-unchanged assignment.
2. Per existing row, per run:
   - If its current `(Fabric Group, Mill Fabric Article #)` matches a
     CURRENT BOM segment exactly: upsert `Placement` and/or `Content`,
     **independently**, only if either actually changed AND the target
     value is itself non-blank. **A blank target value is NEVER pushed**
     (added 2026-09-10, owner spec — fixes a live-confirmed real bug:
     `KTB-00024`/`KTB-00026` carry a real, manually-entered `Content` value
     in DTC while the source's `**MaterialContent` for that exact segment
     is genuinely blank; without this guard, the next run would have
     silently PATCHed `Content: ""`, wiping the real value). The identical
     guard applies to the first-time-enrichment branch below too — a row
     can independently already carry a real Content/Placement value even
     while its Fabric Group is still the placeholder.
   - **Else if `Mill Fabric Article #` is currently BLANK** (added
     2026-09-10 — fixes a live "frozen row" bug, `KTB-00025`/legacy code
     `112358013`: a row first-enriched while the source's `**SupplierRefNo`
     was still blank can never satisfy the exact-match key above once the
     source is later filled in): matched to the target sharing its exact
     Fabric Group, disambiguated by Placement if more than one target
     shares that Fabric Group; if still ambiguous, no backfill is guessed.
     A successful match backfills `Mill Fabric Article #` in place
     (one-way: blank → real only) and counts as "represented" for the
     insert-fan-out step below (never both backfilled AND duplicated).
   - Else if the row is still un-enriched (blank or Phase 1's INSERT-time
     `DUMMY_FABRIC_GROUP` sentinel `"NO TPM BOM"`, 2026-09-11 — supersedes
     the old `"MAIN MATERIAL CONTENT"` placeholder): apply the "Main Fabric"
     segment's FULL field set (first-time enrichment).
   - Else (the row carries some OTHER real value not in the current BOM
     data — e.g. a "Fabric" segment that's since disappeared, or hand-edited
     DTC data): leave it COMPLETELY UNTOUCHED. Phase 10 NEVER reverts or
     blanks existing DTC data just because this run's BOM snapshot no
     longer contains a matching segment.
3. If the style's `custom_fields` has no populated BOM table this run
   (missing/blank, or its "Main Fabric" segment itself is absent): take
   ZERO actions for the WHOLE style — never revert.
4. For **each** "Fabric" segment (0 or more) whose `(Fabric Group, Mill
   Fabric Article #)` key is NOT already represented (by an exact match OR
   a blank-article backfill match) by any existing row **of the SAME
   COLORWAY** for this style: it's genuinely new — duplicate every existing
   row of that colorway once per such segment.
5. **Segment coverage is scoped PER COLORWAY, not globally across the whole
   style** (fixed 2026-09-10 — a live gap, `KTB-00029`/LF Style#
   `LFBP-1WTP0002`: 2 BeProduct colors, BOM = Main Fabric + 2 Fabric
   segments, so 2×3=6 DTC rows expected; only 4 existed because one color's
   rows already "claimed" both Fabric segments globally, permanently
   starving the other color). `plan_style_enrichment()` groups
   `existing_rows` by `color_key` (default `"color"`) internally and runs
   the entire decision tree independently per color group.
6. **Field mapping (CORRECTED 2026-09-09, "2nd revision")**:
   `Fabric Group` ← `**MaterialCategory`; `Placement` ← `**Placement`;
   `Mill Fabric Article #` ← `**SupplierRefNo` (NOT `material_no`/
   `material_name` from the old `bom_unified` source); `Content` ←
   `**MaterialContent` (REINSTATED — Phase 10 writes Content again, from a
   genuinely reliable dedicated column this time, unlike the briefly-tried
   `bom_unified.material_name` mapping that caused live data corruption).
7. **Blank-vs-blank diffing** (fixed 2026-09-10 alongside the color-scoping
   fix): DTC's current blank (`None`) and the source's own blank (`""`) are
   never treated as "different" when diffing Placement/Content — avoids a
   spurious `{"Placement": ""}`-style PATCH on every already-blank,
   otherwise-fully-matched row (Ground Rule #6 lean-PATCH requirement).
8. A BOM row with neither segment is equivalent to "no Main Fabric" above —
   zero actions, never revert.

Raw DTC WIP field names used for the PATCH (live-confirmed via
`GET /v1/views/{WIP_ITS_USE view id}` `dynamicFields`):

| Module field | Raw DTC field | Source `**` column |
|---|---|---|
| `fabric_group` | `Fabric Group` | `**MaterialCategory` |
| `placement` | `Placement` | `**Placement` |
| `mill_fabric_article` | `Mill Fabric Article #` | `**SupplierRefNo` |
| `content` | `Content` | `**MaterialContent` |

## Push mechanics

UPDATEs are sent as `sheetData` PATCH objects keyed by `rowId` (existing
rows, carrying ONLY the fields actually being upserted — see AGENTS.md
Ground rule #6); INSERTs are sent keyed by `rowIndex` (new rows, built by
copying the original row's fields from `data_json` via
`bom.build_insert_row_payload`, MINUS identity fields and any column DTC
marks non-writable — `type=="contact"` or a truthy `formula`, see
`bom.compute_non_writable_cols` — then overriding the 4 BOM fields) —
matches the established "cannot mix `rowId` and `rowIndex` in one PATCH
call" contract (`DTCConnector.patch_rows`).

This notebook never mutates the local Delta `dtc_wip_ktb` table directly —
it pushes to the LIVE DTC sheet only. `repull_dtc_bom` (a full
`p1_pull_masters_to_delta` re-pull, `run_if=ALL_DONE` on `fill_bom_data`, no
gate of its own) runs immediately after so `build_costing_chart` sees the
enrichment; it executes unconditionally so a disabled/skipped/failed Phase
10 never blocks Phase 9a.

## Known live-confirmed risk: fan-out row duplication on source-mapping changes

Any change to which BOM segments/keys are considered "current" (e.g. the
2026-09-09 source revision) can cause already-enriched rows whose OLD key no
longer matches the NEW target set to be treated as "some other real,
unrecognized value" (never reverted, correctly) — but Fabric segments that
no existing row's (possibly-stale) key matches are still considered
"genuinely new" and will fan out N×M new INSERT rows (N existing rows × M
unmatched segments). This can multiply quickly for styles with many
pre-existing physical rows. See AGENTS.md's 2026-09-09 decisions log for a
live case study, and the "Blank Mill Fabric Article # backfill"/"per
colorway" fixes above for two of the specific triggers this has already
caused and fixed.

## Parameters

| Widget / job param | Default | Notes |
|---|---|---|
| `run_phase10` | `true` (deployed job) | Checked INSIDE the notebook (NOT a DAG-level gate — see above). |
| `bom_catalog` | `alb_tpm_uat` | **NOT** derived from `dtc_environment` — the PRD suffix is `_prd`, not `_prod`. |
| `bom_table` | `customer_teckpack_style_latest` | Resolves `latest_techpack_style_log_id` per style — no longer the BOM data source itself (2026-09-09). |
| `bom_log_table` | `customer_teckpack_style_log` | ADDED 2026-09-09 — the actual BOM source (`custom_fields`), joined via `latest_techpack_style_log_id`. |
| `bom_customer_name` | `KONTOOR` | Scoping/perf pre-filter only; the join keys alone are already customer-correct. |
| `folder_name` | `TEST KTB` | Shared with `bp_style_sync`; scopes which BeProduct styles to consider. |
| `dry_run` | `true` (notebook default) / `false` (deployed job) | Same convention as every other phase. |

## Tests

`dtc/tests/test_bom.py` — pure-Python, unit tests all `dtc/python/sync/bom.py`
decision logic (JSON parsing, no-op/update/duplicate fan-out, field mapping,
blank Mill Fabric Article # backfill, per-colorway segment coverage) against
both the owner-supplied spec sample and real KTB style BOM payloads. No
Spark/network required.
