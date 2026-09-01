# Phase 0: DTC XTS Master → BeProduct Directory

**Status:** Implemented ✅ — **wired into the daily DAG** (2026-08-31) as the
FIRST step, gated by `run_phase0` (default `true`):
`wait_cluster → gate_phase0 → phase0_pull → phase0_upsert → phase0_push`.
Every Style/Material/Costing task waits on `phase0_push` (`run_if=ALL_DONE`,
so disabling `run_phase0` only skips Phase 0, never deadlocks the DAG).

Notebooks:
1. `dtc/notebooks/p0_pull_xts_master_to_delta.py` — pull DTC → `dtc_xts_master_ktb`
2. `beproduct/p0_xts_master_to_directory_upsert.py` — upsert → `beproduct_directory`
3. `beproduct/p5utl_beproduct_master_data_sync.py` mode=`PUSH_DIRECTORY` — push to BeProduct (unchanged, already correct — see below)

Phase 0 logically precedes Style/Material/Costing sync: BeProduct partner
(Supplier/Factory) master data should be current before those phases run.

> Field mapping SSOT: `docs/beproduct_directory_xts_interested_fields.txt`
> Pure logic + unit tests: `dtc/python/sync/xts_master.py`, `dtc/tests/test_xts_master.py`
> Verified live findings: `../AGENTS.md` ("DTC 'XTS Master' document")

---

## Source

DTC workspace `KTB`, document `XTS Master`, 2 exact requests in scope
(their `(BACKUP)`-named siblings are deliberately excluded):

| Request reference | partner_type | DTC view |
|---|---|---|
| `XTS Supplier Master` | `SUPPLIER` | `Supplier` |
| `XTS Factory Master` | `FACTORY` | `Factory` |

**`XTS Mill Master` is intentionally out of scope for now** (clarified
2026-08-28) — its "Mill" view has no code column at all, and live checking
found it currently holds no real Mill company data in UAT (100% brand-config
rows). Re-adding it later only requires adding an entry back to
`sync.xts_master.XTS_REQUESTS`/`FIELD_MAP`; the rest of the pipeline is
partner_type-agnostic.

**This is not a rich vendor-master sheet.** None of
address/state/zip/city/phone/fax/website/notes exist anywhere in this
document. See the SSOT doc for the full live-confirmed field list per view.

**Brand-row pollution (critical):** the `Supplier` sheet mixes real company
rows with brand-level access-sharing config rows, distinguishable only by the
`Type` column (`Type="Brand"` — excluded; real rows are `Type="Supplier"`).
Partner type itself comes from which request/view a row was read from,
**never** from the `Type` cell.

---

## Step 1 — Pull (`p0_pull_xts_master_to_delta.py`)

- Discovers the 2 exact requests via `search_requests`, resolves each
  request's partner-type view (`Supplier`/`Factory`), and reads its
  sheet via `get_sheet`.
- Extracts rows via `xts_master.extract_directory_row()`, which:
  - Drops brand-config rows (`xts_master.is_brand_row()`).
  - Drops unnamed rows (name is the Directory match key).
  - Maps DTC columns to `name`/`directory_id`/`country` per partner type
    (see the SSOT doc); all other optional columns are always `NULL`.
- Writes (full overwrite, small dataset):
  - `lft.beproduct.dtc_xts_master_ktb` — one row per kept (partner_type, sheet row)
  - `lft.beproduct.dtc_xts_master_registry` — row_count/last_extracted per request, plus skip reasons for any request/view not found

**Widget naming (live-debugged 2026-09-01):** the notebook's document-name
widget is deliberately named `xts_document`, NOT `dtc_document`. Databricks
Jobs auto-injects every job-level parameter into every task's widgets by
name; this job also has an unrelated job-level `dtc_document` parameter
(default `"KTB WIP"`, used by the WIP-pulling tasks) which silently overrode
a same-named task-level `base_parameters` alias, causing Phase 0 to search
`"KTB WIP"` instead of `"XTS Master"` and pull 0 rows on every run until
fixed. `scripts/deploy_job.py`'s `phase0_pull` task passes
`xts_document: {{job.parameters.xts_document}}`, with no aliasing through
`dtc_document`.

## Step 2 — Upsert (`p0_xts_master_to_directory_upsert.py`)

- Reads `dtc_xts_master_ktb`, deduplicates by `(name, partner_type)` via
  `xts_master.dedupe_by_key()` — BeProduct's real Directory key is the
  **pair**, so the same name under a *different* partner type (e.g. the same
  entity acting as both a supplier and a factory) is NOT a collision and both
  rows are kept as separate records. Only a truly repeated `(name,
  partner_type)` pair is resolved deterministically (prefer a row with a real
  code, then `row_index`) — every true collision is printed, never silently
  dropped.
- `MERGE INTO beproduct_directory ON tgt.name = src.name AND tgt.partner_type = src.partner_type`:
  - `WHEN MATCHED` — every field uses `COALESCE(src.field, tgt.field)`, so a
    `NULL` from XTS (most fields, by design) never destroys real data already
    in `beproduct_directory` from the original full BeProduct pull.
    `partner_type` can never change on a matched row since it's part of the
    join key itself.
  - `WHEN NOT MATCHED` — inserts a new row with `id=NULL` (so
    `PUSH_DIRECTORY` calls `Directory/Add`), `active=true`, and
    `modified_at=now()` (flags it for push).
- Gated by `dry_run` (default `true`) — always computes and prints the
  new/matched plan; only executes the `MERGE` when `dry_run=false`.
- **Never** deactivates rows absent from the XTS source — XTS Master only
  covers ~40-60 of `beproduct_directory`'s ~3,852 records; treating "absent
  from XTS" as "deleted" would incorrectly deactivate almost the entire
  table.

## Step 3 — Push (`p5utl_beproduct_master_data_sync.py` mode=`PUSH_DIRECTORY`)

**Verified correct as-is — no changes needed.** `_is_pending()` selects rows
where `id IS NULL OR extracted_at IS NULL OR modified_at > extracted_at`;
`id IS NULL` rows go to `Directory/Add`, existing rows go to
`Directory/Update/{id}` (excluding `partnerType`), and successes stamp
`extracted_at`/`modified_at` so they aren't re-pushed. Run with `dry_run=true`
first to review, then `dry_run=false` to commit.

---

## Why `name` + `partner_type`, not `directory_id`

BeProduct's Directory record is keyed by `name` + `partner_type` **together**
(confirmed by the project team), despite `id` and `directory_id` columns
existing. This means the same name is fine across different partner types —
19/34 real Supplier rows in UAT share the exact same name and code as a
Factory row (the same physical entity acting as both a supplier and a
factory), and both are kept as separate, valid Directory records. p5utl's
Cell 5b generic template originally matched on `directory_id` alone — that
predates this clarification and has been corrected (both the template and
this Phase 0 implementation now match on `(name, partner_type)`).
