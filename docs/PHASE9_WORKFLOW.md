# Phase 9: LinePlan + Costing Chart (9a) → NT Orbit Duty/HTS/Tariff (9b)

**Status:** Implemented ✅ — both sub-phases live in the deployed DAG,
`run_phase9a=true` / `run_phase9b=true`.

> Field-level SSOT (exact columns, sources, key definitions):
> `docs/costing_interested_fields.txt`. Every gating condition that
> determines whether a WIP row actually reaches `costing_chart` or gets a
> fresh NT Orbit lookup: `docs/PIPELINE_GATES.md` ("Phase 9a"/"Phase 9b"
> sections) — read that first if a specific row isn't showing up where
> expected. Data model for every table named here: `docs/ARCHITECTURE.md`.

Phase 9 is split into **two sub-phases across three tasks in two separate
Databricks jobs** (split 2026-09-03 — see "Job split" below):

```
Main job (BeProduct_DTC_sync_dag):
  ... repull_dtc_bom ──┬─────────────────────────────────────┐
  gate_phase9a ─► pull_lineplan_dtc ───────────────────────► build_costing_chart ─► gate_phase9b ─► push_duty_rates

Separate job (BeProduct_DTC_sync_duty_compute):
  compute_duty_rates   (own schedule, zero DTC dependency, reads/writes costing_chart + nt_orbit_duty_cache)
```

`build_costing_chart` depends on `repull_dtc_bom` (Phase 10's re-pull), NOT
`pull_master_dtc` directly, so it always sees Phase 10's freshest
enrichment. `push_duty_rates` and `compute_duty_rates` do NOT depend on each
other directly (different jobs, different schedules) — `push_duty_rates`
simply re-reads whatever `costing_chart` state exists at the time the main
job runs; see "Sequencing gap" below for the real operational implication
of that.

---

## Phase 9a — LinePlan pull + Costing Chart build

Notebooks: `dtc/notebooks/p9a_pull_lineplan_to_delta.py` (→
`dtc_lineplan_<customer>` + `dtc_lineplan_registry`),
`dtc/notebooks/p9a_build_costing_chart.py` (→ `lft.beproduct.costing_chart`,
**fully overwritten every run**, no incremental).

### LinePlan pull — deliberately unfiltered

Unlike every other DTC document in this pipeline, `p9a_pull_lineplan_to_delta.py`
has its own **independent, unfiltered discovery loop** — it never imports or
calls `phase1.is_in_scope()`, so it reads **every active request** in the
document regardless of name, including `(BACKUP)`-named ones. This is an
explicit, standing project-team decision (2026-09-01): they haven't settled
on a LinePlan naming convention yet and want everything included. Uniqueness
of `"Lineplan Ref #"` across every LinePlan request is therefore a
**human-enforced invariant**, not something this pipeline validates —
`p9a_build_costing_chart.py`'s aggregation step only runs a best-effort
conflict *detector* (warns, does not block) when a ref's `order_quantity`/
`target_ldp`/`target_fob` actually disagree across rows/requests.

### Costing Chart formation

Join: WIP `"Lineplan Ref #"` = LinePlan `"Lineplan Ref #"`, **INNER JOIN**
(changed from LEFT 2026-09-01, owner decision — a blank/unmatched ref is
dropped entirely, not surfaced with null LinePlan fields). Then, per WIP
row, transpose up to 4 vendor/factory slots (`Main`/`1`/`2`/`3`) into up to
4 separate `costing_chart` rows — a slot with a blank vendor is dropped.

**A WIP row must clear THREE independent gates to produce any
`costing_chart` row at all** (see `docs/PIPELINE_GATES.md` for the exact
current conditions of each):
1. Step 1b completeness filter (`material_no`, `bp_style_no`,
   `fabric_content`/Content, `fabric_group == "Main Fabric"` — all
   non-blank/exact-match).
2. The LinePlan INNER JOIN above (non-blank `"Lineplan Ref #"` that actually
   matches a LinePlan row).
3. At least one non-blank vendor slot.

None of these three failure modes produce a distinguishing error message on
their own — a row can look otherwise complete and still produce zero
`costing_chart` rows for any one of them. This is exactly what a live
investigation (`KTB-00030`/"KTB SS28 Collaborations", 2026-09-10) found: two
rows had real Content/Fabric Group/Main Factory but a blank `"Lineplan Ref
#"`, dropping them at gate #2 despite passing gate #1 — a genuine DTC
data-entry gap (a matching LinePlan record existed under a different ref,
`WC-S8009`/`WC-S8010`, just never linked into the WIP row's own cell).

**`costing_chart` key** (`duty.COSTING_KEY`, shared between the main job and
the `duty_compute` job): `[customer, season_code, brand, bp_style_no,
lf_style_no, color_name, lineplan_ref, material_no, supplier_type, supplier,
factory]`. `material_no` (Phase 10's own "Mill Fabric Article #") is
required in the key because Phase 10 can produce multiple physical WIP rows
per style×color (Main Fabric + Fabric-segment duplicates) that would
otherwise collide.

**`tariff_rate` carry-forward** (Step 4b, added 2026-09-07): since Step 4
always resets `tariff_rate` to `NULL` on every full-table overwrite (no live
WIP fallback exists for it yet, unlike `hts_code`/`duty_rate_*`), the
existing table's own prior `tariff_rate` (keyed by `COSTING_KEY`) is
`COALESCE`d back in before writing, so a routine `costing_chart` rebuild
doesn't silently wipe an already-computed value.

---

## Phase 9b — NT Orbit Duty Tools (HTS/Duty/Tariff)

**Split into 2 notebooks/jobs 2026-09-03** (owner decision, motivated by
DTC's concurrent-edit limitation — minimizing which jobs touch live DTC,
and for how long — plus removing NT Orbit's ~30-60s/call latency from
blocking everything else):

- **`p9b1_compute_duty_rates.py`** (own job `BeProduct_DTC_sync_duty_compute`,
  task `compute_duty_rates`) — NT Orbit lookups → `costing_chart` +
  `nt_orbit_duty_cache` ONLY. **Zero DTC dependency of any kind** (no API
  key, no DTC read/write) — fully independent schedule.
- **`p9b2_push_duty_to_wip.py`** (main job, task `push_duty_rates`) — reads
  whatever `costing_chart` state exists, diffs each target field against
  the CURRENT live WIP cell, PATCHes only what actually differs.

The original single notebook `p9b_fill_duty_rates.py` is superseded — kept
as a manual-fallback artifact only, not scheduled.

### Auth — Microsoft Entra ID delegated OAuth2 (NOT the DTC x-api-key scheme)

NT Orbit requires a per-user Entra ID delegated OAuth2 bearer token
(`dtc/python/client/entra_auth.py`; refresh_token → access_token). One-time
interactive setup: `python scripts/nt_orbit_oauth_setup.py` (run locally,
once, as the delegated user — `--flow authcode` is recommended when the
app's redirect URI can be registered; `--flow manual`/`--flow devicecode`
are no-portal-access fallbacks). Entra rotates the refresh_token on most
uses; the notebook auto-persists the rotated value to
`lft.beproduct.nt_orbit_oauth_state` every run (`dbutils.secrets` is
read-only, so it can't write back to the secret scope) and prefers that
table's value over the static secret on subsequent runs — "seed once, then
fully automatic," as long as the job keeps running at least every ~90 days
(it runs 3x/day, so this is a non-issue in practice).

### Lookup logic (`markets_needing_lookup()`)

For each `costing_chart` row: a market (`US`/`CA`/`MX`) needs a lookup if
its `duty_rate_<market>` is blank, OR (US-specific) if `tariff_rate` is
blank even when `duty_rate_us` is already filled (fixes a real bug —
`tariff_rate` has no WIP fallback and always resets on rebuild, so relying
solely on `duty_rate_us` being blank would mean it's never recomputed once
that field is filled even once). A blank `production_country` means zero
lookups for the row (nothing to call NT Orbit with).

### Persistent cache (`lft.beproduct.nt_orbit_duty_cache`)

Since `costing_chart` is fully overwritten every Phase 9a run, an in-run
dedup alone isn't enough. A cache entry keyed on `(product_description,
origin_country, import_country)` — where `product_description` concatenates
`duty.PRODUCT_DESCRIPTION_COLS` (`style_description`, `color_name`,
`fabric_content`, `gender`, `class_name`, `sub_class` — `color_name` was
added 2026-09-07, reversing an earlier design where different colors of the
same style/slot shared one cache entry; owner decision: different colors
now always get their own lookup) — is used directly (no API call) if it's
younger than `cache_ttl_days` (default 180; tariff policy does change over
time). A missing `looked_up_at` timestamp is always treated as stale.

Calls are made **serially by default** (`orbit_parallel_calls=false` job
param) rather than via a thread pool, since NT Orbit's concurrency tolerance
under real load hasn't been validated; set `orbit_parallel_calls=true` (+
`max_workers`, still hardcoded to 4 in the deployed job) to trade safety for
throughput once that's confirmed safe. Each call is ~30s (sometimes
longer — the connector's HTTP timeout was raised 30s→60s 2026-09-01 after
live timeouts were observed).

### Writing values (write-once, then diffed)

`costing_chart` cells are filled write-once (never overwrites an
already-populated cell) from the response's "General Duty" detailed_line
rate (`duty_rate_xx`; NOT the combined `data.duty_rate`, which also
includes tariff + fees) and the sum of any other `type="duty"`
detailed_lines (`tariff_rate`, only ever set from a US-market call).
`push_duty_rates` then diffs each target WIP field against the row's
CURRENT live value and only includes fields that actually differ in the
PATCH (Ground Rule #6 lean-PATCH). Tariff Rate has **no live WIP column
yet** (`duty.WIP_TARIFF_COLS_LIVE = False`, confirmed 2026-07-17) so a
computed tariff value is pushed nowhere — it stays `costing_chart`-only;
the push step logs this as a "skipped" reason rather than silently dropping
it. Flip that flag once DTC adds the columns; no other code change needed.

### Critical: `COSTING_KEY` joins/MERGE must use NULL-safe equality

**Live-confirmed real bug, fixed 2026-09-10**: `lf_style_no` (and in
principle any other `COSTING_KEY` column) can be genuinely `NULL` for a
real style. Standard SQL/Spark equality (`t.c = s.c`, or PySpark's
`.join(other, on=[col_list])` shorthand) treats `NULL = NULL` as `NULL`,
never `TRUE` — so a row with a NULL key column silently never matches its
own counterpart, no error, no log line. This affected BOTH current
`COSTING_KEY` call sites simultaneously: `p9b1_compute_duty_rates.py`'s
Step 4 `MERGE` (fixed with `<=>`, Spark's null-safe equality operator) and
`p9a_build_costing_chart.py`'s Step 4b tariff carry-forward join (fixed
with an explicit `.eqNullSafe()` condition, since the `on=[list]` shorthand
compiles to the same non-null-safe equality). `p9b2_push_duty_to_wip.py`'s
own WIP-row lookup is unaffected — it's a plain Python dict keyed on a
tuple, where `None == None` is `True` (Python semantics differ from SQL).
See the comment on `duty.COSTING_KEY` itself for the durable warning.

### Known sequencing gap (not yet structurally fixed)

`push_duty_rates` (main job) and `compute_duty_rates` (`duty_compute` job)
run on independent schedules with no explicit ordering guarantee between
them. Since every main-job run's OWN `build_costing_chart` wipes
`costing_chart` (except the `tariff_rate` carry-forward above) before
`push_duty_rates` runs immediately after in the SAME run, a value
`duty_compute` fills in BETWEEN two main-job runs can only survive to be
pushed if `duty_compute` happens to run in the exact window between a
`build_costing_chart` run and its own immediately-following
`push_duty_rates` — not guaranteed across two independently-scheduled jobs.
**Manual workaround** (used successfully to unstick a real stale row, see
AGENTS.md decisions log): re-run `duty_compute`, then trigger ONLY
`push_duty_rates` via the Jobs API's task-selective run
(`w.jobs.run_now(job_id=..., only=["push_duty_rates"])`), which pushes
against whatever is CURRENTLY in `costing_chart` without triggering
`build_costing_chart` first.

---

## Parameters

| Widget / job param | Default | Notes |
|---|---|---|
| `run_phase9a` | `true` | Gates `pull_lineplan_dtc`/`build_costing_chart` via `gate_phase9a` (a real DAG-level condition task is safe here, unlike Phase 1/10 — nothing downstream of Phase 9a transitively depends on it the same way). |
| `run_phase9b` | `true` | Gates `push_duty_rates` via `gate_phase9b`. |
| `costing_chart_table` | `lft.beproduct.costing_chart` | Testing override: `lft.beproduct.costing_chart_kei`. `costing_chart` itself has real downstream readers — always test against the `_kei` table. |
| `duty_cache_table` | `lft.beproduct.nt_orbit_duty_cache` | Never wiped by Phase 9a's overwrite (separate table). |
| `cache_ttl_days` | `180` | |
| `orbit_parallel_calls` | `false` | Serial by default; see "Lookup logic" above. |

## Tests

`dtc/tests/test_duty.py` — pure-Python, unit tests all `dtc/python/sync/duty.py`
decision logic (`markets_needing_lookup`, cache staleness, `COSTING_KEY`,
`PRODUCT_DESCRIPTION_COLS`, WIP field name constants). No Spark/network
required.
