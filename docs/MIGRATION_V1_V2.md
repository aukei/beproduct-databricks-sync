# v1 → v2 — why, what changes, and how to roll it out

**Branch `v2`.** Design record for the pipeline revamp. The specification itself
lives in [PIPELINE.md](PIPELINE.md) and [SYNC_CONTRACT.md](SYNC_CONTRACT.md); this
document is the *reasoning*, the *risks*, and the *rollout plan*.

> **Status: LIVE since 2026-09-15; this is the design record.** What runs today,
> and every gate, is in [PIPELINE.md](PIPELINE.md); field directions and exact
> column names in [SYNC_CONTRACT.md](SYNC_CONTRACT.md); symptom runbooks in
> [TROUBLESHOOTING.md](TROUBLESHOOTING.md). Where this document and those
> disagree, they win. `BeProduct_DTC_sync_v2` (367710575109755) runs serverless
> every 8 min on a periodic trigger (since 2026-09-30); `BeProduct_DTC_sync_duty_compute`
> (1026599988408090) runs serverless every 8 min too. The v1 jobs
> `BeProduct_DTC_sync_dag` (294837488757511) and `BeProduct_DTC_sync_images`
> (847087837807970) are PAUSED, kept for rollback only. What is still open
> before the real `KTB` folder is switched on: [Remaining go-live items](#remaining-go-live-items).

---

## The problem

DTC uses permissive optimistic locking. A successful API write moves the
request's server-side `last_read`; any browser session that loaded earlier is
refused on save and silently loses in-progress edits. Confirmed with the DTC
developer (2026-09-14):

- **Only writes** move the timestamp — reads are free.
- **Scope is the whole request.** A write through `WIP_ITS_USE` blocks a user
  editing a *different row* through a *different view*. The mental model is a
  single table lock that does not release until the user refreshes.
- Views are only a subset of request data exposed to the user, not a lock domain.

v1 writes each in-scope request at up to **5 moments per run**, scattered across
the full DAG:

| v1 task | Calls per request |
|---|---|
| `phase1_push` | updates + inserts + orphan marks |
| `fill_bom_data` | updates + inserts |
| `push_duty_rates` | updates |

With two full DTC re-pulls interleaved, the elapsed span between a request's first
and last write is most of the run. Users have to avoid a ~30–40 minute window,
three times a day.

**The requirement that forces the issue:** the target cadence is a run every ~2
hours during active style development. At that frequency the v1 pattern produces
36 write moments a day and roughly 6–8 hours of daily exposure — the request would
be effectively unusable during working hours. Consolidating the write path is not
an optimisation here; it is the precondition for the cadence.

| | Write windows/day | Exposure |
|---|---|---|
| v1 @ 3 runs/day | 9 | ~2 h/day |
| v1 @ 12 runs/day | 36 | ~6–8 h/day |
| **v2 @ 12 runs/day** | **12** | **minutes** |

---

## What v2 changes

### 1. One write window per request

All WIP writes move into a single `wip_push` stage: one live `get_sheet()` per
request, one combined plan, one PATCH of updates (`patch_rows`, keyed by
`rowId`) and one POST of inserts (`append_rows` to `.../rows`), back to back.

> **Changed 2026-09-23.** Inserts were originally a second PATCH keyed by a
> client-computed `rowIndex` (a body cannot mix `rowId` and `rowIndex`). They
> now use the append endpoint, where DTC assigns `rowId`/`rowIndex` itself, so no
> index is computed anywhere. Still two calls, still one window — see
> [PIPELINE.md](PIPELINE.md) design rule 3.

### 2. The material dimension is resolved at plan time

v1's Phase 10 could only enrich rows that already **physically existed** in DTC,
which is the entire reason `repull_dtc` had to run between the style push and the
BOM push. In v2 the material contribution plans against the **projected** row set
— existing live rows *plus* the style contribution's planned inserts — so a
brand-new style × color gets its material fan-out in the same run, and
`repull_dtc` disappears.

> **Corrected during stage 3.** The original plan had the transform emitting a
> final style × color × material staging grain. That is not possible: the fan-out
> depends on which segments a colorway is already represented by in live DTC,
> which the transform cannot see, and `phase1.compute_upsert()` treats a repeated
> `(BP Style#, Color)` as a `duplicate_bp_key` exception. Staging stays at
> style × color; the BOM becomes its own style-keyed Delta table
> (`bom_segments`, written by the new `pull_bom` task); the material grain is
> resolved in `wip_plan`. The re-pull still disappears — planning against intent
> is what removed it, not the staging grain.

### 3. `costing_chart` is built from intent, not from a round-trip

**Implemented as a mode on the existing notebook, not a fork.**
`p9a_build_costing_chart.py` takes `wip_effective_mode`: `"table"` (v1, read the
snapshot as-is) or `"intent"` (v2, overlay the material and style-identity
columns from `bom_segments` + staging first). Everything downstream — the
gates, the LinePlan join, the slot transpose, the WIP fallback, the cache fill
— is shared. One costing implementation, one set of gates.

**Validated against v1: an exact match.** 7 rows, 5 styles in both; key-set
comparison gives 0 rows in v1 only, 0 in v2 only, 7 in both. That is the
strongest available evidence that dropping `repull_dtc_bom` loses nothing.

One deliberate difference: `sub_class` is populated where v1 had `NULL` (5 of 7
rows), because v2 reads it from staging rather than from a WIP cell the
pre-filters had made unreachable. Note the knock-on — `sub_class` is part of
`duty.PRODUCT_DESCRIPTION_COLS`, so the NT Orbit cache key changes and those
rows will take fresh lookups once. Bounded and correct, but not a surprise worth
discovering in production.


v1 built it from the twice-re-pulled WIP table. v2 builds it from **staging**
(the material dimension we own) ⋈ **the start-of-run WIP pull** (the DTC-owned
dimension: `lineplan_ref`, vendor/factory slots, production country) ⋈ **LinePlan**.
`repull_dtc_bom` disappears.

This is only sound because the input split is clean — we never write the
DTC-owned columns, so a start-of-run pull is current by definition — and because
the one dependency that would have blocked it is already gone: `Content` used to
come from a DTC-internal trigger polling `Mill Fabric Article #`, confirmed
unreliable in UAT; since 2026-09-09 Phase 10 writes it directly.

### 4. Serverless everywhere; no condition tasks

Both are simplifications that fall out of the above. See "Serverless" and "No
condition tasks" below.

### Net effect

| | v1 | v2 |
|---|---|---|
| DTC write windows per request per run | 3 | **1** |
| Write calls per request per run | up to 5 | **2** (1 PATCH + 1 POST; more only past `batch_size` rows per side) |
| Full DTC re-pulls per run | 2 | **0** |
| Tasks in the main job | 17 (incl. 5 gates + `wait_cluster`) | **14** (incl. `phase3_images` Stage 45 and `push_customer_code` Stage 55) |
| Planning reads live DTC | style fields only | **all contributions** |

---

## Serverless

**Feasible, and already proven in this repo.** `fill_bom_data` has run on
serverless since 2026-09-02 using exactly the pattern every notebook uses —
`sys.path.append(…/DTC/python)` on a Workspace Files path — importing `sync.bom`,
`sync.phase1` and the DTC connector from there. Workspace Files on `sys.path`
behave identically on serverless.

The usual blocker is cluster-scoped libraries, and there are none: no task in
`deploy_job.py` declares `libraries`, and the notebooks import only `requests` and
`pandas`, both present in the serverless environment. `requirements.txt`'s
remaining entries (`databricks-sql-connector`, `pytest`, `jupyter`) are local-dev
and test-only, never imported by a notebook.

Audited for APIs that break under Spark Connect — clean:

- No `sparkContext`, no `.rdd`, no `spark.conf.set`, no `pandas_udf` /
  `applyInPandas` / `mapInPandas` / `toPandas`.
- `createDataFrame` is always called with an **explicit schema** (a deliberate
  choice — see the "never infers from all-NULL columns" comments), which is the
  supported form.
- `createOrReplaceTempView` + `spark.sql("MERGE …")` works.
- `dbutils.widgets` / `.secrets` / `.notebook.exit` / `.jobs.taskValues` all work.

**One caveat, corrected after a closer audit:** the transform *does* use three
scalar Python UDFs — `format_sample_field` (sample-app JSON → DTC status string)
and `lifecycle.should_include_in_staging` / `is_wip_row_dropped`
(`p1p7_beproduct_to_dtc_transform.py`). Scalar Python UDFs are supported on
serverless, so this is not a blocker, but it is the one construct in this
codebase where serverless behaviour differs most from the classic cluster, and
it should be the **first thing validated** on the v2 transform.

If it does misbehave, the fix is cheap and arguably an improvement: all three
wrap pure functions already unit-tested in `sync/samples.py` and
`sync/lifecycle.py`, over a dataset of ~145 styles. Applying them in plain
Python over collected rows — as `v2_wip_push` already does for the whole plan —
removes the UDF boundary entirely at no meaningful cost.

**Port:** drop `job_cluster_key` from every task, drop `wait_cluster`. The
instance pool is idled to `min_idle_instances=0` but deliberately **kept**: the
paused v1 job definitions reference it, so deleting it would break rollback.

### Live-validated 2026-09-14 (run `66807905429726`)

Everything above was argued from static analysis; this is the confirmation, on
real serverless compute. **12/12 checks passed.** Re-run any time with:

```bash
python scripts/upload_notebooks.py --root /Workspace/Repos/beproduct-sync-v2
python scripts/run_v2_smoke.py
```

`dtc/notebooks/v2_smoke_check.py` is read-only — no Delta write, no DTC or
BeProduct API call, no secret value printed — so it is safe to run while the v1
job is running.

| Confirmed | Evidence |
|---|---|
| **Startup ~5 s** (vs ~3 min from the pool) | `setup_duration=5.0s`, total run 70 s |
| **Workspace Files import, v2-isolated** | `sync.phase1.__file__` → `…/beproduct-sync-v2/DTC/python/sync/phase1.py` |
| **Scalar Python UDFs work** | `format_sample_field` + both `lifecycle` predicates round-tripped |
| **Lakebase readable from an ordinary task** | `alb_tpm_uat.public.customer_teckpack_style_latest` |
| Spark Connect surfaces | explicit-schema `createDataFrame`, temp view + `spark.sql`, `spark.table`, `<=>` |
| dbutils | `secrets.get`, `jobs.taskValues.set` |

The module-isolation check **asserts** the resolved path rather than merely
importing successfully — an import that silently fell through to v1's copy would
otherwise look identical.

**The startup number is the headline.** ~175 s saved per run, ~35 min/day at 12
runs/day, and it lands before any of the merge work.

**Cost is still the open question, not feasibility.** Serverless DBU rates are
higher per second, but the pooled VMs stop being paid for between runs. Tracked
under [Remaining go-live items](#remaining-go-live-items).

**Two things the smoke check turned up that affect how v2 is built:**

- **The Jobs API does not return notebook stdout for serverless runs.**
  `get_run_output(...).logs` is empty; only the `dbutils.notebook.exit` value
  comes back. Any v2 notebook whose result needs to be readable from outside the
  UI should exit a JSON summary, as `v2_smoke_check` now does. This matters for
  `wip_push`, whose per-contribution counters are the main compensation for
  collapsing three task run_ids into one.
- **`upload_notebooks.py` could not deploy to a fresh workspace root at all** —
  the notebook importer does not create parent folders (unlike
  `workspace.upload()`), so all 24 notebooks failed. Never surfaced because the
  v1 root has existed for months. Fixed with an idempotent recursive `mkdirs`
  pass.

> **The UAT validation dataset is small.** Whole-table counts at validation
> time: `ktb_styles`=8, `dtc_wip_ktb`=60, `dtc_request_registry`=85. The v1
> performance figures in `PERFORMANCE.md` were measured against the ~145-style
> `KTB` folder, not the current `TEST KTB` one — so v2 timings taken now will
> **not** extrapolate. Correctness validation is unaffected.

**Consequence for the plan:** because the whole job is serverless, the Lakebase
constraint that forced Phase 10 into its own task costs nothing. The BOM read
still ended up as its own task — `pull_bom` (Stage 20b), running in parallel
with `transform` — but for the grain reason in §2, not for compute. Its source
has moved twice: Lakebase → BeProduct PageBomVariation (2026-09-16) → Lakebase
again (2026-09-22, owner decision); see [PIPELINE.md](PIPELINE.md) Stage 20b.

---

## No condition tasks

Databricks propagates a condition task's `EXCLUDED` outcome to **every** downstream
dependent unconditionally, ignoring `run_if` entirely. v1 learned this twice —
`gate_phase1` and `gate_phase10` were both removed on 2026-09-02 after a
`gate_phase10` evaluating false silently excluded the entire Phase 9a/9b chain.

v2's DAG is linear enough that this failure mode would be routine rather than
occasional: `wip_push` depends on `build_costing`, which depends on `transform`, so
a gate anywhere upstream would excise the whole DTC push. Rather than reason about
it per gate, v2 adopts the blanket rule: **every `run_*` flag is a plain widget
read inside its own notebook, which exits as a SUCCESS no-op when disabled.**

> This also resolves a latent inconsistency in the v1 documentation, which claims
> both that `EXCLUDED` overrides `run_if` unconditionally *and* that
> `gate_phase0` + `run_if=ALL_DONE` lets a disabled `run_phase0` skip without
> deadlocking the rest of the DAG. Those cannot both be true. v2 sidesteps the
> question entirely rather than depending on whichever is right.

---

## What v2 gives up

Stated plainly, because these are real:

- **Failure isolation.** In v1 a BOM or duty bug could only corrupt its own
  fields. Merged, a bad payload builder breaks the whole push.
  **Mitigation:** `wip_plan.py` composes, it does not re-implement — `phase1.py`,
  `bom.py` and `duty.py` keep their logic and tests. A failing contribution must
  degrade to *omitting its keys*, never to failing the row or the request, and
  the composed output must be asserted against the allow-list in
  [SYNC_CONTRACT.md](SYNC_CONTRACT.md).
- **Per-phase gating and retry.** `run_phase10` / `run_phase9b` become plan-level
  flags rather than independently retryable tasks. Repairing a failed duty push
  now means re-running `wip_push`, which re-evaluates everything. Acceptable
  because the plan is idempotent and a no-diff re-run writes nothing.
- **Per-phase run logs.** Three task run_ids collapse to one. Compensate with
  per-contribution counters in the push summary and in
  `beproduct_to_dtc_sync_log`.
- **Mid-run user edits.** v1's re-pulls caught edits made during the run. v2 reads
  each request once at push time — which is *later* in the DAG than v1's
  `pull_master_dtc`, so this is a net improvement, and at a 2-hour cadence
  anything missed is picked up within two hours. Explicitly accepted by the owner.

---

## What v2 does not change

- `compute_duty_rates` stays its own job (`BeProduct_DTC_sync_duty_compute`,
  now serverless, unpaused). Zero DTC contact, ~30–60 s serial NT Orbit calls;
  there is no reason for its latency to sit inside a 2-hour loop. Its *DTC write*
  did move: duty values now reach DTC through `wip_push` (Stage 40), not v1's
  `push_duty_rates` / `p9b2_push_duty_to_wip`.
- `phase3_images` cannot join `wip_push`'s PATCH — image cells are writable only
  through the multipart `/images` endpoint and DTC rejects any `sheetData` write
  to `Style Image`. **Changed 2026-09-15:** it was nonetheless moved INTO the v2
  DAG as Stage 45, immediately after `wip_push`, so its window is adjacent rather
  than independent; it now addresses rows by lowercase `rowid` (2026-09-23).
- Phase 2 (DTC → BeProduct) is untouched. It writes BeProduct, never DTC.
- Every field mapping, match key, gate condition and hard-won bug fix carries over
  unchanged. v2 is a restructuring of *when and how often we write*, not of *what
  we write*.

---

## Rollout (done)

v2 was deployed as a **new job, `BeProduct_DTC_sync_v2`** (367710575109755),
alongside v1, and cut over on **2026-09-15**. All five planned stages of work
shipped:

| # | Deliverable | Windows per request |
|---|---|---|
| 0 | Branch, consolidated docs, `BeProduct_DTC_sync_v2` job definition | — |
| 1 | Serverless port; `module_path` parameter; in-notebook run flags | — |
| 2 | `sync/wip_plan.py` + `test_wip_plan.py` — composition, zero-diff-zero-write | — |
| 3 | `v2_pull_bom_segments` + `v2_wip_push`; drop `repull_dtc` | 3 → 2 |
| 4 | `build_costing` in "intent" mode; drop `repull_dtc_bom` | 2 → 2 |
| 5 | Fold the duty contribution into `wip_push` | 2 → **1** |

Summary of what was proven, in order:

- **Smoke check** (2026-09-14, run `66807905429726`) — serverless feasibility,
  above.
- **End-to-end dry run** (2026-09-15, run `51302327795660`, `dry_run=true`) —
  SUCCESS, dependency order verified programmatically, serverless setup
  **1–4 s per task**. The 278 s wall is **not** comparable to `PERFORMANCE.md`'s
  v1 numbers (`TEST KTB`, 8 styles, vs the ~145-style `KTB` folder).
- **First real run** (`dry_run=false`) — the one-off catch-up: 40 updates
  (`Sub Class` only), **1 PATCH call, 1 write window**, 263 s. `Sub Class` went
  from 15 filled / 45 blank to 57 / 3 (the 3 have no staging row).
- **Second consecutive real run** — 60 noops, **0 PATCH calls, 0 write
  windows**. The zero-diff-zero-write invariant confirmed against live DTC — the
  single property the 2-hourly cadence depends on.
- **Scheduled** every 2 h at :05 on odd hours HKT, 12 runs/day; `phase3_images`
  folded in as Stage 45 the same day; `duty_compute` moved to serverless and
  unpaused; v1 main and images jobs paused.
- **2026-09-23** — inserts switched to the `append_rows` POST (DTC assigns the
  row locators); Stage 45 switched to lowercase `rowid`.

Workspace isolation, which made side-by-side running safe: v1 and v2 notebooks
never share a Workspace path, and every notebook reads its import root from the
`module_path` job parameter (`scripts/upload_notebooks.py --root` selects the
upload target).

```
v1   /Workspace/Repos/beproduct-sync/{beproduct,DTC/notebooks,DTC/python}
v2   /Workspace/Repos/beproduct-sync-v2/{beproduct,DTC/notebooks,DTC/python}
```

> The JOB parameter `module_path` overrides a task base_parameter of the same
> name — every task whose notebook lives under the v2 root must get the v2
> `module_path` at job level (AGENTS.md 2026-09-17).

The `gate_phase*` condition tasks were replaced by in-notebook flags; the full
flag list is the Stages table in [PIPELINE.md](PIPELINE.md).

### Rollback

Pause `BeProduct_DTC_sync_v2`; unpause `BeProduct_DTC_sync_dag` **and**
`BeProduct_DTC_sync_images` (v1 ran images as its own job). `duty_compute` is
shared by both versions and stays as it is. No data migration — both write the
same tables with the same keys, and every write is idempotent and diff-gated.

---

## Decisions made during rollout

### `sync/wip_plan.py` — the composition layer

Composes the three contributions into one plan per request. It **composes; it
does not re-implement** — `phase1.py`, `bom.py` and `duty.py` keep their
decision logic and their existing tests untouched.

The guarantees it exists to provide, all covered by `dtc/tests/test_wip_plan.py`
(54 assertions):

| Guarantee | Why it moved here |
|---|---|
| **Zero diff ⇒ zero PATCH calls** | The cadence-critical invariant. `RequestPlan.is_empty()` is what the notebook checks before issuing *any* call — including the live GET |
| **Allow-list never violated** | v1 audited three separate call sites; v2 has one, so the check lives at the merge point. Offending fields are dropped and recorded, never silently passed and never allowed to abort |
| **Degrade, never abort** | A contribution that raises has its keys omitted and the failure recorded in `degraded`; the rest of the row still goes out. This replaces the blast-radius limit that separate tasks used to provide for free |
| **Provenance** | Every planned field records which contribution produced it, and every override. Three task run_ids collapse into one, so "why did this cell change?" has to stay answerable |

Two behaviours are deliberately **stricter than v1**:

- **Value comparison is conservative.** Blank-vs-blank is never a diff, and
  `0.16` (float, from `costing_chart`) equals `"0.16"` (string, as DTC returns
  it). v1's `p9b2_push_duty_to_wip` used a plain `!=` and would have re-pushed
  on every type mismatch — invisible at 3 runs/day, 12× worse at the target
  cadence.
- **Duty applies to every row sharing `(style, colour, article)`**, not just
  one. v1 indexed with a plain dict and silently kept only the last such row.
  Rows sharing a material are the same material, so the same duty applies to
  all of them; this also removes a dependence on row ordering.

Scale-checked at production size: **linear**, 36 ms for 250 styles / 1500 rows
(0.024 ms/row, flat from 8 to 500 styles), and `is_empty()` holds throughout on
a settled dataset.

### Coverage: both pre-filters are OFF in v2 (2026-09-15)

v1 gates coverage with two **style-level** filters that run *before any field is
compared*: `sync_status = 'pending'` on the staging row, and
`beproduct_modified_at > last_pushed` (`delta_only`). Note the granularity —
`beproduct_modified_at` is the **style header's** timestamp (all colours share
it) and `last_pushed` is stamped on the **request**. So the question asked is
*"was this style touched since anything in this request was last pushed?"*
Field-level diffing happens only afterwards, on whatever survives.

The consequence is that a cell out of sync for any reason other than "this style
just changed" is invisible **permanently**. Live evidence in UAT:
`KTB-00024`/`Black` has six physical rows of the same style and colour and only
one carries `Sub Class`; across the request, 40 of 60 rows are missing it while
all 40 have a staging row holding a real value.

In v1 these filters bought cheap pushes. **In v2 they buy nothing** — the
zero-diff-zero-write invariant means considering every row costs zero extra API
calls when nothing differs (planning measured at 36 ms for 250 styles / 1500
rows). They now only cost correctness, so both default to `false`.

> `costing_chart_kei` was retired on 2026-09-15 once intent mode was proven to
> match v1 exactly; routine runs write `costing_chart` directly. The
> `costing_chart_table_name` parameter remains for future comparison builds,
> and Delta time travel is the recovery path for a bad build
> (`RESTORE TABLE … VERSION AS OF <n>`) since the table is fully overwritten
> every run regardless.

> The first real v2 run was that one-off catch-up — in UAT, 40 cells in one
> PATCH call. A `dry_run=true` run's `sample_changes` (concrete current-vs-new
> values) is what distinguishes a genuine correction from a diffing bug; it
> earned its keep when a `Content` write planned on all 60 rows turned out to be
> notation drift (next section). Repeat that review before the `KTB` switch.

### `Content` is write-once (2026-09-15)

The first full-scan dry run planned a `Content` write on **60 of 60 rows** — and
not one was a semantic change. DTC writes `97% Cotton / 3% Spandex`; the
techpack BOM writes `Cotton 97%, Spandex 3%`. Same fibres, same percentages,
different notation.

Owner ruling: DTC's own trigger will overwrite whatever Phase 10 writes — known
and expected — and the notation does not affect Phase 9's NT Orbit call, which
is what the value feeds. So the only thing the write must achieve is making the
cell **non-blank**, which is exactly what Phase 9a's completeness gate needs.

`Content` is therefore **write-once default-fill** — the same rule already
applied to Supplier / Fabric Group / Placement: fill a blank cell, never touch a
filled one. Parameter: `material_fill_if_blank_columns` (default `"Content"`).

This matters beyond tidiness: re-writing a non-blank `Content` would diff on
*every* run, opening a write window on the request every time — which at a
2-hourly cadence would defeat v2's whole premise on its own.

### `-SUPPLIER` requests are out of scope (2026-09-15)

DTC generates supplier-scoped artifact requests named
`<customer> <seasonCode> <brand>-SUPPLIER <xxx>`; four appeared in one afternoon
in UAT. They are DTC's own artifacts, never sync write targets, but
`is_in_scope()` takes everything after the season code as the brand, so they
parsed as valid in-scope requests and would each have become a push target on
the next registry refresh. `phase1.is_in_scope()` now excludes `-supplier\b`
alongside `\(backup`. In-scope requests dropped from 10 to 6 of 109 live.

### Lifecycle marker words are out of scope (2026-09-15)

`is_in_scope()` now also excludes any reference containing **cancel / backup /
archive / delete**, matched at a word boundary with any suffix. This supersedes
the narrower `\(backup` rule, which required an opening paren and so missed a
bare `BACKUP`, every `Cancel`-named request, and five named simply `DELETED`.

Combined live effect: of 109 requests, in-scope went **10 → 6 → 3**. The
duplicate in-scope name resolved itself — both copies of
`KTB FW26 Cancel Wrangler Global TALISMAN LTD` are `Cancel`-named, so the
inventory now reports zero duplicates.

### Customer-code reverse push → Stage 55

The 2026-09-16 "BOM reverse push" open issue is resolved into a stage of its
own: DTC `"Fabric Customer # or SAP #"` is written to the BeProduct **material
master**, not the BOM row (`CUSTOMER MATERIAL CODE` is not editable on a
material-linked row). It is currently **disabled**, because the Lakebase BOM
carries no `materialId`. Design, gates and re-enable condition:
[PIPELINE.md](PIPELINE.md) Stage 55.

---

## Remaining go-live items

*(This is the "go-live checklist" other docs refer to.)*

`folder_name` is still **`TEST KTB`: 8 styles, 60 WIP rows, 1 in-scope
request.** Production is **~250 styles**, roughly **30×**. Flipping
`folder_name` to `KTB` is the moment that volume arrives; the correctness
results carry over, none of the timing figures do.

| Item | What to do |
|---|---|
| **`folder_name` `TEST KTB` → `KTB`** | The go-live switch itself. Re-check every row below first. |
| **Volume rechecks (~30×)** | `wip_push` planning measured linear (36 ms for 250 styles / 1500 rows) — re-confirm. One live `get_sheet` per in-scope request: free w.r.t. locking, not w.r.t. runtime. More genuinely-blank duty rows ⇒ real NT Orbit lookups at ~30–60 s each, serial, with no call budget or checkpointing — a backlog may need `orbit_parallel_calls=true` or several runs to drain. |
| **`batch_size` / call count** | "2 calls per request" holds only up to `batch_size` (default 100) rows per side; 250 changed rows ⇒ 3 update calls. Still consecutive, still **one window** — tune or re-measure, do not quote the number. |
| **Dry-run catch-up review** | The first `KTB` run carries whatever drift the v1 pre-filters left unreachable — potentially far more than UAT's 40 cells. Run `dry_run=true` and read `sample_changes` first. |
| **Sample-app cadence split** | One `app_get` per (style × app), pinned to `FULL` because app edits do not bump `style.modifiedAt`. ~1,500 calls/run at 250 styles, ~18,000/day at 12 runs — the largest runtime item once the DTC passes are merged, and it scales linearly. Move it to its own 2–3×/day schedule and let the 2-hour loop consume whatever `ktb_styles` holds. |
| **Re-enable Stage 55** | `run_customer_code_push` stays `false` until `v2_probe_material_code` returns `PROVEN` and the live end-to-end proof is repeated ([PIPELINE.md](PIPELINE.md) Stage 55). |
| **Cross-contribution delta filter** | The BOM and costing contributions recompute every style every run — 12× the Lakebase reads and costing rebuilds for data that changes maybe twice a day. The plan builder should carry one delta filter across all contributions (without reintroducing v1's correctness-costing pre-filters). |
| **Serverless cost / pool retirement** | Measure real serverless cost over a representative period; only then delete the idle instance pool — and only once v1 rollback is no longer wanted, since the paused v1 jobs reference it. |
| **`duty_compute` sequencing gap** | Narrowed by Stage 30's cache fill, not structurally closed ([PIPELINE.md](PIPELINE.md), companion job). |
| **DTC concurrent-edit ETA** | Get a date for DTC's websocket / client-merge work — see next section. |
| **DTC "Full" view must expose the lookup columns** | A DTC `lookup`/`formula` field (e.g. `Factory Production Country for …`) is only materialized if it is on the view the user **saves through**. Users save through "Full", where these are hidden, so the stored cell stays NULL and the costing slot silently gets no duty. DTC view configuration, owner action — no repo change (AGENTS.md 2026-09-23). |

Operational notes that are not blockers:

- **Sacrificial request re-created (2026-09-23).** `KTB FW26 Wrangler` now lives
  at `6ab113b708ef2276cf34c0d2` (the old `6a26581854e92e7acd8fa71b` is dead) and
  was used for the `append_rows` validation. The id is recorded in several
  places — grep before assuming.
- **Duplicate request references.** `dtc_request_registry` can hold several rows
  with the same `request_reference` (one active, one inactive sibling).
  Resolving by name alone silently picks the wrong sheet — always filter
  `request_is_active='Y' AND in_scope` and refuse on ambiguity.

---

## Dependency on DTC's concurrent-edit work

The DTC team is building websocket propagation of saved changes to other viewers,
client-side merge into in-progress edits, and a shared `last_read` update — i.e.
limited real concurrent editing. Not shipped, no committed date.

**If it ships, the locking rationale for v2 largely evaporates.** Plan for that
rather than discovering it mid-refactor. The justifications, re-ranked so the work
survives:

1. **Runtime** — a 2-hour loop needs a short run. Removing two full re-pulls and
   collapsing three DTC passes into one is worth real minutes regardless of locking.
2. **Correctness** — v1's Phase 10 and 9b plan against a stale Delta snapshot while
   Phase 1 plans against live. v2 puts all three on one live read. This is a latent
   bug fix that concurrent-edit support does nothing for.
3. **Fewer moving parts** — six fewer tasks, one less `taskValues` hand-off, one
   write path to reason about instead of three.
4. **Locking** — urgent today, and the thing that may become moot.

Even after the revamp ships, lean single-window writes remain strictly better:
fewer server-side merges to propagate, less client churn, fewer opportunities for
merge logic to surprise a user mid-edit. It simply stops being existential.

**Recommendation (made before rollout; all five stages have since shipped):** get
a rough ETA. Weeks → do stages 1–3 and hold stage 5, the most invasive and least
valuable of the three. A quarter or more → do all of it. The ETA is still worth
having: it decides how much further locking-driven work (e.g. more write
consolidation) is worth doing.
