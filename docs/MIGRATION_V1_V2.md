# v1 → v2 — why, what changes, and how to roll it out

**Branch `v2`.** Design record for the pipeline revamp. The specification itself
lives in [PIPELINE.md](PIPELINE.md) and [SYNC_CONTRACT.md](SYNC_CONTRACT.md); this
document is the *reasoning*, the *risks*, and the *rollout plan*.

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
request, one combined plan, one PATCH of updates and one PATCH of inserts, back to
back. Two calls is the floor — `patch_rows` rejects a body mixing `rowId` and
`rowIndex`.

### 2. The transform emits the final grain

v1's transform produced **style × color**; Phase 10 later fanned it out to
**style × color × material** by pushing to DTC, re-pulling, and planning a second
time. v2's transform joins the techpack BOM directly and emits
style × color × material in one pass, so `repull_dtc` disappears.

### 3. `costing_chart` is built from intent, not from a round-trip

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
| PATCH calls per request per run | up to 5 | **≤2** |
| Full DTC re-pulls per run | 2 | **0** |
| Tasks in the main job | 17 (incl. 5 gates + `wait_cluster`) | **11** |
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

**Port:** drop `job_cluster_key` from every task, drop `wait_cluster`, retire the
instance pool.

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
higher per second, but the pooled VMs stop being paid for between runs. Measure
one real full run before deleting the pool.

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
constraint that forced Phase 10 into its own task costs nothing, and the BOM read
collapses into the transform. An earlier draft of this plan had a separate
serverless BOM-staging task purely to work around this; it is no longer needed.

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

- `compute_duty_rates` stays its own job. Zero DTC contact, ~30–60 s serial NT
  Orbit calls; there is no reason for its latency to sit inside a 2-hour loop.
- `phase3_images` stays its own job. It cannot join `wip_push`'s PATCH — image
  cells are writable only through the multipart `/images` endpoint and DTC rejects
  any `sheetData` write to `Style Image`.
- Phase 2 (DTC → BeProduct) is untouched. It writes BeProduct, never DTC.
- Every field mapping, match key, gate condition and hard-won bug fix carries over
  unchanged. v2 is a restructuring of *when and how often we write*, not of *what
  we write*.

---

## Rollout

The v2 job is deployed as a **new job, `BeProduct_DTC_sync_v2`**, alongside the
live v1 job. v1 keeps running on `master` throughout; nothing about it changes.

### Workspace isolation

v1 and v2 notebooks must not share a Workspace path, or checking out the v2 branch
would silently change what the live v1 job executes. v2 deploys to its own root:

```
v1   /Workspace/Repos/beproduct-sync/{beproduct,DTC/notebooks,DTC/python}
v2   /Workspace/Repos/beproduct-sync-v2/{beproduct,DTC/notebooks,DTC/python}
```

`scripts/upload_notebooks.py --root` selects the target.

**Done (stage 1).** Every notebook previously hardcoded
`sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")`, so a v2 notebook
uploaded to the v2 root would still have imported v1's modules. All 17 sites now
read a **`module_path`** widget instead, defaulting to the v1 path — so a task
that passes nothing behaves exactly as before, and the v2 job overrides it to
`/Workspace/Repos/beproduct-sync-v2/DTC/python`.

### Run flags replacing condition tasks

**Done (stage 1).** The reused notebooks relied on `gate_phase*` condition tasks
that v2 does not have, so they would have run unconditionally. Each now reads its
own flag and exits as a SUCCESS no-op when disabled:

| Notebook | Flag | Was |
|---|---|---|
| `p0_pull_xts_master_to_delta` | `run_phase0` | `gate_phase0` |
| `p0_xts_master_to_directory_upsert` | `run_phase0` | downstream of `gate_phase0` |
| `p5utl_beproduct_master_data_sync` | `run_phase0` | downstream of `gate_phase0` |
| `p2_push_dtc_to_beproduct` | `run_phase2` | `gate_phase2` |
| `p9a_pull_lineplan_to_delta` | `run_costing` | `gate_phase9a` |

All default to `"true"`, so ad-hoc and interactive runs that pass nothing are
unaffected; only an explicit `"false"` skips. `p9a_pull_lineplan_to_delta` is
gated on `run_costing` rather than a flag of its own because the LinePlan pull
exists solely to feed `build_costing`.

### Stages of work

| # | Deliverable | Reduces windows to | Status |
|---|---|---|---|
| 0 | Branch, consolidated docs, `BeProduct_DTC_sync_v2` job definition | — | **done** |
| 1 | Serverless port; `module_path` parameter; in-notebook run flags | — | **done** |
| 2 | `sync/wip_plan.py` + `test_wip_plan.py` — composition, zero-diff-zero-write, delta filter | — | |
| 3 | `v2_build_wip_staging` + `v2_wip_push`; drop `repull_dtc` | 3 → 2 | |
| 4 | `v2_build_costing_chart` off staging; drop `repull_dtc_bom` | 2 → 2 | |
| 5 | Fold the duty contribution into `wip_push` | 2 → **1** | |

Each stage ships independently and each reduces either window count or runtime, so
there is benefit before the whole thing lands.

### Validation before cutover

1. Deploy `BeProduct_DTC_sync_v2` **unscheduled** (`--no-schedule`) and with
   `dry_run=true`. Confirm the plan it computes matches what v1 actually pushed
   for the same input state.
2. Run against the sacrificial request `KTB FW26 Wrangler`
   (UAT `6a26581854e92e7acd8fa71b`) with `dry_run=false`.
3. Confirm the zero-diff invariant directly: run twice back to back and assert the
   second run issues **zero** PATCH calls.
4. Compare `costing_chart` built from staging against v1's re-pull-based output for
   the same run. They should be identical except for rows whose WIP state changed
   during the run.
5. Only then attach a schedule and pause v1.

### Rollback

v1 is untouched and independently scheduled throughout. Rollback is: pause
`BeProduct_DTC_sync_v2`, unpause `BeProduct_DTC_sync_dag`. No data migration —
both write the same tables with the same keys, and every write is idempotent and
diff-gated.

---

## Open items

**Blocking the first v2 run**

- The three `NEW` notebooks (`v2_build_wip_staging`, `v2_build_costing_chart`,
  `v2_wip_push`) and `sync/wip_plan.py`. Stages 2–5.

**Cadence-limiting, independent of this refactor**

- **Sample-app enrichment.** One `app_get` per (style × app) — ~876 calls, ~120 s
  for KTB — pinned to `FULL` because app changes do not bump `style.modifiedAt`.
  At 12 runs/day that is ~10,500 BeProduct calls/day, and once the DTC passes are
  merged it becomes the largest single runtime item in the job. Recommended:
  split it onto its own 2–3×/day schedule and let the 2-hour loop consume whatever
  `ktb_styles` holds. Sample statuses changing within 2 hours is unlikely to be
  what is driving the cadence request.
- **Delta filtering across contributions.** v1's style push is `delta_only`
  against `registry.last_pushed`, but Phase 10 and 9b recompute every style every
  run. At 12 runs/day that is 12× the Lakebase reads and costing rebuilds for data
  that changes maybe twice a day. The v2 plan builder should carry a delta filter
  across all three contributions.
- **Serverless cost.** Measure one real run before retiring the instance pool.

**Deferred**

- **The images job is a second write window.** It cannot share the PATCH, but it
  could at least be co-scheduled so the two windows are adjacent rather than
  independent. Worth doing once the main job's window is proven.
- **The `duty_compute` sequencing gap** (PIPELINE.md, companion jobs) is narrowed
  by Stage 30's cache fill but not structurally closed.

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

**Recommendation:** get a rough ETA. Weeks → do stages 1–3 and hold stage 5, the
most invasive and least valuable of the three. A quarter or more → do all of it.
