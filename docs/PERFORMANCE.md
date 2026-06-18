# Pipeline Performance Analysis & Optimizations

Validated against the two most recent `BeProduct_orchestrate_sync` Databricks job
runs (job ID 22324120218492), both on 2026-06-18.

---

## Baseline timing (pre-optimization)

| Step | Description | Run 1 (15:28 HKT) | Run 2 (07:30 HKT) |
|------|-------------|-------------------|-------------------|
| Cluster cold start | New cluster per run | 411 s | 351 s |
| 1 | BeProduct API → ktb_styles (FULL, 145 styles) | 103 s | 123 s |
| 2 | Transform → beproduct_to_dtc_staging | 51 s | 62 s |
| **3** | **DTC pull + registry refresh (66 requests, serial)** | **454 s** | **383 s** |
| 4 | DTC Request Manager | 21 s | 21 s |
| 5 | Phase 1 push (BeProduct → DTC) | 51 s | 31 s |
| 6 | Phase 2 push (DTC → BeProduct) | 32 s | 21 s |
| **7** | **DTC re-pull post-Phase 1 (66 requests, serial, full overwrite)** | **414 s** | **383 s** |
| 8 | Phase 3 image upload | 41 s | 132 s |
| **Total execution** | | **1 167 s** | **1 156 s** |
| **Total wall time** | | **1 578 s (~26 min)** | **1 507 s (~25 min)** |

Steps 3 + 7 combined = **868 s (74%)** of execution time in Run 1.

---

## Root cause

`pull_requests_to_delta.py` looped **sequentially** over all 66 active in-scope
registry entries, calling `connector.get_sheet()` (one HTTP GET per request) with
no parallelism.  At ≈6 s avg per call × 66 = ≈396 s per step.

Additionally Step 7 re-pulled all 66 requests unconditionally even when Phase 1
had only INSERTed rows into a handful of them (Run 1: 1 image uploaded; Run 2: 54).

Step 1 always did a FULL BeProduct API refresh (all 145 styles) regardless of how
many styles had actually changed since the previous run.

---

## Optimizations applied (2026-06-18)

### Opt A — Parallel `get_sheet()` calls (`pull_requests_to_delta.py`)

`ThreadPoolExecutor(max_workers=4)` replaces the serial loop for all
`get_sheet()` calls (both Step 3 and Step 7).  `max_workers` is hard-capped at 4
to protect the 2-node K8S cluster backing the DTC UAT API.

Expected step time: 66 calls / 4 workers × 6 s avg ≈ **100 s** per step
(vs 400 s serial).  Saves ≈ **600 s / run**.

Files changed: `dtc/notebooks/pull_requests_to_delta.py`

### Opt B — Targeted Step 7 re-pull (orchestrator + push notebook)

`beproduct_to_dtc_push.py` now tracks which `request_id`s received at least one
successful INSERT and emits them in a structured exit string:
`"ok inserts=N inserted_ids=id1,id2,..."`.

`orchestrate_sync.py` parses this string and passes the IDs to Step 7 via the
new `request_ids` widget of `pull_requests_to_delta`.  When `request_ids` is
non-empty, the notebook fetches only the listed requests (DELETE stale rows +
append fresh data) instead of overwriting the whole `dtc_wip_ktb` table.

Falls back to a full re-pull automatically when Step 5 reports no INSERTs or its
exit string is unavailable.

Expected Step 7 time on a run with few INSERTs: **< 30 s** (vs 400 s full).
For a run with many INSERTs (e.g. first-time population) it degrades gracefully
to Opt-A speed (proportional to the number of INSERT'd requests).

Files changed:
- `beproduct/beproduct_to_dtc_push.py` — track + emit `inserted_request_ids`
- `beproduct/orchestrate_sync.py` — parse, wire to Step 7 params
- `dtc/notebooks/pull_requests_to_delta.py` — `request_ids` + `max_workers` widgets,
  targeted DELETE+append write path

### Opt D — INCREMENTAL BeProduct refresh default (`orchestrate_sync.py`)

Changed the `refresh_mode` widget default from `"FULL"` to `"INCREMENTAL"`.
`beproduct_style_sync.py` already implements INCREMENTAL using the
`ktb_styles_sync_meta.last_sync_at` timestamp as a `FolderModifiedAt` filter;
it falls back to FULL automatically on first run (no prior timestamp).

Expected Step 1 time on quiet days (few style changes): ≈ 20–30 s vs 103–123 s.

Files changed: `beproduct/orchestrate_sync.py`

---

## Projected timing (post-optimization)

| Step | Projected |
|------|-----------|
| Cluster cold start | 351–411 s (unchanged; use pre-warmed cluster to eliminate) |
| 1 (INCREMENTAL) | ~20–30 s |
| 2 | ~50 s |
| 3 (parallel, 4 workers) | ~100 s |
| 4 | ~21 s |
| 5 | ~30–50 s |
| 6 | ~20–30 s |
| 7 (targeted, parallel) | ~10–100 s (scales with INSERT count) |
| 8 | ~40–130 s |
| **Total execution** | **~300–500 s (~5–8 min)** |
| **Total wall (cold cluster)** | **~650–900 s (~11–15 min)** |

A pre-warmed / keep-alive cluster would further eliminate the 350–410 s cold start,
bringing wall time to ≈ 5–8 min.

---

## Validation run 3 — post Opt A/B/D (2026-06-18, run 57360453476718)

Job run `57360453476718` → task run `274593806072640` (job 22324120218492),
executed AFTER Opt A (parallel `get_sheet`), Opt B (targeted Step 7), and Opt D
(INCREMENTAL Step 1 default). Per-step timings parsed from the orchestrator's own
command-level `startTime`/`finishTime` (parent notebook export):

| Step | Description | Run 3 | vs baseline |
|------|-------------|-------|-------------|
| Cluster cold start (setup) | new cluster | 441 s | ~unchanged |
| 1 | BeProduct → ktb_styles (**INCREMENTAL**) | **133 s** | 103–123 s (FULL) → **no gain** |
| 2 | Transform → staging | 52 s | ~unchanged |
| **3** | **DTC pull + registry refresh** | **494 s** | 383–454 s → **no gain** |
| 4 | DTC Request Manager | 21 s | ~unchanged |
| 5 | Phase 1 push | 62 s | ~unchanged |
| 6 | Phase 2 push | 31 s | ~unchanged |
| **7** | **DTC re-pull post-Phase 1 (targeted)** | **31 s** | 383–414 s → **~13× faster** |
| 8 | Phase 3 image upload | 31 s | 41–132 s |
| **Total execution** | | **887 s** | 1 156–1 167 s |
| **Total wall (incl. 441 s setup)** | | **1 328 s (~22 min)** | ~25–26 min |

**Only Opt B (Step 7) delivered a material gain** (≈383 s → 31 s). Opt A and Opt D
produced no measurable improvement. **The earlier (baseline) root-cause was wrong**:
it blamed serial `get_sheet` ("6 s × 66 ≈ 396 s"), but the measured intra-step
breakdown below shows the DTC API is only ~24 s of Step 3 — the real cost is Spark.

### Measured intra-step breakdown (this is the key correction)

The child notebooks launched by `dbutils.notebook.run` ARE recoverable: they are
`run_type=WORKFLOW_RUN` runs (see the databricks-integration skill for the exact
CLI recipe). Exporting the Step 1 and Step 3 child runs and reading each cell's
`startTime`/`finishTime` gives the true split:

**Step 3 — `pull_requests_to_delta` (child run 476284671935125, 491 s):**

| Cell | Work | Time | Notes |
|------|------|------|-------|
| 2 | connector init + **`registry.refresh`** (66 in-scope enriched, 66×2 serial GETs) | **19.8 s** | API by-id reads are FAST (~0.1 s each) |
| 4 | **parallel `get_sheet`** of 66 sheets (4 workers) + build per-request DataFrames | **3.9 s** | the part Opt A "fixed" — was never the bottleneck |
| **5** | **union 66 DataFrames + `saveAsTable` (overwrite, mergeSchema, columnMapping) + `out.count()`** | **277.1 s** | wrote only **422 rows** |
| **6** | **per-request control-table `UPDATE` loop (66×)** | **179.1 s** | 66 separate Delta UPDATE Spark jobs |
| 0,1,3,7 | docstring, imports, registry read, final `.show()` | ~6 s | |

So **456 s of 491 s (93 %) of Step 3 is Spark overhead**: the 66-way `unionByName`
+ schema-evolving Delta overwrite + redundant post-write `count()` (cell 5, 277 s),
and the 66-iteration control-table `UPDATE` loop (cell 6, 179 s). The DTC API
(registry refresh + all `get_sheet`s) is only ~24 s.

- **Confirms hypothesis 1** (control-table logging as separate Spark jobs):
  cell 6 = **179 s** for 66 serial `UPDATE`s (`pull_requests_to_delta.py:306-316`),
  ~2.7 s per UPDATE.
- **Explains why Opt A gave nothing**: parallelizing `get_sheet` shaved a few
  seconds off a 3.9 s cell. `get_sheet` was never slow because the requests are
  small (422 rows across 66 requests ≈ 6 rows each; ~40 are empty).
- **New, larger bottleneck the hypotheses missed**: cell 5 = **277 s** to write
  422 rows, caused by building one tiny DataFrame *per request* and
  `reduce(unionByName)`-ing 66 of them (66 LocalRelation scans + deep plan), an
  `overwrite` + `mergeSchema` + `delta.columnMapping.mode=name` write, and a
  redundant `out.count()` re-execution.

**Step 1 — `beproduct_style_sync` (child run 849467851218721, 132 s):**

| Cell | Work | Time |
|------|------|------|
| 1 | `pip install beproduct` (subprocess) + imports + widgets | 5.8 s |
| 2 | (monolithic) BeProduct `attributes_list` fetch + transform + `createDataFrame`/`count` + MERGE upsert + metadata insert | **110.4 s** |

Cell 2's stdout was not captured in the export, so the API-vs-Spark split inside
it isn't directly visible. But the step-level comparison settles hypothesis 2:
INCREMENTAL Step 1 = 110 s (cell 2) / 132 s (wrapper) vs the FULL baseline of
103–123 s — **not faster**. **Confirms hypothesis 2**: the BeProduct
`api.style.attributes_list(filters=…)` call costs about the same regardless of the
`FolderModifiedAt` filter; the filter reduces *rows returned*, not API time. This
matches the AGENTS.md note that `FolderModifiedAt` is **folder-scoped** (any change
in the KTB folder re-qualifies every style in that folder), so INCREMENTAL still
enumerates ~the whole folder. The fixed `pip install beproduct` on every run
(`beproduct_style_sync.py:34`) is a smaller, easily-removed cost (bake into the
cluster / use `%pip` cache).

### Suggested next optimizations (re-prioritised by the measured data)

**Step 3 (highest impact — ~456 s of pure Spark overhead):**
- **Build ONE DataFrame for the whole pull, not 66.** Collect all records into a
  single list and `spark.createDataFrame(all_records, schema)` once; write once;
  **drop the redundant `out.count()`** (cell 5). Targets the 277 s.
- **Batch the control-table updates** into a single `MERGE INTO {registry} USING
  <temp view>` keyed on `request_id` (one Spark job instead of 66). Targets the
  179 s (cell 6). This is the highest-confidence win and directly addresses
  hypothesis 1.
- (Lower priority) Parallelizing `registry.refresh` only saves part of ~20 s — do
  it last.

**Step 1:**
- Pre-install the `beproduct` SDK on the cluster image / via an init script to drop
  the ~6 s `pip install` per run.
- The ~110 s BeProduct fetch is API-bound and filter-insensitive; revisit only if
  BeProduct offers a cheaper delta/changed-since endpoint. Opt D (INCREMENTAL) can
  be left on (no harm) but should not be expected to save time.

### Multi-task job refactor — validated 2026-06-19 (job 294837488757511)

The single-notebook orchestrator (`orchestrate_sync.py`, retired) was replaced by
a top-level **multi-task** job `BeProduct_DTC_sync_dag` (`scripts/deploy_job.py`):
8 steps as first-class tasks on ONE shared single-node non-Photon cluster, with
condition-task phase gates and a `dbutils.jobs.taskValues` hand-off for Step 5 ->
Step 7 `inserted_ids`. Validation run 857980233264412 (dry_run) — all tasks
SUCCESS. Per-task exec:

| Task | exec | Notes |
|------|------|-------|
| `bp_style_sync` ∥ `pull_dtc` | 76 s ∥ 278 s | **started simultaneously** — parallelism confirmed |
| `transform` | 37 s | after bp_style_sync |
| `request_manager` | 13 s | |
| `gate_phase1/2/3` | 0–1 s | condition tasks |
| `phase1_push` | 47 s | taskValues.set(inserted_ids) |
| `phase2_push` | 21 s | |
| `repull_dtc` | 197 s | dry-run → empty inserted_ids → full re-pull (real run = targeted, ~30 s) |
| `phase3_images` | 26 s | |
| cluster setup (cold) | **291 s** | single-node started FASTER than the old 2-node Photon (441–591 s) |

Wins confirmed: parallel Step 1-2 ∥ Step 3 saves ~113 s; per-step logs are now
first-class (export any step directly via its `tasks[].run_id` — no `WORKFLOW_RUN`
hunting); the control-table MERGE held at **5.5 s** (was 179 s). Single-node
non-Photon validated as the right cluster shape for this tiny-data workload.

### Cell-5 single-DataFrame write — validated 2026-06-19 (run 345301990331528)

`pull_requests_to_delta.py` now accumulates ALL requests' records into one flat
list, builds a SINGLE DataFrame (union of every request's `col_*`, `rec.get()`
fills missing cols with None), writes once, and uses `len(all_records)` instead of
a Spark `count()`. Result — `pull_dtc` cell-by-cell:

| cell | original | +MERGE | +cell-5 fix |
|------|----------|--------|-------------|
| 2 registry.refresh | 19.8 s | 26.8 s | 42.2 s (API variance) |
| 4 parallel get_sheet | 3.9 s | 4.7 s | 2.1 s |
| **5 build+write** | **277 s** | 201 s | **8.8 s** |
| **6 control update** | **179 s** | 5.5 s | **7.8 s** |
| **Step 3 total** | **491 s** | 483 s | **84 s** |

`repull_dtc` (Step 7) full re-pull also dropped 197 s → **17 s**. Step 3 is now
bottlenecked only by `registry.refresh` (cell 2, ~40 s of serial
`get_request_scope` by-id reads) — the last remaining lever if sub-40 s is wanted
(parallelize that loop with the same `ThreadPoolExecutor(max_workers=4)` pattern).

Follow-up: migrate the cron schedule from the old job (22324120218492) to the new
multi-task job (294837488757511) and pause the old one.

### Changes applied 2026-06-19 (pending live validation)

- **Opt E — batched control-table MERGE.** `pull_requests_to_delta.py` now replaces
  the ~66-iteration per-request `UPDATE` loop (cell 6, was ~179 s) with a single
  `MERGE INTO {registry} USING control_updates_src` (one Spark job). Because Step 7
  runs the same notebook, it benefits automatically; in targeted mode the MERGE
  only touches the filtered requests. Expected Step 3 saving: ~150–175 s.
- **SDK-install isolated (instrumentation, not a fix).** `beproduct_style_sync.py`
  splits the `pip install beproduct` into its own command cell with a
  `time.perf_counter()` print, so the next run's exported model shows the install
  cost separately from imports/fetch/write. If it proves material, bake `beproduct`
  into the cluster image / init script.
- **Opt F — single-DataFrame write (DONE 2026-06-19).** Step 3 cell 5's 66-way
  union + `out.count()` (was 277 s) replaced by one DataFrame + `len()`; validated
  at **8.8 s** (see "Cell-5 single-DataFrame write" below).

---

## Remaining opportunities (not yet implemented)

- **Pre-warmed cluster**: pin job to an all-purpose cluster or enable keep-alive.
  Eliminates 350–410 s cold start with zero code change.
- **Skip known-empty requests in full pull**: 40 of 66 in-scope requests have
  `row_count = 0`.  A `skip_empty_since` threshold in `pull_requests_to_delta`
  could skip re-fetching requests that were last confirmed empty recently.
- **Parallelize `registry.refresh()` inner loop**: `registry.py:308` loops
  sequentially over `get_request_scope()` calls (only runs in Step 3).  Same
  ThreadPoolExecutor pattern as Opt A would help when many new requests are
  discovered. **NOTE (run 3): low impact** — the whole registry refresh measured
  only ~20 s; the by-id GETs are fast. Prioritise the Step 3 Spark fixes above.
