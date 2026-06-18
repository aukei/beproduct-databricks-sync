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

## Remaining opportunities (not yet implemented)

- **Pre-warmed cluster**: pin job to an all-purpose cluster or enable keep-alive.
  Eliminates 350–410 s cold start with zero code change.
- **Skip known-empty requests in full pull**: 40 of 66 in-scope requests have
  `row_count = 0`.  A `skip_empty_since` threshold in `pull_requests_to_delta`
  could skip re-fetching requests that were last confirmed empty recently.
- **Parallelize `registry.refresh()` inner loop**: `registry.py:308` loops
  sequentially over `get_request_scope()` calls (only runs in Step 3).  Same
  ThreadPoolExecutor pattern as Opt A would help when many new requests are
  discovered (less impact once the registry is stable).
