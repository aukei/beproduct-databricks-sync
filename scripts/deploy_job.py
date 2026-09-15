#!/usr/bin/env python3
"""
Deploy the BeProduct <-> DTC sync as a TOP-LEVEL MULTI-TASK Databricks job.

This replaces the single-notebook orchestrator (`beproduct/orchestrate_sync.py`,
now retired) with one job whose pipeline steps are first-class tasks. Benefits:

  * Each step has its own task run_id, duration, logs and retry/repair in the
    Jobs UI run graph — no more digging through hidden `dbutils.notebook.run`
    WORKFLOW_RUN children to read per-step timing.
  * Independent steps run in PARALLEL: the BeProduct chain (Step 1 -> 2) runs
    alongside the DTC pull (Step 3); they converge at Step 4.
  * The Step 5 -> Step 7 hand-off (which requests got INSERTs) uses native
    `dbutils.jobs.taskValues` instead of parsing an exit string.
  * Phase on/off toggles (run_phase1/2/3) are expressed as condition tasks.
  * A root `wait_cluster` task absorbs the cold-start latency so that its
    duration in the run graph shows cluster warm-up separately from Step 1/3.

Jobs (split into 3, 2026-09-03 — see AGENTS.md decisions log)
--------------------------------------------------------------
Motivated by DTC's known concurrent-edit limitation (a browser user's save is
silently rejected/lost against a stale server-side "last_read" timestamp,
including when this pipeline edits the same request while a user has it
open): minimizing which jobs touch live DTC, and for how long, reduces that
contention surface. Splitting also removes Phase 9b's NT Orbit compute
(~30s/call, serial) from blocking everything else.

  * **`main`** → `BeProduct_DTC_sync_dag` (unchanged job ID) — see DAG below.
  * **`duty_compute`** → `BeProduct_DTC_sync_duty_compute` — single root task
    `compute_duty_rates` (`p9b1_compute_duty_rates.py`): NT Orbit lookups →
    `costing_chart` ONLY. Zero DTC dependency of any kind.
  * **`images`** → `BeProduct_DTC_sync_images` — single root task
    `phase3_images` (`p3_beproduct_to_dtc_images.py`), unchanged notebook.
    Needs nothing from the main job's SAME run to be correct — it reads
    `dtc_request_mapping`/`beproduct_to_dtc_staging` (left behind by
    whichever main-job run most recently populated them) and does its own
    live `DTCConnector.get_sheet()` read immediately before writing.

  * **`v2`** → `BeProduct_DTC_sync_v2` (branch `v2`) — the consolidated,
    fully-serverless rewrite of `main`: one DTC write window per request per
    run instead of three, no re-pulls, no condition tasks. Deployed ALONGSIDE
    the live `main` job, which keeps running untouched until cutover. See
    `build_v2_tasks()`, `docs/PIPELINE.md` and `docs/MIGRATION_V1_V2.md`.

Select which to build/deploy with `--job {main,duty_compute,images,v2,all}`
(see `JOB_SPECS`); all share the same `JOB_PARAMS` definitions (each job's
tasks only reference the subset they need via `P(...)`) and the same
Instance Pool (see below), but are otherwise fully independent — separate
schedules, separate clusters per run, separate job IDs.

`main` DAG
----------
    wait_cluster ─► gate_phase0 ─► phase0_pull ─► phase0_upsert ─► phase0_push ─┬─► bp_style_sync ─► transform ─┐
                                                                                │                                └─► request_manager ─► phase1_push ─► repull_dtc ─► fill_bom_data ─► repull_dtc_bom ─┐
                                                                                ├─► pull_master_dtc ─┬─────────────────────────────────────────────────────────┘   (run_if=ALL_DONE)                  │
                                                                                │                    └─► gate_phase2 ─► phase2_push                                                                    │
                                                                                └─► gate_phase9a ─► pull_lineplan_dtc ───────────────────────────────────────────────────────────────────────────────┴─► build_costing_chart ─► gate_phase9b ─► push_duty_rates

Neither `phase1_push` nor `fill_bom_data` sit behind a `gate_phase1`/
`gate_phase10` condition task (unlike `gate_phase0/2/9a/9b`) — DELIBERATE,
see AGENTS.md decisions log. Databricks propagates a condition-task's
EXCLUDED outcome to EVERY downstream dependent UNCONDITIONALLY, ignoring
run_if entirely; since the ENTIRE Phase 9/10 chain now transitively depends
on `phase1_push` (via `repull_dtc`) and `fill_bom_data`, gating either one at
the DAG level would silently exclude everything behind it whenever its
`run_phase*` flag defaults/is set to false. Both tasks instead always run
and check their own `run_phase1`/`run_phase10` widget INSIDE the notebook,
`dbutils.notebook.exit(...)`-ing as a genuine no-op SUCCESS when disabled.

`repull_dtc` is `fill_bom_data`'s unconditional prerequisite — it makes
`phase1_push`'s newly-created style x color rows visible in
`dtc_wip_<customer>`/`dtc_request_registry`, which Phase 10 needs to enrich
the COMPLETE post-Phase-1 state (not `pull_master_dtc`'s pre-Phase-1
snapshot). It is NOT gated by anything (no longer shared with `phase3_images`
either, since that task moved to its own `images` job entirely on 2026-09-03).

Phase 10 (BOM enrichment from externally-processed techpack extraction, see
`docs/v1/PHASE10_WORKFLOW.md`) is placed BEFORE `build_costing_chart` (owner
decision 2026-09-02): Fabric Group/Placement/Mill Fabric Article #/Content
values it fills in must reach `costing_chart`'s `fabric_content` BEFORE the
`duty_compute` job calls NT Orbit, or the duty classification would be
computed against stale/placeholder material data. Since Phase 10 only pushes
to the LIVE DTC sheet (never mutates Delta directly), `repull_dtc_bom`
re-pulls `dtc_wip_<customer>` afterward so `build_costing_chart` sees the
enrichment; `build_costing_chart` depends on `repull_dtc_bom`, NOT
`pull_master_dtc` directly. `repull_dtc_bom` runs unconditionally
(`run_if=ALL_DONE` on `fill_bom_data`, no gate of its own) so a
disabled/skipped/failed Phase 10 never blocks Phase 9a.

`build_costing_chart` (2026-09-07 additions, see AGENTS.md decisions log):
only WIP rows with `Fabric Group == "Main Fabric"` enter `costing_chart` at
all — Phase 10's "Fabric" segment duplicate rows are excluded entirely, so
Duty/Tariff/HTS Code are only ever computed/pushed for "Main Fabric" rows.
Also carries `tariff_rate` forward from the table's own PRIOR state (keyed
by `duty.COSTING_KEY`) before every overwrite, since no live WIP column
exists yet to "fall back" to the way `hts_code`/`duty_rate_*` do.

`push_duty_rates` (Phase 9b's DTC WIP push half only — the NT Orbit compute
half lives in the separate `duty_compute` job) reads whatever `costing_chart`
state the `duty_compute` job's most recent (independently scheduled) run
left behind and diffs each target field against the current DTC WIP cell
before PATCHing, so it's correct regardless of run ordering between the 2
jobs.

Phase 8a/8b (DTC FABRIC → Delta → BeProduct Material Master) are RETIRED
(2026-09-01): confirmed by the project team to be replaced by a separate
"MaterialLib" application, so they are removed from this DAG entirely (not
just gated off). The notebook `dtc/notebooks/p8a_pull_fabric_to_delta.py` and
tables `dtc_fabric_<customer>` / `dtc_fabric_registry` are left in place as
historical/manual-fallback artifacts but are no longer scheduled. See
AGENTS.md's decisions log for detail.

Phase 0 (DTC XTS Master → BeProduct Directory) runs FIRST: pull → upsert →
PUSH_DIRECTORY, then every Style/Material/Costing step proceeds. All downstream
roots wait on `phase0_push` with `run_if=ALL_DONE` so a disabled `run_phase0`
skips only Phase 0 and never deadlocks the rest of the DAG.

Phase 0 (DTC "XTS Master" Supplier/Factory → BeProduct Directory) is the FIRST
step: pull DTC masters → upsert beproduct_directory (match name+partner_type) →
PUSH_DIRECTORY to BeProduct. It logically precedes every Style/Material/Costing
step, which all wait on phase0_push (run_if=ALL_DONE so a disabled run_phase0
doesn't deadlock the rest of the DAG).

Cluster
-------
Each job (main / duty_compute / images) gets its OWN single-node, NON-Photon
job cluster (Classic Preview mode, matching the original live cluster kind)
— fully independent, no shared running cluster across jobs. All 3 draw from
the SAME Instance Pool (`INSTANCE_POOL_ID`, `Standard_D4as_v5`, created
2026-09-03) purely to cut cold-start from ~5-7 min to ~1-2 min; this is a
warm-VM optimization only, not a coupling mechanism. This workload is
tiny-data + driver/IO-bound (≈145 styles, ≈420 rows); Photon and extra
workers add cost without benefit.

Schedule / log destination
--------------------------
JOB_SCHEDULE/CLUSTER_LOG_DEST originate from job 294837488757511's
live-deployed settings (retrieved 2026-06-20) and are shared by all 3 job
specs so future `--reset-existing`/create runs preserve them automatically.
Edit JOB_SCHEDULE / CLUSTER_LOG_DEST below to change them; set JOB_SCHEDULE=None
to deploy without a schedule (safe for brand-new jobs before UAT).

Usage
-----
    python scripts/deploy_job.py --job all                             # preview all 3 (dry-run only)
    python scripts/deploy_job.py --job main --dry-run                   # print one job's task graph + settings
    python scripts/deploy_job.py --job main                             # CREATE a new (unscheduled) job
    python scripts/deploy_job.py --job main --reset-existing 294837488757511
                                                  # overwrite an existing job in place
                                                  # (--job duty_compute / --job images for the other 2)
    python scripts/deploy_job.py --job v2 --dry-run                     # preview the v2 DAG
    python scripts/deploy_job.py --job v2 --no-schedule                 # CREATE BeProduct_DTC_sync_v2
                                                  # unscheduled -- the correct first step; its NEW
                                                  # notebooks must be uploaded to NB_ROOT_V2 first:
                                                  #   python scripts/upload_notebooks.py \
                                                  #       --root /Workspace/Repos/beproduct-sync-v2

Requires DATABRICKS_HOST + DATABRICKS_PAT (.env).
"""

import argparse
import json
import os
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute, jobs

# ── Configuration ───────────────────────────────────────────────────────────
JOB_NAME = "BeProduct_DTC_sync_dag"

NB_BP = "/Workspace/Repos/beproduct-sync/beproduct"
NB_DTC = "/Workspace/Repos/beproduct-sync/DTC/notebooks"

# ── v2 workspace root (branch `v2`) ─────────────────────────────────────────
# v2 notebooks deploy to their OWN Workspace root so that checking out the v2
# branch can never silently change what the live v1 job executes. Upload with
#     python scripts/upload_notebooks.py --root /Workspace/Repos/beproduct-sync-v2
# See docs/MIGRATION_V1_V2.md ("Workspace isolation").
NB_ROOT_V2 = "/Workspace/Repos/beproduct-sync-v2"
NB_BP_V2 = f"{NB_ROOT_V2}/beproduct"
NB_DTC_V2 = f"{NB_ROOT_V2}/DTC/notebooks"
NB_PY_V2 = f"{NB_ROOT_V2}/DTC/python"

SHARED_CLUSTER_KEY = "shared"

# ── Instance Pool (created 2026-09-03) ───────────────────────────────────────
# Shared across all 3 split jobs (main / duty_compute / images) below to cut
# cluster cold-start from ~5-7 min to ~1-2 min WITHOUT merging the jobs into
# one shared running cluster -- each job still gets its own independent
# ephemeral job cluster (full isolation, ClassicPreview single-node as
# before), just provisioned from pre-warmed pooled VMs instead of cold Azure
# VM allocation. min_idle_instances=1 keeps one warm instance ready;
# max_capacity=6 covers all 3 jobs potentially overlapping plus headroom.
# Owner explicitly confirmed independence is fine -- this is purely a
# warm-time optimization, not a coupling mechanism.
INSTANCE_POOL_ID = "0903-055346-hose1-pool-cia9e7xn"  # "beproduct-dtc-sync-pool-v5" (Standard_D4as_v5)

# ── Cluster spec (mirrors live cluster retrieved 2026-06-20) ────────────────
# Classic Preview single-node mode (is_single_node=True, kind=CLASSIC_PREVIEW)
# matches the live cluster. Standard engine, no Photon.
SPARK_VERSION = "17.3.x-scala2.13"
# Changed 2026-09-03 (owner request): Standard_D4s_v3 -> Standard_D4as_v5
# (Azure AMD Ddsv5-series). Instance pools are immutable on node_type_id, so
# this required a NEW pool (old Standard_D4s_v3 pool deleted) rather than an
# in-place edit -- see INSTANCE_POOL_ID above.
NODE_TYPE = "Standard_D4as_v5"  # informational only when INSTANCE_POOL_ID is set (pool determines node type)
# is_single_node / kind / instance_pool_id are set in CLUSTER_EXTRA — do NOT set num_workers.
CLUSTER_EXTRA = {
    "is_single_node": True,
    "kind": "CLASSIC_PREVIEW",
    # enable_elastic_disk is REJECTED by Databricks when instance_pool_id is
    # set ("cannot be supplied when an instance pool ID is provided") --
    # live-discovered 2026-09-03 deploying with the new pool. Elastic disk is
    # a per-VM-instance setting anyway; the pool's own instances are used as-is.
    "instance_pool_id": INSTANCE_POOL_ID,
}
SPARK_CONF = {"spark.master": "local[*]"}

# ── Cluster log destination (Volumes, retrieved 2026-06-20) ─────────────────
# Set to None to disable log delivery.
CLUSTER_LOG_DEST = "/Volumes/lft/beproduct/job_log/BpDtcSync"

# ── Job schedule (retrieved from live job 294837488757511 on 2026-06-20) ────
# Quartz: 07:55:15, 12:55:15, 15:55:15 HKT daily.
# Set to None to deploy without a schedule (safe for brand-new jobs).
JOB_SCHEDULE = jobs.CronSchedule(
    quartz_cron_expression="15 55 7,12,15 * * ?",
    timezone_id="Asia/Hong_Kong",
    pause_status=jobs.PauseStatus.UNPAUSED,
)

# ── Job-level tags and queue (retrieved 2026-06-20) ─────────────────────────
# ── v2 schedule (owner spec 2026-09-15) ─────────────────────────────────────
# Every 2 hours at :05, on ODD hours (01,03,…,23) HKT.
#
# The cadence is the whole reason v2 exists: at 12 runs/day v1's ~3 write
# windows per request per run would have meant ~36 disruptive moments a day.
# v2 opens ONE window per request, and only when something actually changed --
# live-confirmed on 2026-09-15 that a second consecutive run issues zero PATCH
# calls and opens zero windows.
#
# A fixed, predictable minute matters operationally: users can learn that the
# pipeline writes at five past the odd hour, rather than being interrupted at
# arbitrary times.
JOB_SCHEDULE_V2 = jobs.CronSchedule(
    quartz_cron_expression="0 5 1,3,5,7,9,11,13,15,17,19,21,23 * * ?",
    timezone_id="Asia/Hong_Kong",
    pause_status=jobs.PauseStatus.UNPAUSED,
)

# ── duty_compute schedule (owner spec 2026-09-15) ───────────────────────────
# 10:00 and 15:00 HKT daily. Deliberately NOT in the main DAG: NT Orbit is an
# EXTERNAL, rate-limited service whose latency we do not control (~30-60 s per
# uncached call, serial), and this notebook has no call budget and no
# checkpointing -- its cache MERGE happens once at the end, so a timeout
# discards the whole run's lookups. Putting that in front of the 2-hourly write
# window would make the window unpredictable; putting it behind still risks
# overlapping the next run.
#
# Keeping it separate costs nothing in correctness: its DURABLE output is
# `nt_orbit_duty_cache`, and build_costing's Step 4c refills `costing_chart`
# from that cache on every rebuild with zero API calls. Values computed at
# 10:00 are therefore picked up and pushed by the next main run (11:05).
JOB_SCHEDULE_DUTY = jobs.CronSchedule(
    quartz_cron_expression="0 0 10,15 * * ?",
    timezone_id="Asia/Hong_Kong",
    pause_status=jobs.PauseStatus.UNPAUSED,
)

JOB_TAGS = {"userpurpose": "lft-job-bpsync"}
JOB_QUEUE = jobs.QueueSettings(enabled=True)

# ── Job-level parameters (mirror the old orchestrate_sync widgets) ───────────
# Every task references these via {{job.parameters.<name>}}.
JOB_PARAMS = {
    "catalog": "lft",
    "schema": "beproduct",
    # TEMP (2026-08-14): test cycle drops styles into "TEST KTB" instead of "KTB".
    # Revert folder_name to "KTB" once BeProduct switches back to the normal folder.
    "folder_name": "TEST KTB",
    "customer": "KTB",
    "dtc_workspace": "KTB",
    "dtc_document": "KTB WIP",
    "dtc_environment": "uat",
    # FULL on the daily job: sample-app changes do NOT bump style.modifiedAt, so
    # INCREMENTAL would miss app-only updates. Developers can still run Step 1 with
    # refresh_mode=INCREMENTAL ad-hoc from the ADB portal. See beproduct_style_sync.
    "refresh_mode": "FULL",
    "dry_run": "false",
    "delta_only": "true",
    "run_phase0": "true",            # Phase 0: push new/updated DTC Supplier/Factory masters → BeProduct Directory
    "xts_document": "XTS Master",    # DTC document name for Phase 0 (XTS Master → Directory)
    "run_phase1": "true",
    "run_phase2": "true",
    "run_phase3": "true",             # Phase 3: front image upload -- checked INSIDE the notebook (images job), not a DAG gate; see build_images_tasks()
    # Phase 8a/8b RETIRED (2026-09-01): confirmed by the project team to be
    # replaced by a separate "MaterialLib" application. run_phase8a /
    # include_test_sheets / fabric_document removed from the DAG entirely
    # (not just gated off) — see AGENTS.md decisions log.
    "run_phase9a": "true",           # Phase 9a: pull LinePlan + build costing chart
    "lineplan_document": "KTB LinePlan",  # DTC document name for Phase 9a
    "run_phase9b": "true",           # Phase 9b: NT Orbit duty/HTS/tariff fill (live in the DAG 2026-09-01)
    "costing_chart_table": "lft.beproduct.costing_chart",  # override with any unused name to test; costing_chart_kei dropped 2026-09-15
    "duty_cache_table": "lft.beproduct.nt_orbit_duty_cache",  # Phase 9b: persistent cross-run NT Orbit result cache
    "duty_cache_ttl_days": "180",     # Phase 9b: re-query a cached lookup after this many days
    "orbit_parallel_calls": "false",  # Phase 9b: call NT Orbit serially by default (safer; set true + tune max_workers for throughput)
    "orbit_timeout_seconds": "60",    # Phase 9b: per-call NT Orbit HTTP timeout (live-validated 2026-09-01: 30s was too short)
    "run_phase10": "true",            # Phase 10: BOM enrichment from techpack extraction (flipped true 2026-09-03 -- extensively live-validated: upsert semantics, Content backfill, material_no key, 0 errors across multiple runs)
    "bom_catalog": "alb_tpm_uat",      # Phase 10: BOM source catalog (alb_tpm_uat | alb_tpm_prd -- NOT derived from dtc_environment, suffix differs)
    "bom_customer_name": "KONTOOR",    # Phase 10: pre-filter customer_name in the shared multi-customer BOM table (scoping/perf only)
    "push_blanks": "false",
    "img_http_timeout": "30",
    "img_max_uploads": "0",
    # ── v2 only (branch `v2`, job BeProduct_DTC_sync_v2) ────────────────────
    # Every notebook currently hardcodes
    #   sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")
    # which would make a v2 notebook import v1's modules. The v2 notebooks read
    # this parameter instead; the default keeps v1 behaviour for any notebook
    # that hasn't been converted yet. See docs/MIGRATION_V1_V2.md.
    "module_path": "/Workspace/Repos/beproduct-sync/DTC/python",
    # v2 run flags. ALL of these are read as plain widgets INSIDE their own
    # notebook, which exits as a SUCCESS no-op when disabled -- never as a
    # condition task. Databricks propagates a condition task's EXCLUDED outcome
    # to every downstream dependent unconditionally, ignoring run_if, and v2's
    # chain is linear enough that one gate would excise the whole DTC push.
    # See docs/PIPELINE.md design rule 4.
    "run_bom": "true",            # Stage 20: join techpack BOM into staging (was run_phase10)
    "run_costing": "true",        # Stage 30: build costing_chart        (was run_phase9a)
    "run_wip_push": "true",       # Stage 40: the single DTC write       (was run_phase1)
    "run_duty_push": "true",      # Stage 40: duty contribution only     (was run_phase9b)
    "bom_table": "customer_teckpack_style_latest",  # resolves latest_techpack_style_log_id
    "bom_log_table": "customer_teckpack_style_log",  # custom_fields -- the actual BOM source
    "bom_segments_table": "tpm_bom_segments",  # Stage 20b output; read by wip_push + build_costing
    # Unqualified output table name for build_costing. Routine runs write the
    # real table; override it to build a comparison copy without replacing what
    # duty_compute reads and MERGEs. (The old `costing_chart_kei` scratch table
    # was retired 2026-09-15 once "intent" mode was proven to reproduce v1's
    # output exactly.) A bad build is recoverable via Delta time travel.
    "costing_chart_table_name": "costing_chart",
    "explain_limit": "40",            # rows per request in wip_push's stdout provenance trace
    "sample_limit": "12",             # current-vs-new samples per request in wip_push's exit JSON
    "material_exclude_columns": "",    # hard exclusion; empty by default
    # "Content" is WRITE-ONCE (owner decision 2026-09-15): DTC's own trigger
    # overwrites whatever Phase 10 writes, and the two notations are
    # semantically identical for Phase 9's NT Orbit call. Filling a blank cell
    # is all that Phase 9a's completeness gate needs; re-writing a non-blank one
    # would diff on EVERY run and open a write window every time.
    "material_fill_if_blank_columns": "Content",
}


def P(name: str) -> str:
    """Job-parameter reference."""
    return "{{job.parameters." + name + "}}"


# Convenience refs
CAT, SCH = P("catalog"), P("schema")
CUST, WS, DOC, ENV = P("customer"), P("dtc_workspace"), P("dtc_document"), P("dtc_environment")
XTS_DOC      = P("xts_document")
LINEPLAN_DOC = P("lineplan_document")
COSTING_TABLE = P("costing_chart_table")
DRY = P("dry_run")


def nb_task(task_key, notebook_path, params, depends=None, run_if=None, timeout=3600, serverless=False):
    """
    serverless=True omits job_cluster_key entirely, which runs the task on
    SERVERLESS compute instead of the shared classic job cluster. Needed for
    fill_bom_data (Phase 10): alb_tpm_<env>.public.customer_teckpack_style_log
    is a Lakebase database registered in Unity Catalog, and Lakebase catalogs
    can ONLY be queried from serverless compute -- live-confirmed 2026-09-02,
    the classic Standard_D4s_v3 shared cluster gets
    "UnauthorizedAccessException: ... requires serverless compute" from
    spark.table() on that catalog. See AGENTS.md decisions log.
    """
    task = jobs.Task(
        task_key=task_key,
        notebook_task=jobs.NotebookTask(notebook_path=notebook_path, base_parameters=params),
        depends_on=depends or [],
        run_if=run_if,
        timeout_seconds=timeout,
    )
    if not serverless:
        task.job_cluster_key = SHARED_CLUSTER_KEY
    return task


def gate_task(task_key, param_name, depends, run_if=None):
    """Condition task: proceed on the 'true' edge when {{job.parameters.<param>}} == 'true'."""
    return jobs.Task(
        task_key=task_key,
        condition_task=jobs.ConditionTask(
            op=jobs.ConditionTaskOp.EQUAL_TO, left=P(param_name), right="true"
        ),
        depends_on=depends,
        run_if=run_if,
    )


def dep(task_key, outcome=None):
    return jobs.TaskDependency(task_key=task_key, outcome=outcome)


def build_main_tasks():
    """Main job: 00 (Phase 0) -> 10 (Phase 1+4+7) -> 20 (Phase 2) -> 30 (Phase 10)
    -> 40 (Phase 9a) -> 55 (Phase 9b's DTC WIP push only).

    Phase 3 (image upload, "60") and Phase 9b's NT Orbit costing_chart
    compute ("50") are DELIBERATELY NOT here -- split into their own
    independent jobs 2026-09-03 (see build_images_tasks / build_duty_compute_tasks
    and AGENTS.md decisions log). Rationale: DTC has a known concurrent-edit
    limitation (a browser user's save is silently rejected/lost if an older
    "last_read" timestamp than the server's, including when this pipeline
    edits the same request while a user has it open) -- minimizing which
    jobs touch live DTC, and when, reduces that contention surface. Phase 9b's
    NT Orbit compute never touches DTC at all and is the slow part (~30s/call
    serial), so decoupling it removes that latency from this job entirely,
    independent of the DTC-contention rationale. Phase 3 is optional and was
    already established as not needing anything from THIS job's Delta state
    (it does its own live DTC read + only needs dtc_request_mapping/
    beproduct_to_dtc_staging, both left behind by this job's own last run).
    """
    tasks = []

    # Step 0 — cluster warm-up sentinel (root, no dependencies).
    # Absorbs cold-start latency into its own task duration so that Step 1 and
    # Step 3 timings reflect pure compute, not cluster spin-up.
    # Both parallel chains (BeProduct and DTC) depend on this task.
    tasks.append(nb_task("wait_cluster", f"{NB_BP}/wait_cluster", {},
                         timeout=600))  # 10-min cap; warm-up never takes this long

    # ── Phase 0 — DTC XTS Master (Supplier/Factory) → BeProduct Directory ───
    # Runs FIRST, before any Style/Material/Costing step. Chain:
    #   pull (DTC → dtc_xts_master_ktb) → upsert (→ beproduct_directory, match
    #   name+partner_type) → push (PUSH_DIRECTORY → BeProduct Directory API).
    # All downstream Style/Material/Costing steps wait on phase0_push
    # (run_if=ALL_DONE, so a disabled run_phase0 doesn't deadlock them).
    tasks.append(gate_task("gate_phase0", "run_phase0", depends=[dep("wait_cluster")]))
    tasks.append(nb_task("phase0_pull", f"{NB_DTC}/p0_pull_xts_master_to_delta", {
        # IMPORTANT: the notebook's widget is "xts_document", deliberately NOT
        # aliased from "dtc_document" — Databricks auto-injects every
        # job-level parameter into every task's widgets by name, and this job
        # ALSO has an unrelated job-level "dtc_document" parameter (default
        # "KTB WIP", used by the WIP-pulling tasks below). Aliasing this
        # task's "dtc_document" widget to {{job.parameters.xts_document}}
        # gets silently overridden by that auto-injection, so this task must
        # use its own uniquely-named parameter instead. Live-debugged
        # 2026-09-01: this collision caused EVERY Phase 0 run to search "KTB
        # WIP" instead of "XTS Master" and pull 0 rows. See AGENTS.md decisions log.
        "dtc_environment": ENV, "dtc_workspace": WS, "xts_document": XTS_DOC,
        "catalog": CAT, "schema": SCH,
    }, depends=[dep("gate_phase0", outcome="true")]))
    tasks.append(nb_task("phase0_upsert", f"{NB_BP}/p0_xts_master_to_directory_upsert", {
        "catalog": CAT, "schema": SCH, "source_table": "dtc_xts_master_ktb",
        "dry_run": DRY,
    }, depends=[dep("phase0_pull")]))
    tasks.append(nb_task("phase0_push", f"{NB_BP}/p5utl_beproduct_master_data_sync", {
        "catalog": CAT, "schema_name": SCH, "mode": "PUSH_DIRECTORY",
        "dry_run": DRY, "fetch_contacts": "false",
    }, depends=[dep("phase0_upsert")]))

    # Step 1 — BeProduct -> ktb_styles  (Phase 1+7: style sync + sample-app enrichment)
    tasks.append(nb_task("bp_style_sync", f"{NB_BP}/p1p7_beproduct_style_sync", {
        "folder_name": P("folder_name"), "refresh_mode": P("refresh_mode"),
        "catalog": CAT, "schema": SCH, "table_name": "ktb_styles",
    }, depends=[dep("phase0_push")], run_if=jobs.RunIf.ALL_DONE))

    # Step 2 — transform (Phase 1+7: denormalize + sample-status UDFs)
    tasks.append(nb_task("transform", f"{NB_BP}/p1p7_beproduct_to_dtc_transform", {
        "catalog": CAT, "schema": SCH, "source_table": "ktb_styles",
        "staging_table": "beproduct_to_dtc_staging",
        "folder_name": P("folder_name"), "customer_code": CUST,
    }, depends=[dep("bp_style_sync")]))

    # Step 3 — pull DTC WIP + refresh registry (Phase 1; parallel with 1/2)
    tasks.append(nb_task("pull_master_dtc", f"{NB_DTC}/p1_pull_masters_to_delta", {
        "dtc_environment": ENV, "customer": CUST, "dtc_workspace": WS, "dtc_document": DOC,
        "catalog": CAT, "schema": SCH, "write_mode": "overwrite",
        "refresh_registry": "true", "max_workers": "4",
    }, depends=[dep("phase0_push")], run_if=jobs.RunIf.ALL_DONE))

    # Step 4 — request manager (Phase 1: create + share missing requests)
    tasks.append(nb_task("request_manager", f"{NB_BP}/p1_dtc_request_manager", {
        "catalog": CAT, "schema": SCH, "staging_table": "beproduct_to_dtc_staging",
        "dtc_environment": ENV, "customer": CUST, "dtc_workspace": WS, "dtc_document": DOC,
        "dry_run": DRY, "refresh_registry": "false",
    }, depends=[dep("transform"), dep("pull_master_dtc")]))

    # Phase 1+7 gate + push (Step 5)
    # Hardened 2026-09-02 (same pattern as fill_bom_data/run_phase10): no
    # gate_phase1 condition task here -- phase1_push always runs and checks
    # run_phase1 INSIDE the notebook (no-op exit when false). A condition-task
    # gate would make phase1_push EXCLUDED whenever run_phase1=false, and
    # EXCLUDED propagates unconditionally to every downstream dependent
    # (repull_dtc, phase3_images, and now the whole Phase 9/10 chain via
    # fill_bom_data -> repull_dtc). See AGENTS.md decisions log.
    tasks.append(nb_task("phase1_push", f"{NB_BP}/p1p7_beproduct_to_dtc_push", {
        "catalog": CAT, "schema": SCH, "staging_table": "beproduct_to_dtc_staging",
        "dtc_environment": ENV, "dtc_workspace": WS, "dry_run": DRY,
        "delta_only": P("delta_only"), "batch_size": "100",
        "run_phase1": P("run_phase1"),
    }, depends=[dep("request_manager")]))

    # Phase 2 gate + push (Step 6) — DTC-owned fields back to BeProduct
    tasks.append(gate_task("gate_phase2", "run_phase2", depends=[dep("transform"), dep("pull_master_dtc")]))
    tasks.append(nb_task("phase2_push", f"{NB_DTC}/p2_push_dtc_to_beproduct", {
        "catalog": CAT, "schema": SCH, "customer": CUST,
        "staging_table": "beproduct_to_dtc_staging",
        "dtc_environment": ENV, "dry_run": DRY, "push_blanks": P("push_blanks"),
    }, depends=[dep("gate_phase2", outcome="true")]))

    # Targeted re-pull (Step 7): makes phase1_push's newly-created style x
    # color rows visible in Delta -- fill_bom_data (Phase 10, below) needs
    # the COMPLETE post-Phase-1 style x color state, not the pre-Phase-1
    # snapshot from pull_master_dtc. Unconditional (no gate_phase3 dependency
    # -- that was removed 2026-09-02 to avoid an EXCLUDED-cascade risk, back
    # when phase3_images also lived in this job and depended on it; Phase 3
    # has since moved to its own job entirely, see build_images_tasks, and
    # doesn't need this repull anyway -- it does its own live DTC read and
    # only needs dtc_request_mapping/beproduct_to_dtc_staging). run_if=
    # ALL_DONE so a skipped/failed phase1_push doesn't block Phase 10.
    tasks.append(nb_task("repull_dtc", f"{NB_DTC}/p1_pull_masters_to_delta", {
        "dtc_environment": ENV, "customer": CUST, "dtc_workspace": WS, "dtc_document": DOC,
        "catalog": CAT, "schema": SCH, "write_mode": "overwrite", "refresh_registry": "false",
        "request_ids": "{{tasks.phase1_push.values.inserted_ids}}", "max_workers": "4",
    }, depends=[dep("phase1_push")], run_if=jobs.RunIf.ALL_DONE))

    # Phase 8a/8b (DTC FABRIC → Delta → BeProduct Material Master) RETIRED
    # 2026-09-01 — confirmed by the project team to be replaced by a separate
    # "MaterialLib" application. Removed from the DAG entirely (not gated
    # off) — see AGENTS.md decisions log. dtc/notebooks/p8a_pull_fabric_to_delta.py
    # is left in place as a manual-fallback artifact but is no longer scheduled.

    # ── Phase 9a — Pull LinePlan + Build Costing Chart ─────────────────────────
    tasks.append(gate_task("gate_phase9a", "run_phase9a",
                           depends=[dep("phase0_push")], run_if=jobs.RunIf.ALL_DONE))
    tasks.append(nb_task("pull_lineplan_dtc", f"{NB_DTC}/p9a_pull_lineplan_to_delta", {
        # IMPORTANT: widget is "lineplan_document", NOT aliased from
        # "dtc_document" -- same Databricks auto-injection collision as
        # phase0_pull above (this job's job-level "dtc_document" parameter,
        # default "KTB WIP", would silently win over a same-named task
        # widget). Live-debugged 2026-09-01: this collision meant
        # dtc_lineplan_ktb was ALWAYS 0 rows and costing_chart's LinePlan
        # fields (order_quantity/target_ldp/target_fob/supplier_type) were
        # ALWAYS null. See AGENTS.md decisions log.
        "dtc_environment": ENV,
        "customer":        CUST,
        "dtc_workspace":   WS,
        "lineplan_document": LINEPLAN_DOC,
        "catalog":         CAT,
        "schema":          SCH,
        "write_mode":      "overwrite",
        "max_workers":     "4",
    }, depends=[dep("gate_phase9a", outcome="true")]))

    # build_costing_chart reads dtc_wip_<customer> from Delta, so it must wait
    # for repull_dtc_bom (below), NOT pull_master_dtc directly -- Phase 10's
    # BOM enrichment (Fabric Group/Placement/Mill Fabric Article #) pushes to
    # the LIVE DTC sheet, not Delta; only a re-pull makes it visible here.
    # Owner decision 2026-09-02: BOM enrichment must land BEFORE costing_chart
    # so up-to-date material names flow into Phase 9b's NT Orbit calls.
    tasks.append(nb_task("build_costing_chart", f"{NB_DTC}/p9a_build_costing_chart", {
        "catalog":  CAT,
        "schema":   SCH,
        "customer": CUST,
    }, depends=[dep("pull_lineplan_dtc"), dep("repull_dtc_bom")]))

    # ── Phase 9b (part 2/2, "55") — Push filled HTS/Duty/Tariff -> live DTC WIP ─
    # NT Orbit's costing_chart-only compute (part 1/2, "50") moved to its own
    # independent job (build_duty_compute_tasks, 2026-09-03) -- it never
    # touches DTC and is the slow part (~30s/call), so it no longer blocks
    # this job at all. This step just reads costing_chart's CURRENT state
    # (already filled by whatever the compute job's last run produced) and
    # PATCHes a fast, scoped diff onto DTC -- low DTC-contention window, safe
    # to keep in the main job right after Phase 9a. See
    # dtc/notebooks/p9b2_push_duty_to_wip.py and AGENTS.md decisions log.
    tasks.append(gate_task("gate_phase9b", "run_phase9b",
                           depends=[dep("build_costing_chart")]))
    tasks.append(nb_task("push_duty_rates", f"{NB_DTC}/p9b2_push_duty_to_wip", {
        "catalog":             CAT,
        "schema":              SCH,
        "customer":            CUST,
        "costing_chart_table": COSTING_TABLE,
        "dtc_environment":     ENV,
        "dtc_workspace":       WS,
        "dry_run":             DRY,
        "batch_size":          "100",
    }, depends=[dep("gate_phase9b", outcome="true")]))

    # ── Phase 10 — BOM enrichment from externally-processed techpack data ─────
    # Fulfills a Phase 1 gap: BOM data isn't available from the BeProduct API,
    # so it's sourced from a separate techpack-extraction pipeline
    # (alb_tpm_<env>.public.customer_teckpack_style_log) and joined onto
    # ktb_styles by (bp_style_number=style_no, season||' - '||year=style_season).
    #
    # Depends on repull_dtc, NOT pull_master_dtc directly (changed 2026-09-02):
    # Phase 10 must enrich the COMPLETE post-Phase-1 style x color state --
    # pull_master_dtc's snapshot is taken BEFORE phase1_push runs, so it can be
    # missing style x color rows phase1_push just created THIS run. repull_dtc
    # (Step 7) is what makes those rows visible in dtc_wip_<customer> +
    # dtc_request_registry (current rowId/sheet_id/view_id per style), and it's
    # now an unconditional (non-gated) prerequisite shared with phase3_images
    # for exactly this reason. run_if=ALL_DONE: a skipped/failed repull_dtc
    # must not exclude the whole Phase 9/10 chain behind fill_bom_data.
    # NO gate_task here (unlike every other phase) -- DELIBERATE, see below.
    # `fill_bom_data` always runs and checks `run_phase10` INSIDE the
    # notebook (like `dry_run` elsewhere), no-op'ing immediately when false
    # instead of being excluded at the DAG level.
    #
    # Live-discovered 2026-09-02: a condition-task gate here (gate_phase10 ->
    # fill_bom_data[outcome=true]) causes fill_bom_data to become EXCLUDED
    # (not just skipped) whenever run_phase10=false -- and Databricks
    # propagates EXCLUDED status to EVERY downstream dependent UNCONDITIONALLY,
    # ignoring run_if entirely (run_if only tolerates a dependency that
    # actually ran and skipped/failed, not one EXCLUDED via an untaken
    # condition branch). Since repull_dtc_bom -> build_costing_chart ->
    # gate_phase9b -> fill_duty_rates all transitively depended on
    # fill_bom_data, this silently excluded the ENTIRE Phase 9a/9b chain on
    # every scheduled run while run_phase10 defaulted to false (confirmed
    # live: 2026-09-02 15:57 HKT scheduled run). Fixed by removing the gate
    # entirely for this one phase -- see AGENTS.md decisions log.
    tasks.append(nb_task("fill_bom_data", f"{NB_DTC}/p10_pull_bom_and_enrich", {
        "catalog":     CAT,
        "schema":      SCH,
        "customer":    CUST,
        "folder_name": P("folder_name"),
        "dtc_environment": ENV,
        "dtc_workspace":   WS,
        "bom_catalog": P("bom_catalog"),
        "bom_customer_name": P("bom_customer_name"),
        "run_phase10": P("run_phase10"),
        "dry_run":     DRY,
        "batch_size":  "100",
    }, depends=[dep("repull_dtc")], run_if=jobs.RunIf.ALL_DONE, serverless=True))

    # Re-pull WIP after BOM enrichment so build_costing_chart (Phase 9a) sees
    # the enriched Fabric Group/Placement/Mill Fabric Article # data -- Phase
    # 10 only pushes to the LIVE DTC sheet, never mutates Delta directly (see
    # p10_pull_bom_and_enrich.py's module docstring). A FULL re-pull (not
    # targeted by request_ids) is used deliberately: referencing
    # {{tasks.X.values...}} output adds fragility for no real benefit here.
    # fill_bom_data always actually runs now (see its own comment above for
    # why it's not gated at the DAG level) and no-ops internally when
    # run_phase10=false, so this dependency is never EXCLUDED. run_if=ALL_DONE
    # is kept only as a genuine-failure safety net (fill_bom_data erroring for
    # some real reason must still not block build_costing_chart).
    tasks.append(nb_task("repull_dtc_bom", f"{NB_DTC}/p1_pull_masters_to_delta", {
        "dtc_environment": ENV, "customer": CUST, "dtc_workspace": WS, "dtc_document": DOC,
        "catalog": CAT, "schema": SCH, "write_mode": "overwrite", "refresh_registry": "false",
        "max_workers": "4",
    }, depends=[dep("fill_bom_data")], run_if=jobs.RunIf.ALL_DONE))

    return tasks


def build_duty_compute_tasks():
    """Duty-compute job ("50"): NT Orbit -> costing_chart ONLY.

    A single root task, fully independent of the main job. Never touches
    live DTC at all -- no DTC API key, no DTC read/write. Split out
    2026-09-03 specifically because it's the slow part of the old Phase 9b
    (~30s per uncached NT Orbit call, serial by default) and has zero
    DTC-contention risk, so there's no reason for its latency to hold up
    Phase 0/1/2/10/9a or the DTC WIP push in the main job. See
    dtc/notebooks/p9b1_compute_duty_rates.py and AGENTS.md decisions log.
    """
    return [nb_task("compute_duty_rates", f"{NB_DTC}/p9b1_compute_duty_rates", {
        "catalog":               CAT,
        "schema":                SCH,
        "costing_chart_table":   COSTING_TABLE,
        "dry_run":               DRY,
        "parallel_calls":        P("orbit_parallel_calls"),
        "max_workers":           "4",
        "orbit_timeout_seconds": P("orbit_timeout_seconds"),
        "duty_cache_table":      P("duty_cache_table"),
        "cache_ttl_days":        P("duty_cache_ttl_days"),
    })]


def build_images_tasks():
    """Images job ("60"): Phase 3 front-image upload ONLY.

    A single root task, fully independent of the main job. Split out
    2026-09-03 as part of minimizing which jobs touch live DTC and when
    (DTC's known concurrent-edit limitation -- a browser user's save is
    silently rejected/lost against a stale "last_read" timestamp, including
    when this pipeline edits the same request while a user has it open).
    Needs NOTHING from the main job's SAME run to be correct: it reads
    dtc_request_mapping/beproduct_to_dtc_staging (both left behind by the
    main job's most recent run, whenever that was) and does its own live
    DTC `get_sheet()` read for the freshest rowIndex/Style Image state
    immediately before writing -- see p3_beproduct_to_dtc_images.py.
    """
    return [nb_task("phase3_images", f"{NB_BP}/p3_beproduct_to_dtc_images", {
        "catalog": CAT, "schema": SCH, "staging_table": "beproduct_to_dtc_staging",
        "dtc_environment": ENV, "dtc_workspace": WS, "dry_run": DRY,
        "http_timeout": P("img_http_timeout"), "max_uploads": P("img_max_uploads"),
        "run_phase3": P("run_phase3"),
    })]


def build_v2_tasks():
    """v2 main job (`BeProduct_DTC_sync_v2`) -- branch `v2`.

    Full specification: docs/PIPELINE.md. Rationale: docs/MIGRATION_V1_V2.md.

    The one thing this DAG is built around: DTC uses permissive optimistic
    locking at REQUEST granularity. Any successful write moves the request's
    server-side `last_read`, and every browser session that loaded earlier is
    then refused on save and silently loses in-progress edits. Confirmed with
    the DTC developer 2026-09-14: only WRITES move it (reads are free), and the
    scope is the whole request -- a write through WIP_ITS_USE blocks a user
    editing a different row through a different view.

    v1 writes each request at up to 5 moments scattered across the whole DAG
    (phase1_push updates/inserts/orphans, fill_bom_data updates/inserts,
    push_duty_rates updates). At the target cadence -- a run every ~2 hours
    during active style development -- that is 36 write moments a day and
    ~6-8 h/day of user-visible exposure. v2 writes each request at exactly ONE
    point, in <=2 back-to-back calls (2 is the floor: patch_rows rejects a body
    mixing rowId and rowIndex). Consolidating the write path is the
    PRECONDITION for the cadence, not an optimisation.

        p0_pull -> p0_upsert -> p0_push -+-> bp_style_sync ----> transform -+-> request_manager -+
                                         |                                 |                    |
                                         +-> pull_master_dtc --------------+--------------------+
                                         |          |                      |                    |
                                         |          +-> phase2_push        |                    |
                                         |                                 |                    |
                                         +-> pull_lineplan_dtc ------------+-> build_costing ---+
                                                                                                |
                                                                                                v
                                                                                            wip_push
                                                                          (the only DTC write in this job)

    Differences from build_main_tasks() beyond the merge:

      * SERVERLESS everywhere (no job_cluster_key, no instance pool, no
        wait_cluster). Proven in v1: fill_bom_data has run serverless since
        2026-09-02 with the same Workspace-Files sys.path pattern, no task
        declares `libraries`, and nothing uses sparkContext/.rdd/UDFs/
        spark.conf.set. Also removes the Lakebase constraint that forced the
        BOM read into its own task, so it collapses into `transform`.
      * NO CONDITION TASKS. See the run_* parameters in JOB_PARAMS.
      * No repull_dtc / repull_dtc_bom -- `transform` emits style x color x
        material directly, and `build_costing` reads staging + the start-of-run
        pull instead of a round-trip through DTC.
      * Every edge is run_if=ALL_DONE: a failed stage should degrade the run,
        not abort it. Notably wip_push still pushes style/BOM/sample data when
        build_costing failed; it just contributes no duty fields that round.

    Notebooks marked NEW below do not exist yet -- this job definition is the
    spec they are built against. Deploy it with --no-schedule until they land.
    """
    def v2_task(task_key, notebook_path, params, depends=None):
        # serverless=True => no job_cluster_key; ALL_DONE => degrade, don't abort.
        return nb_task(task_key, notebook_path, {"module_path": P("module_path"), **params},
                       depends=depends, run_if=jobs.RunIf.ALL_DONE, serverless=True)

    tasks = []

    # ── Stage 00: DTC XTS Master -> BeProduct Directory (unchanged) ──────────
    tasks.append(v2_task("phase0_pull", f"{NB_DTC_V2}/p0_pull_xts_master_to_delta", {
        "catalog": CAT, "schema": SCH, "customer": CUST, "dtc_workspace": WS,
        "dtc_document": XTS_DOC, "dtc_environment": ENV, "dry_run": DRY,
        "run_phase0": P("run_phase0"),
    }))
    tasks.append(v2_task("phase0_upsert", f"{NB_BP_V2}/p0_xts_master_to_directory_upsert", {
        "catalog": CAT, "schema": SCH, "customer": CUST, "dry_run": DRY,
        "run_phase0": P("run_phase0"),
    }, depends=[dep("phase0_pull")]))
    tasks.append(v2_task("phase0_push", f"{NB_BP_V2}/p5utl_beproduct_master_data_sync", {
        "catalog": CAT, "schema": SCH, "mode": "PUSH_DIRECTORY", "dry_run": DRY,
        "run_phase0": P("run_phase0"),
    }, depends=[dep("phase0_upsert")]))

    # ── Stage 10: three independent source pulls, in parallel ───────────────
    tasks.append(v2_task("bp_style_sync", f"{NB_BP_V2}/p1p7_beproduct_style_sync", {
        "catalog": CAT, "schema": SCH, "folder_name": P("folder_name"),
        "refresh_mode": P("refresh_mode"),
    }, depends=[dep("phase0_push")]))
    tasks.append(v2_task("pull_master_dtc", f"{NB_DTC_V2}/p1_pull_masters_to_delta", {
        "catalog": CAT, "schema": SCH, "customer": CUST, "dtc_workspace": WS,
        "dtc_document": DOC, "dtc_environment": ENV,
    }, depends=[dep("phase0_push")]))
    # Gated by run_costing, not a flag of its own: this pull exists only to feed
    # build_costing, so disabling that makes it pure waste.
    tasks.append(v2_task("pull_lineplan_dtc", f"{NB_DTC_V2}/p9a_pull_lineplan_to_delta", {
        "catalog": CAT, "schema": SCH, "customer": CUST, "dtc_workspace": WS,
        "dtc_document": LINEPLAN_DOC, "dtc_environment": ENV,
        "run_costing": P("run_costing"),
    }, depends=[dep("phase0_push")]))

    # ── Stage 20: denormalize to style x color staging (REUSED, unchanged) ──
    # The v1 transform already produces beproduct_to_dtc_staging correctly and
    # needs no change for v2. Copying it to bolt on a BOM read would duplicate
    # every field mapping, season-code lookup, lifecycle gate and validation
    # rule in 839 lines of it -- so the BOM read is its own parallel task
    # (pull_bom) instead.
    #
    # Staging deliberately stays at style x COLOR grain, NOT style x color x
    # material: (a) the material fan-out depends on which segments a colorway
    # is ALREADY represented by in live DTC, which the transform cannot know,
    # and (b) phase1.compute_upsert() treats a repeated (BP Style#, Color) as a
    # duplicate_bp_key exception. The material dimension is resolved at PLAN
    # time in wip_plan, against live rows. See docs/PIPELINE.md Stage 20.
    tasks.append(v2_task("transform", f"{NB_BP_V2}/p1p7_beproduct_to_dtc_transform", {
        "catalog": CAT, "schema": SCH, "source_table": "ktb_styles",
        "staging_table": "beproduct_to_dtc_staging",
        "folder_name": P("folder_name"), "customer_code": CUST,
    }, depends=[dep("bp_style_sync")]))

    # ── Stage 20b: techpack BOM (Lakebase) -> Delta (NEW) ───────────────────
    # Runs in PARALLEL with `transform`; touches no DTC. In v1 the Lakebase
    # read forced Phase 10 onto its own serverless task and sat BETWEEN two DTC
    # re-pulls; here the whole job is serverless, so it is just another input
    # gathered up front. Materializing it to Delta means neither wip_push nor
    # build_costing has to touch Lakebase.
    tasks.append(v2_task("pull_bom", f"{NB_DTC_V2}/v2_pull_bom_segments", {   # NEW
        "catalog": CAT, "schema": SCH, "folder_name": P("folder_name"),
        "bom_catalog": P("bom_catalog"), "bom_table": P("bom_table"),
        "bom_log_table": P("bom_log_table"),
        "bom_customer_name": P("bom_customer_name"),
        "bom_segments_table": P("bom_segments_table"),
        "run_bom": P("run_bom"),
    }, depends=[dep("bp_style_sync")]))

    # ── Stage 25: resolve / create / share requests (unchanged) ─────────────
    tasks.append(v2_task("request_manager", f"{NB_BP_V2}/p1_dtc_request_manager", {
        "catalog": CAT, "schema": SCH, "customer": CUST, "dtc_workspace": WS,
        "dtc_document": DOC, "dtc_environment": ENV, "dry_run": DRY,
    }, depends=[dep("transform"), dep("pull_master_dtc")]))

    # ── Stage 30: costing_chart from staging, not from a re-pull (NEW) ──────
    # staging (material dimension, which we own) x pull_master_dtc (DTC-owned
    # lineplan_ref / vendor slots / production country, which we never write,
    # so the start-of-run pull is current by definition) x LinePlan.
    # Also fills every duty field directly from nt_orbit_duty_cache (read-only,
    # zero API calls) so a brand-new row is filled the instant its exact
    # product+origin+market combination has ever been looked up.
    # Same notebook as v1 -- ONE costing implementation, one set of gates --
    # switched into "intent" mode. v1 reads the WIP snapshot as-is, which is
    # only correct because it re-pulls the sheet AFTER Phase 10 enriches it
    # (repull_dtc_bom). v2 has no re-pull and runs this BEFORE wip_push, so
    # Step 1a overlays the material and style-identity columns from the SAME
    # sources wip_push will write from (tpm_bom_segments, staging), while every
    # DTC-owned column -- Lineplan Ref #, vendor/factory slots, production
    # country, existing HTS/duty -- still comes from the snapshot, because this
    # pipeline never writes those and they are current by definition.
    tasks.append(v2_task("build_costing", f"{NB_DTC_V2}/p9a_build_costing_chart", {
        "catalog": CAT, "schema": SCH, "customer": CUST,
        "duty_cache_table": P("duty_cache_table"),
        "cache_ttl_days": P("duty_cache_ttl_days"),
        "bom_segments_table": P("bom_segments_table"),
        "staging_table": "beproduct_to_dtc_staging",
        "wip_effective_mode": "intent",
        "output_table": P("costing_chart_table_name"),
        "run_costing": P("run_costing"),
    }, depends=[dep("transform"), dep("pull_bom"), dep("pull_master_dtc"),
                dep("pull_lineplan_dtc")]))

    # ── Stage 40: THE single DTC write window (NEW) ─────────────────────────
    # Replaces v1's phase1_push (Phases 1/4/7) + fill_bom_data (Phase 10) +
    # push_duty_rates (Phase 9b push half). Per request: ONE live get_sheet, one
    # combined plan (sync/wip_plan.py composing phase1/bom/duty -- it composes,
    # it does not re-implement), then one PATCH of updates keyed by rowId and
    # one PATCH of inserts keyed by rowIndex, back to back.
    #
    # INVARIANT: if the combined plan is empty, send NOTHING -- no GET-to-PATCH
    # path, zero calls, zero user disruption. At 12 runs/day this is the
    # difference between safe and intolerable. It is asserted and unit-tested,
    # not left to emerge from per-field diffing.
    tasks.append(v2_task("wip_push", f"{NB_DTC_V2}/v2_wip_push", {            # NEW
        "catalog": CAT, "schema": SCH, "customer": CUST, "dtc_workspace": WS,
        "dtc_document": DOC, "dtc_environment": ENV, "dry_run": DRY,
        "delta_only": P("delta_only"), "batch_size": "100",
        "costing_chart_table": COSTING_TABLE,
        "bom_segments_table": P("bom_segments_table"),
        "explain_limit": P("explain_limit"),
        "sample_limit": P("sample_limit"),
        "material_exclude_columns": P("material_exclude_columns"),
        "material_fill_if_blank_columns": P("material_fill_if_blank_columns"),
        "run_wip_push": P("run_wip_push"), "run_duty_push": P("run_duty_push"),
    }, depends=[dep("request_manager"), dep("build_costing")]))

    # ── Stage 45: Style Image upload -- the SECOND write window ─────────────
    # Folded into this DAG 2026-09-15 (owner decision) rather than left on its
    # own schedule. It CANNOT share wip_push's PATCH: image cells are writable
    # only through the multipart /images endpoint, and DTC rejects any
    # sheetData write to "Style Image" outright (HTTP 400). So it is
    # irreducibly a second write window.
    #
    # What folding it in buys is ADJACENCY. On an independent schedule its
    # window landed at arbitrary times relative to the main run, giving users
    # two unpredictable disruptions per cycle; here it lands seconds after
    # wip_push's, so there is still only one period per run to avoid. It also
    # now sees wip_push's newly-inserted rows in the same run.
    #
    # Cheap in practice: it only uploads where "Style Image" is blank AND a
    # source exists, so in steady state it writes nothing and opens no window
    # at all. run_if=ALL_DONE so a failed push never blocks it and vice versa.
    tasks.append(v2_task("phase3_images", f"{NB_BP_V2}/p3_beproduct_to_dtc_images", {
        "catalog": CAT, "schema": SCH, "staging_table": "beproduct_to_dtc_staging",
        "dtc_environment": ENV, "dtc_workspace": WS, "dry_run": DRY,
        "http_timeout": P("img_http_timeout"), "max_uploads": P("img_max_uploads"),
        "run_phase3": P("run_phase3"),
    }, depends=[dep("wip_push")]))

    # ── Stage 50: DTC -> BeProduct (unchanged; writes BeProduct, never DTC) ──
    tasks.append(v2_task("phase2_push", f"{NB_DTC_V2}/p2_push_dtc_to_beproduct", {
        "catalog": CAT, "schema": SCH, "customer": CUST, "dtc_environment": ENV,
        "dry_run": DRY, "push_blanks": P("push_blanks"), "run_phase2": P("run_phase2"),
    }, depends=[dep("transform"), dep("pull_master_dtc")]))

    return tasks


def _build_cluster() -> compute.ClusterSpec:
    """Build the shared job cluster spec.

    Uses Classic Preview single-node mode (is_single_node / kind) as deployed
    live; falls back gracefully if the SDK version doesn't expose those attrs.
    """
    log_conf = None
    if CLUSTER_LOG_DEST:
        log_conf = compute.ClusterLogConf(
            volumes=compute.VolumesStorageInfo(destination=CLUSTER_LOG_DEST)
        )

    spec = compute.ClusterSpec(
        spark_version=SPARK_VERSION,
        # node_type_id intentionally OMITTED: INSTANCE_POOL_ID (set via
        # CLUSTER_EXTRA below) determines the node type; specifying both is
        # rejected by Databricks ("node_type_id and instance_pool_id are
        # mutually exclusive").
        # num_workers intentionally omitted for is_single_node clusters
        runtime_engine=compute.RuntimeEngine.STANDARD,
        data_security_mode=compute.DataSecurityMode.DATA_SECURITY_MODE_DEDICATED,
        spark_conf=SPARK_CONF,
        cluster_log_conf=log_conf,
    )

    # CLUSTER_EXTRA fields (is_single_node, kind, enable_elastic_disk,
    # instance_pool_id) are set via dict-patch so the script still works if
    # the installed SDK predates them.
    raw = spec.as_dict()
    raw.update(CLUSTER_EXTRA)
    # Rebuild from dict so the SDK object stays consistent
    return compute.ClusterSpec.from_dict(raw)


# ── The 3 split jobs (2026-09-03) ────────────────────────────────────────────
# All 3 share the SAME JOB_PARAMS (simplest: each job's build_tasks() only
# references the subset of parameters it actually needs via P(...); unused
# parameters on a given job are harmless, matching this repo's established
# reliance on Databricks auto-injecting every job-level parameter into every
# task's widgets) and the SAME Instance Pool (INSTANCE_POOL_ID) for warm-time,
# but are otherwise fully independent jobs -- separate schedules (currently
# identical cron, can diverge freely), separate clusters per run, separate
# job IDs. See build_main_tasks/build_duty_compute_tasks/build_images_tasks'
# own docstrings for the rationale.
JOB_SPECS = {
    "main": {
        "display_name": JOB_NAME,
        "build_tasks": build_main_tasks,
    },
    "duty_compute": {
        "display_name": "BeProduct_DTC_sync_duty_compute",
        "build_tasks": build_duty_compute_tasks,
        "schedule": JOB_SCHEDULE_DUTY,
    },
    "images": {
        "display_name": "BeProduct_DTC_sync_images",
        "build_tasks": build_images_tasks,
    },
    # ── v2 (branch `v2`) ────────────────────────────────────────────────────
    # A SEPARATE job deployed alongside the live v1 job, not a replacement for
    # it: v1 keeps running untouched on `master` throughout the migration, so
    # rollback is "pause v2, unpause v1" with no data migration (both write the
    # same tables with the same keys, and every write is idempotent and
    # diff-gated). serverless=True => no job_clusters block at all.
    # Deploy with --no-schedule until the NEW notebooks land; see
    # docs/MIGRATION_V1_V2.md ("Rollout").
    "v2": {
        "display_name": "BeProduct_DTC_sync_v2",
        "build_tasks": build_v2_tasks,
        "serverless": True,
        # v2 imports its modules from its OWN workspace root, never v1's.
        # delta_only OFF in v2: it is a STYLE-level gate that runs before any
        # field comparison, so drift from any cause other than "this style just
        # changed" is invisible to it permanently. v2's zero-diff-zero-write
        # invariant makes a full scan cost ZERO extra API calls. See
        # build_v2_tasks() and docs/MIGRATION_V1_V2.md.
        "param_overrides": {"module_path": NB_PY_V2, "delta_only": "false"},
        "schedule": JOB_SCHEDULE_V2,
    },
}


_SCHEDULE_SENTINEL = object()


def build_settings(job_key: str, schedule=_SCHEDULE_SENTINEL) -> jobs.JobSettings:
    spec = JOB_SPECS[job_key]
    # A spec may carry its OWN cron (v2 runs 2-hourly, the v1 jobs 3x/day).
    # An explicit `schedule=` argument still wins, so --no-schedule works.
    if schedule is _SCHEDULE_SENTINEL:
        schedule = spec.get("schedule", JOB_SCHEDULE)
    # A fully-serverless job declares no job clusters; every task simply omits
    # job_cluster_key (see nb_task(serverless=True)).
    job_clusters = None if spec.get("serverless") else [
        jobs.JobCluster(job_cluster_key=SHARED_CLUSTER_KEY, new_cluster=_build_cluster())
    ]
    params = {**JOB_PARAMS, **spec.get("param_overrides", {})}
    return jobs.JobSettings(
        name=spec["display_name"],
        tasks=spec["build_tasks"](),
        job_clusters=job_clusters,
        parameters=[jobs.JobParameterDefinition(name=k, default=v) for k, v in params.items()],
        max_concurrent_runs=1,
        schedule=schedule,
        tags=JOB_TAGS,
        queue=JOB_QUEUE,
    )


def _preview(settings: jobs.JobSettings):
    sched = settings.schedule
    sched_str = (f"{sched.quartz_cron_expression} ({sched.timezone_id}) "
                 f"pause={sched.pause_status.value if sched.pause_status else 'n/a'}"
                 if sched else "none (deploy manually)")
    print(f"Job name : {settings.name}")
    print(f"Schedule : {sched_str}")
    if settings.job_clusters:
        print(f"Cluster  : {NODE_TYPE} single_node=True engine=STANDARD  pool={INSTANCE_POOL_ID}")
    else:
        print("Cluster  : SERVERLESS (no job cluster, no instance pool)")
    print(f"Log dest : {CLUSTER_LOG_DEST or 'none'}")
    print(f"Tags     : {settings.tags}")
    print("\nTask graph:")
    for t in settings.tasks:
        kind = "condition" if t.condition_task else "notebook"
        deps = ", ".join(
            (d.task_key + (f"[{d.outcome}]" if d.outcome else "")) for d in (t.depends_on or [])
        ) or "(root)"
        extra = f"  run_if={t.run_if.value}" if t.run_if else ""
        print(f"  • {t.task_key:16} [{kind:9}] <- {deps}{extra}")
    print(f"\nJob parameters ({len(settings.parameters)}): "
          + ", ".join(f"{p.name}={p.default}" for p in settings.parameters))


def main():
    ap = argparse.ArgumentParser(
        description="Deploy the BeProduct<->DTC jobs (main / duty_compute / images / v2).")
    ap.add_argument("--job", choices=list(JOB_SPECS) + ["all"], default="main",
                    help="which job spec to act on (default: main, for backward compatibility). "
                         "'all' previews every spec (dry-run only).")
    ap.add_argument("--dry-run", action="store_true", help="print the graph/settings; do not apply")
    ap.add_argument("--reset-existing", metavar="JOB_ID", type=int,
                    help="overwrite an existing job (reset) instead of creating a new one; "
                         "only valid with a single --job (not 'all')")
    ap.add_argument("--no-schedule", action="store_true",
                    help="omit the cron schedule from the deployed settings (useful for test jobs)")
    args = ap.parse_args()

    # Only override when --no-schedule is given; otherwise let each spec supply
    # its own cron (v2 is 2-hourly, the v1 jobs 3x/day).
    schedule = None if args.no_schedule else _SCHEDULE_SENTINEL

    if args.job == "all":
        if args.reset_existing:
            sys.exit("--reset-existing requires a single --job (not 'all').")
        for key in JOB_SPECS:
            settings = build_settings(key, schedule=schedule)
            _preview(settings)
            print()
        print("Dry run — nothing applied. Pass --job <name> to deploy a specific job.")
        return

    settings = build_settings(args.job, schedule=schedule)
    _preview(settings)

    if args.dry_run:
        print("\nDry run — nothing applied.")
        return

    if not (os.environ.get("DATABRICKS_HOST") and
            (os.environ.get("DATABRICKS_TOKEN") or os.environ.get("DATABRICKS_PAT"))):
        sys.exit("Set DATABRICKS_HOST and DATABRICKS_TOKEN/DATABRICKS_PAT (source .env).")
    if os.environ.get("DATABRICKS_PAT") and not os.environ.get("DATABRICKS_TOKEN"):
        os.environ["DATABRICKS_TOKEN"] = os.environ["DATABRICKS_PAT"]

    w = WorkspaceClient()
    if args.reset_existing:
        w.jobs.reset(job_id=args.reset_existing, new_settings=settings)
        job_id = args.reset_existing
        print(f"\nReset existing job {job_id} ({args.job}) to the current task graph.")
    else:
        created = w.jobs.create(
            name=settings.name,
            tasks=settings.tasks,
            job_clusters=settings.job_clusters,
            parameters=settings.parameters,
            max_concurrent_runs=settings.max_concurrent_runs,
            schedule=settings.schedule,
            tags=settings.tags,
            queue=settings.queue,
        )
        job_id = created.job_id
        print(f"\nCreated job {job_id} ({settings.name}).")
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    print(f"   {host}/jobs/{job_id}")


if __name__ == "__main__":
    main()
