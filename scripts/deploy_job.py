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

DAG
---
    wait_cluster ─► gate_phase0 ─► phase0_pull ─► phase0_upsert ─► phase0_push ─┬─► bp_style_sync ─► transform ─┐
                                                                                │                                └─► request_manager ─► phase1_push ─► repull_dtc ─┬─► gate_phase3 ─► phase3_images
                                                                                ├─► pull_master_dtc ─┬─────────────────────────────────────────────────────────┘   │
                                                                                │                    └─► gate_phase2 ─► phase2_push                                  └─► fill_bom_data ─► repull_dtc_bom ─┐
                                                                                │                                                                                       (run_if=ALL_DONE)                  │
                                                                                └─► gate_phase9a ─► pull_lineplan_dtc ─────────────────────────────────────────────────────────────────────────────────────┴─► build_costing_chart ─► gate_phase9b ─► fill_duty_rates

Neither `phase1_push` nor `fill_bom_data` sit behind a `gate_phase1`/
`gate_phase10` condition task (unlike `gate_phase0/2/3/9a/9b`) — DELIBERATE,
see AGENTS.md decisions log. Databricks propagates a condition-task's
EXCLUDED outcome to EVERY downstream dependent UNCONDITIONALLY, ignoring
run_if entirely; since the ENTIRE Phase 3/9/10 chain now transitively depends
on `phase1_push`, and the entire Phase 9 chain transitively depends on
`fill_bom_data`, gating either one at the DAG level would silently exclude
everything behind it whenever its `run_phase*` flag defaults/is set to
false. Both tasks instead always run and check their own `run_phase1`/
`run_phase10` widget INSIDE the notebook, `dbutils.notebook.exit(...)`-ing as
a genuine no-op SUCCESS when disabled.

`repull_dtc` (changed 2026-09-02) is a SHARED, unconditional prerequisite for
both `phase3_images` and `fill_bom_data` — it makes `phase1_push`'s
newly-created style x color rows visible in `dtc_wip_<customer>` /
`dtc_request_registry`, which Phase 10 needs to enrich the COMPLETE
post-Phase-1 state (not `pull_master_dtc`'s pre-Phase-1 snapshot). Only
`phase3_images` itself is gated by `run_phase3` (via `gate_phase3[true]`);
`repull_dtc` runs regardless so Phase 10 is never held hostage to whether
images are wanted.

Phase 10 (BOM enrichment from externally-processed techpack extraction, see
`docs/PHASE10_WORKFLOW.md`) is placed BEFORE `build_costing_chart` (owner
decision 2026-09-02): Fabric Group/Placement/Mill Fabric Article # values it
fills in must reach `costing_chart`'s `fabric_content` (part of
`product_description`) BEFORE Phase 9b calls NT Orbit, or the duty
classification would be computed against stale/placeholder material data.
Since Phase 10 only pushes to the LIVE DTC sheet (never mutates Delta
directly), `repull_dtc_bom` re-pulls `dtc_wip_<customer>` afterward so
`build_costing_chart` sees the enrichment; `build_costing_chart` depends on
`repull_dtc_bom`, NOT `pull_master_dtc` directly. `repull_dtc_bom` runs
unconditionally (`run_if=ALL_DONE` on `fill_bom_data`, no gate of its own) so
a disabled/skipped/failed Phase 10 never blocks Phase 9a — it just becomes an
extra, harmless full re-pull in that case.

`phase3_images` (image upload) and `fill_duty_rates` (Phase 9b HTS/Duty WIP
PATCH) are the two parallel, optional, WIP-mutating leaf branches off this
shared prerequisite chain. They write through disjoint DTC surfaces —
`phase3_images` uses the binary multipart `/images` endpoint (keyed by
`rowindex`, touches only the image cell, re-reads the live sheet itself right
before writing); `fill_duty_rates` uses the JSON `sheetData` PATCH (keyed by
`rowId`, touches only HTS/Duty/Tariff columns, sourced from Delta as of
`repull_dtc_bom`) — so running them concurrently is safe without either
needing to repull immediately before its own write.

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
One SHARED single-node, NON-Photon job cluster (Classic Preview mode, matching the
live cluster kind). This workload is tiny-data + driver/IO-bound (≈145 styles,
≈420 rows); Photon and extra workers add cost without benefit. A shared cluster
also means ONE cold start for the whole run, measured by `wait_cluster`.

Schedule / log destination
--------------------------
These are live-deployed settings retrieved from job 294837488757511 on 2026-06-20
and encoded here so future `--reset-existing` runs preserve them automatically.
Edit JOB_SCHEDULE / CLUSTER_LOG_DEST below to change them; set JOB_SCHEDULE=None
to deploy without a schedule (safe for brand-new jobs before UAT).

Usage
-----
    python scripts/deploy_job.py --dry-run        # print the task graph + settings
    python scripts/deploy_job.py                  # CREATE a new (unscheduled) job
    python scripts/deploy_job.py --reset-existing 294837488757511
                                                  # overwrite an existing job in place

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

SHARED_CLUSTER_KEY = "shared"

# ── Cluster spec (mirrors live cluster retrieved 2026-06-20) ────────────────
# Classic Preview single-node mode (is_single_node=True, kind=CLASSIC_PREVIEW)
# matches the live cluster. Standard engine, no Photon.
SPARK_VERSION = "17.3.x-scala2.13"
NODE_TYPE = "Standard_D4s_v3"
# is_single_node / kind are set in CLUSTER_EXTRA — do NOT set num_workers.
CLUSTER_EXTRA = {
    "is_single_node": True,
    "kind": "CLASSIC_PREVIEW",
    "enable_elastic_disk": True,
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
    "run_phase3": "true",
    # Phase 8a/8b RETIRED (2026-09-01): confirmed by the project team to be
    # replaced by a separate "MaterialLib" application. run_phase8a /
    # include_test_sheets / fabric_document removed from the DAG entirely
    # (not just gated off) — see AGENTS.md decisions log.
    "run_phase9a": "true",           # Phase 9a: pull LinePlan + build costing chart
    "lineplan_document": "KTB LinePlan",  # DTC document name for Phase 9a
    "run_phase9b": "true",           # Phase 9b: NT Orbit duty/HTS/tariff fill (live in the DAG 2026-09-01)
    "costing_chart_table": "lft.beproduct.costing_chart",  # test override: lft.beproduct.costing_chart_kei
    "duty_cache_table": "lft.beproduct.nt_orbit_duty_cache",  # Phase 9b: persistent cross-run NT Orbit result cache
    "duty_cache_ttl_days": "180",     # Phase 9b: re-query a cached lookup after this many days
    "orbit_parallel_calls": "false",  # Phase 9b: call NT Orbit serially by default (safer; set true + tune max_workers for throughput)
    "orbit_timeout_seconds": "60",    # Phase 9b: per-call NT Orbit HTTP timeout (live-validated 2026-09-01: 30s was too short)
    "run_phase10": "false",           # Phase 10: BOM enrichment from techpack extraction (default off until UAT-validated)
    "bom_catalog": "alb_tpm_uat",      # Phase 10: BOM source catalog (alb_tpm_uat | alb_tpm_prd -- NOT derived from dtc_environment, suffix differs)
    "bom_customer_name": "KONTOOR",    # Phase 10: pre-filter customer_name in the shared multi-customer BOM table (scoping/perf only)
    "push_duty_to_wip": "true",      # Phase 9b: also PATCH filled values back to DTC WIP
    "push_blanks": "false",
    "img_http_timeout": "30",
    "img_max_uploads": "0",
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


def build_tasks():
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

    # Phase 3 gate (after Step 4) + targeted re-pull (Step 7) + images (Step 8)
    #
    # repull_dtc is now a SHARED prerequisite (2026-09-02): it makes
    # phase1_push's newly-created style x color rows visible in Delta, which
    # both phase3_images AND fill_bom_data (Phase 10, below) need -- Phase 10
    # must enrich the COMPLETE post-Phase-1 style x color state, not the
    # pre-Phase-1 snapshot from pull_master_dtc. It therefore no longer
    # depends on gate_phase3[true] (that would make it -- and everything
    # transitively behind it, now including the whole Phase 9/10 chain --
    # EXCLUDED whenever run_phase3=false, the same EXCLUDED-cascade class
    # already fixed for gate_phase10). The run_phase3 check moves to
    # phase3_images itself, the only task that's actually optional here.
    # run_if=ALL_DONE so a skipped/failed phase1_push doesn't block Phase 3.
    tasks.append(gate_task("gate_phase3", "run_phase3", depends=[dep("request_manager")]))
    tasks.append(nb_task("repull_dtc", f"{NB_DTC}/p1_pull_masters_to_delta", {
        "dtc_environment": ENV, "customer": CUST, "dtc_workspace": WS, "dtc_document": DOC,
        "catalog": CAT, "schema": SCH, "write_mode": "overwrite", "refresh_registry": "false",
        "request_ids": "{{tasks.phase1_push.values.inserted_ids}}", "max_workers": "4",
    }, depends=[dep("phase1_push")], run_if=jobs.RunIf.ALL_DONE))

    tasks.append(nb_task("phase3_images", f"{NB_BP}/p3_beproduct_to_dtc_images", {
        "catalog": CAT, "schema": SCH, "staging_table": "beproduct_to_dtc_staging",
        "dtc_environment": ENV, "dtc_workspace": WS, "dry_run": DRY,
        "http_timeout": P("img_http_timeout"), "max_uploads": P("img_max_uploads"),
    }, depends=[dep("gate_phase3", outcome="true"), dep("repull_dtc")]))

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

    # ── Phase 9b — Fill HTS/Duty/Tariff via NT Orbit Duty Tools ────────────────
    tasks.append(gate_task("gate_phase9b", "run_phase9b",
                           depends=[dep("build_costing_chart")]))
    tasks.append(nb_task("fill_duty_rates", f"{NB_DTC}/p9b_fill_duty_rates", {
        "catalog":             CAT,
        "schema":              SCH,
        "customer":            CUST,
        "costing_chart_table": COSTING_TABLE,
        "duty_cache_table":    P("duty_cache_table"),
        "cache_ttl_days":      P("duty_cache_ttl_days"),
        "dtc_environment":     ENV,
        "dtc_workspace":       WS,
        "dry_run":             DRY,
        "push_to_wip":         P("push_duty_to_wip"),
        # Serial by default (2026-09-01, live-validated) -- NT Orbit calls are
        # ~30s each and occasionally exceed a 30s timeout; serial + 60s
        # timeout is the safer default. Flip parallel_calls=true to trade
        # safety for throughput once the API's concurrency tolerance is known.
        "parallel_calls":      P("orbit_parallel_calls"),
        "max_workers":         "4",
        "orbit_timeout_seconds": P("orbit_timeout_seconds"),
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
        node_type_id=NODE_TYPE,
        # num_workers intentionally omitted for is_single_node clusters
        runtime_engine=compute.RuntimeEngine.STANDARD,
        data_security_mode=compute.DataSecurityMode.DATA_SECURITY_MODE_DEDICATED,
        spark_conf=SPARK_CONF,
        cluster_log_conf=log_conf,
    )

    # CLUSTER_EXTRA fields (is_single_node, kind, enable_elastic_disk) are set
    # via dict-patch so the script still works if the installed SDK predates them.
    raw = spec.as_dict()
    raw.update(CLUSTER_EXTRA)
    # Rebuild from dict so the SDK object stays consistent
    return compute.ClusterSpec.from_dict(raw)


def build_settings(schedule: "jobs.CronSchedule | None" = JOB_SCHEDULE) -> jobs.JobSettings:
    return jobs.JobSettings(
        name=JOB_NAME,
        tasks=build_tasks(),
        job_clusters=[jobs.JobCluster(job_cluster_key=SHARED_CLUSTER_KEY,
                                      new_cluster=_build_cluster())],
        parameters=[jobs.JobParameterDefinition(name=k, default=v) for k, v in JOB_PARAMS.items()],
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
    print(f"Cluster  : {NODE_TYPE} single_node=True engine=STANDARD")
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
    ap = argparse.ArgumentParser(description="Deploy the BeProduct<->DTC multi-task job.")
    ap.add_argument("--dry-run", action="store_true", help="print the graph/settings; do not apply")
    ap.add_argument("--reset-existing", metavar="JOB_ID", type=int,
                    help="overwrite an existing job (reset) instead of creating a new one")
    ap.add_argument("--no-schedule", action="store_true",
                    help="omit the cron schedule from the deployed settings (useful for test jobs)")
    args = ap.parse_args()

    schedule = None if args.no_schedule else JOB_SCHEDULE
    settings = build_settings(schedule=schedule)
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
        print(f"\nReset existing job {job_id} to the multi-task DAG.")
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
        print(f"\nCreated job {job_id} ({JOB_NAME}).")
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    print(f"   {host}/jobs/{job_id}")


if __name__ == "__main__":
    main()
