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
    wait_cluster ─┬─► bp_style_sync ─► transform ─┐
                  │                                ├─► request_manager ─► gate_phase1 ─► phase1_push ─┐
                  ├─► pull_master_dtc ───────────────────┘         │                                         │
                  │       │                                   └─► gate_phase3 ──────────────┐          │
                  │       └─► gate_phase2 ─► phase2_push                                    ├─► repull_dtc ─► phase3_images
                  │                                                                         (run_if=ALL_DONE)
                  └─► gate_phase8a ─► pull_fabric_dtc   (parallel, independent of WIP chain)
                  └─► gate_phase9a ─► pull_lineplan_dtc ─► build_costing_chart  (parallel)

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
# Quartz: 07:57:15, 12:57:15, 15:57:15 HKT daily.
# Set to None to deploy without a schedule (safe for brand-new jobs).
JOB_SCHEDULE = jobs.CronSchedule(
    quartz_cron_expression="15 57 7,12,15 * * ?",
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
    "folder_name": "KTB",
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
    "run_phase1": "true",
    "run_phase2": "true",
    "run_phase3": "true",
    "run_phase8a": "true",           # Phase 8a: pull DTC FABRIC sheets
    "include_test_sheets": "false",  # false=PROD sheets only; true=include DEV+MILL (for UAT)
    "run_phase9a": "true",           # Phase 9a: pull LinePlan + build costing chart
    "lineplan_document": "KTB LinePlan",  # DTC document name for Phase 9a
    "push_blanks": "false",
    "img_http_timeout": "30",
    "img_max_uploads": "0",
    "fabric_document": "KTB FABRIC", # DTC document name for Phase 8a
}


def P(name: str) -> str:
    """Job-parameter reference."""
    return "{{job.parameters." + name + "}}"


# Convenience refs
CAT, SCH = P("catalog"), P("schema")
CUST, WS, DOC, ENV = P("customer"), P("dtc_workspace"), P("dtc_document"), P("dtc_environment")
FABRIC_DOC   = P("fabric_document")
LINEPLAN_DOC = P("lineplan_document")
DRY = P("dry_run")


def nb_task(task_key, notebook_path, params, depends=None, run_if=None, timeout=3600):
    return jobs.Task(
        task_key=task_key,
        notebook_task=jobs.NotebookTask(notebook_path=notebook_path, base_parameters=params),
        job_cluster_key=SHARED_CLUSTER_KEY,
        depends_on=depends or [],
        run_if=run_if,
        timeout_seconds=timeout,
    )


def gate_task(task_key, param_name, depends):
    """Condition task: proceed on the 'true' edge when {{job.parameters.<param>}} == 'true'."""
    return jobs.Task(
        task_key=task_key,
        condition_task=jobs.ConditionTask(
            op=jobs.ConditionTaskOp.EQUAL_TO, left=P(param_name), right="true"
        ),
        depends_on=depends,
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

    # Step 1 — BeProduct -> ktb_styles  (Phase 1+7: style sync + sample-app enrichment)
    tasks.append(nb_task("bp_style_sync", f"{NB_BP}/p1p7_beproduct_style_sync", {
        "folder_name": P("folder_name"), "refresh_mode": P("refresh_mode"),
        "catalog": CAT, "schema": SCH, "table_name": "ktb_styles",
    }, depends=[dep("wait_cluster")]))

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
    }, depends=[dep("wait_cluster")]))

    # Step 4 — request manager (Phase 1: create + share missing requests)
    tasks.append(nb_task("request_manager", f"{NB_BP}/p1_dtc_request_manager", {
        "catalog": CAT, "schema": SCH, "staging_table": "beproduct_to_dtc_staging",
        "dtc_environment": ENV, "customer": CUST, "dtc_workspace": WS, "dtc_document": DOC,
        "dry_run": DRY, "refresh_registry": "false",
    }, depends=[dep("transform"), dep("pull_master_dtc")]))

    # Phase 1+7 gate + push (Step 5)
    tasks.append(gate_task("gate_phase1", "run_phase1", depends=[dep("request_manager")]))
    tasks.append(nb_task("phase1_push", f"{NB_BP}/p1p7_beproduct_to_dtc_push", {
        "catalog": CAT, "schema": SCH, "staging_table": "beproduct_to_dtc_staging",
        "dtc_environment": ENV, "dtc_workspace": WS, "dry_run": DRY,
        "delta_only": P("delta_only"), "batch_size": "100",
    }, depends=[dep("gate_phase1", outcome="true")]))

    # Phase 2 gate + push (Step 6) — DTC-owned fields back to BeProduct
    tasks.append(gate_task("gate_phase2", "run_phase2", depends=[dep("transform"), dep("pull_master_dtc")]))
    tasks.append(nb_task("phase2_push", f"{NB_DTC}/p2_push_dtc_to_beproduct", {
        "catalog": CAT, "schema": SCH, "customer": CUST,
        "staging_table": "beproduct_to_dtc_staging",
        "dtc_environment": ENV, "dry_run": DRY, "push_blanks": P("push_blanks"),
    }, depends=[dep("gate_phase2", outcome="true")]))

    # Phase 3 gate (after Step 4) + targeted re-pull (Step 7) + images (Step 8)
    tasks.append(gate_task("gate_phase3", "run_phase3", depends=[dep("request_manager")]))
    # Step 7: run_if=ALL_DONE so a skipped/failed phase1_push doesn't block Phase 3.
    tasks.append(nb_task("repull_dtc", f"{NB_DTC}/p1_pull_masters_to_delta", {
        "dtc_environment": ENV, "customer": CUST, "dtc_workspace": WS, "dtc_document": DOC,
        "catalog": CAT, "schema": SCH, "write_mode": "overwrite", "refresh_registry": "false",
        "request_ids": "{{tasks.phase1_push.values.inserted_ids}}", "max_workers": "4",
    }, depends=[dep("gate_phase3", outcome="true"), dep("phase1_push")],
       run_if=jobs.RunIf.ALL_DONE))

    tasks.append(nb_task("phase3_images", f"{NB_BP}/p3_beproduct_to_dtc_images", {
        "catalog": CAT, "schema": SCH, "staging_table": "beproduct_to_dtc_staging",
        "dtc_environment": ENV, "dtc_workspace": WS, "dry_run": DRY,
        "http_timeout": P("img_http_timeout"), "max_uploads": P("img_max_uploads"),
    }, depends=[dep("repull_dtc")]))

    # ── Phase 8a — Pull DTC FABRIC sheets → dtc_fabric_<customer> ──────────────
    tasks.append(gate_task("gate_phase8a", "run_phase8a",
                           depends=[dep("wait_cluster")]))
    tasks.append(nb_task("pull_fabric_dtc", f"{NB_DTC}/p8a_pull_fabric_to_delta", {
        "dtc_environment":     ENV,
        "customer":            CUST,
        "dtc_workspace":       WS,
        "dtc_document":        FABRIC_DOC,
        "catalog":             CAT,
        "schema":              SCH,
        "write_mode":          "overwrite",
        "refresh_registry":    "true",
        "include_test_sheets": P("include_test_sheets"),
        "max_workers":         "4",
    }, depends=[dep("gate_phase8a", outcome="true")]))

    # ── Phase 9a — Pull LinePlan + Build Costing Chart ─────────────────────────
    tasks.append(gate_task("gate_phase9a", "run_phase9a",
                           depends=[dep("wait_cluster")]))
    tasks.append(nb_task("pull_lineplan_dtc", f"{NB_DTC}/p9a_pull_lineplan_to_delta", {
        "dtc_environment": ENV,
        "customer":        CUST,
        "dtc_workspace":   WS,
        "dtc_document":    LINEPLAN_DOC,
        "catalog":         CAT,
        "schema":          SCH,
        "write_mode":      "overwrite",
        "max_workers":     "4",
    }, depends=[dep("gate_phase9a", outcome="true")]))

    tasks.append(nb_task("build_costing_chart", f"{NB_DTC}/p9a_build_costing_chart", {
        "catalog":  CAT,
        "schema":   SCH,
        "customer": CUST,
    }, depends=[dep("pull_lineplan_dtc"), dep("pull_master_dtc")]))

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
