#!/usr/bin/env python3
"""
Deploy the BeProduct <-> DTC sync as a TOP-LEVEL MULTI-TASK Databricks job.

This replaces the single-notebook orchestrator (`beproduct/orchestrate_sync.py`,
now retired) with one job whose 8 pipeline steps are first-class tasks. Benefits:

  * Each step has its own task run_id, duration, logs and retry/repair in the
    Jobs UI run graph — no more digging through hidden `dbutils.notebook.run`
    WORKFLOW_RUN children to read per-step timing.
  * Independent steps run in PARALLEL: the BeProduct chain (Step 1 -> 2) runs
    alongside the DTC pull (Step 3); they converge at Step 4.
  * The Step 5 -> Step 7 hand-off (which requests got INSERTs) uses native
    `dbutils.jobs.taskValues` instead of parsing an exit string.
  * Phase on/off toggles (run_phase1/2/3) are expressed as condition tasks.

DAG
---
    bp_style_sync ─► transform ─┐
                                ├─► request_manager ─► gate_phase1 ─► phase1_push ─┐
    pull_dtc ───────────────────┘         │                                        │
        │                                  └─► gate_phase3 ──────────────┐         │
        └─► gate_phase2 ─► phase2_push                                   ├─► repull_dtc ─► phase3_images
                                                                          (also depends phase1_push, run_if=ALL_DONE)

Cluster
-------
One SHARED single-node, NON-Photon job cluster (see CLUSTER below). This
workload is tiny-data + driver/IO-bound (≈145 styles, ≈420 rows); Photon and
extra workers add cost without speeding up the small LocalRelation unions, Delta
commits and HTTP calls. A shared cluster also means ONE cold start for the whole
run instead of one per task. Flip RUNTIME_ENGINE / NUM_WORKERS below to revert.

Usage
-----
    python scripts/deploy_job.py --dry-run        # print the task graph + settings
    python scripts/deploy_job.py                  # CREATE a new (unscheduled) job
    python scripts/deploy_job.py --reset-existing 22324120218492
                                                  # overwrite an existing job in place

After creating, validate with a manual run, then move the cron schedule from the
old job and pause the old one. Requires DATABRICKS_HOST + DATABRICKS_PAT (.env).
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

# Single-node, no-Photon shared cluster (see module docstring). To revert to the
# old shape: set NUM_WORKERS=2, RUNTIME_ENGINE=PHOTON and drop SINGLE_NODE_CONF.
SPARK_VERSION = "17.3.x-scala2.13"
NODE_TYPE = "Standard_D4s_v3"
NUM_WORKERS = 0
RUNTIME_ENGINE = compute.RuntimeEngine.STANDARD  # STANDARD = no Photon
SINGLE_NODE_CONF = {
    "spark.databricks.cluster.profile": "singleNode",
    "spark.master": "local[*]",
}

# Job-level parameters (mirror the old orchestrate_sync widgets). Every task
# references these via {{job.parameters.<name>}}.
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

    # Step 1 — BeProduct -> ktb_styles
    tasks.append(nb_task("bp_style_sync", f"{NB_BP}/beproduct_style_sync", {
        "folder_name": P("folder_name"), "refresh_mode": P("refresh_mode"),
        "catalog": CAT, "schema": SCH, "table_name": "ktb_styles",
    }))

    # Step 2 — transform (depends on Step 1)
    tasks.append(nb_task("transform", f"{NB_BP}/beproduct_to_dtc_transform", {
        "catalog": CAT, "schema": SCH, "source_table": "ktb_styles",
        "staging_table": "beproduct_to_dtc_staging",
        "folder_name": P("folder_name"), "customer_code": CUST,
    }, depends=[dep("bp_style_sync")]))

    # Step 3 — pull DTC + refresh registry (INDEPENDENT → parallel with 1/2)
    tasks.append(nb_task("pull_dtc", f"{NB_DTC}/pull_requests_to_delta", {
        "dtc_environment": ENV, "customer": CUST, "dtc_workspace": WS, "dtc_document": DOC,
        "catalog": CAT, "schema": SCH, "write_mode": "overwrite",
        "refresh_registry": "true", "max_workers": "4",
    }))

    # Step 4 — request manager (needs Step 2 staging AND Step 3 registry)
    tasks.append(nb_task("request_manager", f"{NB_BP}/dtc_request_manager", {
        "catalog": CAT, "schema": SCH, "staging_table": "beproduct_to_dtc_staging",
        "dtc_environment": ENV, "customer": CUST, "dtc_workspace": WS, "dtc_document": DOC,
        "dry_run": DRY, "refresh_registry": "false",
    }, depends=[dep("transform"), dep("pull_dtc")]))

    # Phase 1 gate + push (Step 5)
    tasks.append(gate_task("gate_phase1", "run_phase1", depends=[dep("request_manager")]))
    tasks.append(nb_task("phase1_push", f"{NB_BP}/beproduct_to_dtc_push", {
        "catalog": CAT, "schema": SCH, "staging_table": "beproduct_to_dtc_staging",
        "dtc_environment": ENV, "dtc_workspace": WS, "dry_run": DRY,
        "delta_only": P("delta_only"), "batch_size": "100",
    }, depends=[dep("gate_phase1", outcome="true")]))

    # Phase 2 gate + push (Step 6) — needs Step 2 + Step 3 only (disjoint fields)
    tasks.append(gate_task("gate_phase2", "run_phase2", depends=[dep("transform"), dep("pull_dtc")]))
    tasks.append(nb_task("phase2_push", f"{NB_DTC}/05_push_dtc_to_beproduct", {
        "catalog": CAT, "schema": SCH, "customer": CUST,
        "staging_table": "beproduct_to_dtc_staging",
        "dtc_environment": ENV, "dry_run": DRY, "push_blanks": P("push_blanks"),
    }, depends=[dep("gate_phase2", outcome="true")]))

    # Phase 3 gate (after Step 4) + targeted re-pull (Step 7) + images (Step 8)
    tasks.append(gate_task("gate_phase3", "run_phase3", depends=[dep("request_manager")]))
    # Step 7 reads inserted_ids from Step 5's task value; if phase1_push was
    # skipped (run_phase1=false) the ref is empty → full re-pull. run_if=ALL_DONE
    # so a skipped/failed phase1_push doesn't block the re-pull.
    tasks.append(nb_task("repull_dtc", f"{NB_DTC}/pull_requests_to_delta", {
        "dtc_environment": ENV, "customer": CUST, "dtc_workspace": WS, "dtc_document": DOC,
        "catalog": CAT, "schema": SCH, "write_mode": "overwrite", "refresh_registry": "false",
        "request_ids": "{{tasks.phase1_push.values.inserted_ids}}", "max_workers": "4",
    }, depends=[dep("gate_phase3", outcome="true"), dep("phase1_push")],
       run_if=jobs.RunIf.ALL_DONE))

    tasks.append(nb_task("phase3_images", f"{NB_BP}/beproduct_to_dtc_images", {
        "catalog": CAT, "schema": SCH, "staging_table": "beproduct_to_dtc_staging",
        "dtc_environment": ENV, "dtc_workspace": WS, "dry_run": DRY,
        "http_timeout": P("img_http_timeout"), "max_uploads": P("img_max_uploads"),
    }, depends=[dep("repull_dtc")]))

    return tasks


def build_settings() -> jobs.JobSettings:
    cluster = compute.ClusterSpec(
        spark_version=SPARK_VERSION,
        node_type_id=NODE_TYPE,
        num_workers=NUM_WORKERS,
        runtime_engine=RUNTIME_ENGINE,
        data_security_mode=compute.DataSecurityMode.SINGLE_USER,
        spark_conf=SINGLE_NODE_CONF if NUM_WORKERS == 0 else None,
        custom_tags={"ResourceClass": "SingleNode"} if NUM_WORKERS == 0 else None,
    )
    return jobs.JobSettings(
        name=JOB_NAME,
        tasks=build_tasks(),
        job_clusters=[jobs.JobCluster(job_cluster_key=SHARED_CLUSTER_KEY, new_cluster=cluster)],
        parameters=[jobs.JobParameterDefinition(name=k, default=v) for k, v in JOB_PARAMS.items()],
        max_concurrent_runs=1,
        # No schedule on purpose: validate manually, then migrate the cron from the
        # old job and pause the old one.
    )


def _preview(settings: jobs.JobSettings):
    print(f"Job name: {settings.name}")
    print(f"Shared cluster: {NODE_TYPE} workers={NUM_WORKERS} "
          f"engine={RUNTIME_ENGINE.value} (single_node={NUM_WORKERS == 0})")
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
    args = ap.parse_args()

    settings = build_settings()
    _preview(settings)

    if args.dry_run:
        print("\n📋 Dry run — nothing applied.")
        return

    if not (os.environ.get("DATABRICKS_HOST") and
            (os.environ.get("DATABRICKS_TOKEN") or os.environ.get("DATABRICKS_PAT"))):
        sys.exit("❌ Set DATABRICKS_HOST and DATABRICKS_TOKEN/DATABRICKS_PAT (source .env).")
    if os.environ.get("DATABRICKS_PAT") and not os.environ.get("DATABRICKS_TOKEN"):
        os.environ["DATABRICKS_TOKEN"] = os.environ["DATABRICKS_PAT"]

    w = WorkspaceClient()
    if args.reset_existing:
        w.jobs.reset(job_id=args.reset_existing, new_settings=settings)
        job_id = args.reset_existing
        print(f"\n✅ Reset existing job {job_id} to the multi-task DAG.")
    else:
        created = w.jobs.create(
            name=settings.name,
            tasks=settings.tasks,
            job_clusters=settings.job_clusters,
            parameters=settings.parameters,
            max_concurrent_runs=settings.max_concurrent_runs,
        )
        job_id = created.job_id
        print(f"\n✅ Created job {job_id} ({JOB_NAME}).")
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    print(f"   {host}/jobs/{job_id}")
    print("   Validate with a manual run, then migrate the schedule off the old job.")


if __name__ == "__main__":
    main()
