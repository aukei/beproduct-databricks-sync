#!/usr/bin/env python3
"""
Run the v2 serverless smoke check as a ONE-OFF Databricks run.

Validates, on real serverless compute, the two assumptions the v2 port rests on
(Workspace Files import via `module_path`, and scalar Python UDFs) plus the
other Spark Connect surfaces v2 uses. See dtc/notebooks/v2_smoke_check.py.

Runs as a small throwaway job tagged `userpurpose = lft-kontoor-dev` (see
scripts/_adhoc.py: `runs/submit` cannot carry tags), with no schedule; such
jobs are pruned after 7 days.

SAFETY: the smoke notebook writes nothing. No Delta write, no DTC or BeProduct
API call, no secret value printed. Safe to run while the v1 job is running.

Usage
-----
    # 1. Upload the v2 notebooks + modules to the v2 workspace root
    python scripts/upload_notebooks.py --root /Workspace/Repos/beproduct-sync-v2

    # 2. Run the smoke check against that root
    python scripts/run_v2_smoke.py

    # Options
    python scripts/run_v2_smoke.py --dry-run       # print what would be submitted
    python scripts/run_v2_smoke.py --root <path>   # a different workspace root
    python scripts/run_v2_smoke.py --timeout 900   # wait longer

Requires DATABRICKS_HOST + DATABRICKS_PAT (.env).
"""

import argparse
import os
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env")

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

from scripts._adhoc import ADHOC_TAGS, run_adhoc

DEFAULT_WS_ROOT = "/Workspace/Repos/beproduct-sync-v2"
RUN_NAME = "v2_serverless_smoke_check"


def main() -> int:
    ap = argparse.ArgumentParser(description="Run the v2 serverless smoke check (one-off, dev-tagged).")
    ap.add_argument("--root", default=DEFAULT_WS_ROOT,
                    help=f"Workspace root the v2 notebooks/modules were uploaded to (default: {DEFAULT_WS_ROOT})")
    ap.add_argument("--catalog", default="lft")
    ap.add_argument("--schema", default="beproduct")
    ap.add_argument("--bom-catalog", default="alb_tpm_uat")
    ap.add_argument("--dtc-environment", default="uat")
    ap.add_argument("--timeout", type=int, default=900, help="seconds to wait for completion (default 900)")
    ap.add_argument("--dry-run", action="store_true", help="print the submission and exit")
    args = ap.parse_args()

    root = args.root.rstrip("/")
    notebook = f"{root}/DTC/notebooks/v2_smoke_check"
    module_path = f"{root}/DTC/python"

    params = {
        "module_path": module_path,
        "catalog": args.catalog,
        "schema": args.schema,
        "bom_catalog": args.bom_catalog,
        "dtc_environment": args.dtc_environment,
    }

    print("=" * 78)
    print("v2 SERVERLESS SMOKE CHECK -- one-off run (dev-tagged throwaway job)")
    print("=" * 78)
    print(f"  notebook    : {notebook}")
    print(f"  module_path : {module_path}")
    print(f"  compute     : SERVERLESS (no job_cluster_key)")
    print(f"  parameters  : {params}")
    print("  writes      : NONE -- reads and in-memory computation only")

    if args.dry_run:
        print("\nDry run -- nothing submitted.")
        return 0

    host = (os.environ.get("DATABRICKS_HOST") or "").rstrip("/")
    if not (host and (os.environ.get("DATABRICKS_TOKEN") or os.environ.get("DATABRICKS_PAT"))):
        sys.exit("Set DATABRICKS_HOST and DATABRICKS_TOKEN/DATABRICKS_PAT (source .env).")
    if os.environ.get("DATABRICKS_PAT") and not os.environ.get("DATABRICKS_TOKEN"):
        os.environ["DATABRICKS_TOKEN"] = os.environ["DATABRICKS_PAT"]

    w = WorkspaceClient()

    # Omitting new_cluster / existing_cluster_id / job_cluster_key => serverless,
    # the same mechanism nb_task(serverless=True) uses in deploy_job.py.
    task = jobs.Task(
        task_key="v2_smoke_check",
        notebook_task=jobs.NotebookTask(notebook_path=notebook, base_parameters=params),
        timeout_seconds=args.timeout,
    )

    print(f"\nSubmitting (tags {ADHOC_TAGS}) …")
    job_id, run_id = run_adhoc(w, RUN_NAME, [task])
    print(f"  run_id: {run_id}")
    print(f"  {host}/jobs/{job_id}/runs/{run_id}")

    deadline = time.time() + args.timeout
    state = None
    while time.time() < deadline:
        run = w.jobs.get_run(run_id=run_id)
        state = run.state
        life = state.life_cycle_state.value if state and state.life_cycle_state else "?"
        if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        print(f"  … {life}")
        time.sleep(10)
    else:
        print(f"\n⏱  Timed out after {args.timeout}s — check the run URL above.")
        return 2

    result = state.result_state.value if state and state.result_state else "?"
    print(f"\n  life_cycle={state.life_cycle_state.value}  result={result}")
    if state.state_message:
        print(f"  message: {state.state_message}")

    # Per-task output: the notebook's exit value plus its stdout.
    run = w.jobs.get_run(run_id=run_id)
    for t in (run.tasks or []):
        try:
            out = w.jobs.get_run_output(run_id=t.run_id)
        except Exception as e:  # noqa: BLE001
            print(f"  (could not fetch output for {t.task_key}: {e})")
            continue
        if out.logs:
            print("\n──── notebook stdout " + "─" * 56)
            print(out.logs.rstrip())
            print("─" * 78)
        if out.notebook_output and out.notebook_output.result:
            print(f"\n  exit value: {out.notebook_output.result}")
        if out.error:
            print(f"\n  ERROR: {out.error}")
        if out.error_trace:
            print(out.error_trace)

    ok = result == "SUCCESS"
    print("\n" + ("✅ SMOKE PASSED — serverless assumptions confirmed, safe to start stage 2."
                  if ok else
                  "❌ SMOKE FAILED — do not start stage 2 until resolved."))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
