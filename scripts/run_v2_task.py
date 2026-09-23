#!/usr/bin/env python3
"""
Run ONE v2 notebook as a one-off serverless Databricks run.

Creates nothing persistent -- no job, no schedule, nothing to clean up. Used to
validate each v2 task against real data before the `BeProduct_DTC_sync_v2` job
is ever scheduled.

The Jobs API returns NO notebook stdout for serverless runs (live-confirmed
2026-09-14) -- only the `dbutils.notebook.exit` value. Every v2 notebook
therefore exits a JSON summary, and this script pretty-prints it.

Usage
-----
    # BOM -> Delta (writes bom_segments; touches no DTC)
    python scripts/run_v2_task.py v2_pull_bom_segments

    # The write window, in DRY RUN (computes + logs the plan, issues no PATCH)
    python scripts/run_v2_task.py v2_wip_push dry_run=true

    # Any extra widget as key=value
    python scripts/run_v2_task.py v2_wip_push dry_run=true delta_only=false

Safety
------
`dry_run` is NOT defaulted here -- each notebook's own widget default applies,
and every v2 notebook defaults it to "true". Pass `dry_run=false` explicitly to
write to live DTC, and only against a request you are willing to touch (the
sacrificial one is `KTB FW26 Wrangler`, UAT `6ab113b708ef2276cf34c0d2`).

Requires DATABRICKS_HOST + DATABRICKS_PAT (.env).
"""

import argparse
import json
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

DEFAULT_WS_ROOT = "/Workspace/Repos/beproduct-sync-v2"

# Where each v2 notebook lives under the workspace root.
NOTEBOOK_DIRS = {
    "v2_pull_bom_segments": "DTC/notebooks",
    "v2_wip_push": "DTC/notebooks",
    "p9a_build_costing_chart": "DTC/notebooks",
    "v2_smoke_check": "DTC/notebooks",
    "v2_probe_write": "DTC/notebooks",
    "v2_probe_material_code": "DTC/notebooks",
    "v2_set_dtc_cell": "DTC/notebooks",
    "v2_push_customer_code": "DTC/notebooks",
    "v2_inspect_requests": "DTC/notebooks",
    "p1p7_beproduct_to_dtc_transform": "beproduct",
    "p1_dtc_request_manager": "beproduct",
    "p1_pull_masters_to_delta": "DTC/notebooks",
    "p9a_pull_lineplan_to_delta": "DTC/notebooks",
}


def main() -> int:
    ap = argparse.ArgumentParser(description="Run one v2 notebook as a one-off serverless run.")
    ap.add_argument("notebook", help=f"notebook name, one of: {', '.join(sorted(NOTEBOOK_DIRS))}")
    ap.add_argument("params", nargs="*", help="extra widget values as key=value")
    ap.add_argument("--root", default=DEFAULT_WS_ROOT)
    ap.add_argument("--timeout", type=int, default=1800)
    ap.add_argument("--dry-run", action="store_true", help="print the submission and exit")
    ap.add_argument("--out", help="write the full exit JSON here (default: /tmp/v2_<nb>_<run>.json)")
    ap.add_argument("--print-chars", type=int, default=12000, help="console truncation limit")
    args = ap.parse_args()

    if args.notebook not in NOTEBOOK_DIRS:
        sys.exit(f"Unknown notebook {args.notebook!r}. Known: {', '.join(sorted(NOTEBOOK_DIRS))}")

    root = args.root.rstrip("/")
    path = f"{root}/{NOTEBOOK_DIRS[args.notebook]}/{args.notebook}"

    params = {"module_path": f"{root}/DTC/python"}
    for kv in args.params:
        if "=" not in kv:
            sys.exit(f"Bad parameter {kv!r}; expected key=value")
        k, v = kv.split("=", 1)
        params[k] = v

    writes_live = params.get("dry_run", "").strip().lower() == "false"

    print("=" * 78)
    print(f"v2 one-off serverless run -- {args.notebook}")
    print("=" * 78)
    print(f"  notebook : {path}")
    print(f"  params   : {params}")
    if writes_live:
        print("  ⚠  dry_run=false -- this run WILL write to live DTC")
    if args.dry_run:
        print("\nDry run -- nothing submitted.")
        return 0

    host = (os.environ.get("DATABRICKS_HOST") or "").rstrip("/")
    if not (host and (os.environ.get("DATABRICKS_TOKEN") or os.environ.get("DATABRICKS_PAT"))):
        sys.exit("Set DATABRICKS_HOST and DATABRICKS_TOKEN/DATABRICKS_PAT (source .env).")
    if os.environ.get("DATABRICKS_PAT") and not os.environ.get("DATABRICKS_TOKEN"):
        os.environ["DATABRICKS_TOKEN"] = os.environ["DATABRICKS_PAT"]

    w = WorkspaceClient()
    # No new_cluster / existing_cluster_id / job_cluster_key => serverless.
    task = jobs.SubmitTask(
        task_key=args.notebook,
        notebook_task=jobs.NotebookTask(notebook_path=path, base_parameters=params),
        timeout_seconds=args.timeout,
    )

    print("\nSubmitting …")
    run_id = w.jobs.submit(run_name=f"v2_oneoff_{args.notebook}", tasks=[task]).run_id
    print(f"  run_id: {run_id}\n  {host}/jobs/runs/{run_id}")

    deadline = time.time() + args.timeout
    state = None
    while time.time() < deadline:
        state = w.jobs.get_run(run_id=run_id).state
        life = state.life_cycle_state.value if state and state.life_cycle_state else "?"
        if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        print(f"  … {life}")
        time.sleep(10)
    else:
        print(f"\n⏱  Timed out after {args.timeout}s — see the run URL above.")
        return 2

    result = state.result_state.value if state and state.result_state else "?"
    print(f"\n  result={result}")
    if state.state_message:
        print(f"  message: {state.state_message}")

    run = w.jobs.get_run(run_id=run_id)
    for t in (run.tasks or []):
        try:
            out = w.jobs.get_run_output(run_id=t.run_id)
        except Exception as e:  # noqa: BLE001
            print(f"  (no output for {t.task_key}: {e})")
            continue
        print(f"  setup={(t.setup_duration or 0)/1000:.1f}s "
              f"exec={(t.execution_duration or 0)/1000:.1f}s")
        if out.notebook_output and out.notebook_output.result:
            raw = out.notebook_output.result
            # Always persist the FULL exit value: it is the only machine-
            # readable output a serverless run produces, and the console print
            # is truncated. Reviewing a full plan (60+ changes) needs the file.
            out_path = Path(args.out) if args.out else (
                Path("/tmp") / f"v2_{args.notebook}_{run_id}.json")
            try:
                parsed = json.loads(raw)
                out_path.write_text(json.dumps(parsed, indent=2))
                print("\n── exit value ──────────────────────────────────────────────")
                print(json.dumps(parsed, indent=2)[:args.print_chars])
                if len(json.dumps(parsed, indent=2)) > args.print_chars:
                    print(f"   … truncated for display")
            except Exception:  # noqa: BLE001
                out_path.write_text(raw)
                print(raw[:args.print_chars])
            print(f"\n  full exit value saved to: {out_path}")
        if out.error:
            print(f"\n  ERROR: {out.error}")
        if out.error_trace:
            print(out.error_trace[:4000])

    return 0 if result == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
