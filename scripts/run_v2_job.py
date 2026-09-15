#!/usr/bin/env python3
"""
Trigger the v2 job end-to-end and report every task's outcome.

Serverless runs return NO notebook stdout via the API -- only each notebook's
`dbutils.notebook.exit` value. Every v2 notebook therefore exits a JSON
summary, and this script collects them all into one report plus a saved file.

Usage
-----
    python scripts/run_v2_job.py --job-id 367710575109755 dry_run=true
    python scripts/run_v2_job.py --job-id 367710575109755 dry_run=false   # WRITES to DTC

Any key=value argument overrides a job parameter for this run only; the job's
stored defaults are untouched.

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


def main() -> int:
    ap = argparse.ArgumentParser(description="Run the v2 job end-to-end and report.")
    ap.add_argument("--job-id", type=int, required=True)
    ap.add_argument("params", nargs="*", help="job-parameter overrides as key=value")
    ap.add_argument("--timeout", type=int, default=5400)
    ap.add_argument("--out", help="write the full report JSON here")
    args = ap.parse_args()

    overrides = {}
    for kv in args.params:
        if "=" not in kv:
            sys.exit(f"Bad parameter {kv!r}; expected key=value")
        k, v = kv.split("=", 1)
        overrides[k] = v

    host = (os.environ.get("DATABRICKS_HOST") or "").rstrip("/")
    if not (host and (os.environ.get("DATABRICKS_TOKEN") or os.environ.get("DATABRICKS_PAT"))):
        sys.exit("Set DATABRICKS_HOST and DATABRICKS_TOKEN/DATABRICKS_PAT (source .env).")
    if os.environ.get("DATABRICKS_PAT") and not os.environ.get("DATABRICKS_TOKEN"):
        os.environ["DATABRICKS_TOKEN"] = os.environ["DATABRICKS_PAT"]

    w = WorkspaceClient()
    job = w.jobs.get(job_id=args.job_id)
    print("=" * 78)
    print(f"END-TO-END RUN -- {job.settings.name}  (job {args.job_id})")
    print("=" * 78)
    print(f"  parameter overrides: {overrides or '(none -- job defaults)'}")
    if overrides.get("dry_run", "").lower() == "false":
        print("  ⚠  dry_run=false -- this run WILL write to live DTC")

    run_id = w.jobs.run_now(job_id=args.job_id,
                            job_parameters=overrides or None).run_id
    print(f"\n  run_id: {run_id}\n  {host}/jobs/{args.job_id}/runs/{run_id}\n")

    deadline = time.time() + args.timeout
    seen = {}
    while time.time() < deadline:
        run = w.jobs.get_run(run_id=run_id)
        for t in (run.tasks or []):
            st = t.state.life_cycle_state.value if t.state and t.state.life_cycle_state else "?"
            res = t.state.result_state.value if t.state and t.state.result_state else ""
            key = (t.task_key, st, res)
            if key not in seen:
                seen[key] = True
                if st in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                    icon = "✅" if res == "SUCCESS" else "❌"
                    dur = (t.execution_duration or 0) / 1000
                    print(f"  {icon} {t.task_key:20} {res:9} {dur:6.0f}s")
                elif st == "RUNNING":
                    print(f"  ▶  {t.task_key:20} running …")
        life = run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else "?"
        if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        time.sleep(15)
    else:
        print(f"\n⏱  Timed out after {args.timeout}s -- see the run URL above.")
        return 2

    run = w.jobs.get_run(run_id=run_id)
    result = run.state.result_state.value if run.state and run.state.result_state else "?"
    total = ((run.end_time or 0) - (run.start_time or 0)) / 1000

    report = {"job_id": args.job_id, "run_id": run_id, "result": result,
              "wall_seconds": round(total, 1), "overrides": overrides, "tasks": {}}

    print("\n" + "=" * 78)
    print(f"RESULT: {result}   wall {total:.0f}s")
    print("=" * 78)
    for t in (run.tasks or []):
        res = t.state.result_state.value if t.state and t.state.result_state else "?"
        entry = {"result": res,
                 "setup_s": round((t.setup_duration or 0) / 1000, 1),
                 "exec_s": round((t.execution_duration or 0) / 1000, 1)}
        try:
            out = w.jobs.get_run_output(run_id=t.run_id)
            if out.notebook_output and out.notebook_output.result:
                raw = out.notebook_output.result
                try:
                    entry["exit"] = json.loads(raw)
                except Exception:  # noqa: BLE001
                    entry["exit"] = raw[:2000]
            if out.error:
                entry["error"] = out.error[:1500]
        except Exception as e:  # noqa: BLE001
            entry["output_error"] = str(e)[:300]
        report["tasks"][t.task_key] = entry

        icon = "✅" if res == "SUCCESS" else "❌"
        print(f"\n{icon} {t.task_key}  ({entry['setup_s']}s setup / {entry['exec_s']}s exec)")
        ex = entry.get("exit")
        if isinstance(ex, dict):
            compact = {k: v for k, v in ex.items()
                       if k not in ("per_request", "requests", "checks")}
            print("   " + json.dumps(compact)[:1400])
        elif ex:
            print(f"   exit: {str(ex)[:400]}")
        if entry.get("error"):
            print(f"   ERROR: {entry['error'][:600]}")

    out_path = Path(args.out) if args.out else Path("/tmp") / f"v2_job_run_{run_id}.json"
    out_path.write_text(json.dumps(report, indent=2))
    print(f"\n  full report saved to: {out_path}")
    return 0 if result == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
