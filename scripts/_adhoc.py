"""
Launch a one-off / ad-hoc Databricks run that carries the DEV cost tag.

Why not `jobs.submit`: `runs/submit` has NO `tags` field (databricks-sdk 0.55
and the 2.2 API alike), and this workspace exposes no budget-policy API, so a
submitted serverless run cannot be attributed to a tag at all. Instead each
ad-hoc run gets its own small job, tagged `ADHOC_TAGS`, which is triggered once.

Those jobs are NOT deleted straight after the run: deleting a job also removes
its run history (the run URL stops working) and would race the billing
pipeline's attribution. Instead every launch prunes ad-hoc jobs older than
`keep_days` that have no active run, so the workspace never holds more than
about a week of them.

Production jobs carry `userpurpose = lft-kontoor-sync` (deploy_job.JOB_TAGS);
everything one-off carries `lft-kontoor-dev`, so the two never mix in
system.billing.
"""

import time
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

ADHOC_TAGS = {"userpurpose": "lft-kontoor-dev"}
ADHOC_PREFIX = "kontoor_adhoc_"
KEEP_DAYS = 7


def prune_adhoc_jobs(w: WorkspaceClient, keep_days: int = KEEP_DAYS) -> int:
    """Delete ad-hoc jobs older than keep_days with no active run. Never raises."""
    cutoff_ms = (time.time() - keep_days * 86400) * 1000
    deleted = 0
    try:
        for j in w.jobs.list():
            s = j.settings
            if not (s and (s.name or "").startswith(ADHOC_PREFIX)):
                continue
            if (s.tags or {}).get("userpurpose") != ADHOC_TAGS["userpurpose"]:
                continue  # only ever delete what this helper created
            if (j.created_time or 0) > cutoff_ms:
                continue
            if next(iter(w.jobs.list_runs(job_id=j.job_id, active_only=True)), None):
                continue
            w.jobs.delete(job_id=j.job_id)
            deleted += 1
    except Exception as e:  # noqa: BLE001 -- housekeeping must not block a run
        print(f"  (ad-hoc job prune skipped: {e})")
    return deleted


def run_adhoc(w: WorkspaceClient, run_name: str, tasks: list) -> tuple:
    """Create a dev-tagged job for `tasks`, trigger it once, return (job_id, run_id).

    `tasks` are `jobs.Task`. A task with no cluster spec runs on serverless.
    """
    pruned = prune_adhoc_jobs(w)
    if pruned:
        print(f"  pruned {pruned} ad-hoc job(s) older than {KEEP_DAYS} days")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    job_id = w.jobs.create(
        name=f"{ADHOC_PREFIX}{run_name}_{stamp}",
        tasks=tasks,
        tags=ADHOC_TAGS,
        max_concurrent_runs=1,
    ).job_id
    run_id = w.jobs.run_now(job_id=job_id).run_id
    return job_id, run_id
