# Databricks notebook source
"""
Daily Sync Orchestrator: BeProduct <-> DTC (Phase 1 + Phase 2)
==============================================================

⚠️  RETIRED (2026-06-19): superseded by the top-level MULTI-TASK job
    `BeProduct_DTC_sync_dag` (job 294837488757511), defined in
    `scripts/deploy_job.py`. That job runs the same 8 steps as first-class tasks
    on one shared cluster, with native per-task logs/timing, parallel Step 1-2 vs
    Step 3, condition-task phase gates, and a `dbutils.jobs.taskValues` hand-off
    for the Step 5 -> Step 7 inserted_ids. This single-notebook orchestrator is
    kept only as a manual fallback; prefer the job. See docs/PERFORMANCE.md.

Runs the full bi-directional sync pipeline in the correct order:

  Step 1  p1p7_beproduct_style_sync           BeProduct API -> lft.beproduct.ktb_styles
  Step 2  p1p7_beproduct_to_dtc_transform     ktb_styles -> lft.beproduct.beproduct_to_dtc_staging
  Step 3  p1_pull_masters_to_delta         DTC API -> lft.beproduct.dtc_wip_<customer>
                                         (also refreshes dtc_request_registry)
  Step 4  p1_dtc_request_manager            Resolve / create DTC requests
                                         -> lft.beproduct.dtc_request_mapping
  Step 5  p1p7_beproduct_to_dtc_push          Phase 1: BeProduct -> DTC (upsert + orphan marks)
  Step 6  p2_push_dtc_to_beproduct       Phase 2: DTC -> BeProduct (pushback)
  Step 7  p1_pull_masters_to_delta         Refresh dtc_wip_<customer> AFTER Phase 1
                                         so it reflects rows Phase 1 just inserted
  Step 8  p3_beproduct_to_dtc_images        Phase 3: BeProduct front image -> DTC
                                         "Style Image" cell (blank cells only)

Steps 5 and 6 write to disjoint field sets and are individually guarded by
run_phase1 / run_phase2 so you can trial one without activating the other.
Step 8 (images) is guarded by run_phase3 and is binary-only: it never rides the
Phase 1 PATCH, so it runs as its own step after the rows exist.

Steps 1+2 are a chain (BeProduct pull -> transform). Step 3 is independent
(DTC pull). Step 4 needs both step 2 (staging rows) and step 3 (refreshed
registry). Step 5 needs step 4. Step 6 needs step 2 (identity map) and
step 3 (dtc_wip table). Step 7 re-pulls DTC so dtc_wip reflects Phase 1 inserts.
Step 8 needs step 4 (resolved mapping) + step 2 (front_image_url in staging);
it re-reads each sheet live for the freshest rowIndex / image state.

Parameters
----------
  catalog / schema            Target Unity Catalog location  (default: lft / beproduct)
  folder_name                 BeProduct folder name          (default: KTB)
  customer                    DTC/BeProduct customer token   (default: KTB)
  dtc_workspace               DTC workspace name             (default: KTB)
  dtc_document                DTC document name              (default: KTB WIP)
  dtc_environment             uat | prod                     (default: uat)
  refresh_mode                FULL | INCREMENTAL for step 1  (default: FULL)
  dry_run                     true = compute+log, no writes  (default: true)
                              Applied to steps 4, 5, 6.
  delta_only                  Only push BP rows modified since last push (default: true)
  run_phase1                  Run step 5 BeProduct->DTC push (default: true)
  run_phase2                  Run step 6 DTC->BeProduct push (default: true)
  run_phase3                  Run steps 7-8 image sync (default: true)
  push_blanks                 Blank DTC values clear BeProduct fields (default: false)
  img_http_timeout            Phase 3 CDN download timeout in seconds (default: 30)
  img_max_uploads             Phase 3 per-run upload cap, 0 = no cap (default: 0)
  notebook_base_beproduct     Databricks workspace path for beproduct/ notebooks
  notebook_base_dtc           Databricks workspace path for DTC/notebooks/ notebooks
"""

# COMMAND ----------

import json
import re
from datetime import datetime, timezone

# ── Widgets ───────────────────────────────────────────────────────────────────
try:
    dbutils.widgets.text("catalog",         "lft",       "Catalog")
    dbutils.widgets.text("schema",          "beproduct", "Schema")
    dbutils.widgets.text("folder_name",     "KTB",       "BeProduct Folder")
    dbutils.widgets.text("customer",        "KTB",       "Customer token")
    dbutils.widgets.text("dtc_workspace",   "KTB",       "DTC Workspace")
    dbutils.widgets.text("dtc_document",    "KTB WIP",   "DTC Document")
    dbutils.widgets.text("dtc_environment", "uat",       "DTC Environment (uat|prod)")
    dbutils.widgets.text("refresh_mode",    "INCREMENTAL", "BP pull mode (FULL|INCREMENTAL)")
    dbutils.widgets.text("dry_run",         "false",     "Dry run (true/false)")
    dbutils.widgets.text("delta_only",      "true",      "Delta push only (true/false)")
    dbutils.widgets.text("run_phase1",      "true",      "Run Phase 1: BP->DTC (true/false)")
    dbutils.widgets.text("run_phase2",      "true",      "Run Phase 2: DTC->BP (true/false)")
    dbutils.widgets.text("run_phase3",      "true",      "Run Phase 3: images (true/false)")
    dbutils.widgets.text("push_blanks",     "false",     "Blanks clear BeProduct (true/false)")
    dbutils.widgets.text("img_http_timeout","30",        "Phase 3 CDN download timeout (s)")
    dbutils.widgets.text("img_max_uploads", "0",         "Phase 3 upload cap (0 = no cap)")
    dbutils.widgets.text(
        "notebook_base_beproduct",
        "/Workspace/Repos/beproduct-sync/beproduct",
        "Workspace path: beproduct/ notebooks",
    )
    dbutils.widgets.text(
        "notebook_base_dtc",
        "/Workspace/Repos/beproduct-sync/DTC/notebooks",
        "Workspace path: DTC/notebooks/ notebooks",
    )
except Exception:
    pass  # widgets already exist on re-run

catalog         = dbutils.widgets.get("catalog").strip()
schema          = dbutils.widgets.get("schema").strip()
folder_name     = dbutils.widgets.get("folder_name").strip()
customer        = dbutils.widgets.get("customer").strip()
dtc_workspace   = dbutils.widgets.get("dtc_workspace").strip()
dtc_document    = dbutils.widgets.get("dtc_document").strip()
dtc_environment = dbutils.widgets.get("dtc_environment").strip().lower()
refresh_mode    = dbutils.widgets.get("refresh_mode").strip().upper()
dry_run         = dbutils.widgets.get("dry_run").strip()
delta_only      = dbutils.widgets.get("delta_only").strip()
run_phase1      = dbutils.widgets.get("run_phase1").strip().lower() in ("true", "1", "yes")
run_phase2      = dbutils.widgets.get("run_phase2").strip().lower() in ("true", "1", "yes")
run_phase3      = dbutils.widgets.get("run_phase3").strip().lower() in ("true", "1", "yes")
push_blanks     = dbutils.widgets.get("push_blanks").strip()
img_http_timeout = dbutils.widgets.get("img_http_timeout").strip()
img_max_uploads  = dbutils.widgets.get("img_max_uploads").strip()
nb_bp           = dbutils.widgets.get("notebook_base_beproduct").strip().rstrip("/")
nb_dtc          = dbutils.widgets.get("notebook_base_dtc").strip().rstrip("/")

started_at = datetime.now(timezone.utc)


def _parse_inserted_ids(exit_str: str) -> str:
    """
    Extract inserted_ids from the p1p7_beproduct_to_dtc_push exit string.

    Format emitted by Step 5: "ok inserts=N inserted_ids=id1,id2,..."
    Returns the raw comma-separated id string (empty string if none / unparseable),
    ready to pass straight to p1_pull_masters_to_delta's request_ids widget.
    """
    if not exit_str:
        return ""
    m = re.search(r"inserted_ids=([^\s]*)", exit_str)
    if not m:
        return ""
    return m.group(1).strip()

print("=" * 80)
print("BEPRODUCT <-> DTC DAILY SYNC ORCHESTRATOR")
print("=" * 80)
print(f"  catalog.schema:    {catalog}.{schema}")
print(f"  folder_name:       {folder_name}")
print(f"  customer:          {customer}")
print(f"  dtc:               {dtc_environment} | {dtc_workspace} / {dtc_document}")
print(f"  refresh_mode:      {refresh_mode}")
print(f"  dry_run:           {dry_run}")
print(f"  delta_only:        {delta_only}")
print(f"  run_phase1:        {run_phase1}")
print(f"  run_phase2:        {run_phase2}")
print(f"  run_phase3:        {run_phase3}")
print(f"  push_blanks:       {push_blanks}")
print(f"  notebook_base_bp:  {nb_bp}")
print(f"  notebook_base_dtc: {nb_dtc}")
print(f"  started_at:        {started_at.isoformat()}")

# COMMAND ----------

# ── Step execution helper ──────────────────────────────────────────────────────
# Each step result is collected in `_steps` for the final summary.
_steps = []  # list of dicts: step, name, status, result, elapsed_s

_STATUS_ICON = {"ok": "✅", "error": "❌", "skipped": "⏭️ "}

def _run_step(
    step_num: int,
    name: str,
    notebook_path: str,
    params: dict,
    timeout_seconds: int = 3600,
    skip: bool = False,
    skip_reason: str = "",
) -> str | None:
    """
    Call a child notebook via dbutils.notebook.run().

    Returns the notebook's exit string on success, None on failure/skip.
    Appends a status dict to _steps regardless of outcome.
    """
    if skip:
        _steps.append({
            "step": step_num, "name": name,
            "status": "skipped", "result": skip_reason, "elapsed_s": 0.0,
        })
        print(f"\n  {_STATUS_ICON['skipped']} STEP {step_num}: {name} — SKIPPED ({skip_reason})")
        return None

    print(f"\n{'─' * 80}")
    print(f"  STEP {step_num}: {name}")
    print(f"  notebook:  {notebook_path}")
    print(f"  params:    {json.dumps(params)}")
    print(f"{'─' * 80}")

    t0 = datetime.now(timezone.utc)
    try:
        result = dbutils.notebook.run(notebook_path, timeout_seconds, params)
        elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
        print(f"\n  {_STATUS_ICON['ok']} STEP {step_num} done in {elapsed:.0f}s — exit: {result!r}")
        _steps.append({
            "step": step_num, "name": name,
            "status": "ok", "result": result or "", "elapsed_s": elapsed,
        })
        return result
    except Exception as exc:
        elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
        print(f"\n  {_STATUS_ICON['error']} STEP {step_num} FAILED in {elapsed:.0f}s: {exc}")
        _steps.append({
            "step": step_num, "name": name,
            "status": "error", "result": str(exc)[:300], "elapsed_s": elapsed,
        })
        return None

# COMMAND ----------

# ── STEP 1: BeProduct API -> Delta (ktb_styles) ───────────────────────────────
# Timeout 3600 s: a FULL refresh fetches all styles and can be slow.
_r1 = _run_step(
    1,
    "BeProduct Style Sync  (BP API -> ktb_styles)",
    f"{nb_bp}/p1p7_beproduct_style_sync",
    {
        "folder_name":  folder_name,
        "refresh_mode": refresh_mode,
        "catalog":      catalog,
        "schema":       schema,
        "table_name":   "ktb_styles",
    },
    timeout_seconds=3600,
)
_step1_ok = _steps[-1]["status"] == "ok"

# COMMAND ----------

# ── STEP 2: Transform / denormalize -> beproduct_to_dtc_staging ──────────────
# Depends on step 1 (ktb_styles must be fresh).
_r2 = _run_step(
    2,
    "Transform / Denormalize  (ktb_styles -> staging)",
    f"{nb_bp}/p1p7_beproduct_to_dtc_transform",
    {
        "catalog":       catalog,
        "schema":        schema,
        "source_table":  "ktb_styles",
        "staging_table": "beproduct_to_dtc_staging",
        "folder_name":   folder_name,
        "customer_code": customer,
    },
    timeout_seconds=1800,
    skip=(not _step1_ok),
    skip_reason="step 1 (p1p7_beproduct_style_sync) failed",
)
_step2_ok = _steps[-1]["status"] == "ok"

# COMMAND ----------

# ── STEP 3: Pull DTC requests -> Delta (+ refreshes registry) ─────────────────
# Independent of steps 1-2 (reads DTC, not BeProduct).
# refresh_registry=true means it also runs sync.registry.refresh before pulling,
# so the registry is current before step 4 resolves against it.
_r3 = _run_step(
    3,
    "Pull DTC Requests -> Delta  (DTC API -> dtc_wip + registry)",
    f"{nb_dtc}/p1_pull_masters_to_delta",
    {
        "dtc_environment":  dtc_environment,
        "customer":         customer,
        "dtc_workspace":    dtc_workspace,
        "dtc_document":     dtc_document,
        "catalog":          catalog,
        "schema":           schema,
        "write_mode":       "overwrite",
        "refresh_registry": "true",
        "max_workers":      "4",
    },
    timeout_seconds=1800,
)
_step3_ok   = _steps[-1]["status"] == "ok"
# NO_IN_SCOPE_REQUESTS is a clean but data-empty exit; downstream push steps
# need real data rows so we treat it as "no data" (not a hard error).
_step3_data = _step3_ok and (_steps[-1]["result"] != "NO_IN_SCOPE_REQUESTS")

# COMMAND ----------

# ── STEP 4: Resolve / create DTC requests ────────────────────────────────────
# Needs step 2 (staging rows to resolve) AND step 3 registry refresh.
# refresh_registry=false: step 3 already refreshed it; skip the extra API scan.
_r4 = _run_step(
    4,
    "DTC Request Manager  (resolve / create requests)",
    f"{nb_bp}/p1_dtc_request_manager",
    {
        "catalog":          catalog,
        "schema":           schema,
        "staging_table":    "beproduct_to_dtc_staging",
        "dtc_environment":  dtc_environment,
        "customer":         customer,
        "dtc_workspace":    dtc_workspace,
        "dtc_document":     dtc_document,
        "dry_run":          dry_run,
        "refresh_registry": "false",
    },
    timeout_seconds=1800,
    skip=(not _step2_ok or not _step3_data),
    skip_reason=(
        "step 2 (transform) failed"
        if not _step2_ok
        else "step 3 returned no in-scope DTC requests"
    ),
)
_step4_ok = _steps[-1]["status"] == "ok"

# COMMAND ----------

# ── STEP 5: Phase 1 — BeProduct -> DTC push ───────────────────────────────────
# Needs step 4 (resolved dtc_request_mapping).
# Gated by run_phase1 widget.
_r5 = _run_step(
    5,
    "Phase 1: BeProduct -> DTC Push  (upsert + orphan marks)",
    f"{nb_bp}/p1p7_beproduct_to_dtc_push",
    {
        "catalog":         catalog,
        "schema":          schema,
        "staging_table":   "beproduct_to_dtc_staging",
        "dtc_environment": dtc_environment,
        "dtc_workspace":   dtc_workspace,
        "dry_run":         dry_run,
        "delta_only":      delta_only,
        "batch_size":      "100",
    },
    timeout_seconds=3600,
    skip=(not run_phase1 or not _step4_ok),
    skip_reason=(
        "run_phase1=false"
        if not run_phase1
        else "step 4 (p1_dtc_request_manager) failed or no resolved requests"
    ),
)

# COMMAND ----------

# ── STEP 6: Phase 2 — DTC -> BeProduct push ───────────────────────────────────
# Needs step 2 (staging identity map: beproduct_style_id + colorway_id)
# and step 3 (dtc_wip_<customer> pulled rows).
# Does NOT depend on steps 4/5 — field sets are disjoint.
# Gated by run_phase2 widget.
_r6 = _run_step(
    6,
    "Phase 2: DTC -> BeProduct Pushback",
    f"{nb_dtc}/p2_push_dtc_to_beproduct",
    {
        "catalog":         catalog,
        "schema":          schema,
        "customer":        customer,
        "staging_table":   "beproduct_to_dtc_staging",
        "dtc_environment": dtc_environment,
        "dry_run":         dry_run,
        "push_blanks":     push_blanks,
    },
    timeout_seconds=3600,
    skip=(not run_phase2 or not _step2_ok or not _step3_data),
    skip_reason=(
        "run_phase2=false"
        if not run_phase2
        else "step 2 (transform) failed"
        if not _step2_ok
        else "step 3 returned no in-scope DTC requests"
    ),
)

# COMMAND ----------

# ── STEP 7: Refresh dtc_wip AFTER Phase 1 (so it reflects inserted rows) ───────
# Phase 1 (step 5) may have INSERTED new DTC rows; re-pull so dtc_wip_<customer>
# is current before image sync. Gated by run_phase3 (only needed for Phase 3) and
# by step 4 success (no point refreshing if nothing was resolved/pushed).
# Opt B: parse the request_ids that had INSERTs from Step 5's exit string and
# pass them as a targeted filter so only those sheets are re-fetched (DELETE +
# append) instead of a full 66-request overwrite. Falls back to full re-pull if
# Step 5 exit is unavailable or reports no inserts.
_step5_inserted_ids = _parse_inserted_ids(_r5 or "")
if _step5_inserted_ids:
    _step7_name = f"Refresh dtc_wip (targeted: {len(_step5_inserted_ids.split(','))} request(s) with INSERTs)"
else:
    _step7_name = "Refresh dtc_wip (post-Phase 1 re-pull, full)"

_r7 = _run_step(
    7,
    _step7_name,
    f"{nb_dtc}/p1_pull_masters_to_delta",
    {
        "dtc_environment":  dtc_environment,
        "customer":         customer,
        "dtc_workspace":    dtc_workspace,
        "dtc_document":     dtc_document,
        "catalog":          catalog,
        "schema":           schema,
        "write_mode":       "overwrite",   # ignored when request_ids is set (uses delete+append)
        "refresh_registry": "false",       # registry already refreshed in step 3
        "request_ids":      _step5_inserted_ids,  # "" = full pull; "id1,id2" = targeted
        "max_workers":      "4",
    },
    timeout_seconds=1800,
    skip=(not run_phase3 or not _step4_ok),
    skip_reason=(
        "run_phase3=false"
        if not run_phase3
        else "step 4 (p1_dtc_request_manager) failed or no resolved requests"
    ),
)

# COMMAND ----------

# ── STEP 8: Phase 3 — BeProduct front image -> DTC "Style Image" ───────────────
# Binary cell upload (separate multipart endpoint); runs after rows exist.
# Needs step 4 (resolved mapping) and step 2 (front_image_url in staging).
# Re-reads each sheet live for the freshest rowIndex + image state.
# Gated by run_phase3 widget.
_r8 = _run_step(
    8,
    "Phase 3: BeProduct Image -> DTC Style Image",
    f"{nb_bp}/p3_beproduct_to_dtc_images",
    {
        "catalog":         catalog,
        "schema":          schema,
        "staging_table":   "beproduct_to_dtc_staging",
        "dtc_environment": dtc_environment,
        "dtc_workspace":   dtc_workspace,
        "dry_run":         dry_run,
        "http_timeout":    img_http_timeout,
        "max_uploads":     img_max_uploads,
    },
    timeout_seconds=3600,
    skip=(not run_phase3 or not _step4_ok or not _step2_ok),
    skip_reason=(
        "run_phase3=false"
        if not run_phase3
        else "step 2 (transform) failed"
        if not _step2_ok
        else "step 4 (p1_dtc_request_manager) failed or no resolved requests"
    ),
)

# COMMAND ----------

# ── Final Summary ──────────────────────────────────────────────────────────────
_total_elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()

print("\n" + "=" * 80)
print("ORCHESTRATOR SUMMARY")
print("=" * 80)
print(f"  Started:       {started_at.isoformat()}")
print(f"  Total elapsed: {_total_elapsed:.0f}s")
print(f"  dry_run:       {dry_run}")
print()

for s in _steps:
    icon    = _STATUS_ICON.get(s["status"], "?")
    elapsed = f"{s['elapsed_s']:.0f}s" if s["elapsed_s"] else "—"
    detail  = f"  ({s['result'][:100]})" if s["result"] else ""
    print(f"  {icon} Step {s['step']}: {s['name']:<52} [{elapsed:>5}]{detail}")

_failed  = [s for s in _steps if s["status"] == "error"]
_skipped = [s for s in _steps if s["status"] == "skipped"]

print()
if not _failed:
    print("✅ All executed steps completed successfully")
    if _skipped:
        print(f"   ({len(_skipped)} step(s) skipped — see reasons above)")
else:
    print(f"❌ {len(_failed)} step(s) failed:")
    for s in _failed:
        print(f"   Step {s['step']} — {s['name']}")
        print(f"     {s['result'][:200]}")

if dry_run.lower() in ("true", "1", "yes"):
    print()
    print("⚠️  DRY RUN — no writes were made to DTC or BeProduct.")
    print("   Set dry_run=false to apply changes.")

print()
print("Review sync logs:")
print(f"  Phase 1: SELECT * FROM {catalog}.{schema}.beproduct_to_dtc_sync_log WHERE stage='push'   ORDER BY log_time DESC LIMIT 100;")
print(f"  Phase 2: SELECT * FROM {catalog}.{schema}.dtc_to_beproduct_sync_log ORDER BY log_time DESC LIMIT 100;")
print(f"  Phase 3: SELECT * FROM {catalog}.{schema}.beproduct_to_dtc_sync_log WHERE stage='images' ORDER BY log_time DESC LIMIT 100;")

# COMMAND ----------

# ── Job exit ──────────────────────────────────────────────────────────────────
# Exit value is surfaced in the Databricks Jobs run history.
# A non-empty exit string on failure causes the job run to be marked FAILED.
if _failed:
    _fail_names = ", ".join(f"Step {s['step']}" for s in _failed)
    dbutils.notebook.exit(f"FAILED: {_fail_names}")
    raise RuntimeError(
        f"Orchestrator finished with {len(_failed)} failed step(s): {_fail_names}. "
        "See step output above for details."
    )
else:
    if _skipped:
        _skipped_csv = ", ".join("Step {}".format(s["step"]) for s in _skipped)
        _skipped_names = "  skipped: " + _skipped_csv
    else:
        _skipped_names = ""
    dbutils.notebook.exit(f"OK  elapsed={_total_elapsed:.0f}s{_skipped_names}")
