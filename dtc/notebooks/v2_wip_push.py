# Databricks notebook source
"""
v2 Stage 40 -- THE single DTC write window.

Replaces THREE v1 tasks:

    p1p7_beproduct_to_dtc_push.py   Phases 1/4/7   updates + inserts + orphans
    p10_pull_bom_and_enrich.py      Phase 10       updates + inserts
    p9b2_push_duty_to_wip.py        Phase 9b push  updates

Per request, v1 wrote at up to 5 moments scattered across the whole DAG, with
two full DTC re-pulls interleaved. DTC's optimistic locking is request-scoped
-- any successful write moves the server-side `last_read` and silently
invalidates every browser session that loaded earlier -- so at the target
cadence (a run every ~2 hours) that is 36 write moments/day and ~6-8 h/day of
user-visible exposure. This notebook writes each request at exactly ONE point,
in <=2 back-to-back calls.

Why <=2 and not 1: `DTCConnector.patch_rows` rejects a body mixing `rowId`
(update) and `rowIndex` (insert), so one call each is the floor.

Shape of the work
-----------------
    for each resolved, still-active request:
        1 live get_sheet()                         <- freshest possible state
        wip_plan.compute_request_plan(...)         <- pure, unit-tested
        if plan.is_empty():  issue NOTHING          <- THE invariant
        else: PATCH updates (rowId), PATCH inserts (rowIndex)

ALL decision logic is pure and lives in `sync/wip_plan.py`, which composes
`phase1` + `bom` + `duty` + orphan marks. This notebook is a thin Spark/IO
wrapper: read Delta, call the planner, call the connector, write logs. Nothing
here decides what to write.

The invariant
-------------
A run that changes nothing must write NOTHING -- no PATCH, and the plan is
checked before any write is attempted. At 12 runs/day this is the difference
between safe and intolerable. `wip_plan` asserts it; this notebook honours it
by branching on `plan.is_empty()`.

Planning against live, not Delta
--------------------------------
v1's Phase 10 and Phase 9b planned against a Delta snapshot and therefore
needed `repull_dtc` / `repull_dtc_bom` to stay honest. Here every contribution
diffs against the SAME single live read, so both re-pulls are gone and the
material fan-out for a brand-new style x color happens in the same run.

Debugging output
----------------
The Jobs API returns NO notebook stdout for serverless runs -- only the
`dbutils.notebook.exit` value (live-confirmed 2026-09-14, run
66807905429726). So this notebook emits THREE layers:

  * stdout           -- full per-request `plan.explain()` traces, for the UI
  * beproduct_to_dtc_sync_log -- one Delta row per operation, queryable later
  * exit value       -- a JSON summary, the only thing retrievable via the API

Three task run_ids collapsed into one in v2, so per-contribution counters
(`fields_by_source`) are the replacement for per-phase logs: they answer "which
contribution changed this cell?" without opening the run.
"""

# COMMAND ----------

import sys

# ── Python module root ──────────────────────────────────────────────────────
_DEFAULT_MODULE_PATH = "/Workspace/Repos/beproduct-sync/DTC/python"
dbutils.widgets.text("module_path", _DEFAULT_MODULE_PATH, "Python module root")
_MODULE_PATH = (dbutils.widgets.get("module_path") or "").strip() or _DEFAULT_MODULE_PATH
for _p in (_MODULE_PATH, _MODULE_PATH.replace("/DTC/", "/dtc/")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import json
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from connectors.dtc import DTCConnector
from sync import bom, phase1, wip_plan

# COMMAND ----------

dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("customer", "KTB", "Customer")
dbutils.widgets.text("dtc_workspace", "KTB", "DTC workspace")
dbutils.widgets.text("dtc_document", "KTB WIP", "DTC document")
dbutils.widgets.text("dtc_environment", "uat", "DTC environment")
dbutils.widgets.text("dry_run", "true", "Compute + log, never PATCH")
# BOTH coverage pre-filters default OFF in v2. They are style-level gates that
# run BEFORE any field is compared, so anything out of sync for a reason other
# than "this style just changed" is invisible to them -- permanently.
#
# Live-confirmed 2026-09-15 on KTB-00024/Black: 6 physical rows of the SAME
# style and colour, only 1 carrying "Sub Class". v1 cannot repair the other 5,
# because the style's beproduct_modified_at is now older than the request's
# last_pushed and its staging rows are marked 'pushed'.
#
# In v1 these filters bought cheap pushes. In v2 they buy nothing: the
# zero-diff-zero-write invariant means considering every row costs ZERO extra
# API calls when nothing differs (measured: 36 ms of planning for 250 styles /
# 1500 rows). They only cost correctness now. See docs/MIGRATION_V1_V2.md.
dbutils.widgets.text("delta_only", "false", "Only styles modified since last_pushed (v1 default: true)")
dbutils.widgets.text("staging_pending_only", "false", "Only staging rows with sync_status='pending'")
dbutils.widgets.text("batch_size", "100", "Rows per PATCH call")
dbutils.widgets.text("costing_chart_table", "lft.beproduct.costing_chart", "Duty source")
dbutils.widgets.text("bom_segments_table", "tpm_bom_segments", "BOM source (Stage 20b)")
dbutils.widgets.text("run_wip_push", "true", "Run this stage (false = no-op)")
dbutils.widgets.text("run_duty_push", "true", "Include the duty contribution")
# Material columns to plan but NEVER write (hard exclusion). Empty by default.
dbutils.widgets.text("material_exclude_columns", "", "Material columns to plan but not write")
# Material columns written ONLY into a blank cell -- write-once default-fill,
# the same treatment Supplier / Fabric Group / Placement already get.
#
# "Content" belongs here (owner decision 2026-09-15). DTC's own Content trigger
# will overwrite whatever Phase 10 writes, and the two notations
# ("97% Cotton / 3% Spandex" vs "Cotton 97%, Spandex 3%") are semantically
# identical -- they feed Phase 9's NT Orbit product_description and do not
# change its results. The ONLY thing the Content write must achieve is making
# the cell non-blank, which is what Phase 9a's completeness gate needs. Filling
# it once and then leaving the trigger alone gets exactly that, and avoids a
# diff that would otherwise reappear on EVERY run -- 60 of 60 rows in UAT --
# opening a write window every time. See docs/MIGRATION_V1_V2.md.
dbutils.widgets.text("material_fill_if_blank_columns", "Content",
                     "Material columns written only when the cell is blank")
dbutils.widgets.text("explain_limit", "40", "Rows per request in the stdout trace")
dbutils.widgets.text("sample_limit", "12", "current-vs-new samples per request in the exit JSON")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
customer = dbutils.widgets.get("customer")
workspace = dbutils.widgets.get("dtc_workspace")
environment = dbutils.widgets.get("dtc_environment")
dry_run = (dbutils.widgets.get("dry_run") or "true").strip().lower() == "true"
delta_only = (dbutils.widgets.get("delta_only") or "false").strip().lower() == "true"
pending_only = (dbutils.widgets.get("staging_pending_only") or "false").strip().lower() == "true"
batch_size = int(dbutils.widgets.get("batch_size") or 100)
costing_table = dbutils.widgets.get("costing_chart_table")
bom_table = f"{catalog}.{schema}.{dbutils.widgets.get('bom_segments_table')}"
enable_duty = (dbutils.widgets.get("run_duty_push") or "true").strip().lower() == "true"
explain_limit = int(dbutils.widgets.get("explain_limit") or 40)
sample_limit = int(dbutils.widgets.get("sample_limit") or 12)
material_exclude = frozenset(
    c.strip() for c in (dbutils.widgets.get("material_exclude_columns") or "").split(",")
    if c.strip())
material_fill_if_blank = frozenset(
    c.strip() for c in (dbutils.widgets.get("material_fill_if_blank_columns") or "").split(",")
    if c.strip())

staging_full = f"{catalog}.{schema}.beproduct_to_dtc_staging"
mapping_full = f"{catalog}.{schema}.dtc_request_mapping"
registry_full = f"{catalog}.{schema}.dtc_request_registry"
sync_log_full = f"{catalog}.{schema}.beproduct_to_dtc_sync_log"

now = datetime.now(timezone.utc)
run_id = now.strftime("%Y%m%d%H%M%S")

print("=" * 78)
print("v2 Stage 40 -- THE single DTC write window")
print("=" * 78)
print(f"  env={environment}  dry_run={dry_run}  batch_size={batch_size}")
print(f"  coverage: delta_only={delta_only}  staging_pending_only={pending_only}"
      + ("   (FULL SCAN -- every row diffed against live)"
         if not (delta_only or pending_only) else "   ⚠ PRE-FILTERED"))
print(f"  duty contribution: {'ON' if enable_duty else 'OFF'}")
if material_exclude:
    print(f"  material columns EXCLUDED from writes: {sorted(material_exclude)}")
if material_fill_if_blank:
    print(f"  material columns WRITE-ONCE (fill blank only): {sorted(material_fill_if_blank)}")
print(f"  BOM source       : {bom_table}")
print(f"  duty source      : {costing_table}")

if (dbutils.widgets.get("run_wip_push") or "true").strip().lower() != "true":
    print("\nrun_wip_push=false -- skipping entirely (no reads, no writes).")
    dbutils.notebook.exit(json.dumps({"status": "SKIPPED_run_wip_push_false"}))

# COMMAND ----------

# ── Step 1: load Delta inputs ───────────────────────────────────────────────
print("\nStep 1: loading Delta inputs …")

try:
    df_map = spark.table(mapping_full)
except Exception:
    df_map = None
if df_map is None or df_map.count() == 0:
    print("⚠  No resolved requests in dtc_request_mapping -- run request_manager first.")
    dbutils.notebook.exit(json.dumps({"status": "NO_RESOLVED_REQUESTS"}))

mapping = {r.dtc_request_name: r for r in df_map.collect()}
inputs = {}   # every input count, echoed into the exit value
reg_by_id = {r.request_id: r for r in
             spark.table(registry_full).where(F.col("environment") == environment).collect()}
inputs["requests_resolved"] = len(mapping)
print(f"  resolved requests : {len(mapping)}")

df_staging_all = spark.table(staging_full)
df_staging = (df_staging_all.where(F.col("sync_status") == "pending")
              if pending_only else df_staging_all)
inputs["staging_rows_total"] = df_staging_all.count()
inputs["staging_rows_considered"] = df_staging.count()
inputs["staging_pending_only"] = pending_only
inputs["delta_only"] = delta_only
print(f"  staging rows : {inputs['staging_rows_considered']} considered "
      f"of {inputs['staging_rows_total']} total "
      f"(pending_only={pending_only}, delta_only={delta_only})")

# BOM for EVERY style, not just the ones in a delta-filtered staging slice.
# A style whose BeProduct data is unchanged can still have NEW BOM data, and
# its existing DTC rows must still be enriched -- v1's Phase 10 ran
# independently of staging deltas and this preserves that.
bom_by_style = {}
bom_parse_errors = []
try:
    for r in spark.table(bom_table).collect():
        if r["parse_error"]:
            bom_parse_errors.append(f"{r['bp_style_number']}: {r['parse_error']}")
            continue
        bom_by_style[r["bp_style_number"]] = r["custom_fields"]
    inputs["styles_with_bom"] = len(bom_by_style)
    print(f"  styles with BOM data : {len(bom_by_style)}"
          + (f"  ⚠ {len(bom_parse_errors)} unparseable" if bom_parse_errors else ""))
except Exception as e:  # noqa: BLE001
    # Degrade, never abort: no BOM table yet (first run, or run_bom=false)
    # means no material contribution -- never a revert of existing DTC data.
    print(f"  ⚠ BOM table unavailable ({e}) -- material contribution DISABLED this run")

duty_by_request = {}
if enable_duty:
    try:
        for r in spark.table(costing_table).collect():
            d = r.asDict()
            duty_by_request.setdefault(
                (d.get("customer"), d.get("season_code"), d.get("brand")), []).append(d)
        inputs["costing_chart_groups"] = len(duty_by_request)
        inputs["costing_chart_rows"] = sum(len(v) for v in duty_by_request.values())
        print(f"  costing_chart groups : {len(duty_by_request)} "
              f"({inputs['costing_chart_rows']} rows)")
    except Exception as e:  # noqa: BLE001
        print(f"  ⚠ costing_chart unavailable ({e}) -- duty contribution DISABLED this run")

# Global key map for orphan detection: which request does each (style, color)
# key CURRENTLY belong to? Built from the FULL staging (not delta-filtered), so
# a key that moved is detected even when neither side changed this run.
key_to_requests = {}
for r in df_staging_all.select("dtc_request_name", "bp_style_number", "color").collect():
    k = (phase1.norm(r["bp_style_number"]), phase1.norm(r["color"]))
    if k == (None, None):
        continue
    key_to_requests.setdefault(k, set()).add(r["dtc_request_name"])

# COMMAND ----------

# ── Step 2: plan + push, one request at a time ─────────────────────────────
print("\nStep 2: planning and pushing …")

LOG_COLS = ["log_time", "run_id", "stage", "environment", "dtc_request_name", "request_id",
            "operation", "lf_style_number", "color", "match_key", "status", "reason",
            "detail", "payload"]
SYNC_LOG_SCHEMA = StructType([
    StructField("log_time", TimestampType()), StructField("run_id", StringType()),
    StructField("stage", StringType()), StructField("environment", StringType()),
    StructField("dtc_request_name", StringType()), StructField("request_id", StringType()),
    StructField("operation", StringType()), StructField("lf_style_number", StringType()),
    StructField("color", StringType()), StructField("match_key", StringType()),
    StructField("status", StringType()), StructField("reason", StringType()),
    StructField("detail", StringType()), StructField("payload", StringType()),
])

log_rows = []


def log(name, request_id, operation, key, status, reason="", detail="", payload=None):
    lf, color = (key or (None, None))
    log_rows.append((now, run_id, "wip_push", environment, name, request_id, operation,
                     lf, color, f"{lf} | {color}", status, reason, detail,
                     json.dumps(payload) if payload is not None else None))


api_key = dbutils.secrets.get(scope="beproduct", key=f"dtc_api_key_{environment}")
connector = DTCConnector(api_key=api_key, environment=environment, workspace_name=workspace)

FALLBACK_COLS = {c for c in phase1.FIELD_MAPPING.values() if c != phase1.STYLE_IMAGE_COL}

totals = {"requests": 0, "requests_written": 0, "requests_noop": 0,
          "updates": 0, "inserts": 0, "noops": 0, "exceptions": 0,
          "patch_calls": 0, "patch_failed": 0, "orphan_marks": 0}
by_source_totals = {}
diagnostics_totals = {}
columns_totals = {}
columns_unseen_totals = set()
all_degraded, all_violations = [], []
per_request = []

for name, m in mapping.items():
    totals["requests"] += 1
    request_id, sheet_id, view_id = m.request_id, m.sheet_id, m.view_id
    print(f"\n--- {name}  (request_id={request_id}) ---")

    # Re-validate by request_id, never by name: DTC permits two concurrently
    # active requests with the same name, and a request can go inactive between
    # the resolver and the push ("inactive" == hidden from users == deleted).
    reg_entry = reg_by_id.get(request_id)
    active = str(getattr(reg_entry, "request_is_active", "") or "").upper() in ("Y", "TRUE", "1")
    if reg_entry is None or not active:
        print(f"  ⛔ inactive/missing in registry -- refusing to push")
        log(name, request_id, "REQUEST_INACTIVE", None, "error", "request_inactive_at_push",
            "re-run request_manager to recreate")
        continue

    # THE single live read. Every contribution diffs against this one snapshot.
    try:
        sheet = connector.get_sheet(sheet_id, view_id)
        dtc_rows = sheet.get("sheetData", [])
        # FALLBACK_COLS is unioned in because get_view_definition() 403s for
        # this API key, so get_view_column_names() degrades to a DATA SCAN --
        # which cannot see a column that is blank in every current row. Keeping
        # the two sets separate lets us WARN about the blind spot instead of
        # just living with it (see columns_not_seen_in_view below).
        view_cols = set(connector.get_view_column_names(sheet_id, view_id))
        allowed = view_cols | FALLBACK_COLS
    except Exception as e:  # noqa: BLE001
        print(f"  ❌ sheet read failed: {e}")
        log(name, request_id, "ERROR", None, "error", "sheet_read_failed", str(e)[:300])
        totals["patch_failed"] += 1
        continue

    # Columns DTC rejects writes to (image / formula types). get_view_definition
    # 403s for this API key on some views -- degrade to the safe minimum rather
    # than failing the request.
    non_writable = frozenset()
    try:
        vd = connector.get_view_definition(view_id) or {}
        non_writable = bom.compute_non_writable_cols(vd.get("dynamicFields") or [])
    except Exception as e:  # noqa: BLE001
        print(f"  (view definition unavailable: {e} -- using INSERT_EXCLUDE_COLS only)")

    sdf = df_staging.where(F.col("dtc_request_name") == name)
    last_pushed = getattr(reg_entry, "last_pushed", None)
    if delta_only and last_pushed is not None:
        sdf = sdf.where(F.col("beproduct_modified_at") > F.lit(last_pushed))
    bp_rows = [r.asDict() for r in sdf.collect()]

    bp_keys_here = {k for k, reqs in key_to_requests.items() if name in reqs}
    moved_elsewhere = {k for k, reqs in key_to_requests.items() if name not in reqs}

    duty_rows = duty_by_request.get(
        (getattr(m, "customer", customer), m.season_code, m.brands), []) if enable_duty else []

    plan = wip_plan.compute_request_plan(
        {"season_code": m.season_code, "brand": m.brands},
        dtc_rows, bp_rows,
        bom_by_style=bom_by_style,
        duty_rows=duty_rows,
        bp_keys_this_request=bp_keys_here,
        moved_elsewhere_keys=moved_elsewhere,
        allowed_cols=allowed,
        non_writable_cols=non_writable,
        enable_duty=enable_duty,
        material_exclude_cols=material_exclude,
        material_fill_if_blank_cols=material_fill_if_blank,
        request_name=name,
    )

    print(plan.explain(limit=explain_limit))
    s = plan.summary()
    s["live_rows_read"] = len(dtc_rows)
    s["staging_rows_considered"] = len(bp_rows)
    s["sample_changes"] = plan.sample_changes(limit=sample_limit)

    # ── Silent-write-failure detector (added 2026-09-15) ────────────────────
    # Live-confirmed on this very request: v1 pushed {"Sub Class": ...} for 13
    # rows, DTC returned 204, the sync log recorded "ok" -- and the value was
    # NEVER stored. DTC accepts a sheetData key it does not recognise and drops
    # it silently. v1 could not notice, because `delta_only` then advanced
    # last_pushed past the style's modified_at, so the row was never
    # reconsidered and the cell stayed blank permanently.
    #
    # A column we are writing that the live view never reports is the signal.
    # It is a WARNING, not an error: the data-scan fallback also cannot see a
    # genuinely-present column that happens to be blank in every current row,
    # so the two cases are indistinguishable from here. Either way it deserves
    # a human look -- if the column really is absent, every push to it is going
    # into the void.
    unseen = sorted(set(s["columns_changed"]) - view_cols)
    if unseen:
        s["columns_not_seen_in_view"] = unseen
        columns_unseen_totals.update(unseen)
        print(f"  ⚠ writing column(s) the live view never reports: {unseen}")
        print("     Either the column does not exist (every write to it is")
        print("     silently discarded by DTC), or it is blank in every row so")
        print("     the data-scan fallback cannot see it. Verify against the")
        print("     DTC view definition before trusting these writes.")
        log(name, request_id, "COLUMN_NOT_IN_VIEW", None, "warn",
            "column_not_seen_in_view", ", ".join(unseen))
    per_request.append(s)
    for k, v in (s["fields_by_source"] or {}).items():
        by_source_totals[k] = by_source_totals.get(k, 0) + v
    for k, v in (s["diagnostics"] or {}).items():
        diagnostics_totals[k] = diagnostics_totals.get(k, 0) + v
    for k, v in (s["columns_changed"] or {}).items():
        columns_totals[k] = columns_totals.get(k, 0) + v
    all_degraded.extend(plan.degraded)
    all_violations.extend(plan.violations)
    totals["updates"] += len(plan.updates)
    totals["inserts"] += len(plan.inserts)
    totals["noops"] += len(plan.noops)
    totals["exceptions"] += len(plan.exceptions)
    totals["orphan_marks"] += plan.counts.get("orphan_marks", 0)

    for ex in plan.exceptions:
        log(name, request_id, "EXCEPTION", ex.match_key, "error", ex.reason, ex.detail)
    for op in plan.noops:
        log(name, request_id, "NOOP", op.match_key, "ok", "no_field_changes")

    # ── THE INVARIANT ──────────────────────────────────────────────────────
    # Nothing to change => issue NOTHING. Not a PATCH with an empty body, not
    # a "touch" -- no call at all, so this request's `last_read` does not move
    # and nobody editing it is invalidated.
    if plan.is_empty():
        totals["requests_noop"] += 1
        print("  ⏩ nothing to write -- 0 API calls (users editing this request are untouched)")
        continue

    totals["requests_written"] += 1

    # UPDATEs (rowId) then INSERTs (rowIndex), back to back. Never mixed: the
    # API rejects a body containing both.
    for label, rows_sd, ops in (
        ("UPDATE", plan.update_sheet_data(), plan.updates),
        ("INSERT", plan.insert_sheet_data(), plan.inserts),
    ):
        if not rows_sd:
            continue
        for chunk_sd, chunk_ops in zip(phase1.chunked(rows_sd, batch_size),
                                       phase1.chunked(ops, batch_size)):
            try:
                if not dry_run:
                    connector.patch_rows(sheet_id, view_id, chunk_sd)
                totals["patch_calls"] += 1
                for op in chunk_ops:
                    log(name, request_id, label, op.match_key, "ok",
                        "dry_run" if dry_run else "",
                        f"sources={sorted(set(op.sources.values()))}", op.fields)
                print(f"  ✅ {label}: {len(chunk_sd)} row(s)"
                      + ("  [dry_run]" if dry_run else ""))
            except Exception as e:  # noqa: BLE001
                totals["patch_failed"] += 1
                print(f"  ❌ {label} PATCH failed: {e}")
                for op in chunk_ops:
                    log(name, request_id, label, op.match_key, "error",
                        "patch_failed", str(e)[:300], op.fields)

    if not dry_run:
        ts = now.isoformat()
        msg = (f"v2 u={len(plan.updates)} i={len(plan.inserts)} "
               f"noop={len(plan.noops)} exc={len(plan.exceptions)}")
        spark.sql(f"""
          UPDATE {registry_full}
          SET last_pushed = timestamp('{ts}'), msgs = '{msg}',
              updated_at = timestamp('{ts}')
          WHERE environment = '{environment}' AND request_id = '{request_id}'
        """)

connector.close()

# COMMAND ----------

# ── Step 3: logs + summary ─────────────────────────────────────────────────
if log_rows:
    (spark.createDataFrame(log_rows, SYNC_LOG_SCHEMA)
     .write.format("delta").mode("append").saveAsTable(sync_log_full))
    print(f"\n✅ logged {len(log_rows)} row(s) to {sync_log_full}")

print("\n" + "=" * 78)
print("v2 Stage 40 COMPLETE")
print("=" * 78)
for k, v in totals.items():
    print(f"  {k:20} {v}")
print(f"  {'fields_by_source':20} {by_source_totals}")
print(f"  {'diagnostics':20} {diagnostics_totals}")
print(f"  {'inputs':20} {inputs}")
print(f"  {'columns changed':20} {columns_totals}")
if columns_unseen_totals:
    print(f"\n  ⚠ COLUMNS THE LIVE VIEW NEVER REPORTS: {sorted(columns_unseen_totals)}")
    print("     Every write to a column DTC does not recognise is accepted (204)")
    print("     and silently discarded -- v1 hit exactly this with 'Sub Class'.")

# Write windows opened = requests we actually PATCHed. This is the number the
# whole v2 design exists to minimize; v1's equivalent was ~3x this.
print(f"\n  DTC write windows opened : {totals['requests_written']} "
      f"of {totals['requests']} request(s)")
print(f"  PATCH calls issued       : {totals['patch_calls']} "
      f"(<= 2 per written request)")

if all_degraded:
    print(f"\n  ⚠ DEGRADED contributions ({len(all_degraded)}) -- keys omitted, rest pushed:")
    for d in all_degraded[:20]:
        print(f"      {d}")
if all_violations:
    print(f"\n  ⚠ ALLOW-LIST VIOLATIONS ({len(all_violations)}) -- dropped before the PATCH:")
    for v in all_violations[:20]:
        print(f"      {v}")
if bom_parse_errors:
    print(f"\n  ⚠ unparseable BOM payloads ({len(bom_parse_errors)}):")
    for b in bom_parse_errors[:20]:
        print(f"      {b}")

summary = {
    "status": "OK",
    "dry_run": dry_run,
    # Input counts matter as much as output counts here: "0 writes" is the
    # SUCCESS case, so the only way to tell a correct no-op from an empty or
    # mis-wired input is to see what actually went in.
    "inputs": inputs,
    "material_exclude_columns": sorted(material_exclude),
    "material_fill_if_blank_columns": sorted(material_fill_if_blank),
    "totals": totals,
    "fields_by_source": by_source_totals,
    "diagnostics": diagnostics_totals,
    "columns_changed": dict(sorted(columns_totals.items(), key=lambda kv: (-kv[1], kv[0]))),
    "columns_not_seen_in_view": sorted(columns_unseen_totals),
    "write_windows_opened": totals["requests_written"],
    "degraded": all_degraded[:50],
    "violations": all_violations[:50],
    "bom_parse_errors": bom_parse_errors[:50],
    "per_request": per_request,
}

# A violation means a contribution tried to write outside the canonical
# allow-list. The field was already dropped, so the push itself was safe --
# but it is a contract breach and must not pass silently into a green run.
if all_violations:
    summary["status"] = "COMPLETED_WITH_VIOLATIONS"
elif columns_unseen_totals:
    summary["status"] = "COMPLETED_WITH_WARNINGS"

print("\n" + json.dumps({k: v for k, v in summary.items() if k != "per_request"}, indent=2))
dbutils.notebook.exit(json.dumps(summary))
