# Databricks notebook source
"""
v2 Stage 55 -- DTC "Fabric Customer # or SAP #" -> BeProduct MATERIAL MASTER.

Writes to **BeProduct**, never to DTC. It therefore does NOT open a DTC write
window and is free to run alongside anything else.

Why the material and not the BOM row
------------------------------------
The obvious target would be the BOM row's `CUSTOMER MATERIAL CODE`. It is not
writable there -- live-confirmed 2026-09-17:

    POST .../Variation/{v}/Update
    -> 400 Field [customer_material_code] is not editable on a material-linked row.

The BOM row only DISPLAYS that code, read through from the linked material
(owner: "BomVariation now lookup this code (via LF Material No) and display
inline"). Proven live: writing the material's own `customer_material_code` made
the BOM row show the new value immediately.

Resolution chain (all decisions are pure, in sync/bom_push.py):

    DTC row --(Fabric Group, Mill Fabric Article #)--> BOM segment
            --materialId-->                           material master

The pair is unique within a style (owner confirmation) and is the SAME
`bom.segment_key()` the enrichment direction uses, so both directions agree on
what "the same fabric assignment" means. The write always uses the GUID.

How the GUID is obtained -- CHANGED by the 2026-09-22 source walkback
--------------------------------------------------------------------
PageBomVariation segments carried `materialId` outright. The Lakebase techpack
source that replaced it has no such column; it offers only `**MaterialCode`,
which IS the material master's `headerNumber` (e.g. "LF-BD26-000002--SH" --
the "--SH" suffix is part of the key). So this notebook now resolves
code -> GUID itself, via `resolve_material_ids()`, before planning.

That resolution is the one place this stage can go wrong in a way the
read-back verify would not save us from, so it refuses rather than guesses:
`attributes_list` is used directly (never `attributes_get_by_number`, which
hides a second match behind `next(..., None)`), the server's `Eq` result is
post-filtered to an exact `headerNumber`, and a code matching zero or several
materials is reported in the `unresolved` bucket and never written.

It also re-introduces a risk the GUID route did not have: it depends on
`headerNumber` staying globally unique once material master is reorganised
into per-customer folders. The lookup is deliberately folder-agnostic.

The hazard this stage is built around
-------------------------------------
**Materials are SHARED across styles.** One material is referenced by many BOM
rows in many styles, so two DTC rows can disagree about its customer code.
"Last row wins" would silently corrupt a master record other styles depend on,
so a material whose candidate values disagree is NEVER written -- the conflict
is reported instead.

A blank DTC value is never pushed, so this stage can only set or change a code,
never clear one.
"""

# COMMAND ----------

import sys
import subprocess
import time

print("Installing BeProduct SDK …")
_t0 = time.perf_counter()
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "beproduct"])
print(f"  ✅ installed in {time.perf_counter() - _t0:.1f}s")

# COMMAND ----------

_DEFAULT_MODULE_PATH = "/Workspace/Repos/beproduct-sync/DTC/python"
dbutils.widgets.text("module_path", _DEFAULT_MODULE_PATH, "Python module root")
_MODULE_PATH = (dbutils.widgets.get("module_path") or "").strip() or _DEFAULT_MODULE_PATH
for _p in (_MODULE_PATH, _MODULE_PATH.replace("/DTC/", "/dtc/")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Python 3.10 compatibility shim -- Databricks serverless runs 3.10 and the
# BeProduct SDK uses `datetime.UTC` (3.11+) unconditionally. See
# v2_pull_bom_segments for the full note.
import datetime as _dt_mod

if not hasattr(_dt_mod, "UTC"):
    _dt_mod.UTC = _dt_mod.timezone.utc

import itertools
import json
from datetime import datetime, timezone

from beproduct.sdk import BeProduct
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from sync import bom, bom_push

# COMMAND ----------

dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("customer", "KTB", "Customer")
dbutils.widgets.text("bom_segments_table", "bom_segments", "Stage 20b output")
dbutils.widgets.text("dry_run", "true", "Compute + log, never write BeProduct")
dbutils.widgets.text("run_customer_code_push", "true", "false = no-op")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
customer = dbutils.widgets.get("customer").strip().lower()
bom_table = f"{catalog}.{schema}.{dbutils.widgets.get('bom_segments_table').strip()}"
wip_table = f"{catalog}.{schema}.dtc_wip_{customer}"
log_table = f"{catalog}.{schema}.beproduct_to_dtc_sync_log"
dry_run = (dbutils.widgets.get("dry_run") or "true").strip().lower() == "true"
now = datetime.now(timezone.utc)
run_id = now.strftime("%Y%m%d%H%M%S")

print("=" * 78)
print("v2 Stage 55 -- DTC customer code -> BeProduct material master")
print("=" * 78)
print(f"  DTC source column : {bom_push.DTC_CUSTOMER_CODE_COL!r}")
print(f"  material field    : {bom_push.MATERIAL_CUSTOMER_CODE_FIELD!r}")
print(f"  dry_run           : {dry_run}")

if (dbutils.widgets.get("run_customer_code_push") or "true").strip().lower() != "true":
    print("\nrun_customer_code_push=false -- skipping (no reads, no writes).")
    dbutils.notebook.exit(json.dumps({"status": "SKIPPED"}))

# COMMAND ----------

# ── Step 1: inputs ──────────────────────────────────────────────────────────
print("\nStep 1: loading DTC rows and BOM segments …")

segments_by_style = {}
bom_mode = None
try:
    _df = spark.table(bom_table)
    # Accept EITHER Stage 20b shape, like wip_push and build_costing already do.
    # This notebook did not sniff before 2026-09-22 and read segments_json
    # unconditionally, so the source walkback would have made it exit
    # NO_BOM_SEGMENTS on a table that is perfectly readable.
    bom_mode = bom.segments_table_mode(_df.columns)
    _errcol = bom.segments_table_error_col(_df.columns)
    for r in _df.collect():
        if r[_errcol]:
            continue
        segs = bom.segments_from_delta_value(r[bom_mode], bom_mode)
        if segs:
            segments_by_style[r["bp_style_number"]] = segs
except Exception as e:  # noqa: BLE001
    print(f"  ⚠ {bom_table} unavailable ({e}) -- nothing can be resolved; exiting.")
    dbutils.notebook.exit(json.dumps({"status": "NO_BOM_SEGMENTS", "error": str(e)[:300]}))
print(f"  styles with BOM segments : {len(segments_by_style)}  (source column: {bom_mode})")

dtc_rows = []
for r in spark.table(wip_table).select("data_json").collect():
    if r["data_json"]:
        try:
            dtc_rows.append(json.loads(r["data_json"]))
        except Exception:  # noqa: BLE001
            continue
print(f"  DTC rows                 : {len(dtc_rows)}")

with_value = sum(1 for r in dtc_rows
                 if (r.get(bom_push.DTC_CUSTOMER_CODE_COL) or "").strip())
print(f"  DTC rows with a value    : {with_value}")

# COMMAND ----------

# ── Step 2: current material values, so a no-change write is never issued ───
# Only the materials actually referenced by a DTC row that HAS a value are
# fetched -- there is no point reading every material in the folder.
client = BeProduct(
    client_id=dbutils.secrets.get(scope="beproduct", key="client_id"),
    client_secret=dbutils.secrets.get(scope="beproduct", key="client_secret"),
    refresh_token=dbutils.secrets.get(scope="beproduct", key="refresh_token"),
    company_domain=dbutils.secrets.get(scope="beproduct", key="company_domain"),
)


def material_code(material_id):
    """Current `customer_material_code` on a material, or None."""
    d = client.material.attributes_get(material_id)
    d = d.get("data", d) if isinstance(d, dict) else d
    for f in (d.get("headerData") or {}).get("fields") or []:
        if f.get("id") == bom_push.MATERIAL_CUSTOMER_CODE_FIELD:
            return f.get("value")
    return None


def resolve_material_ids(codes):
    """{code: material GUID} for codes resolving to EXACTLY ONE material, plus
    {code: reason} for every code that did not. Never raises, never guesses.

    Needed since the 2026-09-22 source walkback: Lakebase BOM rows carry only
    `**MaterialCode` (the material's `headerNumber`), not the `materialId` GUID
    the PageBomVariation source gave us.

    `attributes_get_by_number()` is deliberately NOT used. It is
    `next(attributes_list(...), None)` -- it returns the first hit and silently
    DISCARDS a second, which is exactly the guess this module exists to refuse.
    We take the full match set and require it to be unique.
    """
    resolved, refused = {}, {}
    for code in codes:
        try:
            hits = list(itertools.islice(client.material.attributes_list(
                filters=[{"field": "header_number", "operator": "Eq",
                          "value": code}]), 5))
            # `Eq` is the server's notion of equality, not ours. Post-filter on
            # an exact headerNumber so a prefix/fuzzy match can never resolve.
            # .strip() only: a leading-space header_number is real live data
            # (AGENTS.md). Case is NOT folded -- that would be a guess.
            exact = [h for h in hits
                     if str(h.get("headerNumber") or "").strip() == code.strip()]
            if len(exact) == 1:
                resolved[code] = exact[0].get("id")
            elif not exact:
                refused[code] = (f"no material with header_number={code!r}"
                                 + (f" ({len(hits)} fuzzy hit(s) rejected)" if hits else ""))
            else:
                # A shared master record must not be decided by result order.
                refused[code] = (f"header_number={code!r} matches {len(exact)} "
                                 f"materials -- refusing to pick")
        except Exception as e:  # noqa: BLE001
            # One flaky lookup must not sink a run that would write the others.
            refused[code] = f"lookup failed: {type(e).__name__}: {str(e)[:160]}"
    return resolved, refused


# Only codes that a DTC row carrying a value actually needs -- so with the
# column blank on every row (live state 2026-09-17) this makes ZERO API calls.
# Materials are shared (60 rows -> 8 materials), so this is also the cache:
# one lookup per distinct code per run.
_codes = bom_push.required_material_codes(dtc_rows, segments_by_style)
material_id_by_code, unresolved_codes = ({}, {})
if _codes:
    material_id_by_code, unresolved_codes = resolve_material_ids(_codes)
print(f"\nStep 1b: resolved {len(material_id_by_code)}/{len(_codes)} LF material code(s)")
for _c, _why in sorted(unresolved_codes.items()):
    print(f"    ⚠ UNRESOLVED {_c}: {_why}")

# A first pass with no `current` map tells us WHICH materials matter; then we
# read only those and re-plan with the lean-write check applied.
probe = bom_push.plan_customer_code_push(
    dtc_rows, segments_by_style, material_id_by_code=material_id_by_code)
current = {}
for wr in probe.writes:
    try:
        current[wr.material_id] = material_code(wr.material_id)
    except Exception as e:  # noqa: BLE001
        print(f"  ⚠ could not read material {wr.material_id}: {str(e)[:160]}")
print(f"\nStep 2: read current code for {len(current)} material(s)")

plan = bom_push.plan_customer_code_push(
    dtc_rows, segments_by_style, current_by_material=current,
    material_id_by_code=material_id_by_code)

# COMMAND ----------

# ── Step 3: report, then write ──────────────────────────────────────────────
print("\nStep 3: plan")
print(f"  {json.dumps(plan.summary())}")
for wr in plan.writes:
    print(f"    WRITE material={wr.material_id} lf={wr.lf_material_id!r} "
          f"{wr.current_value!r} -> {wr.value!r}  from {wr.sources}")
for c in plan.conflicts:
    print(f"    ⚠ CONFLICT material={c['material_id']} lf={c['lf_material_id']!r} "
          f"values={c['values']}  -- NOT written")
for u in plan.unmatched[:20]:
    print(f"    ⚠ UNMATCHED {u['bp_style_number']}/{u['color']} "
          f"({u['fabric_group']}, {u['mill_fabric_article']}) value={u['value']!r}")
for a in plan.ad_hoc_skipped[:20]:
    print(f"    ⚠ AD-HOC ROW skipped {a['bp_style_number']}/{a['color']}")
for u in plan.unresolved[:20]:
    print(f"    ⚠ UNRESOLVED {u['bp_style_number']}/{u['color']} "
          f"code={u['material_code']!r} value={u['value']!r} -- {u['reason']}")

log_rows, written, failed = [], 0, 0
if plan.is_empty():
    print("\n  ⏩ nothing to write.")
else:
    for wr in plan.writes:
        try:
            if not dry_run:
                client.material.attributes_update(
                    wr.material_id,
                    fields={bom_push.MATERIAL_CUSTOMER_CODE_FIELD: wr.value})
                # Verify -- a 200 from a vendor API means accepted, not stored.
                # This pipeline has been burned by that twice (DTC Sub Class,
                # PageBomVariation Update), so the write is always read back.
                got = material_code(wr.material_id)
                if (got or "") != wr.value:
                    raise RuntimeError(
                        f"write accepted but NOT stored: material still {got!r}")
            written += 1
            print(f"  ✅ {wr.material_id} -> {wr.value!r}"
                  + ("  [dry_run]" if dry_run else "  (verified)"))
            log_rows.append((now, run_id, "customer_code_push", wr.material_id,
                             wr.lf_material_id, "ok",
                             "dry_run" if dry_run else "verified",
                             json.dumps({"from": wr.current_value, "to": wr.value,
                                         "sources": [list(s) for s in wr.sources]})))
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"  ❌ {wr.material_id}: {str(e)[:220]}")
            log_rows.append((now, run_id, "customer_code_push", wr.material_id,
                             wr.lf_material_id, "error", "write_failed",
                             json.dumps({"to": wr.value, "error": str(e)[:300]})))

for c in plan.conflicts:
    log_rows.append((now, run_id, "customer_code_push", c["material_id"],
                     c["lf_material_id"], "warn", "conflicting_values",
                     json.dumps(c["values"])))

# An LF code that resolved to zero or several materials. No material_id by
# definition -- the code itself is the only handle, so it goes in the
# lf_material_id slot where an operator will look for it.
for u in plan.unresolved:
    log_rows.append((now, run_id, "customer_code_push", None,
                     u["material_code"], "warn", "unresolved_material_code",
                     json.dumps({"bp_style_number": u["bp_style_number"],
                                 "color": u["color"], "value": u["value"],
                                 "reason": u["reason"]})))

if log_rows:
    SCHEMA = StructType([
        StructField("log_time", TimestampType()), StructField("run_id", StringType()),
        StructField("stage", StringType()), StructField("material_id", StringType()),
        StructField("lf_material_id", StringType()), StructField("status", StringType()),
        StructField("reason", StringType()), StructField("detail", StringType()),
    ])
    (spark.createDataFrame(log_rows, SCHEMA)
     .write.format("delta").mode("append")
     .option("mergeSchema", "true")
     .saveAsTable(f"{catalog}.{schema}.beproduct_material_push_log"))
    print(f"\n  logged {len(log_rows)} row(s)")

summary = {
    "status": "OK" if not failed else "COMPLETED_WITH_ERRORS",
    "dry_run": dry_run,
    "inputs": {
        "dtc_rows": len(dtc_rows),
        "dtc_rows_with_value": with_value,
        "styles_with_bom_segments": len(segments_by_style),
        "bom_source_column": bom_mode,
        "material_codes_needed": len(_codes),
        "material_codes_resolved": len(material_id_by_code),
    },
    "plan": plan.summary(),
    "written": written,
    "failed": failed,
    "conflicts": plan.conflicts[:20],
    "unmatched": plan.unmatched[:20],
    "ad_hoc_skipped": plan.ad_hoc_skipped[:20],
    "unresolved": plan.unresolved[:20],
    "unresolved_codes": dict(sorted(unresolved_codes.items())[:20]),
}
# Worst outcome wins. An unresolved code means work was silently NOT done, so
# it must not be reported as OK -- but it ranks below a conflict, which means
# two rows actively disagree about a shared master record.
if plan.unresolved:
    summary["status"] = "COMPLETED_WITH_UNRESOLVED"
if plan.conflicts:
    summary["status"] = "COMPLETED_WITH_CONFLICTS"
if failed:
    summary["status"] = "COMPLETED_WITH_ERRORS"
print("\n" + json.dumps(summary, indent=2)[:4000])
dbutils.notebook.exit(json.dumps(summary))
