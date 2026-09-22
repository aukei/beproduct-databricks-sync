# Databricks notebook source
"""
DIAGNOSTIC PROBE -- can a Lakebase `**MaterialCode` resolve to a BeProduct
material? Not part of any job. Run by hand via scripts/run_v2_task.py.

Why this exists
---------------
The 2026-09-22 source walkback moved the BOM read back to the Lakebase techpack
tables, which carry NO `materialId`. Stage 55's reverse push (DTC
"Fabric Customer # or SAP #" -> the material master's `customer_material_code`)
resolved its write target through exactly that GUID, so it now has to resolve
one from `**MaterialCode` instead -- the material's `headerNumber`.

Writing the WRONG material's customer code would silently corrupt a master
record that many styles share. Stage 55 is disarmed
(`run_customer_code_push=false`) until this probe passes.

What makes this a confirmation rather than a discovery
------------------------------------------------------
Three independent records already point the same way:
  * AGENTS.md: material `b95b1ae6-9e08-4204-b244-c1de59735aa5` has
    LF MATERIAL ID `LF-BD26-000005--TW`.
  * AGENTS.md: the old `bom_unified.material_no` for that style held
    `LF-BD26-000005--TW`.
  * sync/bom.py: `**MaterialCode` corresponds to that same `material_no`.
  * sync/bom_push.py: LF MATERIAL ID *is* the material's `headerNumber`.
So the expected answer is known. **The `--SH` / `--TW` suffix is PART OF THE
KEY** -- anything that strips it should match nothing, and Step C checks that.

RUN THIS BEFORE RESTORING STAGE 20b
-----------------------------------
Ground truth lives only in the live `bom_segments` table while it still holds
BeProduct-sourced rows (`segments_json`, carrying `lf_material_id` +
`material_id` per segment). Restored Stage 20b overwrites that table. Step 0
snapshots it first; everything else can then be re-run at any time.

Safety: READ-ONLY against BeProduct and Lakebase. The only write is the
snapshot table, and it refuses to clobber an existing one.
"""

# COMMAND ----------

import sys
import subprocess

print("Installing BeProduct SDK …")
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "beproduct"])

# COMMAND ----------

_DEFAULT_MODULE_PATH = "/Workspace/Repos/beproduct-sync/DTC/python"
dbutils.widgets.text("module_path", _DEFAULT_MODULE_PATH, "Python module root")
_MODULE_PATH = (dbutils.widgets.get("module_path") or "").strip() or _DEFAULT_MODULE_PATH
for _p in (_MODULE_PATH, _MODULE_PATH.replace("/DTC/", "/dtc/")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Python 3.10 shim -- Databricks serverless runs 3.10 and the BeProduct SDK uses
# `datetime.UTC` (3.11+) unconditionally. See v2_pull_bom_segments for the note.
import datetime as _dt_mod

if not hasattr(_dt_mod, "UTC"):
    _dt_mod.UTC = _dt_mod.timezone.utc

import itertools
import json

from beproduct.sdk import BeProduct
from pyspark.sql import functions as F

from sync import bom

# COMMAND ----------

dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("folder_name", "TEST KTB", "BeProduct folder")
dbutils.widgets.text("bom_segments_table", "bom_segments", "Stage 20b output (BeProduct shape)")
dbutils.widgets.text("snapshot_table", "bom_segments_bp_snapshot", "Where to preserve it")
dbutils.widgets.text("bom_catalog", "alb_tpm_uat", "Lakebase catalog")
dbutils.widgets.text("bom_schema", "public", "Lakebase schema")
dbutils.widgets.text("bom_table", "customer_teckpack_style_latest", "Resolves latest_techpack_style_log_id")
dbutils.widgets.text("bom_log_table", "customer_teckpack_style_log", "custom_fields -- the BOM source")
dbutils.widgets.text("bom_customer_name", "KONTOOR", "Scoping pre-filter")
dbutils.widgets.text("do_snapshot", "true", "Step 0: preserve bom_segments first")
dbutils.widgets.text("max_api_codes", "40", "Cap on Step D lookups")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
folder_name = dbutils.widgets.get("folder_name")
seg_table = f"{catalog}.{schema}.{dbutils.widgets.get('bom_segments_table').strip()}"
snap_table = f"{catalog}.{schema}.{dbutils.widgets.get('snapshot_table').strip()}"
bom_catalog = dbutils.widgets.get("bom_catalog")
bom_schema = dbutils.widgets.get("bom_schema")
bom_source = f"{bom_catalog}.{bom_schema}.{dbutils.widgets.get('bom_table')}"
bom_log_source = f"{bom_catalog}.{bom_schema}.{dbutils.widgets.get('bom_log_table')}"
bom_customer_name = dbutils.widgets.get("bom_customer_name")
styles_table = f"{catalog}.{schema}.ktb_styles"
max_api_codes = int(dbutils.widgets.get("max_api_codes") or 40)

report = {"status": "UNKNOWN"}
print("=" * 78)
print("PROBE -- does **MaterialCode resolve to a BeProduct material?")
print("=" * 78)

# COMMAND ----------

# ── Step 0: preserve the ground truth ───────────────────────────────────────
# bom_segments is the ONLY place the known-good (lf_material_id, material_id)
# pairs exist, and restored Stage 20b overwrites it.
print("\nStep 0: snapshotting the BeProduct-sourced segments …")
snap_note = "skipped"
if (dbutils.widgets.get("do_snapshot") or "true").strip().lower() == "true":
    if spark.catalog.tableExists(snap_table):
        snap_note = f"{snap_table} already exists -- left untouched"
    else:
        spark.sql(f"CREATE TABLE {snap_table} AS SELECT * FROM {seg_table}")
        snap_note = f"created {snap_table} ({spark.table(snap_table).count()} rows)"
print(f"  {snap_note}")
report["snapshot"] = snap_note

# COMMAND ----------

# ── Ground truth: (style, segment_key) -> (lf_material_id, material_id) ─────
_seg_df = spark.table(seg_table)
_mode = bom.segments_table_mode(_seg_df.columns)
if _mode != "segments_json":
    # Already walked back. The probe's left-hand side is gone; re-point
    # bom_segments_table at the snapshot and re-run.
    dbutils.notebook.exit(json.dumps({
        "status": "NO_GROUND_TRUTH",
        "reason": f"{seg_table} is in '{_mode}' shape, not the BeProduct "
                  f"'segments_json' shape this probe compares against",
        "fix": f"re-run with bom_segments_table={snap_table.split('.')[-1]}",
    }))

truth = {}                       # (style, fabric_group, article) -> (lf, guid)
known_pairs = {}                 # lf_material_id -> material_id
for r in _seg_df.collect():
    if r["error"] or not r["segments_json"]:
        continue
    for s in json.loads(r["segments_json"]):
        k = (r["bp_style_number"],) + bom.segment_key(s)
        truth[k] = (s.get("lf_material_id"), s.get("material_id"))
        if s.get("lf_material_id") and s.get("material_id"):
            known_pairs[s["lf_material_id"].strip()] = s["material_id"]
print(f"\nGround truth: {len(truth)} segment(s), "
      f"{len(known_pairs)} distinct (LF code -> GUID) pair(s)")

# COMMAND ----------

# ── Step A: offline join -- does **MaterialCode == lf_material_id? ──────────
print("\nStep A: joining Lakebase **MaterialCode against the known pairs …")

styles = (spark.table(styles_table)
          .where(F.col("folder_name") == folder_name)
          .select(F.col("bp_style_number"),
                  F.concat(F.col("season"), F.lit(" - "), F.col("year")).alias("style_season"))
          .where(F.col("bp_style_number").isNotNull()
                 & F.col("season").isNotNull() & F.col("year").isNotNull()))

bom_latest = (spark.table(bom_source)
              .where(F.col("customer_name") == bom_customer_name)
              .where(F.col("latest_techpack_style_log_id").isNotNull())
              .select("style_no", "customer_department", "style_season",
                      "latest_techpack_style_log_id")
              .dropDuplicates(["style_no", "customer_department", "style_season"]))
bom_log = (spark.table(bom_log_source)
           .where(F.col("custom_fields").isNotNull())
           .select("teckpack_style_log_id", "custom_fields"))
latest_with_fields = (bom_latest.join(
    bom_log,
    bom_latest.latest_techpack_style_log_id == bom_log.teckpack_style_log_id,
    "inner").select(bom_latest.style_no, bom_latest.style_season, bom_log.custom_fields))
joined = (styles.join(
    latest_with_fields,
    (styles.bp_style_number == latest_with_fields.style_no)
    & (styles.style_season == latest_with_fields.style_season),
    "inner").select(styles.bp_style_number, latest_with_fields.custom_fields))


def _suffixless(v):
    return v.rsplit("--", 1)[0] if v and "--" in v else v


verdicts = {}
examples = {}
lakebase_codes = set()


def _record(v, detail):
    verdicts[v] = verdicts.get(v, 0) + 1
    examples.setdefault(v, [])
    if len(examples[v]) < 50:
        examples[v].append(detail)


for r in joined.collect():
    style = r["bp_style_number"]
    # Only Main Fabric / Fabric rows: Lakebase also carries Trim/Label/Stitch
    # rows the BeProduct segment list never had, and counting those as misses
    # would falsely disprove the mapping.
    for detail in bom._extract_bom_table_rows(r["custom_fields"]):
        fields = bom.extract_enrichment_fields(detail)
        group = (fields.get("fabric_group") or "").strip()
        if group not in (bom.SEGMENT_MAIN_FABRIC, bom.SEGMENT_FABRIC):
            continue
        code = fields.get("lf_material_id")
        if code:
            lakebase_codes.add(code.strip())
        k = (style,) + bom.segment_key(fields)
        if k not in truth:
            _record("KEY_ONLY_IN_LAKEBASE",
                    {"style": style, "key": list(k[1:]), "material_code": code})
            continue
        lf, guid = truth[k]
        d = {"style": style, "key": list(k[1:]), "lakebase": code, "beproduct": lf}
        if not code:
            _record("LAKEBASE_BLANK", d)
        elif lf and code.strip() == lf.strip():
            _record("EXACT", d)
        elif lf and code.strip().casefold() == lf.strip().casefold():
            _record("CASE_OR_WS_ONLY", d)
        elif lf and _suffixless(code.strip()) == _suffixless(lf.strip()):
            _record("SUFFIX_STRIP", d)
        else:
            _record("DIFFERENT", d)

comparable = sum(verdicts.get(v, 0) for v in
                 ("EXACT", "CASE_OR_WS_ONLY", "SUFFIX_STRIP", "DIFFERENT"))
agreeing = verdicts.get("EXACT", 0) + verdicts.get("CASE_OR_WS_ONLY", 0)
print(f"  verdicts: {json.dumps(verdicts)}")
print(f"  comparable pairs: {comparable}, agreeing: {agreeing}")
report["step_a"] = {"verdicts": verdicts, "comparable": comparable,
                    "agreeing": agreeing,
                    "examples": {k: v[:10] for k, v in examples.items()}}

# COMMAND ----------

# ── Step B/C/D: the API side ───────────────────────────────────────────────
client = BeProduct(
    client_id=dbutils.secrets.get(scope="beproduct", key="client_id"),
    client_secret=dbutils.secrets.get(scope="beproduct", key="client_secret"),
    refresh_token=dbutils.secrets.get(scope="beproduct", key="refresh_token"),
    company_domain=dbutils.secrets.get(scope="beproduct", key="company_domain"),
)


def lookup(code, take=5):
    """Full match set for one header_number. Never raises."""
    try:
        return list(itertools.islice(client.material.attributes_list(
            filters=[{"field": "header_number", "operator": "Eq",
                      "value": code}]), take)), None
    except Exception as e:  # noqa: BLE001
        return [], f"{type(e).__name__}: {str(e)[:200]}"


print("\nStep B: resolving known-good LF codes through the API …")
step_b = {"checked": 0, "correct": 0, "wrong": [], "multi": [], "missing": [],
          "errors": [], "result_keys": None}
for code, expect_guid in sorted(known_pairs.items()):
    hits, err = lookup(code)
    step_b["checked"] += 1
    if step_b["result_keys"] is None and hits:
        # Dump the shape once so the resolver is written against reality, not
        # a guess about which key holds the id / number / folder.
        step_b["result_keys"] = sorted(hits[0].keys())
        step_b["result_sample"] = {k: hits[0].get(k) for k in
                                   ("id", "headerId", "headerNumber",
                                    "header_number", "number", "folderId")}
    if err:
        step_b["errors"].append({"code": code, "error": err})
    elif not hits:
        step_b["missing"].append(code)
    elif len(hits) > 1:
        step_b["multi"].append({"code": code, "n": len(hits)})
    elif hits[0].get("id") == expect_guid:
        step_b["correct"] += 1
    else:
        step_b["wrong"].append({"code": code, "expected": expect_guid,
                                "got": hits[0].get("id")})
print(f"  {step_b['correct']}/{step_b['checked']} resolved to the EXPECTED GUID")
report["step_b"] = step_b

# COMMAND ----------

print("\nStep C: is the `Eq` filter actually exact? …")
step_c = {}
_sample = next(iter(sorted(known_pairs)), None)
if _sample:
    trunc = _suffixless(_sample)
    for label, probe_code in (("truncated", trunc),
                              ("lowercased", _sample.lower()),
                              ("garbage", "ZZZ-NOT-A-MATERIAL")):
        if probe_code == _sample:
            step_c[label] = "skipped (identical to the real code)"
            continue
        hits, err = lookup(probe_code)
        step_c[label] = {"probe": probe_code, "hits": len(hits), "error": err}
    # A hit on `truncated` is the single most consequential result after Step A:
    # it means `Eq` is a prefix match and the resolver MUST post-filter.
    step_c["eq_is_exact"] = (isinstance(step_c.get("truncated"), dict)
                             and step_c["truncated"].get("hits") == 0)
    step_c["case_sensitive"] = (isinstance(step_c.get("lowercased"), dict)
                                and step_c["lowercased"].get("hits") == 0)
print(f"  {json.dumps(step_c, indent=2)}")
report["step_c"] = step_c

# COMMAND ----------

print("\nStep D: coverage across every distinct Lakebase code …")
todo = sorted(lakebase_codes)[:max_api_codes]
hist = {"0": 0, "1": 0, "2+": 0, "error": 0}
step_d = {"distinct_codes": len(lakebase_codes), "probed": len(todo),
          "not_found": [], "ambiguous": []}
for code in todo:
    hits, err = lookup(code)
    if err:
        hist["error"] += 1
    elif not hits:
        hist["0"] += 1
        step_d["not_found"].append(code)
    elif len(hits) == 1:
        hist["1"] += 1
    else:
        hist["2+"] += 1
        step_d["ambiguous"].append({"code": code, "n": len(hits)})
step_d["histogram"] = hist
print(f"  {json.dumps(hist)}")
report["step_d"] = step_d

# COMMAND ----------

# ── Verdict ─────────────────────────────────────────────────────────────────
# Decision rule: the mapping is PROVEN only if every comparable pair agrees.
# A shared master record is not a place for a 95%-confident key, so ANY
# DIFFERENT pair disproves it.
blockers = []
if verdicts.get("DIFFERENT"):
    blockers.append(f"{verdicts['DIFFERENT']} pair(s) DIFFERENT -- codes disagree")
if verdicts.get("SUFFIX_STRIP"):
    blockers.append(f"{verdicts['SUFFIX_STRIP']} pair(s) match only after suffix "
                    f"strip -- contradicts the --TW anchor, investigate")
if not comparable:
    blockers.append("no comparable pairs at all -- the join found nothing")
if step_b["wrong"]:
    blockers.append(f"{len(step_b['wrong'])} code(s) resolved to the WRONG GUID")
if step_b["checked"] and not step_b["correct"]:
    blockers.append("no known-good code resolved correctly")

report["status"] = "DISPROVEN" if blockers else "PROVEN"
report["blockers"] = blockers
report["notes"] = []
if verdicts.get("CASE_OR_WS_ONLY"):
    report["notes"].append("case/whitespace-only matches exist -- the resolver "
                           "strips but must NOT casefold unless step_c says the "
                           "server is case-insensitive")
if verdicts.get("LAKEBASE_BLANK"):
    report["notes"].append(f"{verdicts['LAKEBASE_BLANK']} segment(s) have a blank "
                           f"**MaterialCode -- a hard ceiling on coverage; these "
                           f"land in Stage 55's `unresolved` bucket")
if step_d.get("ambiguous"):
    report["notes"].append("some codes match several materials -- correctly "
                           "refused, but check the rate is operable")

print("\n" + "=" * 78)
print(f"VERDICT: {report['status']}")
for b in blockers:
    print(f"  ❌ {b}")
for n in report["notes"]:
    print(f"  ⚠  {n}")
print("=" * 78)
print(json.dumps(report, indent=2)[:4000])
dbutils.notebook.exit(json.dumps(report))
