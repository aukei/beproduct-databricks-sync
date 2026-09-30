# Databricks notebook source
"""
v2 Stage 20b -- techpack BOM (Lakebase) -> Delta `bom_segments`.

Materializes the techpack BOM for every in-scope BeProduct style into an
ordinary Delta table, so the rest of the pipeline never has to touch Lakebase
again. Writes NOTHING to DTC.

WALKBACK 2026-09-22 -- this file is CURRENT, not stale
------------------------------------------------------
Between 2026-09-16 and 2026-09-22 this notebook read the BeProduct
`PageBomVariation` API instead (commit 15b122b). The owner reversed that
decision, and this restores the Lakebase source verbatim. Do not "fix" this
file back towards BeProduct on the strength of a doc or comment written during
that week.

The BeProduct parsing layer is NOT deleted -- it sits dormant and unit-tested
in `sync/bom.py` under "SOURCE 2", and `test_wip_plan.py [7l]` still proves both
sources produce an identical plan. Re-switching is therefore a one-notebook
change, exactly as this walkback was. The full snapshot of the BeProduct
implementation is on branch `v2-bomvariation`.

Two things the walkback gives up, both known and accepted:
  * `KTB-00029`'s placements are blank in Lakebase where BeProduct gave
    BODICE / LINING / HEM. The one-way blank guard in
    `bom.plan_style_enrichment()` means the values already written to DTC
    survive -- this loses future corrections, it does not revert live data.
  * `Content` notation returns to "Cotton 97%, Spandex 3%", which disagrees
    with DTC's own trigger ("97% Cotton / 3% Spandex"). `Content` therefore
    MUST stay write-once (`material_fill_if_blank_columns`), or the two systems
    overwrite each other and open a DTC write window on every run.

What this source does NOT carry: the BOM `materialId` GUID. Stage 55's reverse
push resolved its write target through it, so that stage is disarmed
(`run_customer_code_push=false`) pending an LF-Material-Code -> materialId
resolver. See AGENTS.md.

Why this is a separate notebook, not folded into the transform
---------------------------------------------------------------
The v1 transform (`p1p7_beproduct_to_dtc_transform.py`, 839 lines) already
produces `beproduct_to_dtc_staging` correctly and needs no change for v2.
Copying it just to bolt on a BOM read would duplicate every field mapping,
season-code lookup, lifecycle gate and validation rule in it -- four SSOT
violations waiting to drift. This notebook adds the one genuinely new input as
its own small, independently-testable step that runs in PARALLEL with it.

Why staging is NOT at style x color x material grain
-----------------------------------------------------
An earlier draft of docs/PIPELINE.md said the v2 transform would emit the final
style x color x material grain. That is **not possible, and not desirable**:

  1. The material fan-out depends on what ALREADY EXISTS in DTC -- which
     "Fabric" segments a given colorway is already represented by. The
     transform has no live DTC state, so it cannot compute it. Only
     `wip_plan.compute_request_plan()`, which sees the live rows, can.
  2. `phase1.compute_upsert()` treats a repeated (BP Style#, Color / Wash) as
     a `duplicate_bp_key` exception. Feeding it material-grain rows would make
     every multi-material style raise.

So: staging stays at **style x color**, this table holds **style -> BOM**, and
the material dimension is resolved at PLAN time against live DTC rows. See
docs/PIPELINE.md Stage 20.

Source (unchanged from v1 Phase 10's "2nd revision", 2026-09-09) -- two hops:

    customer_teckpack_style_latest   resolves WHICH log row is current
        .latest_techpack_style_log_id
          -> customer_teckpack_style_log.teckpack_style_log_id
             .custom_fields -> xts_data -> TECH_PACK_EXTRACTION
                            -> Table[Type="BOM"]

joined onto `ktb_styles` on (bp_style_number = style_no,
season || ' - ' || year = style_season), INNER throughout both hops. A style
with no match is simply not processed this run -- never an error, never a
revert.

`alb_tpm_*` are Lakebase databases registered in Unity Catalog and are
queryable ONLY from serverless compute. In v1 that forced Phase 10 onto its own
serverless task; in v2 the whole job is serverless, so this costs nothing
(live-confirmed 2026-09-14, run 66807905429726: an ORDINARY serverless task
reads them fine).

Output: `<catalog>.<schema>.bom_segments`, fully overwritten each run. The table
KEPT the name it was given during the BeProduct week; only its COLUMN SHAPE
reverts (`custom_fields` / `parse_error`, not `segments_json` / `error`). Both
downstream consumers sniff which shape they were handed, so neither needed a
change -- see `v2_wip_push.py` and `p9a_build_costing_chart.py`.
"""

# COMMAND ----------

import sys

# ── Python module root ──────────────────────────────────────────────────────
# Parameterized, never hardcoded: v2 deploys its modules under its own
# workspace root so checking out the v2 branch can never change what the live
# v1 job imports. See docs/MIGRATION_V1_V2.md ("Workspace isolation").
_DEFAULT_MODULE_PATH = "/Workspace/Repos/beproduct-sync/DTC/python"
dbutils.widgets.text("module_path", _DEFAULT_MODULE_PATH, "Python module root")
_MODULE_PATH = (dbutils.widgets.get("module_path") or "").strip() or _DEFAULT_MODULE_PATH
for _p in (_MODULE_PATH, _MODULE_PATH.replace("/DTC/", "/dtc/")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import json
from datetime import datetime, timezone

from pyspark.sql import functions as F, Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType,
)

from sync import bom

# COMMAND ----------

dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("folder_name", "TEST KTB", "BeProduct folder")
dbutils.widgets.text("bom_catalog", "alb_tpm_uat", "Lakebase catalog (NOT derived from dtc_environment)")
dbutils.widgets.text("bom_schema", "public", "Lakebase schema")
dbutils.widgets.text("bom_table", "customer_teckpack_style_latest", "Resolves latest_techpack_style_log_id")
dbutils.widgets.text("bom_log_table", "customer_teckpack_style_log", "custom_fields -- the actual BOM source")
dbutils.widgets.text("bom_customer_name", "KONTOOR", "Scoping/perf pre-filter only")
dbutils.widgets.text("bom_segments_table", "bom_segments", "Output table")
dbutils.widgets.text("run_bom", "true", "Run the BOM join (false = no-op)")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
folder_name = dbutils.widgets.get("folder_name")
bom_catalog = dbutils.widgets.get("bom_catalog")
bom_schema = dbutils.widgets.get("bom_schema")
bom_table = dbutils.widgets.get("bom_table")
bom_log_table = dbutils.widgets.get("bom_log_table")
bom_customer_name = dbutils.widgets.get("bom_customer_name")
out_table = f"{catalog}.{schema}.{dbutils.widgets.get('bom_segments_table')}"

styles_table = f"{catalog}.{schema}.ktb_styles"
bom_source = f"{bom_catalog}.{bom_schema}.{bom_table}"
bom_log_source = f"{bom_catalog}.{bom_schema}.{bom_log_table}"
now = datetime.now(timezone.utc)

print("=" * 78)
print("v2 Stage 20b -- techpack BOM -> Delta")
print("=" * 78)
print(f"  styles     : {styles_table}  (folder_name={folder_name!r})")
print(f"  BOM latest : {bom_source}")
print(f"  BOM log    : {bom_log_source}  (custom_fields)")
print(f"  customer   : {bom_customer_name!r}")
print(f"  output     : {out_table}")

# Checked HERE, not via a condition task: Databricks propagates a condition
# task's EXCLUDED outcome to every downstream dependent unconditionally,
# ignoring run_if. See docs/PIPELINE.md design rule 4.
if (dbutils.widgets.get("run_bom") or "true").strip().lower() != "true":
    print("\nrun_bom=false -- skipping. Downstream stages will see whatever this")
    print("table already holds; they never revert on missing BOM data.")
    dbutils.notebook.exit(json.dumps({"status": "SKIPPED_run_bom_false"}))

# COMMAND ----------

# ── Step 1: styles ⋈ BOM 'latest' ⋈ BOM 'log' (INNER throughout) ────────────
print("\nStep 1: joining ktb_styles <-> BOM …")

# The season is matched on a NORMALISED key, not the literal string. Lakebase
# changed its `style_season` from "Spring - 2028" to "Spring 2028" (found
# 2026-09-28), and the literal `season || ' - ' || year` join then matched
# 0 of 10 styles with no error -- bom_segments came out EMPTY and BOM
# enrichment silently stopped. Lowercase + alphanumerics only makes
# "Spring - 2028", "Spring 2028" and "SPRING-2028" all "spring2028".
def _season_key(c):
    return F.regexp_replace(F.lower(c), "[^a-z0-9]", "")

styles = (spark.table(styles_table)
          .where(F.col("folder_name") == folder_name)
          .select(
              F.col("bp_style_number"),
              F.concat(F.col("season"), F.lit(" - "), F.col("year")).alias("style_season"),
          )
          .withColumn("season_key", _season_key(F.col("style_season")))
          .where(F.col("bp_style_number").isNotNull()
                 & F.col("season").isNotNull() & F.col("year").isNotNull()))
n_styles = styles.count()
print(f"  BeProduct styles with a valid season/year : {n_styles}")

# `customer_teckpack_style_latest` guarantees at most one row per
# (style_no, customer_name, customer_department, style_season). The
# dropDuplicates is a near-zero-cost safety net in case that is ever violated
# for a customer/environment this pipeline has not seen; it should be a no-op.
bom_latest = (spark.table(bom_source)
              .where(F.col("customer_name") == bom_customer_name)
              .where(F.col("latest_techpack_style_log_id").isNotNull())
              .select("style_no", "customer_department", "style_season",
                      "latest_techpack_style_log_id")
              .withColumn("season_key", _season_key(F.col("style_season")))
              # Two spellings of one season now share a key -- keep the newest
              # log row, never both (both would duplicate every BOM segment).
              .withColumn("_rk", F.row_number().over(
                  Window.partitionBy("style_no", "customer_department", "season_key")
                        .orderBy(F.col("latest_techpack_style_log_id").desc())))
              .where(F.col("_rk") == 1).drop("_rk"))
n_latest = bom_latest.count()
print(f"  BOM 'latest' rows for {bom_customer_name!r}            : {n_latest}")

# Second hop: latest_techpack_style_log_id -> teckpack_style_log_id is a real
# FK (one specific log row per "latest" row), so no further dedup is needed.
bom_log = (spark.table(bom_log_source)
           .where(F.col("custom_fields").isNotNull())
           .select(F.col("teckpack_style_log_id"), F.col("custom_fields")))

latest_with_fields = (bom_latest.join(
    bom_log,
    on=bom_latest.latest_techpack_style_log_id == bom_log.teckpack_style_log_id,
    how="inner",
).select(bom_latest.style_no, bom_latest.customer_department,
         bom_latest.style_season, bom_latest.season_key, bom_log.custom_fields))

joined = (styles.join(
    latest_with_fields,
    on=(styles.bp_style_number == latest_with_fields.style_no)
       & (styles.season_key == latest_with_fields.season_key),
    how="inner",
).select(styles.bp_style_number, latest_with_fields.customer_department,
         latest_with_fields.style_season, latest_with_fields.custom_fields))

matched = joined.collect()
print(f"  Matched (style x BOM) pairs              : {len(matched)}")
print(f"  Styles with NO BOM this run              : {n_styles - len(matched)}"
      f"   (not an error -- they keep their placeholders and are retried next run)")

# COMMAND ----------

# ── Step 2: parse + diagnose ────────────────────────────────────────────────
# Parsing here (rather than only at push time) means a malformed payload shows
# up in THIS task's output, attributed to a specific style, instead of silently
# degrading a contribution three stages later.
print("\nStep 2: parsing BOM segments (diagnostics) …")

rows = []
n_main = n_no_main = n_unparseable = 0
fabric_total = 0
no_main_styles, unparseable_styles = [], []

for r in matched:
    style = r["bp_style_number"]
    cf = r["custom_fields"]
    cf_str = cf if isinstance(cf, str) else json.dumps(cf)

    main_count = fabric_count = 0
    parse_error = None
    try:
        parsed = bom.parse_bom_segments(cf)
        # main_fabric is an Optional[dict] (at most one by construction);
        # fabric_list holds every "Fabric" segment in document order.
        main_count = 1 if parsed.main_fabric else 0
        fabric_count = len(parsed.fabric_list)
    except Exception as e:  # noqa: BLE001
        parse_error = f"{type(e).__name__}: {e}"
        n_unparseable += 1
        if len(unparseable_styles) < 20:
            unparseable_styles.append(f"{style}: {parse_error}")

    if parse_error is None:
        if main_count:
            n_main += 1
        else:
            n_no_main += 1
            if len(no_main_styles) < 20:
                no_main_styles.append(style)
        fabric_total += fabric_count

    rows.append((style, r["customer_department"], r["style_season"], cf_str,
                 int(main_count), int(fabric_count), parse_error,
                 bom_catalog, now))

print(f"  Styles with a Main Fabric segment        : {n_main}")
print(f"  Styles with BOM but NO Main Fabric       : {n_no_main}"
      f"   (zero material actions for these -- never a revert)")
print(f"  Styles whose custom_fields failed to parse: {n_unparseable}")
print(f"  Total 'Fabric' segments across all styles : {fabric_total}")
if no_main_styles:
    print(f"    no Main Fabric: {', '.join(no_main_styles)}"
          + (" …" if n_no_main > len(no_main_styles) else ""))
for u in unparseable_styles:
    print(f"    ⚠ unparseable {u}")

# COMMAND ----------

# ── Step 3: write ───────────────────────────────────────────────────────────
print(f"\nStep 3: writing {out_table} …")

SCHEMA = StructType([
    StructField("bp_style_number", StringType()),
    StructField("customer_department", StringType()),
    StructField("style_season", StringType()),
    # The raw payload. Kept verbatim rather than pre-parsed into segments so
    # the single source of truth for BOM parsing stays `sync/bom.py` -- the
    # push re-parses it with the very same function the tests cover.
    StructField("custom_fields", StringType()),
    StructField("main_fabric_count", IntegerType()),
    StructField("fabric_count", IntegerType()),
    StructField("parse_error", StringType()),
    StructField("source_catalog", StringType()),
    StructField("extracted_at", TimestampType()),
])

(spark.createDataFrame(rows, SCHEMA)
 .write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(out_table))

print(f"  ✅ wrote {len(rows)} row(s)")

summary = {
    "status": "OK",
    "table": out_table,
    "styles_considered": n_styles,
    "styles_matched": len(matched),
    "styles_with_main_fabric": n_main,
    "styles_without_main_fabric": n_no_main,
    "styles_unparseable": n_unparseable,
    "fabric_segments_total": fabric_total,
    "source_catalog": bom_catalog,
}
print("\n" + json.dumps(summary, indent=2))

# The Jobs API returns NO notebook stdout for serverless runs -- only this exit
# value (live-confirmed 2026-09-14). Anything that must be readable outside the
# Databricks UI has to travel here.
dbutils.notebook.exit(json.dumps(summary))
