# Databricks notebook source
"""
Phase 9a — Build Costing Chart
================================

Joins the DTC WIP master chart with the DTC LinePlan to produce a denormalized
"Costing Chart" at the Style × Color × Vendor/Factory level.

Inputs
------
  lft.beproduct.dtc_wip_<customer>         WIP rows (data_json contains all fields)
  lft.beproduct.dtc_lineplan_<customer>    LinePlan rows (data_json)

Join key
--------
  WIP "Lineplan Ref #"  =  LinePlan "Lineplan Ref #"
  (note: WIP column is plain "Lineplan Ref #"; spec said "(GC)" but actual
   DTC column name has no suffix — confirmed live 2026-07-17)

  INNER JOIN (changed from LEFT 2026-09-01, owner decision): a WIP row with
  a blank "Lineplan Ref #", or one that doesn't match any LinePlan row, is
  DROPPED entirely rather than surfacing in costing_chart with null
  order_quantity/target_ldp/target_fob. costing_chart should only ever
  contain rows with real, matched LinePlan data.

Transpose
---------
  Each WIP row (style × color) that survives the INNER JOIN above is
  expanded into up to 4 costing rows by exploding the four vendor/factory
  pairs:
    slot  vendor_col                factory_col
    ────  ──────────────────────    ──────────────────────────
    Main  "Main Vendor (Sampling)"  "Main Factory (Sampling)"
    1     "Vendor 1"                "Factory 1"
    2     "Vendor 2"                "Factory 2"
    3     "Vendor 3"                "Factory 3"
  Slots where vendor is blank are STILL dropped independently of the join
  above (a style can have a matched Lineplan Ref# but zero vendors assigned
  yet, producing zero costing rows for that style — this is expected, not a
  join bug). A style may produce 0–4 rows.

HTS / Duty / Tariff
-------------------
  Read from the corresponding WIP fields per slot.
  "Tariff Rate" is NOT currently in WIP view — Phase 9b will fill it from
  NT Orbit Duty Tools.  The column is present (null) as a placeholder.

Output
------
  lft.beproduct.costing_chart   — fully overwritten on every run

Costing chart schema (field → source):
  customer            from WIP request_reference
  season_code         from WIP
  brand               from WIP data_json "Brand"
  bp_style_no         from WIP data_json "BP Style#"
  lf_style_no         from WIP data_json "LF Style#"
  legacy_code         from WIP data_json "Legacy Code"
  style_description   from WIP data_json "Style Description"
  color_name          from WIP data_json "Color / Wash"
  lineplan_ref        from WIP data_json "Lineplan Ref #"
  fabric_content      from WIP data_json "Content"  (corrected 2026-09-03 --
                       was mistakenly "Fabric Group"; "Content" is a
                       DIFFERENT column, populated by BOTH DTC's own
                       internal trigger AND Phase 10 -- Phase 10 briefly
                       stopped writing it (2026-09-09 morning) then
                       REINSTATED it the same day, "2nd revision", from a
                       genuinely reliable new source (`**MaterialContent`);
                       see the filter note below and sync/bom.py's docstring)
  fabric_type         from WIP data_json "Fabric Type" (new 2026-09-03)
  gender              from WIP data_json "Gender"
  class               from WIP data_json "Class"
  sub_class           from WIP data_json "Sub Class"
  supplier_type       GENERATED from WIP structure: "Main" | "1" | "2" | "3"
                       (which of the 4 vendor/factory column-pairs this row
                       came from -- per original spec "Generated from Master
                       Chart data"; corrected 2026-09-01: this is NOT the
                       LinePlan "INTERNAL/ SOURCED" field -- that value does
                       not flow into costing_chart at all)
  supplier            vendor column for the slot
  factory             factory column for the slot
  production_country  from WIP per-slot production country field
  order_quantity      from LinePlan "PROJECTED VOLUME (season)"
  target_ldp          from LinePlan "TARGET SAP w/ Tariff impact"
  target_fob          from LinePlan "TARGET FOB"
  hts_code            from WIP per-slot HTS field  (Phase 9b: NT Orbit fallback)
  duty_rate_us        from WIP per-slot duty (US)  (Phase 9b: NT Orbit fallback)
  duty_rate_ca        from WIP per-slot duty (CA)  (Phase 9b: NT Orbit fallback)
   duty_rate_mx        from WIP per-slot duty (MX)  (Phase 9b: NT Orbit fallback)
   tariff_rate         NULL placeholder              (Phase 9b: from NT Orbit)
   updated_at          current timestamp
   material_no         from WIP data_json "Mill Fabric Article #" (new 2026-09-03 --
                        see "Costing chart key" below)

Costing chart key (REVISED 2026-09-03, owner spec)
----------------------------------------------------
The match/merge key for a costing chart row is
`[bp_style_no, lineplan_ref, material_no]` (plus `supplier_type`/`supplier`/
`factory` to distinguish the 4 transposed vendor slots — see `COSTING_KEY`
in `p9b1_compute_duty_rates.py`), NOT `fabric_content` (an earlier
same-day iteration used `fabric_content`, but "Content" is free text that
MULTIPLE distinct `material_no` values can legitimately share, and multiple
STYLES can share one `lineplan_ref` — neither `fabric_content` nor
`lineplan_ref` alone is a reliable material-level discriminator).
`material_no` (Phase 10's own "Mill Fabric Article #") is the real,
unambiguous per-material identifier.

Fabric-details completeness filter (REVISED 2026-09-09, owner spec — see
below; supersedes the 2026-09-03 revision, kept for history)
----------------------------------------------------------------------
Originally gated on WIP's "Content"/"Fabric Type" columns (populated by a
DTC-internal trigger polling "Mill Fabric Article #") both being non-blank
— but live-confirmed 2026-09-03 that trigger's timing/conditions in UAT are
unreliable (every KTB test row still blank days after Mill Fabric Article #
was set), which blocked Phase 9a entirely. Fixed at the SOURCE at the time:
Phase 10 started writing "Content" itself from the SAME BOM segment's
`material_name`, removing the dependency on DTC's own trigger for that
column, and the completeness filter was changed to gate on `material_no`
(Mill Fabric Article #) instead of Content/Fabric Type.

**REVERSED 2026-09-09 (owner spec)**: Phase 10 writing "Content" from
`material_name` turned out to push material-CODE-shaped garbage into DTC
for a newer batch of test styles (live-discovered 2026-09-08 — see
`dtc/python/sync/bom.py`'s module docstring for the full history). Phase 10
now NEVER writes "Content" at all — it is left entirely to DTC's own
(previously-unreliable-in-UAT) trigger. Consequently, the completeness
filter gains BACK an explicit "Content" (WIP `fabric_content`) non-blank
requirement, layered on TOP of the still-required `material_no` check (not
a replacement for it — `material_no` remains the costing-chart key
component and is still required independently): a "Main Fabric" WIP row
whose "Content" cell is still blank this round (DTC's trigger hasn't caught
up yet, or hasn't run at all) is EXCLUDED from `costing_chart` THIS ROUND,
even though it's otherwise a fully-qualifying "Main Fabric" row with a real
`material_no`. This is consistent with the "never revert, just wait for the
next round" philosophy used elsewhere in this pipeline (see Step 3's
LinePlan-ref handling) — NOT a permanent exclusion: once DTC's trigger (or
a manual edit) eventually fills in Content, that row will start appearing
in `costing_chart` on a later run without any code change. Expect this to
significantly reduce `costing_chart`'s row count vs. the 2026-09-03-era
behavior, since DTC's Content trigger was already confirmed unreliable in
UAT — this is an accepted, intentional trade-off (owner decision), not a
regression to fix. `fabric_type` ("Fabric Type") is still extracted and
carried through to `costing_chart.fabric_type` for traceability, but
remains NOT part of the filter or the NT Orbit description string (it
remains solely DTC-trigger-populated and may still be blank in practice).

**REINSTATED again, same day 2026-09-09 ("2nd revision" of the BOM source,
owner spec)**: the BOM developer's push-back on `customer_teckpack_style_
latest` led to a NEW, genuinely reliable Content source
(`customer_teckpack_style_log.custom_fields` -> `**MaterialContent`, via a
real dedicated per-material column, NOT the overloaded `material_name`
that caused the 2026-09-08 corruption) — see `sync/bom.py`'s module
docstring for the full path/structure. Phase 10 writes "Content" from this
source again. The completeness filter's LOGIC in this notebook is
UNCHANGED (still gates on `fabric_content` non-blank, same as the
2026-09-09-morning paragraph above) — only the reason a row's Content is
populated changed (a real Phase 10 write again, not solely DTC's trigger).
Live-confirmed 2026-09-09 (correcting an earlier same-day investigation
mistake that used the wrong JSON path): 14 of 16 KTB test styles already
have real Main Fabric BOM data via this new source, so this filter is
expected to start passing for most of them on Phase 10's next real run,
not stay permanently blocked.

**"Main Fabric" only (added 2026-09-07, project team decision)**: ONLY a
style's "Main Fabric" WIP row (`fabric_group == "Main Fabric"`) enters
costing_chart at all. The "Fabric" segment duplicate rows Phase 10 creates
(one extra physical WIP row per "Fabric" segment — see `sync/bom.py`) are
EXCLUDED entirely, never reaching costing_chart or NT Orbit. Consequently,
Duty/Tariff rate/HTS Code (Phase 9b) are only ever computed and pushed back
to WIP for "Main Fabric" rows — "Fabric" segment rows never receive them.

Phase 9b hook
-------------
  After this notebook runs, Phase 9b will:
    1. For rows where hts_code / duty_rate_* / tariff_rate are null,
       call the NT Orbit Duty Tools API (with caching).
    2. Fill in the values on costing_chart.
    3. Push changed values back to the corresponding WIP "HTS code" / "Duty Rate"
       fields (the per-slot WIP columns).
"""

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

from functools import reduce
from datetime import datetime, timezone

from sync import duty
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import StringType, StructType, StructField, TimestampType, LongType

# ── Parameters ────────────────────────────────────────────────────────────────
dbutils.widgets.text("catalog",   "lft",       "Catalog")
dbutils.widgets.text("schema",    "beproduct", "Schema")
dbutils.widgets.text("customer",  "KTB",       "Customer code")
# Added 2026-09-10 (owner spec) for Step 4c's direct persistent-cache fill --
# see that step's docstring below.
dbutils.widgets.text("duty_cache_table", "lft.beproduct.nt_orbit_duty_cache",
                     "Persistent cross-run NT Orbit result cache (fully-qualified)")
dbutils.widgets.text("cache_ttl_days", str(duty.DEFAULT_CACHE_TTL_DAYS),
                     "Days before a cached lookup is considered too stale to reuse here")

catalog  = dbutils.widgets.get("catalog")
schema   = dbutils.widgets.get("schema")
customer = dbutils.widgets.get("customer").strip().upper()
duty_cache_table = dbutils.widgets.get("duty_cache_table").strip()
cache_ttl_days   = int(dbutils.widgets.get("cache_ttl_days") or duty.DEFAULT_CACHE_TTL_DAYS)

wip_table      = f"{catalog}.{schema}.dtc_wip_{customer.lower()}"
lineplan_table = f"{catalog}.{schema}.dtc_lineplan_{customer.lower()}"
output_table   = f"{catalog}.{schema}.costing_chart"

now = datetime.now(timezone.utc)

print("=" * 72)
print("PHASE 9a — Build Costing Chart")
print("=" * 72)
print(f"  WIP input     : {wip_table}")
print(f"  LinePlan input: {lineplan_table}")
print(f"  Output        : {output_table}")

# COMMAND ----------

# ── Helper: extract a field from data_json ────────────────────────────────────
def jcol(json_col: str, field_name: str, alias: str):
    """get_json_object wrapper using bracket notation (handles spaces + special chars)."""
    return F.get_json_object(F.col(json_col), f"$['{field_name}']").alias(alias)

# COMMAND ----------

# ── Step 1: Extract WIP fields from data_json ─────────────────────────────────
print("\nStep 1: Extracting WIP fields …")
wip_raw = spark.table(wip_table)
print(f"  WIP rows: {wip_raw.count()}")

wip = wip_raw.select(
    # Routing / key
    F.col("customer"),
    F.col("season_code"),
    F.col("request_reference"),
    # Style identity (from fixed columns + data_json)
    F.col("bp_style_number").alias("bp_style_no"),
    F.col("lf_style_number").alias("lf_style_no"),
    F.col("color_wash").alias("color_name"),
    jcol("data_json", "Legacy Code",       "legacy_code"),
    jcol("data_json", "Style Description", "style_description"),
    jcol("data_json", "Brand",             "brand"),
    jcol("data_json", "Content",           "fabric_content"),   # corrected 2026-09-03 (was "Fabric Group")
    jcol("data_json", "Fabric Type",       "fabric_type"),      # new 2026-09-03 -- traceability only, NOT the filter/key
    jcol("data_json", "Mill Fabric Article #", "material_no"),  # new 2026-09-03 -- the real costing-chart key + filter column
    jcol("data_json", "Fabric Group",      "fabric_group"),     # new 2026-09-07 -- the "Main Fabric" only filter, see Step 1b
    jcol("data_json", "Gender",            "gender"),
    jcol("data_json", "Class",             "class_"),           # avoid Python keyword
    jcol("data_json", "Sub Class",         "sub_class"),
    jcol("data_json", "Lineplan Ref #",    "lineplan_ref"),
    # Vendor / Factory pairs (4 slots)
    jcol("data_json", "Main Vendor (Sampling)",   "vendor_main"),
    jcol("data_json", "Main Factory (Sampling)",  "factory_main"),
    jcol("data_json", "Vendor 1",                 "vendor_1"),
    jcol("data_json", "Factory 1",                "factory_1"),
    jcol("data_json", "Vendor 2",                 "vendor_2"),
    jcol("data_json", "Factory 2",                "factory_2"),
    jcol("data_json", "Vendor 3",                 "vendor_3"),
    jcol("data_json", "Factory 3",                "factory_3"),
    # Production country per slot
    jcol("data_json", "Factory Production Country for Main Factory", "prod_country_main"),
    jcol("data_json", "Factory Production Country for Factory 1",    "prod_country_1"),
    jcol("data_json", "Factory Production Country for Factory 2",    "prod_country_2"),
    jcol("data_json", "Factory Production Country for Factory 3",    "prod_country_3"),
    # HTS code per slot
    jcol("data_json", "Main Factory HTS Code",   "hts_main"),
    jcol("data_json", "Factory 1 - HTS code",    "hts_1"),
    jcol("data_json", "Factory 2 - HTS code",    "hts_2"),
    jcol("data_json", "Factory 3 - HTS code",    "hts_3"),
    # Duty Rate (US) per slot
    jcol("data_json", "Main Factory Duty Rate (US)",  "duty_us_main"),
    jcol("data_json", "Factory 1 - Duty Rate (US)",   "duty_us_1"),
    jcol("data_json", "Factory 2 - Duty Rate (US)",   "duty_us_2"),
    jcol("data_json", "Factory 3 - Duty Rate (US)",   "duty_us_3"),
    # Duty Rate (CA) per slot
    jcol("data_json", "Main Factory Duty Rate (CA)",  "duty_ca_main"),
    jcol("data_json", "Factory 1 - Duty Rate (CA)",   "duty_ca_1"),
    jcol("data_json", "Factory 2 - Duty Rate (CA)",   "duty_ca_2"),
    jcol("data_json", "Factory 3 - Duty Rate (CA)",   "duty_ca_3"),
    # Duty Rate (MX) per slot
    jcol("data_json", "Main Factory Duty Rate (MX)",  "duty_mx_main"),
    jcol("data_json", "Factory 1 - Duty Rate (MX)",   "duty_mx_1"),
    jcol("data_json", "Factory 2 - Duty Rate (MX)",   "duty_mx_2"),
    jcol("data_json", "Factory 3 - Duty Rate (MX)",   "duty_mx_3"),
    # Tariff Rate — NOT in WIP view; NULL placeholder for Phase 9b
    # "Main Factory Tariff rate", "Factory 1 - Tariff rate" etc. do not exist yet
)

print(f"  WIP columns extracted: {len(wip.columns)}")

# COMMAND ----------

# ── Step 1b: Drop WIP rows Phase 10 hasn't enriched yet (or DTC hasn't
#             filled Content for) yet ────────────────────────────────────────
# See module docstring "Fabric-details completeness filter" for the full
# history. Four independent conditions, ALL required (AND):
#   1. `material_no` (Mill Fabric Article #) non-blank -- the real
#      completeness signal that Phase 10 has assigned a material, and also
#      the costing-chart key component (see "Costing chart key" above).
#   2. `bp_style_no` non-blank -- excludes legacy "(BACKUP)"-named WIP
#      request pollution (added 2026-09-07; see AGENTS.md decisions log).
#   3. `fabric_content` (WIP "Content") non-blank -- RE-ADDED 2026-09-09
#      (owner spec), now layered on top of #1 rather than replacing it.
#      Phase 10 no longer writes "Content" at all (see `sync/bom.py` --
#      REVERSED 2026-09-09 after live-discovering it could push material-
#      CODE-shaped garbage there), so this is now a genuine "wait for DTC's
#      own Content-population trigger to catch up" gate: a row is excluded
#      from costing_chart THIS ROUND if Content is still blank, even if it's
#      otherwise a fully-qualifying "Main Fabric" row with a real
#      material_no. Not a permanent exclusion -- consistent with this
#      pipeline's "never revert, just wait for a later round" philosophy
#      (see Step 3's LinePlan-ref handling): once Content is eventually
#      filled (by DTC's trigger, or a manual edit), the row starts appearing
#      in costing_chart on a later run with no code change needed. The
#      `!= "Main Fabric"` sub-check guards against the literal Fabric Group
#      placeholder string leaking into Content (a real, now-historical bug
#      -- see AGENTS.md's "fabric_content was reading the WRONG WIP column"
#      -- kept as cheap defense-in-depth even though Phase 10 can no longer
#      cause it directly).
#   4. `fabric_group == "Main Fabric"` -- added 2026-09-07 (owner spec): ONLY
#      a style's "Main Fabric" WIP row enters costing_chart at all. The
#      "Fabric" segment duplicate rows Phase 10 creates (see `sync/bom.py`)
#      are excluded entirely, never reaching costing_chart or NT Orbit.
#      Duty/Tariff rate/HTS Code (Phase 9b) are therefore only ever computed
#      for "Main Fabric" rows.
#
# Reported as separate per-reason counts below (not just one aggregate) so
# the Content-completeness gate's real-world impact is visible/auditable,
# since it is now expected to be the dominant reason for exclusion (DTC's
# own Content trigger was already confirmed unreliable in UAT).
print("\nStep 1b: Filtering out WIP rows with no material_no, no bp_style_no, "
      "blank/placeholder Content, or a non-'Main Fabric' fabric_group …")
wip_before_fabric_filter = wip.count()

cond_material_no = F.col("material_no").isNotNull() & (F.trim(F.col("material_no")) != "")
cond_bp_style_no = F.col("bp_style_no").isNotNull() & (F.trim(F.col("bp_style_no")) != "")
cond_content = (F.col("fabric_content").isNotNull() & (F.trim(F.col("fabric_content")) != "")
                & (F.trim(F.col("fabric_content")) != "Main Fabric"))
cond_main_fabric = F.trim(F.col("fabric_group")) == "Main Fabric"

dropped_no_material_no = wip.filter(~cond_material_no).count()
dropped_no_bp_style_no = wip.filter(cond_material_no & ~cond_bp_style_no).count()
dropped_no_content = wip.filter(cond_material_no & cond_bp_style_no & ~cond_content).count()
dropped_not_main_fabric = wip.filter(
    cond_material_no & cond_bp_style_no & cond_content & ~cond_main_fabric).count()

wip = wip.filter(cond_material_no & cond_bp_style_no & cond_content & cond_main_fabric)
dropped_incomplete_fabric = wip_before_fabric_filter - wip.count()

print(f"  WIP rows before filter        : {wip_before_fabric_filter}")
print(f"  WIP rows after filter         : {wip.count()}")
print(f"  Dropped total                 : {dropped_incomplete_fabric}")
print(f"    - blank material_no         : {dropped_no_material_no}")
print(f"    - blank bp_style_no         : {dropped_no_bp_style_no}")
print(f"    - blank/placeholder Content : {dropped_no_content}  "
      f"(Phase 10 writes Content again as of 2026-09-09 -- this should shrink "
      f"once styles are re-enriched from the new custom_fields source)")
print(f"    - fabric_group != 'Main Fabric' (i.e. a 'Fabric' segment row) : "
      f"{dropped_not_main_fabric}")

# COMMAND ----------

# ── Step 2: Extract LinePlan fields from data_json ────────────────────────────
print("\nStep 2: Extracting LinePlan fields …")
lp_raw = spark.table(lineplan_table)
print(f"  LinePlan rows: {lp_raw.count()}")

# Per project team decision (2026-09-01): the project team maintains MULTIPLE
# DTC LinePlan requests with NO naming convention (e.g. season/backup naming
# is not a reliable filter) — this pull deliberately has NO name-pattern
# filter and pulls every active request. Uniqueness of "Lineplan Ref #" ACROSS
# ALL requests is a HUMAN-enforced invariant, not a code-enforced one. Since
# F.first(ignorenulls=True) below would otherwise silently pick an arbitrary
# winner on a conflict, detect and loudly warn on any ref whose plan values
# actually disagree across rows/requests, so a human can catch and fix it.
conflict_check = (lp_raw
    .groupBy("lineplan_ref")
    .agg(
        F.collect_set("request_reference").alias("requests"),
        F.countDistinct(F.coalesce(F.col("projected_volume"), F.lit("~null~"))).alias("n_qty"),
        F.countDistinct(F.coalesce(F.col("target_ldp"),       F.lit("~null~"))).alias("n_ldp"),
        F.countDistinct(F.coalesce(F.col("target_fob"),       F.lit("~null~"))).alias("n_fob"),
    )
    .filter((F.col("n_qty") > 1) | (F.col("n_ldp") > 1) | (F.col("n_fob") > 1))
)
conflicts = conflict_check.collect()
if conflicts:
    print(f"\n⚠️  WARNING: {len(conflicts)} 'Lineplan Ref #' value(s) have CONFLICTING "
          f"order_quantity/target_ldp/target_fob across rows/requests — human-in-the-loop "
          f"uniqueness appears to be violated. An arbitrary non-null value is being used "
          f"below; please ask the DTC LinePlan owner(s) to reconcile these refs:")
    for c in conflicts:
        print(f"     {c['lineplan_ref']}  (found in requests: {c['requests']})")

lp = (lp_raw
    .select(
        F.col("lineplan_ref"),                        # already a fixed column
        F.col("projected_volume").alias("order_quantity"),
        F.col("target_ldp"),
        F.col("target_fob"),
        # NOTE (corrected 2026-09-01): LinePlan's "INTERNAL/ SOURCED" field is
        # intentionally NOT selected here. The original spec's "Supplier Type"
        # is "Generated from Master Chart [WIP] data" -- i.e. which of the 4
        # vendor/factory column-pairs a row came from -- NOT this LinePlan
        # business classification, which does not flow into costing_chart at
        # all. See the "supplier_type" column built in Step 4 below.
    )
    # LinePlan may have multiple rows per lineplan_ref (different colors/regions,
    # or -- per the human-in-the-loop policy above -- different requests); use
    # the first non-null aggregate per ref as the plan values. See the conflict
    # check above for cases where this arbitrary pick actually matters.
    .groupBy("lineplan_ref")
    .agg(
        F.first("order_quantity", ignorenulls=True).alias("order_quantity"),
        F.first("target_ldp",     ignorenulls=True).alias("target_ldp"),
        F.first("target_fob",     ignorenulls=True).alias("target_fob"),
    )
)
print(f"  LinePlan distinct refs: {lp.count()}")

# COMMAND ----------

# ── Step 3: Join WIP + LinePlan on Lineplan Ref # ─────────────────────────────
# INNER JOIN (changed from LEFT 2026-09-01, owner decision): costing_chart
# should ONLY contain WIP rows that actually have a matching LinePlan row --
# a WIP row with a blank/unmatched "Lineplan Ref #" is dropped entirely
# rather than surfacing with null order_quantity/target_ldp/target_fob.
print("\nStep 3: Joining WIP + LinePlan on 'Lineplan Ref #' (INNER) …")
wip_with_ref = wip.filter(F.col("lineplan_ref").isNotNull() & (F.trim(F.col("lineplan_ref")) != ""))
dropped_no_ref = wip.count() - wip_with_ref.count()
joined = wip_with_ref.join(lp, on="lineplan_ref", how="inner")
joined_count = joined.count()
print(f"  WIP rows dropped (blank Lineplan Ref #): {dropped_no_ref}")
print(f"  WIP rows with a Lineplan Ref #          : {wip_with_ref.count()}")
print(f"  Joined rows (matched to LinePlan)        : {joined_count}")
unmatched_ref = wip_with_ref.count() - joined_count
if unmatched_ref:
    print(f"  ⚠️  {unmatched_ref} WIP row(s) have a Lineplan Ref # that does NOT "
          f"exist in {lineplan_table} — dropped by the inner join. Check for typos "
          f"or a ref# not yet entered in the LinePlan sheet.")

# COMMAND ----------

# ── Step 4: Transpose vendor/factory slots into one row each ──────────────────
print("\nStep 4: Transposing 4 vendor/factory slots …")

# Common output columns (same for all slots)
COMMON_COLS = [
    "customer", "season_code", "brand", "bp_style_no", "lf_style_no",
    "legacy_code", "style_description", "color_name", "lineplan_ref",
    "fabric_content", "fabric_type", "material_no", "gender", "class_", "sub_class",
    "order_quantity", "target_ldp", "target_fob",
]

def _slot_df(
    df: DataFrame,
    slot_name: str,
    vendor_col: str, factory_col: str, country_col: str,
    hts_col: str, du_col: str, dc_col: str, dm_col: str,
) -> DataFrame:
    """Build a single-slot DataFrame and rename to canonical column names.

    "supplier_type" here IS the single flag distinguishing the 4 transposed
    rows of a style: "Main" | "1" | "2" | "3", GENERATED from which
    vendor/factory column-pair this row came from in WIP (Master Chart) --
    matches the original spec's "Supplier Type - Generated from Master Chart
    data" (corrected 2026-09-01; this is NOT LinePlan's "INTERNAL/ SOURCED",
    which is a different, per-style-only business classification that does
    not flow into costing_chart -- see Step 2 above).
    """
    return (df
        .withColumn("supplier_type",     F.lit(slot_name))
        .withColumn("supplier",          F.col(vendor_col))
        .withColumn("factory",           F.col(factory_col))
        .withColumn("production_country", F.col(country_col))
        .withColumn("hts_code",          F.col(hts_col))
        .withColumn("duty_rate_us",      F.col(du_col))
        .withColumn("duty_rate_ca",      F.col(dc_col))
        .withColumn("duty_rate_mx",      F.col(dm_col))
        .withColumn("tariff_rate",       F.lit(None).cast(StringType()))  # Phase 9b
        .withColumn("updated_at",        F.lit(now.isoformat()).cast("timestamp"))
        # Drop rows where vendor is blank — no vendor = no costing row
        .filter(F.col("supplier").isNotNull() & (F.trim(F.col("supplier")) != ""))
        .select(
            *COMMON_COLS,
            "supplier_type", "supplier", "factory", "production_country",
            "hts_code", "duty_rate_us", "duty_rate_ca", "duty_rate_mx",
            "tariff_rate", "updated_at",
        )
    )

slot_dfs = [
    _slot_df(joined, "Main",
             "vendor_main",  "factory_main",  "prod_country_main",
             "hts_main",     "duty_us_main",  "duty_ca_main",  "duty_mx_main"),
    _slot_df(joined, "1",
             "vendor_1",     "factory_1",     "prod_country_1",
             "hts_1",        "duty_us_1",     "duty_ca_1",     "duty_mx_1"),
    _slot_df(joined, "2",
             "vendor_2",     "factory_2",     "prod_country_2",
             "hts_2",        "duty_us_2",     "duty_ca_2",     "duty_mx_2"),
    _slot_df(joined, "3",
             "vendor_3",     "factory_3",     "prod_country_3",
             "hts_3",        "duty_us_3",     "duty_ca_3",     "duty_mx_3"),
]

costing_chart = reduce(DataFrame.unionByName, slot_dfs)

# Rename class_ back to class_name for output (avoid Python keyword confusion)
costing_chart = costing_chart.withColumnRenamed("class_", "class_name")

total_costing = costing_chart.count()
print(f"  Costing chart rows after transpose: {total_costing}")
print(f"  Breakdown by slot:")
costing_chart.groupBy("supplier_type").count().orderBy("supplier_type").show()

# COMMAND ----------

# ── Step 4b: Carry forward tariff_rate from the table's OWN prior state ──────
# Live-discovered 2026-09-07: `hts_code`/`duty_rate_us/ca/mx` survive a full
# rebuild via the WIP "fallback" (Step 4 above re-reads them from the live
# WIP row's own per-slot columns, which persist across rebuilds once
# `push_duty_rates` has written them there once). `tariff_rate` has NO such
# fallback -- no live WIP column exists for it yet (`duty.WIP_TARIFF_COLS_
# LIVE = False`) -- so every Step 4 slot-build hardcodes it to `NULL`
# (unconditionally, regardless of any prior value). Since `build_costing_
# chart` runs on a REGULAR SCHEDULE (3x/day, same job as everything else),
# this wiped out every NT Orbit-computed `tariff_rate` within hours of it
# ever being filled by the separate `duty_compute` job -- confirmed live: a
# routine scheduled run erased a `tariff_rate` that had just been correctly
# computed and pushed minutes earlier. Fixed the same way `hts_code`/
# `duty_rate_*` are protected: read the EXISTING `costing_chart` table's OWN
# prior `tariff_rate` (keyed by `duty.COSTING_KEY`, the same key `duty_
# compute`'s MERGE uses) and carry it forward via `COALESCE(new, old)` --
# `new` here is always NULL from Step 4, so this is effectively "keep
# whatever was already there", exactly mirroring the WIP-fallback semantics
# for the other duty fields, without requiring a live WIP column.
print("\nStep 4b: Carrying forward tariff_rate from the table's own prior state …")
try:
    _key_cols = list(duty.COSTING_KEY)
    # NULL-SAFE join (fixed 2026-09-10, live-confirmed real bug) -- PySpark's
    # `.join(other, on=[col_list])` shorthand generates a standard (NOT
    # null-safe) equi-join under the hood: `NULL = NULL` is NULL, never TRUE,
    # so any row with a genuinely-NULL key column (e.g. `lf_style_no`, which
    # is blank for some real test styles) NEVER matches its own prior row --
    # `_prior_tariff_rate` silently comes back NULL for it every single
    # rebuild, identical in root cause to the sibling bug just fixed in
    # p9b1_compute_duty_rates.py's Step 4 MERGE (same `duty.COSTING_KEY`).
    # Prefixing + `eqNullSafe()` (Spark's `<=>`) avoids both the NULL-match
    # gotcha and a column-name collision from using an explicit join
    # condition instead of the `on=[list]` shorthand.
    prior_tariff = (spark.table(output_table)
        .select(*[F.col(c).alias(f"_pk_{c}") for c in _key_cols],
                 F.col("tariff_rate").alias("_prior_tariff_rate"))
        .dropDuplicates([f"_pk_{c}" for c in _key_cols]))
    _join_cond = None
    for c in _key_cols:
        _cond = costing_chart[c].eqNullSafe(prior_tariff[f"_pk_{c}"])
        _join_cond = _cond if _join_cond is None else (_join_cond & _cond)
    costing_chart = (costing_chart
        .join(prior_tariff, on=_join_cond, how="left")
        .withColumn("tariff_rate", F.coalesce(F.col("tariff_rate"), F.col("_prior_tariff_rate")))
        .drop(*([f"_pk_{c}" for c in _key_cols] + ["_prior_tariff_rate"])))
    carried = costing_chart.filter(F.col("tariff_rate").isNotNull()).count()
    print(f"  tariff_rate carried forward for {carried} row(s) (prior table existed)")
except Exception as e:
    print(f"  ⚠️  No prior {output_table} to carry tariff_rate forward from "
          f"(first-ever run, or read failed: {e}) -- tariff_rate stays NULL, "
          f"will be filled by the next duty_compute run.")

# COMMAND ----------

# ── Step 4c: Fill hts_code/duty_rate_*/tariff_rate DIRECTLY from the
#             persistent NT Orbit cache -- independent of BOTH the WIP
#             fallback AND costing_chart's own prior-run state (added
#             2026-09-10, owner spec) ────────────────────────────────────────
# Step 4b above only helps `tariff_rate`, and only when the EXACT same
# `COSTING_KEY` survived from a prior `costing_chart` snapshot -- a
# genuinely NEW row (e.g. a style reaching costing_chart for the first
# time) has no "prior row" to carry forward from, even if the persistent
# cache already has the answer for its exact (product_description,
# origin_country, market) combination (e.g. because another style/color
# with the identical description+origin was already looked up). The
# persistent `nt_orbit_duty_cache` table is the REAL source of truth for
# "have we already computed this" -- it is keyed purely on
# (product_description, origin_country_code, import_country_code), with NO
# dependency on style/color/lineplan/vendor identity at all, so consulting
# it directly here (read-only, zero API calls) fills in EVERY duty field
# (not just tariff_rate) the moment a matching entry exists, regardless of
# whether this exact row existed in a prior costing_chart build or whether
# `push_duty_rates` ever wrote anything back to the live WIP row.
# Reuses the EXACT same pure functions `p9b1_compute_duty_rates.py` uses to
# decide whether to call NT Orbit (`duty.markets_needing_lookup()`,
# `duty.cache_key()`, `duty.is_cache_entry_stale()`, `duty.
# merge_lookup_into_row()`) -- this step is that same logic with the live
# API call simply never made; a miss here just leaves the field NULL for
# `duty_compute` (or a future rebuild) to fill in later, same as always.
print(f"\nStep 4c: Filling hts_code/duty_rate_*/tariff_rate directly from the "
      f"persistent NT Orbit cache ({duty_cache_table}) …")
try:
    persistent_cache_rows = spark.table(duty_cache_table).collect()
except Exception as e:
    persistent_cache_rows = []
    print(f"  ⚠️  Persistent cache table unavailable ({e}) -- skipping this step.")

persistent_cache = {}
for _r in persistent_cache_rows:
    _rd = _r.asDict()
    persistent_cache[(_rd["product_description"], _rd["origin_country_code"],
                       _rd["import_country_code"])] = _rd
print(f"  Persistent cache has {len(persistent_cache)} entrie(s)")

_chart_schema = costing_chart.schema
_chart_rows = [r.asDict() for r in costing_chart.collect()]
_filled_rows = 0
for _row in _chart_rows:
    _row_filled = False
    for _market in duty.markets_needing_lookup(_row):
        _key = duty.cache_key(_row, _market)
        _cache_row = persistent_cache.get(_key)
        if _cache_row is None or duty.is_cache_entry_stale(
            _cache_row.get("looked_up_at"), now, ttl_days=cache_ttl_days
        ):
            continue
        _result = duty.cache_row_to_result(_cache_row)
        _updates = duty.merge_lookup_into_row(_row, _market, _result)
        if _updates:
            _row.update(_updates)
            _row_filled = True
    if _row_filled:
        _filled_rows += 1

print(f"  Rows with >=1 field filled directly from cache: {_filled_rows} of {len(_chart_rows)}")
costing_chart = spark.createDataFrame(_chart_rows, _chart_schema)

# COMMAND ----------

# ── Step 5: Write costing_chart (full overwrite) ──────────────────────────────
print("\nStep 5: Writing costing_chart …")
(costing_chart.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(output_table))
print(f"✅ Wrote {total_costing} rows → {output_table}  (full overwrite)")

# COMMAND ----------

# ── Step 6: Summary ───────────────────────────────────────────────────────────
print(f"\n{'='*72}")
print("SUMMARY")
print(f"{'='*72}")
print(f"  WIP input rows        : {wip_raw.count()}")
print(f"  LinePlan input rows   : {lp_raw.count()}")
print(f"  WIP rows dropped (no material_no/bp_style_no, or fabric_content blank/'Main Fabric') : {dropped_incomplete_fabric}")
print(f"  WIP rows w/o Lineplan Ref # (dropped) : {dropped_no_ref}")
print(f"  WIP rows matched to LinePlan (INNER)  : {joined_count}")
print(f"  Costing chart rows    : {total_costing}")
print(f"  Output table          : {output_table}")
print()
print("  Sample output (first 5 rows):")
spark.table(output_table).select(
    "bp_style_no", "color_name", "supplier_type", "supplier", "factory",
    "production_country", "hts_code", "duty_rate_us", "order_quantity", "target_ldp"
).show(5, truncate=40)
print()
print("  Phase 9b TODO: call NT Orbit Duty Tools for rows where")
print("    hts_code IS NULL OR duty_rate_us IS NULL OR tariff_rate IS NULL")
print("    and fill in values + push changes back to WIP.")
print()
print("✅ Phase 9a Costing Chart build complete")
