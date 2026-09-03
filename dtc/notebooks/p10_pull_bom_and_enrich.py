# Databricks notebook source
"""
Phase 10 — BOM enrichment from externally-processed techpack data
====================================================================

Fulfills a Phase 1 gap: BOM (Bill of Materials) data is not available from
the BeProduct API at all and instead relies on a SEPARATE techpack-extraction
pipeline, landed in:

    alb_tpm_uat.public.customer_teckpack_style_latest   (UAT)
    alb_tpm_prd.public.customer_teckpack_style_latest   (PRD)

Changed 2026-09-03 (owner spec): reads `customer_teckpack_style_latest`, NOT
`customer_teckpack_style_log` — the "latest" table pre-resolves the
multi-version-per-style history the "log" table required this notebook to
dedupe itself (see "Versioning" below for what that used to look like).
Both catalogs are live-confirmed reachable directly from this workspace's
Unity Catalog metastore (`SHOW CATALOGS` lists them) — no federation/JDBC
setup needed, just `spark.table(...)`. NOTE the catalog naming is NOT
symmetric with `dtc_environment` ("uat"/"prod" elsewhere in this repo vs.
"uat"/"prd" here) — `bom_catalog` is therefore its OWN widget, never derived
from `dtc_environment`.

Join (live-validated 2026-09-02/03 against the KONTOOR/Wrangler test data
already used throughout Phase 9a/9b — KTB-00016..KTB-00023 all appear in
this table with style_season="Spring - 2028", matching
`ktb_styles.season="Spring"` + `.year="2028"`):

    ktb_styles.bp_style_number = customer_teckpack_style_latest.style_no
    AND (ktb_styles.season || " - " || ktb_styles.year) = customer_teckpack_style_latest.style_season

INNER JOIN only. `style_season` format varies WILDLY by customer in this
shared table ("SS26", "SS 2027", "FH 2026", "Spring - 2028", ...) — this
notebook pre-filters `customer_name = bom_customer_name` (default "KONTOOR",
the live-confirmed customer_name for Wrangler/Kontoor Brands data) purely as
a scoping/performance optimization; the join keys alone are already
customer-correct without it.

Enrichment decision logic — UPSERT semantics (pure, unit-tested in
dtc/python/sync/bom.py; REVISED 2026-09-03, see the decisions log in
AGENTS.md for the full history including the earlier all-or-nothing design):
  1. Parse `bom_unified` (JSON), keep only "Main Fabric" / "Fabric" segments
     (the ONLY two `bom_detail_name` values this phase cares about — live
     data also has "Stitch/Seam", "Trim", "Label", all ignored). By
     construction there is exactly ONE "Main Fabric" per style, and ZERO OR
     MORE "Fabric" segments.
  2. The match key between a BOM segment and an existing WIP row is
     (Fabric Group, Mill Fabric Article #) TOGETHER — `Placement` is
     deliberately excluded from the key since it's the one field expected
     to still legitimately change for an otherwise-unchanged assignment.
  3. Per existing row: if its current (Fabric Group, Mill Fabric Article #)
     matches a CURRENT BOM segment exactly, UPSERT `Placement` ONLY (and
     only if it changed); if the row is still un-enriched (blank/
     placeholder), apply "Main Fabric"'s full field set (first-time
     enrichment, unchanged from the original design); otherwise (the row
     carries some OTHER real value not in the current BOM data — e.g. a
     "Fabric" segment that has since disappeared) leave it COMPLETELY
     UNTOUCHED.
  4. If `bom_unified` is missing/blank this run, or its "Main Fabric"
     segment itself is absent: take ZERO actions for the WHOLE style —
     NEVER revert or blank already-enriched DTC data. Live-confirmed real
     trigger (2026-09-03): switching to `customer_teckpack_style_latest`
     left `bom_unified` NULL for some previously-BOM-bearing test styles
     (KTB-00016, KTB-00021) that had already been correctly enriched by an
     earlier run — this rule is what keeps that data intact.
  5. For each "Fabric" segment whose (Fabric Group, Mill Fabric Article #)
     key isn't already represented by ANY existing row for the style, it's
     genuinely new: duplicate every existing row once per such segment
     (unchanged fan-out shape: N colorway rows x each new segment produces
     N new INSERTs).
  6. `Fabric Group` is set to the segment's own `bom_detail_name` (literally
     "Main Fabric" or "Fabric"), NOT `material_name`. `Placement` / `Mill
     Fabric Article #` map from `placement` / `material_no`.

Versioning: the older `customer_teckpack_style_log` table had a
`current_version` column and could carry MULTIPLE rows per (style_no,
style_season) over time (re-extracted techpacks), requiring this notebook to
dedupe by highest `current_version` (tie-broken by `timestamp_lf_captured`).
`customer_teckpack_style_latest` pre-resolves this — it guarantees at most
one row per (`style_no`, `customer_name`, `customer_department`,
`style_season`), live-confirmed for `customer_name='KONTOOR'` (2026-09-03).
NOTE `customer_department` IS part of that key even though it's a constant,
non-null value for KONTOOR ("Wrangler Collaborations") in this environment —
a defensive `row_number()` dedup (on the 3 style/season columns +
`customer_department`, tie-broken by `timestamp_lf_captured`) is still
applied as a near-zero-cost safety net in case that guarantee is ever
violated for a customer/environment this notebook hasn't seen yet.

Push mechanics: UPDATEs are sent as `sheetData` PATCH objects keyed by
`rowId` (existing rows); INSERTs are sent keyed by `rowIndex` (new rows,
values taken by copying the FULL original row's fields from `data_json` and
overriding just the 3 BOM fields) — matches the established "cannot mix
rowId and rowIndex in one PATCH call" contract (see `DTCConnector.patch_rows`
/ AGENTS.md's Phase 1 `create_sheet`/PATCH notes). `rowIndex` values are
assigned sequentially starting from `get_max_row_index() + 1` per sheet.

This notebook does NOT directly mutate the local Delta `dtc_wip_ktb` table
after pushing — like Phase 1's push, it pushes to the LIVE DTC sheet only.

DAG placement (owner decision 2026-09-02): this notebook runs BEFORE
`build_costing_chart`, not after — the whole point of Phase 10 is to get
up-to-date material names into `costing_chart`'s `fabric_content` (part of
`product_description`) so Phase 9b's NT Orbit duty classification is computed
against real BOM data, not the "MAIN MATERIAL CONTENT" placeholder. Since
this notebook never mutates Delta directly, `scripts/deploy_job.py` runs a
dedicated `repull_dtc_bom` task (a full `p1_pull_masters_to_delta` re-pull)
immediately afterward, and `build_costing_chart` depends on THAT re-pull, not
on the earlier `pull_master_dtc`. See that file's DAG diagram for the exact
task graph.
"""

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

import json
from datetime import datetime, timezone

from connectors.dtc import DTCConnector
from sync import bom
from pyspark.sql import functions as F

# ── Parameters ────────────────────────────────────────────────────────────────
dbutils.widgets.text("catalog",  "lft",       "Catalog")
dbutils.widgets.text("schema",   "beproduct", "Schema")
dbutils.widgets.text("customer", "KTB",       "Customer code")
dbutils.widgets.text("folder_name", "TEST KTB", "BeProduct folder (ktb_styles.folder_name filter)")
dbutils.widgets.text("dtc_environment", "uat", "DTC Environment")
dbutils.widgets.text("dtc_workspace",   "KTB", "DTC Workspace")
# NOT derived from dtc_environment -- alb_tpm's PRD catalog suffix is "_prd",
# not "_prod", so this must be its own parameter (see module docstring).
dbutils.widgets.text("bom_catalog", "alb_tpm_uat", "BOM source catalog (alb_tpm_uat | alb_tpm_prd)")
dbutils.widgets.text("bom_schema",  "public", "BOM source schema")
dbutils.widgets.text("bom_table",   "customer_teckpack_style_latest", "BOM source table")
dbutils.widgets.text("bom_customer_name", "KONTOOR",
                     "Pre-filter customer_name (scoping/perf only -- the join keys alone are already correct without it)")
dbutils.widgets.text("dry_run", "true", "Dry run (true/false) -- compute + log, skip the live DTC push")
dbutils.widgets.text("batch_size", "100", "Rows per PATCH call")
# Checked INSIDE the notebook (like dry_run), NOT via a DAG-level condition
# task -- live-discovered 2026-09-02: gating this task's SCHEDULING via a
# gate_phase10 condition made it become EXCLUDED (not just skipped) whenever
# run_phase10=false, and Databricks propagates EXCLUDED to every downstream
# dependent UNCONDITIONALLY (ignoring run_if entirely). Since
# repull_dtc_bom -> build_costing_chart -> gate_phase9b -> fill_duty_rates
# all transitively depend on this task, that silently excluded the ENTIRE
# Phase 9a/9b chain on every run while run_phase10 defaulted to false. Fixed
# by always scheduling this task and no-op'ing internally instead -- see
# AGENTS.md decisions log.
dbutils.widgets.text("run_phase10", "true",
                     "Enable Phase 10 (true/false) -- checked HERE, not via a DAG gate; see comment above")

catalog       = dbutils.widgets.get("catalog")
schema        = dbutils.widgets.get("schema")
customer      = dbutils.widgets.get("customer").strip().upper()
folder_name   = dbutils.widgets.get("folder_name").strip()
environment   = dbutils.widgets.get("dtc_environment").strip().lower()
workspace     = dbutils.widgets.get("dtc_workspace").strip()
bom_catalog   = dbutils.widgets.get("bom_catalog").strip()
bom_schema    = dbutils.widgets.get("bom_schema").strip()
bom_table     = dbutils.widgets.get("bom_table").strip()
bom_customer_name = dbutils.widgets.get("bom_customer_name").strip()
dry_run       = dbutils.widgets.get("dry_run").strip().lower() == "true"
batch_size    = int(dbutils.widgets.get("batch_size") or 100)
run_phase10   = dbutils.widgets.get("run_phase10").strip().lower() == "true"

styles_table    = f"{catalog}.{schema}.ktb_styles"
wip_table       = f"{catalog}.{schema}.dtc_wip_{customer.lower()}"
registry_table  = f"{catalog}.{schema}.dtc_request_registry"
bom_source      = f"{bom_catalog}.{bom_schema}.{bom_table}"
now = datetime.now(timezone.utc)

print("=" * 72)
print("PHASE 10 — BOM enrichment from techpack extraction")
print("=" * 72)

if not run_phase10:
    print("run_phase10=false — skipping entirely (no Lakebase/DTC access made).")
    dbutils.notebook.exit("SKIPPED_run_phase10_false")

print(f"  BeProduct styles : {styles_table}  (folder_name={folder_name!r})")
print(f"  WIP table        : {wip_table}")
print(f"  BOM source       : {bom_source}  (customer_name={bom_customer_name!r})")
print(f"  dry_run={dry_run}")

# COMMAND ----------

# ── Step 1: Resolve the BeProduct <-> BOM join (INNER JOIN, style_no + style_season) ──
print("\nStep 1: Joining ktb_styles <-> BOM on (bp_style_number=style_no, "
      "season||' - '||year=style_season) …")

styles = (spark.table(styles_table)
          .where(F.col("folder_name") == folder_name)
          .select(
              F.col("bp_style_number"),
              F.concat(F.col("season"), F.lit(" - "), F.col("year")).alias("style_season"),
          )
          .where(F.col("bp_style_number").isNotNull()
                 & F.col("season").isNotNull() & F.col("year").isNotNull()))
print(f"  BeProduct styles with a valid season/year : {styles.count()}")

# customer_teckpack_style_latest already pre-resolves the multi-version-per-
# style history that the older customer_teckpack_style_log table required
# this notebook to dedupe itself (current_version DESC / timestamp_lf_captured
# tie-break) -- switched 2026-09-03 (owner spec). It guarantees at most one
# row per (style_no, customer_name, customer_department, style_season);
# customer_department IS part of that key even though it's a constant
# non-null value for KONTOOR ("Wrangler Collaborations") in this environment
# -- live-confirmed 0 duplicate groups for customer_name='KONTOOR' on the
# full 4-column key (2026-09-03). A defensive row_number() dedup is still
# applied (belt-and-suspenders, near-zero cost) in case that guarantee is
# ever violated for a customer/environment this notebook hasn't seen yet;
# it should always be a no-op today.
bom_raw = (spark.table(bom_source)
           .where(F.col("customer_name") == bom_customer_name)
           .where(F.col("bom_unified").isNotNull()))

from pyspark.sql import Window
w = Window.partitionBy("style_no", "customer_department", "style_season").orderBy(
    F.col("timestamp_lf_captured").desc_nulls_last(),
)
bom_latest = (bom_raw
              .withColumn("_rn", F.row_number().over(w))
              .where(F.col("_rn") == 1)
              .select("style_no", "customer_department", "style_season", "bom_unified"))

joined = (styles.join(
    bom_latest,
    on=(styles.bp_style_number == bom_latest.style_no)
       & (styles.style_season == bom_latest.style_season),
    how="inner",
).select(styles.bp_style_number, bom_latest.customer_department, bom_latest.bom_unified))

matched = joined.collect()
print(f"  Matched (style x BOM) pairs : {len(matched)}")

# COMMAND ----------

# ── Step 2: Load current WIP rows per matched style ───────────────────────────
print("\nStep 2: Loading current WIP rows for matched styles …")

matched_styles = {r["bp_style_number"] for r in matched}
wip_rows_by_style: dict = {}
wip_meta = {}  # request_id -> {sheet_id, view_id} via registry

reg = {r["request_id"]: r.asDict()
       for r in spark.table(registry_table).where(F.col("environment") == environment).collect()}

for r in spark.table(wip_table).where(F.col("bp_style_number").isin(list(matched_styles))).collect():
    wr = r.asDict()
    style = wr["bp_style_number"]
    row_fields = json.loads(wr["data_json"])
    wip_rows_by_style.setdefault(style, []).append({
        "row_id": wr.get("row_id"),
        "fabric_group": row_fields.get(bom.WIP_FIELD_FABRIC_GROUP),
        "mill_fabric_article": row_fields.get(bom.WIP_FIELD_MILL_FABRIC_ARTICLE),
        "placement": row_fields.get(bom.WIP_FIELD_PLACEMENT),
        "request_id": wr.get("request_id"),
        "data_json": wr.get("data_json"),
    })
    wip_meta[wr.get("request_id")] = {
        "sheet_id": reg.get(wr.get("request_id"), {}).get("sheet_id"),
        "view_id": reg.get(wr.get("request_id"), {}).get("view_id"),
    }

print(f"  Styles with existing WIP rows : {len(wip_rows_by_style)}")

# COMMAND ----------

# ── Step 3: Plan enrichment per style (pure logic, dtc/python/sync/bom.py) ────
# Upsert semantics (see bom.py module docstring, revised 2026-09-03): no more
# whole-style "already enriched" gate -- bom.plan_style_enrichment() itself
# decides, per existing row, whether to upsert Placement-only, apply
# first-time full enrichment, insert a new "Fabric" segment row, or leave a
# row completely untouched. A style with no "Main Fabric" segment this run
# (BOM missing entirely, or Main Fabric itself vanished) always yields []
# from plan_style_enrichment -- Phase 10 NEVER reverts existing DTC data.
print("\nStep 3: Planning enrichment …")

all_actions = []   # list of (request_id, sheet_id, view_id, bom.RowAction)
skipped_no_actions = 0
skipped_no_wip_rows = 0

for r in matched:
    style = r["bp_style_number"]
    existing_rows = wip_rows_by_style.get(style)
    if not existing_rows:
        skipped_no_wip_rows += 1
        continue

    actions = bom.plan_style_enrichment(existing_rows, r["bom_unified"])
    if not actions:
        skipped_no_actions += 1
        continue

    # An "update" action's request_id is looked up by its row_id; an
    # "insert" action's request_id comes straight from its base_row (which
    # IS one of existing_rows, carrying "request_id" -- see Step 2).
    row_id_to_request_id = {row["row_id"]: row["request_id"] for row in existing_rows}
    for action in actions:
        request_id = (row_id_to_request_id.get(action.row_id) if action.kind == "update"
                      else action.base_row.get("request_id"))
        meta = wip_meta.get(request_id, {})
        all_actions.append((request_id, meta.get("sheet_id"), meta.get("view_id"), action))

print(f"  Styles skipped (no upsert actions -- BOM missing/vanished, or fully")
print(f"                  up to date already)      : {skipped_no_actions}")
print(f"  Styles skipped (no WIP rows yet)        : {skipped_no_wip_rows}")
print(f"  Total actions planned                   : {len(all_actions)}"
      f"  (updates: {sum(1 for *_, a in all_actions if a.kind == 'update')},"
      f"   inserts: {sum(1 for *_, a in all_actions if a.kind == 'insert')})")

# COMMAND ----------

# ── Step 4: Push to live DTC WIP (UPDATE by rowId, INSERT by rowIndex) ────────
if all_actions:
    print(f"\nStep 4: Pushing to DTC (env={environment}) …")
    secret_key = f"dtc_api_key_{environment}"
    dtc_api_key = dbutils.secrets.get(scope="beproduct", key=secret_key)
    dtc = DTCConnector(api_key=dtc_api_key, environment=environment, workspace_name=workspace)

    from sync.phase1 import chunked

    # Group by (sheet_id, view_id); within each, updates and inserts must be
    # SEPARATE PATCH calls (cannot mix rowId and rowIndex in one call).
    by_sheet_updates: dict = {}
    by_sheet_inserts: dict = {}
    skipped_no_meta = 0

    # Per-view_id cache of non-writable column names (see
    # bom.compute_non_writable_cols's docstring). Live-discovered
    # 2026-09-02: `isReadOnly` in the view definition is NOT reliable (it
    # was `false` on every field DTC then rejected a write to); the real
    # signals are `type == "contact"` (the image field) and a truthy
    # `formula` key (computed/derived columns). Fetched once per distinct
    # view_id, not once per insert, since dynamicFields is view-scoped.
    non_writable_cache: dict = {}

    def _exclude_cols_for(view_id: str) -> frozenset:
        if view_id not in non_writable_cache:
            try:
                defn = dtc.get_view_definition(view_id)
                non_writable_cache[view_id] = (
                    bom.INSERT_EXCLUDE_COLS
                    | bom.compute_non_writable_cols(defn.get("dynamicFields", []))
                )
            except Exception as e:
                print(f"  ⚠️  get_view_definition({view_id}) failed: {e} "
                      f"-- falling back to bom.INSERT_EXCLUDE_COLS only")
                non_writable_cache[view_id] = bom.INSERT_EXCLUDE_COLS
        return non_writable_cache[view_id]

    for request_id, sheet_id, view_id, action in all_actions:
        if not sheet_id or not view_id:
            skipped_no_meta += 1
            continue
        sheet_key = (sheet_id, view_id)
        if action.kind == "update":
            by_sheet_updates.setdefault(sheet_key, []).append(
                {**action.wip_fields, "rowId": action.row_id})
        else:  # insert
            base_fields = json.loads(action.base_row["data_json"])
            # Full copy of the original row's fields, minus identity fields
            # (rowId/rowIndex) and every column DTC itself marks as
            # non-writable (image-type + formula fields) -- see
            # bom.build_insert_row_payload / bom.compute_non_writable_cols.
            # New duplicate rows get a blank Style Image cell (and blank
            # formula-field cells, which DTC computes itself); phase3_images
            # picks up the blank image on its next run like any other row.
            new_row = bom.build_insert_row_payload(
                base_fields, action.wip_fields, exclude_cols=_exclude_cols_for(view_id))
            by_sheet_inserts.setdefault(sheet_key, []).append(new_row)

    pushed_updates, pushed_inserts, push_errors = 0, 0, 0

    for (sheet_id, view_id), rows in by_sheet_updates.items():
        for chunk in chunked(rows, batch_size):
            try:
                if not dry_run:
                    dtc.patch_rows(sheet_id, view_id, chunk)
                pushed_updates += len(chunk)
            except Exception as e:
                print(f"  ❌ UPDATE PATCH sheet={sheet_id} view={view_id} failed: {e}")
                push_errors += len(chunk)

    for (sheet_id, view_id), rows in by_sheet_inserts.items():
        try:
            next_index = 0 if dry_run else dtc.get_max_row_index(sheet_id, view_id) + 1
        except Exception as e:
            print(f"  ❌ get_max_row_index sheet={sheet_id} view={view_id} failed: {e}")
            push_errors += len(rows)
            continue
        indexed_rows = []
        for i, row in enumerate(rows):
            indexed_rows.append({**row, "rowIndex": next_index + i})
        for chunk in chunked(indexed_rows, batch_size):
            try:
                if not dry_run:
                    dtc.patch_rows(sheet_id, view_id, chunk)
                pushed_inserts += len(chunk)
            except Exception as e:
                print(f"  ❌ INSERT PATCH sheet={sheet_id} view={view_id} failed: {e}")
                push_errors += len(chunk)

    dtc.close()
    print(f"  Pushed updates: {pushed_updates}  Pushed inserts: {pushed_inserts}  "
          f"errors: {push_errors}  (no sheet/view metadata: {skipped_no_meta})")
    print("  New/updated rows will be reflected in Delta by the DAG's "
          "repull_dtc_bom task, which runs immediately after this one.")
else:
    print("\nStep 4: Nothing to push.")

# COMMAND ----------

print(f"\n{'='*72}")
print("SUMMARY")
print(f"{'='*72}")
print(f"  Matched (style x BOM) pairs : {len(matched)}")
print(f"  Total actions planned       : {len(all_actions)}")
print(f"  dry_run={dry_run}")
print("\n✅ Phase 10 BOM enrichment complete")
