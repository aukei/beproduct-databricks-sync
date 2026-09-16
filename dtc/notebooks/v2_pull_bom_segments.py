# Databricks notebook source
"""
v2 Stage 20b -- BOM segments from the BeProduct PageBomVariation API -> Delta.

REWRITTEN 2026-09-16: reads BeProduct directly and no longer touches the
`alb_tpm_uat` / `alb_tpm_prd` Lakebase techpack tables at all. Writes NOTHING
to DTC and nothing to BeProduct -- this stage is read-only on both.

Why the source changed
----------------------
BOM data used to arrive via a separate techpack-extraction pipeline landing in
Lakebase. BeProduct now exposes it directly (5 new PageBomVariation endpoints,
live 2026-09-16), which removes a whole intermediate system, its
serverless-only access constraint, and the two-hop join that went with it.

Access pattern (live-verified -- the original spec's field names were mostly
wrong; see AGENTS.md for the full correction table):

    style.app_list(header_id)                -> find the "BOMVariations" page.
                                                Its pageId is FOLDER-CONSTANT,
                                                so it is discovered ONCE and
                                                reused for every style.
    style.app_get(header_id, page_id)        -> the VARIATION LIST for a style
                                                ([{id, variationName, order,
                                                isDefault, ...}]). There is no
                                                separate list endpoint.
    GET Style/{header}/PageBomVariation/{page}/Variation/{vid}
                                             -> {metadata, id, ..., rows[]}

`beproduct._raw_api.RawApi` is used for the last call because the SDK has no
wrapper for it yet. NOTE: `client.public_api_url` ALREADY ends in
`/api/{company}` -- prefixing `api/{company}` yourself gives a doubled path and
a 404.

Field mapping lives in `sync/bom.py` (`extract_variation_row_fields`), not
here, so this notebook stays a thin IO wrapper and the mapping keeps its unit
tests. In particular `rows[].group` is a GUID, NOT the group name -- the name
is `fields["Group"]`, whose values are exactly the two segments the decision
tree already knows: "Main Fabric" and "Fabric".

Colorway affinity is deliberately IGNORED (owner decision 2026-09-16):
variations carry `syncColorways` / `selectedVariationColorways`, but every
variation is treated as applying to all colorways. Per-colorway segment
coverage remains `plan_style_enrichment()`'s job downstream.

Output: `<catalog>.<schema>.<bom_segments_table>`, fully overwritten each run.
Stores the PARSED segments (not the raw payload) because parsing now happens
through a unit-tested pure function rather than being re-derived downstream.
"""

# COMMAND ----------

import sys
import subprocess
import time

print("Installing BeProduct SDK …")
_t0 = time.perf_counter()
try:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "beproduct"])
    print(f"  ✅ installed in {time.perf_counter() - _t0:.1f}s")
except Exception as e:  # noqa: BLE001
    print(f"  ❌ install failed after {time.perf_counter() - _t0:.1f}s: {e}")
    raise

# COMMAND ----------

_DEFAULT_MODULE_PATH = "/Workspace/Repos/beproduct-sync/DTC/python"
dbutils.widgets.text("module_path", _DEFAULT_MODULE_PATH, "Python module root")
_MODULE_PATH = (dbutils.widgets.get("module_path") or "").strip() or _DEFAULT_MODULE_PATH
for _p in (_MODULE_PATH, _MODULE_PATH.replace("/DTC/", "/dtc/")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# ── Python 3.10 compatibility shim for the BeProduct SDK ───────────────────
# Live-hit 2026-09-16: `client.style.app_list()` raises
#   AttributeError: module 'datetime' has no attribute 'UTC'
# on Databricks SERVERLESS, which runs Python 3.10. `datetime.UTC` is a 3.11+
# alias for `datetime.timezone.utc`, and the installed SDK uses it
# unconditionally. It works on a 3.11 dev machine, so this only ever surfaces
# on the cluster -- exactly the class of bug that local testing cannot catch.
#
# Aliasing it back is safe and total: `datetime.UTC` IS `timezone.utc` in 3.11,
# so this makes 3.10 behave identically rather than approximating it.
import datetime as _dt_mod

if not hasattr(_dt_mod, "UTC"):
    _dt_mod.UTC = _dt_mod.timezone.utc
    print("  (applied datetime.UTC shim for Python "
          f"{sys.version_info.major}.{sys.version_info.minor})")

from beproduct.sdk import BeProduct
from beproduct._raw_api import RawApi
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType,
)

from sync import bom

# COMMAND ----------

dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("folder_name", "TEST KTB", "BeProduct folder")
dbutils.widgets.text("bom_segments_table", "bom_segments", "Output table")
dbutils.widgets.text("bom_page_id", "", "BOMVariations pageId (blank = auto-discover)")
dbutils.widgets.text("bom_max_workers", "8", "Parallel BeProduct fetch workers")
dbutils.widgets.text("run_bom", "true", "Run the BOM pull (false = no-op)")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
folder_name = dbutils.widgets.get("folder_name")
out_table = f"{catalog}.{schema}.{dbutils.widgets.get('bom_segments_table').strip()}"
page_id_override = (dbutils.widgets.get("bom_page_id") or "").strip()
max_workers = int(dbutils.widgets.get("bom_max_workers") or 8)
styles_table = f"{catalog}.{schema}.ktb_styles"
now = datetime.now(timezone.utc)

print("=" * 78)
print("v2 Stage 20b -- BeProduct PageBomVariation -> Delta")
print("=" * 78)
print(f"  styles : {styles_table}  (folder_name={folder_name!r})")
print(f"  output : {out_table}")
print(f"  workers: {max_workers}")

if (dbutils.widgets.get("run_bom") or "true").strip().lower() != "true":
    print("\nrun_bom=false -- skipping. Downstream sees whatever this table already")
    print("holds; nothing downstream ever reverts on missing BOM data.")
    dbutils.notebook.exit(json.dumps({"status": "SKIPPED_run_bom_false"}))

# COMMAND ----------

# ── Step 1: styles in scope ─────────────────────────────────────────────────
print("\nStep 1: loading in-scope BeProduct styles …")
styles = [
    (r["bp_style_number"], r["id"])
    for r in (spark.table(styles_table)
              .where(F.col("folder_name") == folder_name)
              .select("bp_style_number", "id")
              .where(F.col("id").isNotNull() & F.col("bp_style_number").isNotNull())
              .collect())
]
print(f"  styles: {len(styles)}")

# COMMAND ----------

# ── Step 2: BeProduct client ────────────────────────────────────────────────
client = BeProduct(
    client_id=dbutils.secrets.get(scope="beproduct", key="client_id"),
    client_secret=dbutils.secrets.get(scope="beproduct", key="client_secret"),
    refresh_token=dbutils.secrets.get(scope="beproduct", key="refresh_token"),
    company_domain=dbutils.secrets.get(scope="beproduct", key="company_domain"),
)
raw = RawApi(client)

# The BOMVariations pageId is folder-constant, so discover it ONCE rather than
# per style (that would double the API calls for no benefit).
page_id = page_id_override
_probe = {"tried": 0, "app_types_seen": [], "errors": []}
if not page_id:
    print("\nStep 2: discovering the BOMVariations pageId …")
    for _num, _sid in styles[:5]:          # 5 is plenty; they share a folder
        _probe["tried"] += 1
        try:
            apps = client.style.app_list(_sid)
            if isinstance(apps, dict):
                apps = apps.get("data", apps)
            for app in (apps or []):
                atype = str(app.get("appType") or app.get("type") or "")
                if atype not in _probe["app_types_seen"]:
                    _probe["app_types_seen"].append(atype)
                if atype == "BOMVariations":
                    page_id = app.get("id")
                    break
        except Exception as e:  # noqa: BLE001
            _probe["errors"].append(f"{_num}: {type(e).__name__}: {str(e)[:200]}")
        if page_id:
            print(f"  pageId = {page_id}  (discovered from {_num})")
            break
if not page_id:
    # Serverless returns NO stdout, so the diagnosis has to travel in the exit
    # value -- otherwise "NO_BOM_PAGE" is unactionable.
    print("\n❌ No BOMVariations page found. Probe detail:")
    print(json.dumps(_probe, indent=2))
    dbutils.notebook.exit(json.dumps({"status": "NO_BOM_PAGE", "probe": _probe}))

# COMMAND ----------

# ── Step 3: fetch every style's variations, in parallel ─────────────────────
print(f"\nStep 3: fetching BOM variations for {len(styles)} style(s) …")


def fetch(style_number: str, style_id: str):
    """-> (style_number, style_id, variation_payloads, error). Never raises."""
    try:
        listing = client.style.app_get(style_id, page_id)
        if isinstance(listing, dict):
            listing = listing.get("data", listing)
        payloads = []
        for v in (listing or []):
            vid = v.get("id")
            if not vid:
                continue
            body = raw.get(
                f"Style/{style_id}/PageBomVariation/{page_id}/Variation/{vid}")
            # Keep the variation-list entry as `metadata` so downstream
            # ordering (`metadata.order`) works even if the GET omits it.
            if isinstance(body, dict):
                body.setdefault("metadata", v)
            payloads.append(body)
        return style_number, style_id, payloads, None
    except Exception as e:  # noqa: BLE001
        return style_number, style_id, [], f"{type(e).__name__}: {str(e)[:300]}"


results = []
_t0 = time.perf_counter()
with ThreadPoolExecutor(max_workers=max_workers) as pool:
    futures = [pool.submit(fetch, num, sid) for num, sid in styles]
    for fut in as_completed(futures):
        results.append(fut.result())
print(f"  fetched in {time.perf_counter() - _t0:.1f}s")

# COMMAND ----------

# ── Step 4: parse into segments (pure, unit-tested) ────────────────────────
print("\nStep 4: parsing variations into BOM segments …")

rows, errors, no_main = [], [], []
n_main = fabric_total = 0
for style_number, style_id, payloads, err in sorted(results):
    segments = None
    if err is None:
        try:
            segments = bom.build_target_segments_from_variations(payloads)
        except Exception as e:  # noqa: BLE001
            err = f"parse: {type(e).__name__}: {str(e)[:300]}"
    if err:
        errors.append(f"{style_number}: {err}")
    elif segments is None:
        # No "Main Fabric" segment -> zero actions for this style downstream.
        # Never an error, never a revert.
        no_main.append(style_number)
    else:
        n_main += 1
        fabric_total += len(segments) - 1
    rows.append((
        style_number, style_id, page_id,
        len(payloads),
        json.dumps(segments) if segments is not None else None,
        1 if segments else 0,
        (len(segments) - 1) if segments else 0,
        err, now,
    ))

print(f"  styles with a Main Fabric segment  : {n_main}")
print(f"  styles with variations but NO Main : {len(no_main)}"
      f"   (zero actions downstream -- never a revert)")
print(f"  styles that FAILED to fetch/parse  : {len(errors)}")
print(f"  total 'Fabric' segments            : {fabric_total}")
for s in no_main[:20]:
    print(f"    no Main Fabric: {s}")
for e in errors[:20]:
    print(f"    ⚠ {e}")

# COMMAND ----------

# ── Step 5: write ───────────────────────────────────────────────────────────
print(f"\nStep 5: writing {out_table} …")
SCHEMA = StructType([
    StructField("bp_style_number", StringType()),
    StructField("beproduct_style_id", StringType()),
    StructField("bom_page_id", StringType()),
    StructField("variation_count", IntegerType()),
    # PARSED segments, as a JSON array of the four enrichment fields per
    # segment (Main Fabric first). Parsing happens here, through
    # sync/bom.py's unit-tested pure functions, rather than being re-derived
    # downstream -- so the raw payload shape stays an implementation detail of
    # this stage.
    StructField("segments_json", StringType()),
    StructField("main_fabric_count", IntegerType()),
    StructField("fabric_count", IntegerType()),
    StructField("error", StringType()),
    StructField("extracted_at", TimestampType()),
])
(spark.createDataFrame(rows, SCHEMA)
 .write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(out_table))
print(f"  ✅ wrote {len(rows)} row(s)")

summary = {
    "status": "OK" if not errors else "COMPLETED_WITH_ERRORS",
    "source": "beproduct_PageBomVariation",
    "table": out_table,
    "bom_page_id": page_id,
    "styles_considered": len(styles),
    "styles_with_main_fabric": n_main,
    "styles_without_main_fabric": len(no_main),
    "styles_failed": len(errors),
    "fabric_segments_total": fabric_total,
    "errors": errors[:50],
}
print("\n" + json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
