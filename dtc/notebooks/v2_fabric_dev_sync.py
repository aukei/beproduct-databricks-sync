# Databricks notebook source
"""
v2 Stage 65 -- BeProduct style BOM -> DTC "KTB FABRIC" "<request> - DEV" lines,
one line per fabric x colorway. Owner spec 2026-11-23. Pure logic and the full
rule set: `sync/fabric_dev.py`.

  1. In-scope styles from `ktb_styles` (colorways from `data_json`, so HIDDEN
     colorways are included -- `colorways_json` drops them).
  2. Each style's BOM straight from BeProduct (PageBomVariation), NOT the
     techpack BOM `wip_push` uses. Main Fabric + Fabric rows only.
  3. Per "<Customer> <SeasonCode> <Brand> - DEV" request: read it LIVE, plan,
     then PATCH (in-place colour fills/renames) and POST (copied lines), back
     to back -- one write window per request. Nothing to do -> NO write.
  4. Re-read and re-plan after a write; anything still pending is reported.
  5. Gaps (no DEV request, fabric line missing, BOM fabric with no article)
     -> `fabric_dev_gaps`, overwritten each run, read by `v_data_gaps`.

Never creates a request or a fabric line, never deletes a line.
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

_DEFAULT_MODULE_PATH = "/Workspace/Repos/beproduct-sync-v2/DTC/python"
dbutils.widgets.text("module_path", _DEFAULT_MODULE_PATH, "Python module root")
_MODULE_PATH = (dbutils.widgets.get("module_path") or "").strip() or _DEFAULT_MODULE_PATH
for _p in (_MODULE_PATH, _MODULE_PATH.replace("/DTC/", "/dtc/")):
    if _p not in sys.path:
        sys.path.insert(0, _p)
print(f"module_path: {_MODULE_PATH}")

# Python 3.10 shim: serverless runs 3.10, the BeProduct SDK uses datetime.UTC.
import datetime as _dt_mod

if not hasattr(_dt_mod, "UTC"):
    _dt_mod.UTC = _dt_mod.timezone.utc

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from beproduct.sdk import BeProduct
from beproduct._raw_api import RawApi
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from connectors.dtc import DTCConnector
from sync import color_palette as cp
from sync import fabric_dev as fd

# COMMAND ----------

dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("customer", "KTB", "Customer code")
dbutils.widgets.text("folder_name", "TEST KTB", "BeProduct style folder")
dbutils.widgets.text("dtc_environment", "uat", "DTC environment")
dbutils.widgets.text("dtc_workspace", "KTB", "DTC workspace")
dbutils.widgets.text("fabric_document", "KTB FABRIC", "DTC document")
dbutils.widgets.text("fabric_view", "WIP_ITS_USE", "DTC view")
dbutils.widgets.text("bom_page_id", "", "BOMVariations pageId (blank = discover)")
dbutils.widgets.text("bom_max_workers", "8", "Parallel BeProduct fetch workers")
dbutils.widgets.text("batch_size", "100", "Rows per DTC call")
dbutils.widgets.text("dry_run", "true", "Plan + log, never write DTC")
dbutils.widgets.text("run_fabric_dev", "true", "false = no-op")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
customer = dbutils.widgets.get("customer").strip()
folder_name = dbutils.widgets.get("folder_name").strip()
environment = dbutils.widgets.get("dtc_environment").strip()
workspace = dbutils.widgets.get("dtc_workspace").strip()
document = dbutils.widgets.get("fabric_document").strip()
view_name = dbutils.widgets.get("fabric_view").strip()
page_id = (dbutils.widgets.get("bom_page_id") or "").strip()
max_workers = max(1, int(dbutils.widgets.get("bom_max_workers") or 8))
batch_size = max(1, int(dbutils.widgets.get("batch_size") or 100))
dry_run = (dbutils.widgets.get("dry_run") or "true").strip().lower() == "true"
gaps_table = f"{catalog}.{schema}.fabric_dev_gaps"
sync_log_full = f"{catalog}.{schema}.beproduct_to_dtc_sync_log"

if (dbutils.widgets.get("run_fabric_dev") or "true").strip().lower() != "true":
    print("run_fabric_dev=false -> no-op")
    dbutils.notebook.exit(json.dumps({"status": "SKIPPED"}))

now = datetime.now(timezone.utc)
run_id = now.strftime("%Y%m%d%H%M%S")
print(f"dry_run={dry_run}  folder={folder_name!r}  target={document!r} / {view_name!r}")

# COMMAND ----------

# ── Step 1: styles + request names ─────────────────────────────────────────
season_prefixes = {(r["BPSEASON"] or "").strip().upper(): (r["DTCCODE"] or "").strip()
                   for r in spark.table(f"{catalog}.{schema}.dtc_seasoncode_mapping")
                                 .where(f"upper(CUSTOMER) = upper('{customer}')").collect()}
styles = []
for r in (spark.table(f"{catalog}.{schema}.ktb_styles")
          .where(F.col("folder_name") == folder_name)
          .select("bp_style_number", "id", "season", "year", "brand", "data_json").collect()):
    code = cp.season_code(r["season"], r["year"], season_prefixes)
    brand = cp.text(r["brand"])
    wip_name = f"{customer} {code} {brand}" if code and brand else None
    colorways = (json.loads(r["data_json"] or "{}") or {}).get("colorways") or []
    styles.append({"num": r["bp_style_number"], "id": r["id"], "colorways": colorways,
                   "request": fd.dev_request_name(wip_name)})
no_request_name = [s["num"] for s in styles if not s["request"]]
print(f"Step 1: {len(styles)} style(s); {len(no_request_name)} without a season mapping / brand")

# COMMAND ----------

# ── Step 2: each style's BOM from BeProduct ────────────────────────────────
client = BeProduct(
    client_id=dbutils.secrets.get(scope="beproduct", key="client_id"),
    client_secret=dbutils.secrets.get(scope="beproduct", key="client_secret"),
    refresh_token=dbutils.secrets.get(scope="beproduct", key="refresh_token"),
    company_domain=dbutils.secrets.get(scope="beproduct", key="company_domain"),
)
raw = RawApi(client)
todo = [s for s in styles if s["request"] and s["id"]]

# The BOMVariations pageId is folder-constant: discover it once.
for s in todo[:5]:
    if page_id:
        break
    try:
        apps = client.style.app_list(s["id"])
        apps = apps.get("data", apps) if isinstance(apps, dict) else apps
        page_id = next((a.get("id") for a in apps or []
                        if (a.get("appType") or a.get("type")) == "BOMVariations"), "")
    except Exception as e:  # noqa: BLE001
        print(f"  app_list {s['num']}: {e}")
if todo and not page_id:
    dbutils.notebook.exit(json.dumps({"status": "NO_BOM_PAGE", "styles": len(todo)}))


def fetch(s):
    """-> (style, variation GET bodies, error). Never raises."""
    try:
        listing = client.style.app_get(s["id"], page_id)
        listing = listing.get("data", listing) if isinstance(listing, dict) else listing
        bodies = []
        for v in listing or []:
            body = raw.get(f"Style/{s['id']}/PageBomVariation/{page_id}/Variation/{v['id']}")
            if isinstance(body, dict):
                body.setdefault("metadata", v)
                bodies.append(body)
        return s, bodies, None
    except Exception as e:  # noqa: BLE001
        return s, [], f"{type(e).__name__}: {str(e)[:300]}"


_t0 = time.perf_counter()
with ThreadPoolExecutor(max_workers=max_workers) as pool:
    fetched = list(pool.map(fetch, todo))
print(f"Step 2: fetched {len(fetched)} BOM(s) in {time.perf_counter() - _t0:.1f}s (page {page_id})")

GAP_SCHEMA = StructType([StructField(c, StringType()) for c in
                         ("request", "bp_style", "article", "gap_code", "detail")]
                        + [StructField("as_of", TimestampType())])
gaps = []
needs_by_request, styles_by_article, fetch_errors = {}, {}, []
for s, bodies, err in fetched:
    if err:
        fetch_errors.append(f"{s['num']}: {err}")
        continue
    n = fd.style_fabric_needs(s["colorways"], bodies)
    for row in n.no_article:
        gaps.append((s["request"], s["num"], None, fd.GAP_NO_ARTICLE, json.dumps(row), now))
    for article, colors in n.needs.items():
        dst = needs_by_request.setdefault(s["request"], {}).setdefault(article, [])
        dst.extend(c for c in colors if c not in dst)
        styles_by_article.setdefault((s["request"], article), set()).add(s["num"])
print(f"        {len(needs_by_request)} DEV request(s) needed; {len(fetch_errors)} fetch error(s)")

# COMMAND ----------

# ── Step 3: per DEV request -- read live, plan, write, verify ───────────────
api_key = dbutils.secrets.get(scope="beproduct", key=f"dtc_api_key_{environment}")
dtc = DTCConnector(api_key=api_key, environment=environment, workspace_name=workspace)
active = dtc.search_requests(workspace, document_name=document, filters={"requestIsActive": "Y"})

SYNC_LOG_SCHEMA = StructType([
    StructField("log_time", TimestampType()), StructField("run_id", StringType()),
    StructField("stage", StringType()), StructField("environment", StringType()),
    StructField("dtc_request_name", StringType()), StructField("request_id", StringType()),
    StructField("operation", StringType()), StructField("lf_style_number", StringType()),
    StructField("color", StringType()), StructField("match_key", StringType()),
    StructField("status", StringType()), StructField("reason", StringType()),
    StructField("detail", StringType()), StructField("payload", StringType()),
])
log_rows, results = [], []
status_word = "dry_run" if dry_run else "ok"


def log(req, rid, op, article, color, status, reason="", detail="", payload=None):
    log_rows.append((now, run_id, "fabric_dev", environment, req, rid, op, None, color,
                     article, status, reason, detail,
                     json.dumps(payload, default=str) if payload is not None else None))


def request_gap(req, code, detail):
    for (r, article), nums in styles_by_article.items():
        if r == req:
            gaps.extend((req, num, article, code, detail, now) for num in sorted(nums))


for req, needs in sorted(needs_by_request.items()):
    matches = [r for r in active if (r.get("requestReference") or "").strip() == req]
    if len(matches) != 1:
        code = fd.GAP_REQUEST_NOT_FOUND if not matches else fd.GAP_REQUEST_AMBIGUOUS
        request_gap(req, code, f"{len(matches)} active request(s) named {req!r} in {document!r}")
        results.append({"request": req, "status": code})
        continue
    rid = matches[0].get("requestId")
    res = {"request": req, "request_id": rid}
    try:
        sheet_id = dtc.get_request(rid)["sheetId"]
        view_id = next(v["viewId"] for v in dtc.get_views(rid) if v.get("viewName") == view_name)
        vdef = dtc.get_view_definition(view_id)
        fields = (vdef.get("data", vdef) or {}).get("dynamicFields") or []
        missing_cols = {fd.COL_ARTICLE, fd.COL_COLOR} - {f.get("fieldName") for f in fields}
        if missing_cols:
            raise RuntimeError(f"view {view_name!r} lacks {sorted(missing_cols)}")
        exclude = fd.non_copyable_cols(fields)
        live = dtc.get_sheet(sheet_id, view_id).get("sheetData", [])
        plan = fd.plan_request(needs, live, exclude)
    except Exception as e:  # noqa: BLE001
        res.update(status="ERROR", error=f"{type(e).__name__}: {str(e)[:300]}")
        log(req, rid, "EXCEPTION", None, None, "error", "read_failed", res["error"])
        results.append(res)
        continue

    for article in plan.missing_articles:
        for num in sorted(styles_by_article[(req, article)]):
            gaps.append((req, num, article, fd.GAP_LINE_NOT_FOUND,
                         f"no line with Mill Fabric Article # {article!r} in {req!r}", now))

    # The write window: PATCH then POST, back to back. Empty plan -> no call.
    calls, failed, inserted_ids = 0, 0, []
    by_id = {r.get("rowId"): r for r in live}
    for i in range(0, len(plan.updates), batch_size):
        chunk = plan.updates[i:i + batch_size]
        ok = True
        if not dry_run:
            try:
                dtc.patch_rows(sheet_id, view_id, chunk)
                calls += 1
            except Exception as e:  # noqa: BLE001
                ok, failed = False, failed + len(chunk)
                err = str(e)[:300]
        for u in chunk:
            row = by_id.get(u["rowId"], {})
            log(req, rid, "UPDATE", row.get(fd.COL_ARTICLE), u[fd.COL_COLOR],
                status_word if ok else "error", "color_filled" if ok else "patch_failed",
                f"was {row.get(fd.COL_COLOR)!r}" if ok else err, u)
    for i in range(0, len(plan.inserts), batch_size):
        chunk = plan.inserts[i:i + batch_size]
        ok = True
        if not dry_run:
            try:
                inserted_ids += [a.get("rowId") for a in dtc.append_rows(sheet_id, view_id, chunk)]
                calls += 1
            except Exception as e:  # noqa: BLE001
                ok, failed = False, failed + len(chunk)
                err = str(e)[:300]
        for r in chunk:
            log(req, rid, "INSERT", r.get(fd.COL_ARTICLE), r.get(fd.COL_COLOR),
                status_word if ok else "error", "line_copied" if ok else "append_failed",
                "" if ok else err, r)

    residual = None
    if not dry_run and plan.has_writes:
        residual = fd.plan_request(needs, dtc.get_sheet(sheet_id, view_id).get("sheetData", []),
                                   exclude).summary()
    res.update(status="OK", live_rows=len(live), plan=plan.summary(), write_calls=calls,
               failed_rows=failed, inserted_row_ids=inserted_ids[:50], residual_after_write=residual,
               sample_updates=plan.updates[:5],
               sample_inserts=[{fd.COL_ARTICLE: r.get(fd.COL_ARTICLE), fd.COL_COLOR: r.get(fd.COL_COLOR)}
                               for r in plan.inserts[:10]])
    if failed:
        res["status"] = "COMPLETED_WITH_ERRORS"
    elif residual and (residual["updates"] or residual["inserts"]):
        res["status"] = "VERIFY_MISMATCH"
    results.append(res)
    print(f"  {req}: {res['status']} {plan.summary()} calls={calls}")

# COMMAND ----------

# ── Step 4: gaps table (always overwritten, so a fixed gap disappears) + log ─
(spark.createDataFrame(gaps, GAP_SCHEMA).write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(gaps_table))
if log_rows:
    (spark.createDataFrame(log_rows, SYNC_LOG_SCHEMA)
     .write.format("delta").mode("append").option("mergeSchema", "true")
     .saveAsTable(sync_log_full))

bad = [r for r in results if r["status"] not in ("OK", fd.GAP_REQUEST_NOT_FOUND)]
summary = {
    "status": "OK" if not (bad or fetch_errors) else "COMPLETED_WITH_ERRORS",
    "dry_run": dry_run,
    "styles": len(styles),
    "styles_without_request_name": no_request_name[:50],
    "bom_page_id": page_id,
    "fetch_errors": fetch_errors[:50],
    "gaps": {code: sum(1 for g in gaps if g[3] == code) for code in sorted({g[3] for g in gaps})},
    "requests": results,
}
print(json.dumps(summary, indent=2, default=str)[:6000])
dbutils.notebook.exit(json.dumps(summary, default=str))
