# Databricks notebook source
"""
v2 SERVERLESS SMOKE CHECK -- read-only, writes nothing.

Purpose: de-risk stages 2-5 by proving, on real serverless compute, the two
assumptions the whole v2 port rests on. Both were argued from static analysis
in docs/MIGRATION_V1_V2.md; this is the live confirmation.

  1. **Workspace Files on sys.path work under serverless.** The `sync` /
     `connectors` / `client` packages are deployed as workspace FILES (not
     notebooks) and imported via the `module_path` widget. This is the
     mechanism every v2 notebook depends on, and the one thing that would
     silently make a v2 notebook import v1's modules.
  2. **Scalar Python UDFs work under serverless.** The transform uses three
     (`format_sample_field`, `lifecycle.should_include_in_staging`,
     `is_wip_row_dropped`). A first-pass audit wrongly claimed there were none;
     they are supported, but this is the construct that differs most from the
     classic cluster, so it gets checked explicitly rather than assumed.

Also verifies the other Spark Connect surfaces v2 relies on: `spark.table`
reads, `createDataFrame` with an explicit schema, `createOrReplaceTempView` +
`spark.sql`, and `dbutils.secrets`.

SAFETY: every operation is a read or an in-memory computation. No Delta table
is written, no DTC or BeProduct API is called, no secret value is printed --
only whether the secret RESOLVES. Safe to run at any time, including while the
v1 job is running.

Exits `SMOKE_OK ...` on success, or raises with the failing check named.
"""

# COMMAND ----------

import sys

# ── Python module root ──────────────────────────────────────────────────────
# Identical block to every other v2 notebook -- that is the point: if this
# resolves, they all do. See docs/MIGRATION_V1_V2.md ("Workspace isolation").
_DEFAULT_MODULE_PATH = "/Workspace/Repos/beproduct-sync/DTC/python"
dbutils.widgets.text("module_path", _DEFAULT_MODULE_PATH, "Python module root")
_MODULE_PATH = (dbutils.widgets.get("module_path") or "").strip() or _DEFAULT_MODULE_PATH
for _p in (_MODULE_PATH, _MODULE_PATH.replace("/DTC/", "/dtc/")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("bom_catalog", "alb_tpm_uat", "Lakebase catalog (serverless-only)")
dbutils.widgets.text("dtc_environment", "uat", "DTC environment (secret resolution only)")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
bom_catalog = dbutils.widgets.get("bom_catalog")
environment = dbutils.widgets.get("dtc_environment")

results = []       # (name, "PASS"/"FAIL"/"SKIP", detail)
failures = []


def check(name):
    """Decorator: run a check, record PASS/FAIL, never abort mid-suite."""
    def wrap(fn):
        try:
            detail = fn() or ""
            results.append((name, "PASS", str(detail)))
        except Exception as e:
            results.append((name, "FAIL", f"{type(e).__name__}: {e}"))
            failures.append(name)
        return fn
    return wrap


print("=" * 78)
print("v2 SERVERLESS SMOKE CHECK -- read-only")
print("=" * 78)
print(f"  module_path : {_MODULE_PATH}")
print(f"  catalog     : {catalog}.{schema}")
print(f"  bom_catalog : {bom_catalog}")

# COMMAND ----------

# ── 1. Workspace Files import (THE critical one) ────────────────────────────


@check("1a. import sync.* from module_path")
def _():
    from sync import phase1, phase2, phase3, bom, duty, lifecycle, samples, registry  # noqa: F401
    # Prove they came from module_path, NOT some other root that happened to be
    # on sys.path -- the whole v1/v2 isolation guarantee rests on this.
    origin = phase1.__file__
    if not origin.startswith(_MODULE_PATH.rstrip("/")):
        raise AssertionError(
            f"sync.phase1 loaded from {origin!r}, NOT from module_path "
            f"{_MODULE_PATH!r} -- v1/v2 module isolation is BROKEN")
    return origin


@check("1b. import connectors/client from module_path")
def _():
    from connectors.dtc import DTCConnector  # noqa: F401
    from client.rest_client import RestClient  # noqa: F401
    import connectors.dtc as m
    return m.__file__


@check("1c. pure-logic sanity (constants resolve)")
def _():
    from sync import phase1, bom, duty
    return (f"FIELD_MAPPING={len(phase1.FIELD_MAPPING)} "
            f"DUMMY_FABRIC_GROUP={bom.DUMMY_FABRIC_GROUP!r} "
            f"COSTING_KEY={len(duty.COSTING_KEY)} cols")


# COMMAND ----------

# ── 2. Scalar Python UDFs (the corrected-audit item) ────────────────────────

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
)


@check("2a. scalar Python UDF over a pure function (samples.format_sample_field)")
def _():
    from sync.samples import format_sample_field
    udf_fmt = F.udf(format_sample_field, StringType())
    df = spark.createDataFrame(
        [("[]",), (None,)],
        StructType([StructField("raw", StringType())]),
    )
    out = df.withColumn("formatted", udf_fmt(F.col("raw"))).collect()
    return f"{len(out)} rows through the UDF boundary"


@check("2b. scalar Python UDF over lifecycle predicates")
def _():
    from sync import lifecycle
    inc = F.udf(lifecycle.should_include_in_staging, BooleanType())
    drop = F.udf(lifecycle.is_wip_row_dropped, BooleanType())
    df = spark.createDataFrame(
        [("Development", "Development", None), ("Finalized", "Finalized", None)],
        StructType([
            StructField("product_status", StringType()),
            StructField("wip_product_status", StringType()),
            StructField("wip_active_dropped", StringType()),
        ]),
    )
    out = (df.withColumn("_include", inc(F.col("product_status"), F.col("wip_product_status")))
             .withColumn("_dropped", drop(F.col("wip_active_dropped")))
             .collect())
    # Non-terminal always included; terminal whose WIP has caught up is not.
    return f"include flags = {[r['_include'] for r in out]}"


# COMMAND ----------

# ── 3. Other Spark Connect surfaces v2 depends on ──────────────────────────


@check("3a. createDataFrame with explicit schema")
def _():
    df = spark.createDataFrame(
        [(None, None)],
        StructType([StructField("a", StringType()), StructField("b", StringType())]),
    )
    return f"{df.count()} row, all-NULL columns typed without inference"


@check("3b. createOrReplaceTempView + spark.sql")
def _():
    spark.createDataFrame(
        [("x",)], StructType([StructField("k", StringType())])
    ).createOrReplaceTempView("_v2_smoke_tmp")
    return f"count={spark.sql('SELECT COUNT(*) c FROM _v2_smoke_tmp').collect()[0]['c']}"


@check("3c. read existing Delta tables (no write)")
def _():
    seen = []
    for tbl in (f"{catalog}.{schema}.ktb_styles",
                f"{catalog}.{schema}.dtc_wip_ktb",
                f"{catalog}.{schema}.dtc_request_registry"):
        seen.append(f"{tbl.split('.')[-1]}={spark.table(tbl).count()}")
    return ", ".join(seen)


@check("3d. null-safe equality operator (COSTING_KEY requirement)")
def _():
    # duty.COSTING_KEY joins MUST use <=> / eqNullSafe: lf_style_no can be NULL
    # and `NULL = NULL` is NULL, so a row silently never matches itself.
    r = spark.sql("SELECT (NULL <=> NULL) AS nullsafe, (NULL = NULL) AS plain").collect()[0]
    if r["nullsafe"] is not True:
        raise AssertionError("`<=>` did not evaluate NULL <=> NULL as TRUE")
    return f"NULL<=>NULL={r['nullsafe']}, NULL=NULL={r['plain']}"


# COMMAND ----------

# ── 4. Lakebase reachability (the v1 serverless-only constraint) ────────────


@check("4a. Lakebase catalog readable (alb_tpm_*, serverless-only)")
def _():
    # v1 had to give fill_bom_data its own serverless task for exactly this.
    # A LIMIT 1 read, no write -- proves the whole job can now reach it.
    t = f"{bom_catalog}.public.customer_teckpack_style_latest"
    n = spark.table(t).limit(1).count()
    return f"{t} readable (limit-1 count={n})"


# COMMAND ----------

# ── 5. dbutils surfaces ────────────────────────────────────────────────────


@check("5a. dbutils.secrets resolves (value never printed)")
def _():
    v = dbutils.secrets.get(scope="beproduct", key=f"dtc_api_key_{environment}")
    if not v:
        raise AssertionError("secret resolved empty")
    return f"dtc_api_key_{environment} resolved, length={len(v)}"


@check("5b. dbutils.jobs.taskValues available")
def _():
    dbutils.jobs.taskValues.set(key="smoke", value="ok")
    return "taskValues.set accepted"


# COMMAND ----------

# ── Report ─────────────────────────────────────────────────────────────────

print()
print("=" * 78)
for name, status, detail in results:
    icon = "PASS" if status == "PASS" else "FAIL"
    print(f"  [{icon}] {name}")
    if detail:
        print(f"         {detail}")
print("=" * 78)
print(f"  {len(results) - len(failures)}/{len(results)} passed")

if failures:
    raise RuntimeError("SMOKE FAILED: " + "; ".join(failures))

# The Jobs API does NOT return notebook stdout for serverless runs -- only the
# exit value comes back (`get_run_output(...).logs` is empty). So the per-check
# detail goes INTO the exit value, otherwise the evidence is only visible by
# opening the run in the UI.
import json

dbutils.notebook.exit(json.dumps({
    "status": f"SMOKE_OK {len(results)}/{len(results)}",
    "module_path": _MODULE_PATH,
    "checks": {name: detail for name, _s, detail in results},
}, indent=2))
