# Databricks notebook source
"""
Phase 9b — Fill HTS Code / Duty Rate / Tariff Rate via NT Orbit Duty Tools
============================================================================

For every ``lft.beproduct.costing_chart`` row (built by Phase 9a,
``p9a_build_costing_chart.py``) that is missing ``hts_code`` and/or
``duty_rate_us`` / ``duty_rate_ca`` / ``duty_rate_mx`` and/or ``tariff_rate``:

  1. Call the NT Orbit Duty Tools API (https://orbitduty.neotangent.com) once
     per market (US/CA/MX) still needing a value, with in-run caching so an
     identical (product_description, origin_country, import_country) tuple is
     never looked up twice.
  2. Merge the results back onto ``costing_chart`` (write-once — an existing
     non-blank value is never overwritten; matches Phase 1's default-fill
     semantics).
  3. Optionally (``push_to_wip=true``) PATCH the newly-filled HTS/Duty values
     back onto the corresponding per-vendor-slot columns of the live DTC WIP
     sheet (same ``sheetData`` PATCH contract as Phase 1 — see
     connectors.dtc.DTCConnector.patch_rows). "Tariff Rate" columns do not
     exist in the WIP view yet (confirmed 2026-07-17); those pushes are
     skipped and logged, the value stays in ``costing_chart`` only.

Auth (Microsoft Entra ID delegated OAuth2, NOT the DTC x-api-key scheme)
-------------------------------------------------------------------------
The Orbit API takes a per-user Entra ID access token. This notebook only
performs step 2 of the flow (refresh_token -> access_token, roughly hourly);
step 1 (the ONE-TIME interactive authorization-code login as
"auchunkei@lifung.com") is done locally via
``scripts/nt_orbit_oauth_setup.py``, whose output seeds the Databricks secret
scope. See dtc/python/client/entra_auth.py for the full flow.

Because ``dbutils.secrets`` is READ-ONLY, a rotated refresh token (Entra
commonly rotates it on every use) cannot be written back into the secret
scope from here. Instead the latest refresh_token is persisted into a small
control table (``nt_orbit_oauth_state``), which is preferred over the static
secret on every subsequent run.

Costing chart table name is a PARAMETER (widget ``costing_chart_table``):
  default:  lft.beproduct.costing_chart
  testing:  lft.beproduct.costing_chart_kei
"""

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from client.entra_auth import EntraTokenProvider
from connectors.nt_orbit import NTOrbitConnector
from connectors.dtc import DTCConnector
from sync import duty
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
)

# ── Parameters ────────────────────────────────────────────────────────────────
dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("customer", "KTB", "Customer code")
dbutils.widgets.text("costing_chart_table", "lft.beproduct.costing_chart",
                     "Costing Chart table (fully-qualified; test override e.g. "
                     "lft.beproduct.costing_chart_kei)")
dbutils.widgets.text("dtc_environment", "uat", "DTC Environment")
dbutils.widgets.text("dtc_workspace", "KTB", "DTC Workspace")
dbutils.widgets.text("dry_run", "true", "Dry run (true/false) — skip writes")
dbutils.widgets.text("push_to_wip", "false", "Push filled values back to DTC WIP (true/false)")
dbutils.widgets.text("max_workers", "4", "Parallel NT Orbit call threads")
dbutils.widgets.text("batch_size", "100", "Rows per WIP PATCH call")

catalog       = dbutils.widgets.get("catalog")
schema        = dbutils.widgets.get("schema")
customer      = dbutils.widgets.get("customer").strip().upper()
costing_table = dbutils.widgets.get("costing_chart_table").strip()
environment   = dbutils.widgets.get("dtc_environment").strip().lower()
workspace     = dbutils.widgets.get("dtc_workspace").strip()
dry_run       = dbutils.widgets.get("dry_run").strip().lower() == "true"
push_to_wip   = dbutils.widgets.get("push_to_wip").strip().lower() == "true"
max_workers   = int(dbutils.widgets.get("max_workers") or 4)
batch_size    = int(dbutils.widgets.get("batch_size") or 100)

wip_table       = f"{catalog}.{schema}.dtc_wip_{customer.lower()}"
registry_table  = f"{catalog}.{schema}.dtc_request_registry"
oauth_state_tbl = f"{catalog}.{schema}.nt_orbit_oauth_state"
now = datetime.now(timezone.utc)

print("=" * 72)
print("PHASE 9b — Fill HTS Code / Duty Rate / Tariff Rate (NT Orbit Duty Tools)")
print("=" * 72)
print(f"  Costing chart : {costing_table}")
print(f"  WIP table     : {wip_table}")
print(f"  dry_run={dry_run}  push_to_wip={push_to_wip}  max_workers={max_workers}")

# COMMAND ----------

# ── Auth: Entra ID delegated OAuth2 -> NT Orbit bearer token ─────────────────
# Prefer a rotated refresh_token persisted from a prior run (control table);
# fall back to the static secret seeded by scripts/nt_orbit_oauth_setup.py.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {oauth_state_tbl} (
  provider STRING, refresh_token STRING, updated_at TIMESTAMP
) USING DELTA
""")

_stored = (spark.table(oauth_state_tbl)
           .where(F.col("provider") == "nt_orbit")
           .orderBy(F.col("updated_at").desc())
           .limit(1)
           .collect())

nt_orbit_tenant_id     = dbutils.secrets.get(scope="beproduct", key="nt_orbit_tenant_id")
nt_orbit_client_id     = dbutils.secrets.get(scope="beproduct", key="nt_orbit_client_id")
try:
    nt_orbit_client_secret = dbutils.secrets.get(scope="beproduct", key="nt_orbit_client_secret")
except Exception:
    nt_orbit_client_secret = None  # public/native app registration; secret not required

if _stored:
    nt_orbit_refresh_token = _stored[0]["refresh_token"]
    print("  Using refresh_token persisted from a prior rotation.")
else:
    nt_orbit_refresh_token = dbutils.secrets.get(scope="beproduct", key="nt_orbit_refresh_token")
    print("  Using refresh_token from the 'beproduct' secret scope (first run / no rotation yet).")


def _persist_rotated_refresh_token(access_token, refresh_token, expires_in):
    """Entra ID rotates refresh tokens; dbutils.secrets is read-only, so persist
    the new one to a control table instead (preferred on the next run, above)."""
    ts = now.isoformat()
    spark.sql(f"DELETE FROM {oauth_state_tbl} WHERE provider = 'nt_orbit'")
    spark.createDataFrame(
        [("nt_orbit", refresh_token, now)],
        StructType([
            StructField("provider", StringType()),
            StructField("refresh_token", StringType()),
            StructField("updated_at", TimestampType()),
        ]),
    ).write.format("delta").mode("append").saveAsTable(oauth_state_tbl)
    print(f"  🔄 refresh_token rotated and persisted to {oauth_state_tbl} at {ts}")


token_provider = EntraTokenProvider(
    tenant_id=nt_orbit_tenant_id,
    client_id=nt_orbit_client_id,
    refresh_token=nt_orbit_refresh_token,
    client_secret=nt_orbit_client_secret,
    on_token_refreshed=_persist_rotated_refresh_token,
)

orbit = NTOrbitConnector(bearer_token_provider=token_provider.get_access_token)

if not orbit.is_healthy():
    raise RuntimeError(
        "NT Orbit /api/v1/health check failed — the Entra identity used may not "
        "be granted access to the Orbit API yet, or the refresh_token is stale. "
        "Re-run scripts/nt_orbit_oauth_setup.py to re-authorize."
    )
print("✅ NT Orbit health check OK")

# COMMAND ----------

# ── Step 1: Load costing_chart rows needing a lookup ──────────────────────────
print(f"\nStep 1: Reading {costing_table} …")
chart_df = spark.table(costing_table)
chart_rows = [r.asDict() for r in chart_df.collect()]
print(f"  Total costing_chart rows: {len(chart_rows)}")

# COSTING_KEY mirrors docs/costing_interested_fields.txt "Costing chart key".
COSTING_KEY = [
    "customer", "season_code", "brand", "bp_style_no", "lf_style_no",
    "color_name", "lineplan_ref", "factory_slot", "supplier", "factory",
]

needing = [r for r in chart_rows if duty.row_needs_any_lookup(r)]
print(f"  Rows needing at least one NT Orbit lookup: {len(needing)}")

# COMMAND ----------

# ── Step 2: Call NT Orbit (with in-run caching) ───────────────────────────────
print(f"\nStep 2: Calling NT Orbit with {max_workers} worker(s) …")

cache: dict = {}       # duty.cache_key(row, country) -> DutyLookupResult | Exception
cache_lock_hits = 0
call_jobs = []         # (row_idx, country_code, cache_key)

for idx, row in enumerate(needing):
    for country_code in duty.markets_needing_lookup(row):
        key = duty.cache_key(row, country_code)
        call_jobs.append((idx, country_code, key))

unique_keys = {k for _, _, k in call_jobs}
print(f"  Lookup calls needed: {len(call_jobs)}  (unique: {len(unique_keys)})")


def _call(key):
    description, origin, country_code = key
    row_for_desc = next(
        r for r in needing
        if duty.build_product_description(r) == description and r.get("production_country") == origin
    )
    payload = duty.build_calc_request(row_for_desc, country_code)
    response = orbit.calculate_single(payload)
    return key, duty.extract_duty_fields(response)


errors = {}
if unique_keys:
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_call, k): k for k in unique_keys}
        for future in as_completed(futures):
            key = futures[future]
            try:
                _, result = future.result()
                cache[key] = result
            except Exception as e:
                errors[key] = str(e)
                print(f"  ❌ {key}: {e}")

print(f"  ✅ {len(cache)} unique lookups OK, {len(errors)} failed")

# COMMAND ----------

# ── Step 3: Merge results onto each row (write-once) ──────────────────────────
print("\nStep 3: Merging lookup results onto costing_chart rows …")

row_updates: list = []   # (row_idx, {col: value})
for idx, row in enumerate(needing):
    combined: dict = {}
    for country_code in duty.markets_needing_lookup(row):
        key = duty.cache_key(row, country_code)
        result = cache.get(key)
        if result is None:
            continue  # failed lookup for this market; skip
        combined.update(duty.merge_lookup_into_row({**row, **combined}, country_code, result))
    if combined:
        row_updates.append((idx, combined))

print(f"  Rows with at least one filled field: {len(row_updates)}")

# COMMAND ----------

# ── Step 4: Write merged fields back to costing_chart (batched MERGE) ─────────
if row_updates and not dry_run:
    print(f"\nStep 4: Writing {len(row_updates)} row update(s) to {costing_table} …")

    UPDATE_SCHEMA = StructType(
        [StructField(c, StringType()) for c in COSTING_KEY]
        + [
            StructField("hts_code", StringType()),
            StructField("duty_rate_us", DoubleType()),
            StructField("duty_rate_ca", DoubleType()),
            StructField("duty_rate_mx", DoubleType()),
            StructField("tariff_rate", DoubleType()),
            StructField("updated_at", TimestampType()),
        ]
    )

    merge_rows = []
    for idx, fields in row_updates:
        row = needing[idx]
        merge_rows.append(
            tuple(row.get(c) for c in COSTING_KEY)
            + (
                fields.get("hts_code"),
                fields.get("duty_rate_us"),
                fields.get("duty_rate_ca"),
                fields.get("duty_rate_mx"),
                fields.get("tariff_rate"),
                now,
            )
        )

    spark.createDataFrame(merge_rows, UPDATE_SCHEMA).createOrReplaceTempView("_p9b_updates")

    on_clause = " AND ".join(f"t.{c} = s.{c}" for c in COSTING_KEY)
    set_clause = ", ".join(
        f"t.{c} = COALESCE(t.{c}, s.{c})"
        for c in ("hts_code", "duty_rate_us", "duty_rate_ca", "duty_rate_mx", "tariff_rate")
    ) + ", t.updated_at = s.updated_at"

    spark.sql(f"""
      MERGE INTO {costing_table} t
      USING _p9b_updates s
        ON {on_clause}
      WHEN MATCHED THEN UPDATE SET {set_clause}
    """)
    print(f"✅ Merged {len(merge_rows)} row(s) into {costing_table}")
elif row_updates and dry_run:
    print(f"\nStep 4: DRY RUN — would write {len(row_updates)} row update(s) to {costing_table}")
else:
    print("\nStep 4: No costing_chart updates to write.")

# COMMAND ----------

# ── Step 5: Push filled HTS/Duty values back to the live DTC WIP sheet ───────
if push_to_wip and row_updates:
    print(f"\nStep 5: Pushing filled values back to DTC WIP (env={environment}) …")

    secret_key = f"dtc_api_key_{environment}"
    dtc_api_key = dbutils.secrets.get(scope="beproduct", key=secret_key)
    dtc = DTCConnector(api_key=dtc_api_key, environment=environment, workspace_name=workspace)

    # WIP rows keyed by (bp_style_number, color_wash) -> {row_id, sheet_id, view_id}
    reg = {r["request_id"]: r.asDict()
           for r in spark.table(registry_table).where(F.col("environment") == environment).collect()}
    wip_index: dict = {}
    for r in spark.table(wip_table).collect():
        wr = r.asDict()
        key = (wr.get("bp_style_number"), wr.get("color_wash"))
        reg_entry = reg.get(wr.get("request_id"), {})
        wip_index[key] = {
            "row_id": wr.get("row_id"),
            "sheet_id": reg_entry.get("sheet_id"),
            "view_id": reg_entry.get("view_id"),
        }

    # Group patch fields by (sheet_id, view_id) so each target sheet gets one
    # batched PATCH call (UPDATE-only: all WIP rows here already exist).
    by_sheet: dict = {}
    push_skipped_reasons: set = set()
    push_no_match = 0

    for idx, fields in row_updates:
        row = needing[idx]
        wip_key = (row.get("bp_style_no"), row.get("color_name"))
        target = wip_index.get(wip_key)
        if not target or not target.get("row_id"):
            push_no_match += 1
            continue
        plan = duty.build_wip_patch_fields(row.get("factory_slot"), fields)
        for reason in plan.skipped:
            push_skipped_reasons.add(reason)
        if not plan.fields:
            continue
        sheet_key = (target["sheet_id"], target["view_id"])
        by_sheet.setdefault(sheet_key, []).append({**plan.fields, "rowId": target["row_id"]})

    pushed, push_errors = 0, 0
    for (sheet_id, view_id), sheet_data in by_sheet.items():
        if not sheet_id or not view_id:
            push_errors += len(sheet_data)
            continue
        from sync.phase1 import chunked
        for chunk in chunked(sheet_data, batch_size):
            try:
                if not dry_run:
                    dtc.patch_rows(sheet_id, view_id, chunk)
                pushed += len(chunk)
            except Exception as e:
                print(f"  ❌ PATCH sheet={sheet_id} view={view_id} failed: {e}")
                push_errors += len(chunk)

    dtc.close()
    print(f"  Pushed rows: {pushed}  (errors: {push_errors}, no WIP match: {push_no_match})")
    for reason in push_skipped_reasons:
        print(f"  ⚠️  {reason}")
elif push_to_wip:
    print("\nStep 5: push_to_wip=true but there is nothing to push.")
else:
    print("\nStep 5: push_to_wip=false — costing_chart was updated but WIP was not touched.")

orbit.close()

# COMMAND ----------

print(f"\n{'='*72}")
print("SUMMARY")
print(f"{'='*72}")
print(f"  Costing chart rows scanned : {len(chart_rows)}")
print(f"  Rows needing lookup        : {len(needing)}")
print(f"  Unique NT Orbit calls made : {len(cache)}  (failed: {len(errors)})")
print(f"  Rows with filled fields    : {len(row_updates)}")
print(f"  dry_run={dry_run}  push_to_wip={push_to_wip}")
print("\n✅ Phase 9b duty-rate fill complete")
