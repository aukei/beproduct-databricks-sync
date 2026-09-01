# Databricks notebook source
"""
Phase 9b — Fill HTS Code / Duty Rate / Tariff Rate via NT Orbit Duty Tools
============================================================================

For every ``lft.beproduct.costing_chart`` row (built by Phase 9a,
``p9a_build_costing_chart.py``) that is missing ``hts_code`` and/or
``duty_rate_us`` / ``duty_rate_ca`` / ``duty_rate_mx`` and/or ``tariff_rate``:

  1. Call the NT Orbit Duty Tools API (https://orbitduty.neotangent.com) once
     per market (US/CA/MX) still needing a value.
  2. Merge the results back onto ``costing_chart`` (write-once — an existing
     non-blank value is never overwritten; matches Phase 1's default-fill
     semantics).
  3. Optionally (``push_to_wip=true``) PATCH the newly-filled HTS/Duty values
     back onto the corresponding per-vendor-slot columns of the live DTC WIP
     sheet (same ``sheetData`` PATCH contract as Phase 1 — see
     connectors.dtc.DTCConnector.patch_rows). "Tariff Rate" columns do not
     exist in the WIP view yet (confirmed 2026-07-17); those pushes are
     skipped and logged, the value stays in ``costing_chart`` only.

PERSISTENT cross-run caching (critical — each NT Orbit call is ~30s)
----------------------------------------------------------------------
``costing_chart`` is FULLY OVERWRITTEN by every Phase 9a run (see its own
docstring), which wipes every hts_code/duty_rate_*/tariff_rate value Phase 9b
previously filled. Without a cache that survives ACROSS runs, every single
daily run would have to re-look-up EVERY row from scratch at ~30s/call — for
hundreds of rows that's tens of minutes of pure API latency, for data that
almost never actually changes.

So this notebook keeps a separate, NEVER-overwritten Delta table,
``duty_cache_table`` (default ``lft.beproduct.nt_orbit_duty_cache``), keyed on
``sync.duty.DUTY_CACHE_KEY_COLS`` = (product_description, origin_country_code,
import_country_code). Same input -> same output, so before calling NT Orbit
for any (row, market) that still needs a value, this table is checked first;
a hit is used directly (skips the network call entirely) unless it's older
than ``cache_ttl_days`` (default 180 — tariff/duty POLICY does change over
time, e.g. Section 301/122 rate changes, so this is not cached forever).
Only genuinely new (style/content/gender/class/sub_class, origin, market)
combinations, or ones that have gone stale, ever reach the live API. Newly
fetched results are written back into this table (never deleted), so the
NT Orbit cost for a given combination is paid roughly once per
``cache_ttl_days``, not once per Phase 9a rebuild.

Auth (Microsoft Entra ID delegated OAuth2, NOT the DTC x-api-key scheme)
-------------------------------------------------------------------------
The Orbit API takes a per-user Entra ID access token. This notebook only
performs step 2 of the flow (refresh_token -> access_token, roughly hourly);
step 1 (the ONE-TIME interactive authorization-code login as
"auchunkei@lifung.com") is done locally via
``scripts/nt_orbit_oauth_setup.py`` (delegated login as auchunkei@lifung.com
against a dedicated app registration created for this integration — a
confidential client with its own client_secret), whose output seeds the
Databricks secret scope. ``nt_orbit_client_secret`` IS expected to be present
for this setup; it is read best-effort (falls back to None) purely so the
same code also works unchanged if a future setup switches to a public-client
flow (device-code/manual) that has no secret. See
dtc/python/client/entra_auth.py for the full flow.

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
dbutils.widgets.text("duty_cache_table", "lft.beproduct.nt_orbit_duty_cache",
                     "Persistent cross-run NT Orbit result cache (fully-qualified)")
dbutils.widgets.text("cache_ttl_days", str(duty.DEFAULT_CACHE_TTL_DAYS),
                     "Days before a cached lookup is re-queried")

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
duty_cache_table = dbutils.widgets.get("duty_cache_table").strip()
cache_ttl_days   = int(dbutils.widgets.get("cache_ttl_days") or duty.DEFAULT_CACHE_TTL_DAYS)

wip_table       = f"{catalog}.{schema}.dtc_wip_{customer.lower()}"
registry_table  = f"{catalog}.{schema}.dtc_request_registry"
oauth_state_tbl = f"{catalog}.{schema}.nt_orbit_oauth_state"
now = datetime.now(timezone.utc)

print("=" * 72)
print("PHASE 9b — Fill HTS Code / Duty Rate / Tariff Rate (NT Orbit Duty Tools)")
print("=" * 72)
print(f"  Costing chart : {costing_table}")
print(f"  WIP table     : {wip_table}")
print(f"  Duty cache    : {duty_cache_table}  (ttl={cache_ttl_days}d)")
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
    nt_orbit_client_secret = None  # tolerate a future public-client (no-secret) setup

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
# "supplier_type" (renamed from "factory_slot" 2026-09-01) is the single
# "Main"|"1"|"2"|"3" flag generated from WIP structure -- see
# p9a_build_costing_chart.py's module docstring.
COSTING_KEY = [
    "customer", "season_code", "brand", "bp_style_no", "lf_style_no",
    "color_name", "lineplan_ref", "supplier_type", "supplier", "factory",
]

needing = [r for r in chart_rows if duty.row_needs_any_lookup(r)]
print(f"  Rows needing at least one NT Orbit lookup: {len(needing)}")

# COMMAND ----------

# ── Step 2: Consult the PERSISTENT cross-run cache before calling NT Orbit ───
# Each call is ~30s and costing_chart is fully rebuilt by every Phase 9a run,
# so the persistent cache (not just in-run dedup) is what keeps this notebook
# fast on the 2nd+ run. See module docstring.
print(f"\nStep 2: Checking persistent cache {duty_cache_table} …")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {duty_cache_table} (
  product_description STRING, origin_country_code STRING, import_country_code STRING,
  hts_code STRING, duty_rate DOUBLE, tariff_rate DOUBLE, classification_name STRING,
  looked_up_at TIMESTAMP
) USING DELTA
""")

persistent_cache_rows = spark.table(duty_cache_table).collect()
persistent_cache: dict = {}   # duty.cache_key(...) -> cache row dict
for r in persistent_cache_rows:
    rd = r.asDict()
    persistent_cache[(rd["product_description"], rd["origin_country_code"],
                      rd["import_country_code"])] = rd
print(f"  Persistent cache has {len(persistent_cache)} entrie(s)")

call_jobs = []          # (row_idx, country_code, cache_key)
for idx, row in enumerate(needing):
    for country_code in duty.markets_needing_lookup(row):
        key = duty.cache_key(row, country_code)
        call_jobs.append((idx, country_code, key))

unique_keys = {k for _, _, k in call_jobs}

cache: dict = {}         # duty.cache_key(...) -> DutyLookupResult  (used for ALL keys this run)
keys_from_cache_hit = set()
keys_to_call = set()
for key in unique_keys:
    cache_row = persistent_cache.get(key)
    if cache_row is not None and not duty.is_cache_entry_stale(
        cache_row.get("looked_up_at"), now, ttl_days=cache_ttl_days
    ):
        cache[key] = duty.cache_row_to_result(cache_row)
        keys_from_cache_hit.add(key)
    else:
        keys_to_call.add(key)

print(f"  Lookups needed: {len(unique_keys)} unique  "
      f"({len(keys_from_cache_hit)} served from cache, {len(keys_to_call)} require a live call)")

# COMMAND ----------

# ── Step 2b: Call NT Orbit ONLY for keys not served by the persistent cache ──
print(f"\nStep 2b: Calling NT Orbit for {len(keys_to_call)} uncached key(s) "
      f"with {max_workers} worker(s) …")


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
newly_fetched: dict = {}   # duty.cache_key(...) -> DutyLookupResult (to persist below)
if keys_to_call:
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_call, k): k for k in keys_to_call}
        for future in as_completed(futures):
            key = futures[future]
            try:
                _, result = future.result()
                cache[key] = result
                newly_fetched[key] = result
            except Exception as e:
                errors[key] = str(e)
                print(f"  ❌ {key}: {e}")

print(f"  ✅ {len(newly_fetched)} new live lookup(s) OK, {len(errors)} failed "
      f"(+ {len(keys_from_cache_hit)} served from cache = {len(cache)} total)")

# COMMAND ----------

# ── Step 2c: Persist newly-fetched results into the cross-run cache ─────────
if newly_fetched and not dry_run:
    print(f"\nStep 2c: Writing {len(newly_fetched)} new entrie(s) to {duty_cache_table} …")
    cache_rows = [duty.build_cache_row(key, result, looked_up_at=now)
                  for key, result in newly_fetched.items()]
    CACHE_SCHEMA = StructType([
        StructField("product_description", StringType()),
        StructField("origin_country_code", StringType()),
        StructField("import_country_code", StringType()),
        StructField("hts_code", StringType()),
        StructField("duty_rate", DoubleType()),
        StructField("tariff_rate", DoubleType()),
        StructField("classification_name", StringType()),
        StructField("looked_up_at", TimestampType()),
    ])
    (spark.createDataFrame(cache_rows, CACHE_SCHEMA)
          .createOrReplaceTempView("_p9b_cache_updates"))
    on_clause = " AND ".join(f"t.{c} = s.{c}" for c in duty.DUTY_CACHE_KEY_COLS)
    spark.sql(f"""
      MERGE INTO {duty_cache_table} t
      USING _p9b_cache_updates s
        ON {on_clause}
      WHEN MATCHED THEN UPDATE SET
        t.hts_code = s.hts_code, t.duty_rate = s.duty_rate,
        t.tariff_rate = s.tariff_rate, t.classification_name = s.classification_name,
        t.looked_up_at = s.looked_up_at
      WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"✅ Cache now has {spark.table(duty_cache_table).count()} entrie(s)")
elif newly_fetched:
    print(f"\nStep 2c: DRY RUN — would write {len(newly_fetched)} new entrie(s) to {duty_cache_table}")
else:
    print("\nStep 2c: No new cache entries to write (everything was already cached).")

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
        plan = duty.build_wip_patch_fields(row.get("supplier_type"), fields)
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
print(f"  Costing chart rows scanned   : {len(chart_rows)}")
print(f"  Rows needing lookup          : {len(needing)}")
print(f"  Unique keys needed           : {len(unique_keys)}")
print(f"    served from persistent cache : {len(keys_from_cache_hit)}  (no API call, no ~30s wait)")
print(f"    fetched live from NT Orbit   : {len(newly_fetched)}  (failed: {len(errors)})")
print(f"  Rows with filled fields      : {len(row_updates)}")
print(f"  Cache TTL                    : {cache_ttl_days} day(s)  (table: {duty_cache_table})")
print(f"  dry_run={dry_run}  push_to_wip={push_to_wip}")
print("\n✅ Phase 9b duty-rate fill complete")
