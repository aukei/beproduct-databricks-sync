# Databricks notebook source
"""
BeProduct Master Data & Directory Sync
=======================================

Admin-triggered notebook — NOT part of the scheduled DAG.

PURPOSE
-------
Bulk pull and/or push BeProduct Master Data (dropdown/multiselect choices)
and Directory (vendors, factories, their contacts) via the BeProduct Public API.

MODES  (widget: mode)
---------------------
PULL_ONLY         Pull MasterData + Directory from BeProduct → Delta tables.
                  Full refresh — existing tables are overwritten.
                  NOTE: directory_list() iterates ~3800 records at ~2 rec/s
                  (~30 min); only run when a fresh snapshot is needed.
PUSH_ONLY         Push both MasterData choices AND Directory changes back to
                  BeProduct. Skips the slow pull entirely — reads from the
                  existing Delta tables only.
PUSH_MASTER_DATA  Push MasterData choices only (no pull, no directory push).
                  Sends the full choices list from each beproduct_master_* table
                  as an overwrite. Safe because dropdown values rarely change.
PUSH_DIRECTORY    Push Directory changes only (no pull, no MasterData push).
                  Change-detected: only rows where modified_at > extracted_at
                  (or id IS NULL) are pushed. Fast when few records changed.

DIRECTORY CHANGE-TRACKING
--------------------------
beproduct_directory carries three timestamp columns for change detection:

  extracted_at   When this row was last pulled from BeProduct (set by PULL_ONLY).
                 NULL for admin-added rows that have never been pulled.
  bp_modified_at BeProduct's own modifiedAt value for the record (from the API).
                 Reflects when BeProduct last changed this company.
  modified_at    When this Delta row was last changed — by the pull (= extracted_at
                 after a fresh pull) or by an external upsert. This is the field
                 admins/automation should SET to signal a pending change.

PUSH filter: id IS NULL  OR  extracted_at IS NULL  OR  modified_at > extracted_at
After a successful push the notebook writes  extracted_at = modified_at  back
to the Delta row so it is not re-pushed on the next run.

TYPICAL ADMIN WORKFLOW (external upsert → push)
-------------------------------------------------
1. An external pipeline upserts massaged data into beproduct_directory,
   setting modified_at = current_timestamp() on changed rows:

     MERGE INTO lft.beproduct.beproduct_directory AS tgt
     USING <source_table> AS src
     ON tgt.directory_id = src.directory_id
     WHEN MATCHED AND (<fields changed> OR tgt.modified_at <= tgt.extracted_at)
       THEN UPDATE SET
         name = src.name, address = src.address, ...,
         modified_at = current_timestamp()      -- flags for push
     WHEN NOT MATCHED THEN INSERT (
       id, directory_id, name, partner_type, ...,
       extracted_at, bp_modified_at, modified_at
     ) VALUES (
       NULL, src.directory_id, src.name, src.partner_type, ...,
       NULL, NULL, current_timestamp()          -- NULL id = Add on push
     )

2. Run this notebook with mode=PUSH_DIRECTORY, dry_run=true → review plan
3. Re-run with dry_run=false → commits only pending rows to BeProduct

API surface used
----------------
Pull  — MasterData:  GET  /api/{co}/MasterData/{fieldId}
        Directory:   SDK  api.directory.directory_list()         (paginated)
                     SDK  api.directory.directory_contact_list(header_id=<uuid>)
Push  — MasterData:  POST /api/{co}/MasterData/{fieldId}/Update  (raw_api)
        Directory Add:    SDK  api.directory.directory_add(fields=…)
        Directory Update: POST /api/{co}/Directory/Update/{id}   (raw_api — SDK has no Update)
        Contact Add:      SDK  api.directory.directory_contact_add(header_id, fields)
        Contact Update:   POST /api/{co}/Directory/{dId}/Contact/{cId}/Update (raw_api)

API restrictions
----------------
- partnerType CANNOT be changed after a directory company is created.
- Contact email / firstName / lastName cannot be updated for fully-registered users.
- MasterData Update is PATCH: omitted choices are left as-is in BeProduct.
- There is no Directory Delete endpoint; set active=false to deactivate.

Delta tables produced (schema: {catalog}.{schema_name})
-------------------------------------------------------
  beproduct_master_brands
  beproduct_master_teams
  beproduct_master_seasons
  beproduct_master_years
  beproduct_master_product_status
  beproduct_master_product_category
  beproduct_master_product_sub_category
  beproduct_master_division
  beproduct_master_techpack_stage
  beproduct_master_parent_vendor
  beproduct_master_factory
  beproduct_directory
  beproduct_directory_contacts

Note: garment_finish is a free-text field — it has no Choices array and is
not included in master data sync.

Parameters
----------
catalog     : Databricks catalog    (default: lft)
schema_name : Databricks schema     (default: beproduct)
mode        : PULL_ONLY | PUSH_ONLY | PUSH_MASTER_DATA | PUSH_DIRECTORY
dry_run     : true | false  — preview push changes without writing to BeProduct
"""

# COMMAND ----------
# ============================================================================
# CELL 1 — Install packages  (isolated cell so install time is measurable)
# ============================================================================

import sys
import subprocess
import time

print("=" * 80)
print("INSTALL: beproduct, requests")
print("=" * 80)

_t0 = time.perf_counter()
try:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "beproduct", "requests"])
    print(f"✅ Installed in {time.perf_counter() - _t0:.1f}s")
except Exception as _e:
    print(f"❌ Install failed: {_e}")
    raise

# COMMAND ----------
# ============================================================================
# CELL 2 — Imports and parameters
# ============================================================================

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from beproduct.sdk import BeProduct
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ── Widget definitions ────────────────────────────────────────────────────────
# Use schema_name (not schema) to avoid shadowing pyspark StructType variables.
dbutils.widgets.text("catalog", "lft", "Catalog Name")
dbutils.widgets.text("schema_name", "beproduct", "Schema Name")
dbutils.widgets.dropdown(
    "mode",
    "PULL_ONLY",
    ["PULL_ONLY", "PUSH_ONLY", "PUSH_MASTER_DATA", "PUSH_DIRECTORY"],
    "Sync Mode",
)
dbutils.widgets.dropdown(
    "dry_run", "true", ["true", "false"], "Dry Run (push only)"
)
dbutils.widgets.dropdown(
    "fetch_contacts", "false", ["false", "true"],
    "Fetch contacts during pull (slow — extra 3800 API calls)",
)

# ── Read parameters ───────────────────────────────────────────────────────────
CATALOG     = dbutils.widgets.get("catalog")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
MODE        = dbutils.widgets.get("mode").upper()
DRY_RUN        = dbutils.widgets.get("dry_run").lower() == "true"
FETCH_CONTACTS = dbutils.widgets.get("fetch_contacts").lower() == "true"

print(f"catalog:        {CATALOG}")
print(f"schema_name:    {SCHEMA_NAME}")
print(f"mode:           {MODE}")
print(f"dry_run:        {DRY_RUN}")
print(f"fetch_contacts: {FETCH_CONTACTS}")

# Derived mode flags — keep all mode logic in one place
_DO_PULL_MASTER   = MODE == "PULL_ONLY"
_DO_PULL_DIR      = MODE == "PULL_ONLY"
_DO_PUSH_MASTER   = MODE in ("PUSH_ONLY", "PUSH_MASTER_DATA")
_DO_PUSH_DIR      = MODE in ("PUSH_ONLY", "PUSH_DIRECTORY")


def tbl(name: str) -> str:
    """Return fully-qualified Delta table path."""
    return f"{CATALOG}.{SCHEMA_NAME}.{name}"


# ── MasterData field map ──────────────────────────────────────────────────────
# Keys  = Delta table suffix  (beproduct_master_{key})
# Values = BeProduct fieldId  (used in /api/{co}/MasterData/{fieldId})
MASTER_DATA_FIELDS: Dict[str, str] = {
    "brands":               "brands_multi",
    "teams":                "team",
    "seasons":              "season",
    "years":                "year",
    "product_status":       "style_status",
    "product_category":     "product_category",
    "product_sub_category": "product_sub_category",
    "division":             "division",
    "techpack_stage":       "techpack_stage",
    # garment_finish omitted — free-text field, no Choices array
    "parent_vendor":        "parent_vendor",
    "factory":              "factory",
}

print(f"\n📋 {len(MASTER_DATA_FIELDS)} MasterData fields configured")
print("✅ Setup complete")

# COMMAND ----------
# ============================================================================
# CELL 3 — Authenticate with BeProduct
# ============================================================================

print("=" * 80)
print("AUTH: BeProduct SDK")
print("=" * 80)

_client_id      = dbutils.secrets.get(scope="beproduct", key="client_id")
_client_secret  = dbutils.secrets.get(scope="beproduct", key="client_secret")
_refresh_token  = dbutils.secrets.get(scope="beproduct", key="refresh_token")
_company_domain = dbutils.secrets.get(scope="beproduct", key="company_domain")

api = BeProduct(
    client_id=_client_id,
    client_secret=_client_secret,
    refresh_token=_refresh_token,
    company_domain=_company_domain,
)

# raw_api handles OAuth token refresh automatically — no manual Bearer token needed
print("✅ BeProduct SDK initialized (token refresh is automatic)")

# COMMAND ----------
# ============================================================================
# CELL 4 — PULL: MasterData dropdown/multiselect choices
#
# API:  GET /api/{company}/MasterData/{fieldId}
# SDK:  api.raw_api.get("MasterData/{fieldId}")
#
# Response structure:
#   {
#     "fieldId":   "...",
#     "fieldName": "...",
#     "fieldType": "DropDown" | "MultiSelect" | ...,
#     "properties": {
#       "Choices": [
#         {"value": "...", "code": "...", "active": true, "parentValues": []},
#         ...
#       ],
#       ...
#     }
#   }
#
# Output table schema  (one table per field: beproduct_master_{data_type}):
#   field_id  STRING NOT NULL  — BeProduct fieldId (e.g. "brands_multi")
#   value     STRING NOT NULL  — choice display value (primary key in BeProduct)
#   code      STRING           — short code / abbreviation (optional)
#   active    BOOLEAN          — false = deactivated choice in BeProduct
#   data_json STRING NOT NULL  — full raw choice JSON for reference
#   synced_at STRING NOT NULL  — ISO-8601 UTC timestamp of this pull
# ============================================================================

_MASTER_SCHEMA = StructType(
    [
        StructField("field_id",  StringType(),  nullable=False),
        StructField("value",     StringType(),  nullable=False),
        StructField("code",      StringType(),  nullable=True),
        StructField("active",    BooleanType(), nullable=True),
        StructField("data_json", StringType(),  nullable=False),
        StructField("synced_at", StringType(),  nullable=False),
    ]
)


def _parse_choices(api_result: Any) -> List[Dict]:
    """Extract the Choices list from a MasterData API response dict."""
    if not isinstance(api_result, dict):
        return []
    props = api_result.get("properties") or {}
    choices_raw = props.get("Choices") if isinstance(props, dict) else []
    if isinstance(choices_raw, dict):
        # Rare: Choices returned as a dict of {id: choice_obj} instead of a list
        choices_raw = list(choices_raw.values())
    return choices_raw if isinstance(choices_raw, list) else []


if not _DO_PULL_MASTER:
    print(f"Skipping MasterData pull (mode={MODE})")
else:
    print("=" * 80)
    print("PULL: MasterData")
    print("=" * 80)

    _pull_now = datetime.now(timezone.utc).isoformat()
    _pull_stats: Dict[str, int] = {}

    for _data_type, _field_id in MASTER_DATA_FIELDS.items():
        print(f"\n  [{_data_type}]  fieldId={_field_id}")
        try:
            _result = api.raw_api.get(f"MasterData/{_field_id}")
            _choices = _parse_choices(_result)

            if not _choices:
                _ftype = _result.get("fieldType", "?") if isinstance(_result, dict) else "?"
                print(f"    ⚠️  No Choices array (fieldType={_ftype}) — skipping")
                _pull_stats[_data_type] = 0
                continue

            _rows: List[Dict] = []
            for _c in _choices:
                if not isinstance(_c, dict):
                    continue
                # Different field types use different sub-keys for display value
                _val = (
                    _c.get("value")
                    or _c.get("name")
                    or _c.get("code")
                    or _c.get("id")
                    or ""
                )
                _rows.append(
                    {
                        "field_id":  _field_id,
                        "value":     str(_val),
                        "code":      str(_c["code"]) if _c.get("code") is not None else None,
                        "active":    bool(_c.get("active", True)),
                        "data_json": json.dumps(_c),
                        "synced_at": _pull_now,
                    }
                )

            _full_path = tbl(f"beproduct_master_{_data_type}")
            _df = spark.createDataFrame(_rows, schema=_MASTER_SCHEMA)
            spark.sql(f"DROP TABLE IF EXISTS {_full_path}")
            _df.write.format("delta").mode("overwrite").saveAsTable(_full_path)
            _pull_stats[_data_type] = len(_rows)
            print(f"    ✅ {len(_rows)} choices → beproduct_master_{_data_type}")

        except Exception as _e:
            print(f"    ❌ Error: {_e}")
            import traceback; traceback.print_exc()
            _pull_stats[_data_type] = -1

    _total_choices = sum(v for v in _pull_stats.values() if v > 0)
    _ok_fields = sum(1 for v in _pull_stats.values() if v >= 0)
    print(f"\n✅ MasterData pull done: {_total_choices} choices across {_ok_fields}/{len(MASTER_DATA_FIELDS)} fields")

# COMMAND ----------
# ============================================================================
# CELL 5 — PULL: Directory records and contacts
#
# RUNS ONLY when mode = PULL_ONLY  (3800 records ≈ 30 min at ~2 rec/s).
#
# SDK:
#   api.directory.directory_list()
#       Paginated iterator; each item is a dict with BeProduct's DirectoryCompany
#       fields: id (UUID), directoryId (code), name, partnerType, address,
#       country, state, zip, city, phone, website, createdAt, modifiedAt, ...
#
#   api.directory.directory_contact_list(header_id=<company UUID>)
#       Iterator of contact dicts: id, email, firstName, lastName, title,
#       mobilePhone, workPhone, role, active, ...
#
# Output tables:
#   beproduct_directory
#     id             STRING  — BeProduct UUID (needed for Update; NULL for new rows)
#     directory_id   STRING  — human-readable partner code (e.g. "FACTORY001")
#     name / partner_type / address / country / state / zip / city
#     phone / fax / website / notes  STRING
#     active         BOOLEAN
#     data_json      STRING  — full raw company JSON
#     extracted_at   STRING  — when this row was pulled (ISO-8601 UTC)
#     bp_modified_at STRING  — BeProduct's own modifiedAt for this record
#     modified_at    STRING  — when this Delta row was last changed.
#                              Initially = extracted_at after a clean pull.
#                              Set to current_timestamp() by external upserts
#                              to flag pending changes for PUSH_DIRECTORY.
#
#   beproduct_directory_contacts
#     directory_id / contact_id / email / first_name / last_name / title
#     mobile_phone / work_phone / role  STRING
#     active  BOOLEAN
#     data_json / synced_at  STRING
# ============================================================================

# Schema definitions are unconditional — push cells also reference them.

_DIRECTORY_SCHEMA = StructType(
    [
        StructField("id",             StringType(),  nullable=True),
        StructField("directory_id",   StringType(),  nullable=True),
        StructField("name",           StringType(),  nullable=True),
        StructField("partner_type",   StringType(),  nullable=True),
        StructField("address",        StringType(),  nullable=True),
        StructField("country",        StringType(),  nullable=True),
        StructField("state",          StringType(),  nullable=True),
        StructField("zip",            StringType(),  nullable=True),
        StructField("city",           StringType(),  nullable=True),
        StructField("phone",          StringType(),  nullable=True),
        StructField("fax",            StringType(),  nullable=True),
        StructField("website",        StringType(),  nullable=True),
        StructField("notes",          StringType(),  nullable=True),
        StructField("active",         BooleanType(), nullable=True),
        StructField("data_json",      StringType(),  nullable=False),
        # ── change-tracking columns ───────────────────────────────────────────
        StructField("extracted_at",   StringType(),  nullable=True),   # set by pull
        StructField("bp_modified_at", StringType(),  nullable=True),   # from BeProduct API
        StructField("modified_at",    StringType(),  nullable=True),   # set by pull OR external upsert
    ]
)

_CONTACTS_SCHEMA = StructType(
    [
        StructField("directory_id",  StringType(),  nullable=False),
        StructField("contact_id",    StringType(),  nullable=True),
        StructField("email",         StringType(),  nullable=True),
        StructField("first_name",    StringType(),  nullable=True),
        StructField("last_name",     StringType(),  nullable=True),
        StructField("title",         StringType(),  nullable=True),
        StructField("mobile_phone",  StringType(),  nullable=True),
        StructField("work_phone",    StringType(),  nullable=True),
        StructField("role",          StringType(),  nullable=True),
        StructField("active",        BooleanType(), nullable=True),
        StructField("data_json",     StringType(),  nullable=False),
        StructField("synced_at",     StringType(),  nullable=False),
    ]
)

if not _DO_PULL_DIR:
    print(f"Skipping Directory pull (mode={MODE})")
else:
    print("=" * 80)
    print("PULL: Directory (companies + contacts)  [~30 min for 3800 records]")
    print("=" * 80)

    _dir_now = datetime.now(timezone.utc).isoformat()
    _dir_rows: List[Dict] = []
    _contact_rows: List[Dict] = []
    _dir_count = 0
    _contact_fetch_errors = 0

    for _rec in api.directory.directory_list():
        _dir_count += 1
        if not isinstance(_rec, dict):
            continue

        _rec_id   = _rec.get("id")
        _bp_modAt = _rec.get("modifiedAt")

        _dir_rows.append(
            {
                "id":             str(_rec_id) if _rec_id else None,
                "directory_id":   _rec.get("directoryId"),
                "name":           _rec.get("name"),
                "partner_type":   _rec.get("partnerType"),
                "address":        _rec.get("address"),
                "country":        _rec.get("country"),
                "state":          _rec.get("state"),
                "zip":            _rec.get("zip"),
                "city":           _rec.get("city"),
                "phone":          _rec.get("phone"),
                "fax":            _rec.get("fax"),
                "website":        _rec.get("website"),
                "notes":          _rec.get("notes"),
                "active":         bool(_rec.get("active", True)),
                "data_json":      json.dumps(_rec),
                # Change-tracking: after a clean pull modified_at == extracted_at
                # (no pending changes). External upserts set modified_at = now()
                # to flag a row for the next PUSH_DIRECTORY run.
                "extracted_at":   _dir_now,
                "bp_modified_at": str(_bp_modAt) if _bp_modAt else None,
                "modified_at":    _dir_now,
            }
        )

        # Fetch contacts for this company — opt-in only (fetch_contacts=true)
        if FETCH_CONTACTS and _rec_id:
            try:
                for _ct in api.directory.directory_contact_list(header_id=str(_rec_id)):
                    if not isinstance(_ct, dict):
                        continue
                    _contact_rows.append(
                        {
                            "directory_id": str(_rec_id),
                            "contact_id":   str(_ct["id"]) if _ct.get("id") else None,
                            "email":        _ct.get("email"),
                            "first_name":   _ct.get("firstName"),
                            "last_name":    _ct.get("lastName"),
                            "title":        _ct.get("title"),
                            "mobile_phone": _ct.get("mobilePhone"),
                            "work_phone":   _ct.get("workPhone"),
                            "role":         _ct.get("role"),
                            "active":       bool(_ct.get("active", True)),
                            "data_json":    json.dumps(_ct),
                            "synced_at":    _dir_now,
                        }
                    )
            except Exception as _ce:
                print(f"  ⚠️  Contact fetch failed for company {_rec_id}: {_ce}")
                _contact_fetch_errors += 1

    _contacts_note = (
        f", {len(_contact_rows)} contacts"
        + (f"  ({_contact_fetch_errors} errors)" if _contact_fetch_errors else "")
        if FETCH_CONTACTS
        else "  (contacts skipped — fetch_contacts=false)"
    )
    print(f"\nFetched {_dir_count} companies{_contacts_note}")

    if _dir_rows:
        _df_dir = spark.createDataFrame(_dir_rows, schema=_DIRECTORY_SCHEMA)
        spark.sql(f"DROP TABLE IF EXISTS {tbl('beproduct_directory')}")
        _df_dir.write.format("delta").mode("overwrite").saveAsTable(tbl("beproduct_directory"))
        print(f"✅ {len(_dir_rows)} companies → beproduct_directory")
    else:
        print("⚠️  No directory records — beproduct_directory not written")

    if _contact_rows:
        _df_contacts = spark.createDataFrame(_contact_rows, schema=_CONTACTS_SCHEMA)
        spark.sql(f"DROP TABLE IF EXISTS {tbl('beproduct_directory_contacts')}")
        _df_contacts.write.format("delta").mode("overwrite").saveAsTable(
            tbl("beproduct_directory_contacts")
        )
        print(f"✅ {len(_contact_rows)} contacts → beproduct_directory_contacts")
    else:
        print("⚠️  No contacts found — beproduct_directory_contacts not written")

# COMMAND ----------
# ============================================================================
# CELL 5b — DATA MASSAGE: Upsert external data into beproduct_directory
#
# Run this cell AFTER PULL_ONLY and BEFORE PUSH_DIRECTORY / PUSH_ONLY.
#
# The template below shows the recommended MERGE pattern.
# Replace the source query and field list, then uncomment spark.sql().
#
# HOW MODIFIED_AT DRIVES PUSH DETECTION
# ──────────────────────────────────────
# After PULL_ONLY every row has  modified_at == extracted_at  (no pending change).
# This MERGE sets  modified_at = current_timestamp()  only on rows that differ
# from the source, which is the signal that PUSH_DIRECTORY picks up.
#
# Row states after merge:
#   id = UUID  + modified_at > extracted_at  → UPDATE to BeProduct on push
#   id = NULL  (any timestamps)              → ADD   to BeProduct on push
#   id = UUID  + modified_at == extracted_at → skip (already in sync)
# ============================================================================

_MERGE_TEMPLATE = f"""
-- ================================================================
-- MERGE massaged data → {tbl('beproduct_directory')}
-- ================================================================
-- Replace <your_source> with a table name or inline CTE.
-- Adjust the MATCHED condition and SET/VALUES columns as needed.
-- ================================================================

MERGE INTO {tbl('beproduct_directory')} AS tgt
USING (
    -- TODO: replace with your source table or transformation query
    -- Example:  SELECT * FROM lft.beproduct.your_staging_table
    SELECT
        directory_id,   -- match key (human-readable partner code)
        name,
        address,
        country,
        state,
        zip,
        city,
        phone,
        fax,
        website,
        notes,
        active,
        partner_type    -- used only for new inserts (cannot change after creation)
    FROM <your_source>
) AS src
ON tgt.directory_id = src.directory_id

-- ── Existing record: only update when something actually changed ─────────────
WHEN MATCHED AND (
       tgt.name     IS DISTINCT FROM src.name
    OR tgt.address  IS DISTINCT FROM src.address
    OR tgt.country  IS DISTINCT FROM src.country
    OR tgt.state    IS DISTINCT FROM src.state
    OR tgt.zip      IS DISTINCT FROM src.zip
    OR tgt.city     IS DISTINCT FROM src.city
    OR tgt.phone    IS DISTINCT FROM src.phone
    OR tgt.fax      IS DISTINCT FROM src.fax
    OR tgt.website  IS DISTINCT FROM src.website
    OR tgt.notes    IS DISTINCT FROM src.notes
    OR tgt.active   IS DISTINCT FROM src.active
    -- TODO: add / remove field comparisons to match your source columns
)
THEN UPDATE SET
    tgt.name        = src.name,
    tgt.address     = src.address,
    tgt.country     = src.country,
    tgt.state       = src.state,
    tgt.zip         = src.zip,
    tgt.city        = src.city,
    tgt.phone       = src.phone,
    tgt.fax         = src.fax,
    tgt.website     = src.website,
    tgt.notes       = src.notes,
    tgt.active      = src.active,
    tgt.modified_at = current_timestamp()   -- ← flags this row for PUSH_DIRECTORY

-- ── New record: id = NULL so PUSH_DIRECTORY calls Directory/Add ──────────────
WHEN NOT MATCHED BY TARGET
THEN INSERT (
    id,
    directory_id, name, partner_type,
    address, country, state, zip, city, phone, fax, website, notes,
    active,
    data_json,
    extracted_at, bp_modified_at,
    modified_at
)
VALUES (
    NULL,                       -- id = NULL  → Add on push; filled in after push
    src.directory_id,
    src.name,
    src.partner_type,           -- set once; cannot change after creation
    src.address, src.country, src.state, src.zip, src.city,
    src.phone, src.fax, src.website, src.notes,
    COALESCE(src.active, true),
    NULL,                       -- data_json populated on next PULL_ONLY
    NULL,                       -- extracted_at = NULL = never pulled from BeProduct
    NULL,                       -- bp_modified_at = NULL = never pulled
    current_timestamp()         -- modified_at flags the row for push immediately
)

-- ── Optional: deactivate records absent from the source ──────────────────────
-- Uncomment if your source is authoritative (i.e. missing = deleted).
-- WHEN NOT MATCHED BY SOURCE AND tgt.active = true
-- THEN UPDATE SET
--     tgt.active      = false,
--     tgt.modified_at = current_timestamp()
"""

print("=" * 80)
print("CELL 5b — DATA MASSAGE template (read-only display)")
print("=" * 80)
print("Copy and adapt the MERGE SQL below, then uncomment spark.sql().")
print(_MERGE_TEMPLATE)

# ── Uncomment and fill in your source query to run the merge ─────────────────
# spark.sql(_MERGE_TEMPLATE)

# COMMAND ----------
# ============================================================================
# CELL 6 — PUSH: MasterData choices
#          Runs when mode = PUSH_ONLY or PUSH_MASTER_DATA.
#
# API: POST /api/{company}/MasterData/{fieldId}/Update
# SDK: api.raw_api.post("MasterData/{fieldId}/Update", body=payload)
#
# Strategy: simple full-list overwrite.
# Dropdown values are static and small (dozens of rows). The entire choices
# list from each beproduct_master_* Delta table is sent as the desired state.
#
# PATCH semantics still apply server-side (choices absent from the payload
# are untouched in BeProduct), but since the table holds ALL desired choices
# after a pull + any admin edits, sending the full list achieves an effective
# overwrite without needing a diff.
#
# To deactivate a choice: set active = false in the Delta table.
# To add a new choice:    insert a row with the new value string.
# No delete support (deactivate is the safe equivalent).
#
# Payload per field:
#   {"choices": {"items": [{"value": "…", "code": "…", "active": true/false}, …]}}
# ============================================================================

if not _DO_PUSH_MASTER:
    print(f"Skipping MasterData push (mode={MODE})")
else:
    print("=" * 80)
    print(f"PUSH: MasterData choices  [dry_run={DRY_RUN}]")
    print("=" * 80)

    for _data_type, _field_id in MASTER_DATA_FIELDS.items():
        _tbl_path = tbl(f"beproduct_master_{_data_type}")
        print(f"\n  [{_data_type}]  fieldId={_field_id}")

        try:
            _df = spark.table(_tbl_path)
            _rows = [r.asDict() for r in _df.collect()]
        except Exception as _e:
            print(f"    ❌ Cannot read {_tbl_path}: {_e}")
            continue

        if not _rows:
            print(f"    ⚠️  Empty table — skipping")
            continue

        # Build flat choice items: value (required key), code, active
        _items: List[Dict[str, Any]] = []
        for _r in _rows:
            _val = _r.get("value", "")
            if not _val:
                continue
            _item: Dict[str, Any] = {"value": _val}
            if _r.get("code") is not None:
                _item["code"] = _r["code"]
            _item["active"] = bool(_r.get("active", True))
            _items.append(_item)

        _payload = {"choices": {"items": _items}}

        if DRY_RUN:
            print(f"    [DRY RUN] Would POST MasterData/{_field_id}/Update")
            print(f"    {len(_items)} items  |  sample: {_items[0] if _items else 'N/A'}")
        else:
            try:
                _resp = api.raw_api.post(
                    f"MasterData/{_field_id}/Update", body=_payload
                )
                _updated_id = (
                    _resp.get("fieldId", _field_id)
                    if isinstance(_resp, dict)
                    else "OK"
                )
                print(f"    ✅ Pushed {len(_items)} items → fieldId={_updated_id}")
            except Exception as _e:
                print(f"    ❌ Push failed: {_e}")
                import traceback; traceback.print_exc()

    print("\n✅ MasterData push complete")

# COMMAND ----------
# ============================================================================
# CELL 7 — PUSH: Directory companies + contacts  (skipped unless PUSH_DIRECTORY/ALL)
#
# Company push rules:
#   id = NULL   → Add   via SDK  api.directory.directory_add(fields=…)
#                              POST /api/{co}/Directory/Add
#   id = <uuid> → Update via raw_api  api.raw_api.post("Directory/Update/{id}", …)
#                              POST /api/{co}/Directory/Update/{id}
#   NOTE: partnerType is included in Add payload only — cannot be changed after create.
#
# Contact push rules (requires parent company UUID in directory_id column):
#   contact_id = NULL   → Add    via SDK  api.directory.directory_contact_add(…)
#   contact_id = <uuid> → Update via raw_api  api.raw_api.post(…)
#   NOTE: email/firstName/lastName cannot be changed for fully-registered BeProduct users.
#
# For new companies added in this run, contacts in beproduct_directory_contacts whose
# directory_id matches the company's NEW uuid will be processed in the contacts loop.
# If contacts for a brand-new company are embedded in the row's contacts_json column
# they will be included in the Add payload directly (atomic create + contacts).
# ============================================================================

if not _DO_PUSH_DIR:
    print(f"Skipping Directory push (mode={MODE})")
else:
    print("=" * 80)
    print(f"PUSH: Directory companies and contacts  [dry_run={DRY_RUN}]")
    print("=" * 80)

    # ── Companies ─────────────────────────────────────────────────────────────
    print("\n─── Companies ───")
    try:
        _df_dir = spark.table(tbl("beproduct_directory"))
        _all_company_rows = [r.asDict() for r in _df_dir.collect()]
    except Exception as _e:
        print(f"  ❌ Cannot read beproduct_directory: {_e}")
        _all_company_rows = []

    # ── Change detection ──────────────────────────────────────────────────────
    # Push only rows where:
    #   id IS NULL          → new record (call Directory/Add)
    #   extracted_at IS NULL → added externally, never pulled
    #   modified_at > extracted_at → changed after the last pull (pending change)
    def _is_pending(r: Dict) -> bool:
        if not r.get("id"):
            return True
        if not r.get("extracted_at"):
            return True
        return (r.get("modified_at") or "") > (r.get("extracted_at") or "")

    _company_rows = [r for r in _all_company_rows if _is_pending(r)]
    _skipped = len(_all_company_rows) - len(_company_rows)
    print(
        f"  {len(_all_company_rows)} total rows  |  "
        f"{len(_company_rows)} pending  |  {_skipped} already in sync (skipped)"
    )

    _c_add = _c_upd = _c_err = 0
    _pushed_update_ids: List[str] = []   # UUIDs of successfully updated companies
    _pushed_add_codes:  List[str] = []   # directory_id codes of successfully added companies

    for _row in _company_rows:
        _rec_id      = _row.get("id")
        _dir_id_code = _row.get("directory_id")

        # Build shared write payload (same fields for Add and Update)
        _payload: Dict[str, Any] = {}
        for _src, _dst in [
            ("directory_id", "directoryId"),
            ("name",         "name"),
            ("address",      "address"),
            ("country",      "country"),
            ("zip",          "zip"),
            ("state",        "state"),
            ("city",         "city"),
            ("phone",        "phone"),
            ("fax",          "fax"),
            ("website",      "website"),
            ("notes",        "notes"),
        ]:
            _v = _row.get(_src)
            if _v is not None:
                _payload[_dst] = _v
        if _row.get("active") is not None:
            _payload["active"] = bool(_row["active"])

        try:
            if not _rec_id:
                # ── New company ─────────────────────────────────────────────
                if _row.get("partner_type"):
                    _payload["partnerType"] = _row["partner_type"]
                # Embed pre-defined contacts if contacts_json column is present
                _contacts_json = _row.get("contacts_json")
                if _contacts_json:
                    try:
                        _payload["contacts"] = json.loads(_contacts_json)
                    except Exception:
                        pass  # malformed JSON — skip embedding, add contacts separately

                if DRY_RUN:
                    print(
                        f"  [DRY RUN] ADD: {_payload.get('name', '?')}"
                        f"  ({_payload.get('directoryId', '?')})"
                        f"  partnerType={_payload.get('partnerType', '?')}"
                    )
                else:
                    _resp = api.directory.directory_add(fields=_payload)
                    _new_uuid = _resp.get("id") if isinstance(_resp, dict) else None
                    if _new_uuid and _dir_id_code:
                        # Stamp the returned UUID + mark as synced in the Delta table
                        spark.sql(f"""
                            UPDATE {tbl('beproduct_directory')}
                            SET id           = '{_new_uuid}',
                                extracted_at = current_timestamp(),
                                modified_at  = current_timestamp()
                            WHERE directory_id = '{_dir_id_code}' AND id IS NULL
                        """)
                    print(
                        f"  ✅ Added: {_payload.get('name', '?')}"
                        f"  → id={_new_uuid}"
                    )
                    if _dir_id_code:
                        _pushed_add_codes.append(_dir_id_code)
                _c_add += 1

            else:
                # ── Existing company — Update ────────────────────────────────
                # partnerType excluded from Update (API restriction)
                if DRY_RUN:
                    print(
                        f"  [DRY RUN] UPDATE id={_rec_id}:"
                        f"  {_payload.get('name', '?')}"
                    )
                else:
                    _resp = api.raw_api.post(
                        f"Directory/Update/{_rec_id}", body=_payload
                    )
                    print(f"  ✅ Updated: {_payload.get('name', '?')}  (id={_rec_id})")
                    _pushed_update_ids.append(_rec_id)
                _c_upd += 1

        except Exception as _e:
            print(
                f"  ❌ Error for {_row.get('name', '?')}"
                f"  (id={_rec_id}): {_e}"
            )
            _c_err += 1

    print(f"\n  Companies — add: {_c_add}, update: {_c_upd}, errors: {_c_err}")

    # ── Mark updated companies as synced (extracted_at ← now) ─────────────────
    # For ADD rows the per-row UPDATE above already wrote the new UUID + timestamps.
    # For UPDATE rows we do a single batched SQL to avoid N round-trips.
    if not DRY_RUN and _pushed_update_ids:
        _ids_sql = ", ".join(f"'{_i}'" for _i in _pushed_update_ids)
        spark.sql(f"""
            UPDATE {tbl('beproduct_directory')}
            SET extracted_at = current_timestamp(),
                modified_at  = current_timestamp()
            WHERE id IN ({_ids_sql})
        """)
        print(f"  ✅ extracted_at updated for {len(_pushed_update_ids)} updated companies")

    # ── Contacts ───────────────────────────────────────────────────────────────
    print("\n─── Contacts ───")
    _contacts_tbl = tbl("beproduct_directory_contacts")
    if not spark.catalog.tableExists(_contacts_tbl):
        print(f"  ⚪ {_contacts_tbl} does not exist yet — skipping.")
        print(f"     (Run mode=PULL_ONLY with fetch_contacts=true to create it.)")
        _contact_push_rows = []
    else:
        try:
            _df_ct = spark.table(_contacts_tbl)
            _contact_push_rows = [r.asDict() for r in _df_ct.collect()]
        except Exception as _e:
            print(f"  ❌ Cannot read {_contacts_tbl}: {_e}")
            _contact_push_rows = []

    _ct_add = _ct_upd = _ct_err = 0

    for _row in _contact_push_rows:
        _parent_uuid = _row.get("directory_id")  # company UUID
        _contact_id  = _row.get("contact_id")

        if not _parent_uuid:
            print(
                f"  ⚠️  Skipped contact {_row.get('email', '?')}"
                f" — missing directory_id (company UUID)"
            )
            continue

        _payload_c: Dict[str, Any] = {}
        for _src, _dst in [
            ("email",        "email"),
            ("first_name",   "firstName"),
            ("last_name",    "lastName"),
            ("title",        "title"),
            ("mobile_phone", "mobilePhone"),
            ("work_phone",   "workPhone"),
            ("role",         "role"),
        ]:
            _v = _row.get(_src)
            if _v is not None:
                _payload_c[_dst] = _v
        if _row.get("active") is not None:
            _payload_c["active"] = bool(_row["active"])

        try:
            if not _contact_id:
                # ── New contact ──────────────────────────────────────────────
                if DRY_RUN:
                    print(
                        f"  [DRY RUN] ADD contact {_payload_c.get('email', '?')}"
                        f"  → dir {_parent_uuid}"
                    )
                else:
                    _resp = api.directory.directory_contact_add(
                        header_id=_parent_uuid,
                        fields=_payload_c,
                    )
                    print(f"  ✅ Added contact {_payload_c.get('email', '?')}")
                _ct_add += 1

            else:
                # ── Existing contact — Update ────────────────────────────────
                # email/firstName/lastName ignored by API for fully-registered users
                if DRY_RUN:
                    print(
                        f"  [DRY RUN] UPDATE contact {_contact_id}"
                        f"  → dir {_parent_uuid}"
                    )
                else:
                    _resp = api.raw_api.post(
                        f"Directory/{_parent_uuid}/Contact/{_contact_id}/Update",
                        body=_payload_c,
                    )
                    print(f"  ✅ Updated contact {_contact_id}")
                _ct_upd += 1

        except Exception as _e:
            print(
                f"  ❌ Contact error {_row.get('email', '?')}"
                f"  (id={_contact_id}): {_e}"
            )
            _ct_err += 1

    print(f"\n  Contacts — add: {_ct_add}, update: {_ct_upd}, errors: {_ct_err}")
    print("\n✅ Directory push complete")

# COMMAND ----------
# ============================================================================
# CELL 8 — Summary
# ============================================================================

print("=" * 80)
print("SYNC SUMMARY")
print("=" * 80)
print(f"  mode:      {MODE}")
print(f"  dry_run:   {DRY_RUN}")
print(f"  catalog:   {CATALOG}")
print(f"  schema:    {SCHEMA_NAME}")
print(f"  timestamp: {datetime.now(timezone.utc).isoformat()}")
print()

# Row counts for all relevant tables
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

_all_tables = (
    [f"beproduct_master_{k}" for k in MASTER_DATA_FIELDS]
    + ["beproduct_directory", "beproduct_directory_contacts"]
)

print("Delta table row counts:")
for _t in _all_tables:
    # Use tableExists first — avoids SQLQueryContextLogger noise on missing tables.
    if not spark.catalog.tableExists(f"{CATALOG}.{SCHEMA_NAME}.{_t}"):
        print(f"  -  {_t}: (not created)")
        continue
    try:
        _cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {tbl(_t)}").collect()[0]["c"]
        _mark = "✓" if _cnt > 0 else "○"
        print(f"  {_mark}  {_t}: {_cnt}")
    except Exception as _ex:
        print(f"  ?  {_t}: (count failed — {_ex})")

print()
print("✅ Done")
