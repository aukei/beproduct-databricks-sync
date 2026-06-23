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
PUSH_MASTER_DATA  Read beproduct_master_* Delta tables and push choice changes
                  back to BeProduct. Uses PATCH semantics: only choices present
                  in the payload are touched; absent choices are left unchanged.
PUSH_DIRECTORY    Read beproduct_directory + beproduct_directory_contacts Delta
                  tables and push record changes to BeProduct.
PUSH_ALL          PUSH_MASTER_DATA + PUSH_DIRECTORY in one run.

TYPICAL ADMIN WORKFLOW
----------------------
1. Run PULL_ONLY        → inspect Delta tables in Databricks SQL / notebook
2. Edit table rows:
     - New company/contact  → add a row with id / contact_id = NULL
     - Update existing      → edit relevant columns
     - Deactivate a choice  → set active = false (NOT deleted unless
                              delete_choice column is also set to true)
3. Run PUSH_* with dry_run = true  → review planned changes in cell output
4. Re-run with dry_run = false     → commit to BeProduct

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
mode        : PULL_ONLY | PUSH_MASTER_DATA | PUSH_DIRECTORY | PUSH_ALL
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
    ["PULL_ONLY", "PUSH_MASTER_DATA", "PUSH_DIRECTORY", "PUSH_ALL"],
    "Sync Mode",
)
dbutils.widgets.dropdown(
    "dry_run", "true", ["true", "false"], "Dry Run (push only)"
)

# ── Read parameters ───────────────────────────────────────────────────────────
CATALOG     = dbutils.widgets.get("catalog")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
MODE        = dbutils.widgets.get("mode").upper()
DRY_RUN     = dbutils.widgets.get("dry_run").lower() == "true"

print(f"catalog:     {CATALOG}")
print(f"schema_name: {SCHEMA_NAME}")
print(f"mode:        {MODE}")
print(f"dry_run:     {DRY_RUN}")


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
#     id           STRING  — BeProduct UUID (needed for Update; NULL for new rows)
#     directory_id STRING  — human-readable partner code (e.g. "FACTORY001")
#     name         STRING
#     partner_type STRING  — VENDOR / FACTORY / etc.  CANNOT be changed after create
#     address / country / state / zip / city / phone / fax / website / notes
#     active       BOOLEAN
#     data_json    STRING  — full raw company JSON
#     synced_at    STRING
#
#   beproduct_directory_contacts
#     directory_id STRING NOT NULL — parent company UUID (FK to beproduct_directory.id)
#     contact_id   STRING          — BeProduct contact id (NULL → new contact to add)
#     email / first_name / last_name / title / mobile_phone / work_phone / role
#     active       BOOLEAN
#     data_json    STRING
#     synced_at    STRING
# ============================================================================

_DIRECTORY_SCHEMA = StructType(
    [
        StructField("id",           StringType(),  nullable=True),
        StructField("directory_id", StringType(),  nullable=True),
        StructField("name",         StringType(),  nullable=True),
        StructField("partner_type", StringType(),  nullable=True),
        StructField("address",      StringType(),  nullable=True),
        StructField("country",      StringType(),  nullable=True),
        StructField("state",        StringType(),  nullable=True),
        StructField("zip",          StringType(),  nullable=True),
        StructField("city",         StringType(),  nullable=True),
        StructField("phone",        StringType(),  nullable=True),
        StructField("fax",          StringType(),  nullable=True),
        StructField("website",      StringType(),  nullable=True),
        StructField("notes",        StringType(),  nullable=True),
        StructField("active",       BooleanType(), nullable=True),
        StructField("data_json",    StringType(),  nullable=False),
        StructField("synced_at",    StringType(),  nullable=False),
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

print("=" * 80)
print("PULL: Directory (companies + contacts)")
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

    _rec_id = _rec.get("id")

    _dir_rows.append(
        {
            "id":           str(_rec_id) if _rec_id else None,
            "directory_id": _rec.get("directoryId"),
            "name":         _rec.get("name"),
            "partner_type": _rec.get("partnerType"),
            "address":      _rec.get("address"),
            "country":      _rec.get("country"),
            "state":        _rec.get("state"),
            "zip":          _rec.get("zip"),
            "city":         _rec.get("city"),
            "phone":        _rec.get("phone"),
            "fax":          _rec.get("fax"),
            "website":      _rec.get("website"),
            "notes":        _rec.get("notes"),
            "active":       bool(_rec.get("active", True)),
            "data_json":    json.dumps(_rec),
            "synced_at":    _dir_now,
        }
    )

    # Fetch contacts for this company (keyed by company UUID)
    if _rec_id:
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

print(
    f"\nFetched {_dir_count} companies, {len(_contact_rows)} contacts"
    + (f"  ({_contact_fetch_errors} contact-fetch errors)" if _contact_fetch_errors else "")
)

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
# CELL 6 — PUSH: MasterData choices  (skipped unless mode includes PUSH_MASTER_DATA)
#
# API: POST /api/{company}/MasterData/{fieldId}/Update
# SDK: api.raw_api.post("MasterData/{fieldId}/Update", body=payload)
#
# Payload:
#   {
#     "choices": {
#       "items": [
#         {
#           "value":       "<existing or new choice string>",  ← match key
#           "code":        "<short code>",        ← optional
#           "active":      true | false,           ← optional; false = deactivate
#           "updateValue": "<new display string>", ← optional; renames the choice
#           "deleteChoice": true                   ← optional; permanently removes
#         }
#       ]
#     }
#   }
#
# Patch semantics:
#   - Choices included in `items` are created (if new value) or updated.
#   - Choices absent from `items` are left unchanged in BeProduct.
#   - To deactivate: include with active=false.
#   - To rename:     include with value=<old> + updateValue=<new>.
#   - To delete:     include with value=<old> + deleteChoice=true.
#
# The Delta table drives the desired state.
# Optional extra columns admins can add to the table:
#   update_value  STRING  — renames the choice to this new display string
#   delete_choice BOOLEAN — permanently deletes the choice when true
# ============================================================================

if MODE not in ("PUSH_MASTER_DATA", "PUSH_ALL"):
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

        # Build choices items list
        _items: List[Dict[str, Any]] = []
        for _r in _rows:
            _val = _r.get("value", "")
            if not _val:
                continue
            _item: Dict[str, Any] = {"value": _val}
            if _r.get("code") is not None:
                _item["code"] = _r["code"]
            if _r.get("active") is not None:
                _item["active"] = bool(_r["active"])
            # Optional admin-managed columns
            if _r.get("update_value"):
                _item["updateValue"] = _r["update_value"]
            if _r.get("delete_choice"):
                _item["deleteChoice"] = True
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

if MODE not in ("PUSH_DIRECTORY", "PUSH_ALL"):
    print(f"Skipping Directory push (mode={MODE})")
else:
    print("=" * 80)
    print(f"PUSH: Directory companies and contacts  [dry_run={DRY_RUN}]")
    print("=" * 80)

    # ── Companies ─────────────────────────────────────────────────────────────
    print("\n─── Companies ───")
    try:
        _df_dir = spark.table(tbl("beproduct_directory"))
        _company_rows = [r.asDict() for r in _df_dir.collect()]
    except Exception as _e:
        print(f"  ❌ Cannot read beproduct_directory: {_e}")
        _company_rows = []

    _c_add = _c_upd = _c_err = 0

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
                    print(
                        f"  ✅ Added: {_payload.get('name', '?')}"
                        f"  → id={_new_uuid}"
                    )
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
                _c_upd += 1

        except Exception as _e:
            print(
                f"  ❌ Error for {_row.get('name', '?')}"
                f"  (id={_rec_id}): {_e}"
            )
            _c_err += 1

    print(f"\n  Companies — add: {_c_add}, update: {_c_upd}, errors: {_c_err}")

    # ── Contacts ───────────────────────────────────────────────────────────────
    print("\n─── Contacts ───")
    try:
        _df_ct = spark.table(tbl("beproduct_directory_contacts"))
        _contact_push_rows = [r.asDict() for r in _df_ct.collect()]
    except Exception as _e:
        print(f"  ❌ Cannot read beproduct_directory_contacts: {_e}")
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
    try:
        _cnt = spark.sql(
            f"SELECT COUNT(*) AS c FROM {tbl(_t)}"
        ).collect()[0]["c"]
        _mark = "✓" if _cnt > 0 else "○"
        print(f"  {_mark}  {_t}: {_cnt}")
    except Exception:
        print(f"  -  {_t}: (not found)")

print()
print("✅ Done")
