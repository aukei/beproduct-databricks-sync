"""
Phase 0 — XTS Master (DTC) -> BeProduct Directory master data.

Pure-Python helpers (no Spark, no network) for staging DTC's "XTS Master"
document (workspace "KTB") into `lft.beproduct.beproduct_directory`, unit
tested in `dtc/tests/test_xts_master.py`. Notebooks
(`dtc/notebooks/p0_pull_xts_master_to_delta.py` and
`beproduct/p0_xts_master_to_directory_upsert.py`) are thin Spark/IO wrappers
around this module.

SCOPE (clarified 2026-08-28): only Supplier and Factory are in scope for now.
"XTS Mill Master" is intentionally excluded — the Mill sheet has no code
column and (per live discovery below) currently holds no real company data
in UAT anyway. Re-adding Mill later only requires adding it back to
XTS_REQUESTS/FIELD_MAP/EXCLUDE_TYPE_VALUES; the rest of the pipeline is
partner_type-agnostic.

Source (live-validated 2026-08-28 against DTC UAT via `get_view_definition` +
sample `get_sheet` rows):

  DTC workspace "KTB", document "XTS Master" has exactly 2 in-scope requests
  (there are also "(BACKUP)"-named siblings, deliberately excluded — only the
  exact names below are pulled; a 3rd real request, "XTS Mill Master", also
  exists but is out of scope per the note above):

    request reference       -> partner_type  view used
    "XTS Supplier Master"   -> SUPPLIER       "Supplier"
    "XTS Factory Master"    -> FACTORY        "Factory"

  IMPORTANT — this document is NOT a rich vendor-master sheet. Its views are
  authoritatively (confirmed via `GET /v1/views/{id}`, not just sample rows,
  which can hide always-blank columns):

    "Supplier": Supplier Name, Supplier Code, Customer Vendor ID, Type,
                Request Element, Brands Users (using email address), Brand
                Views, Supplier/Mill (using email address), Supplier Views,
                Group Users Name, Agent Alert Recipient (using email
                address), Agent Alert Recipient - cc (...), Request email
                updated, Request email updated date
    "Factory":  Factory Name, Factory Code, Customer Factory ID,
                Production Country

  So in practice:
    - "Type" (present on Supplier, absent on Factory) does NOT itself hold a
      SUPPLIER/FACTORY partner type — partner_type is derived from WHICH
      REQUEST/view the row came from (per XTS_REQUESTS below). HOWEVER "Type"
      is NOT purely metadata either — on the Supplier sheet it is also used
      to mark BRAND-level access-sharing rows interleaved with real company
      rows, and those must be excluded (see next point).
    - **CRITICAL (live-verified 2026-08-28, full Type-value scan):** the
      "Supplier" sheet is a MIX of real supplier companies (`Type="Supplier"`,
      34/42 rows in UAT, each with a real `Supplier Code`) AND brand-level
      view-sharing config rows (`Type="Brand"`, 8/42 rows, e.g. "Wrangler",
      "Blue Bell", "Slam Jam" — these are BRAND names, not companies, and
      always have a blank code). `EXCLUDE_TYPE_VALUES` filters these brand
      rows out; without it, `beproduct_directory` would be polluted with fake
      "SUPPLIER" partner records that are actually just brand names.
      (The excluded "XTS Mill Master" sheet was, for reference, 100% of this
      same kind of brand row in UAT — `Type="Fabric Brand"`, no real Mill
      company data existed there at all.)
    - None of address/state/zip/city/phone/fax/website/notes exist ANYWHERE
      in this document (both views) -> always None for every XTS-sourced row
      (`DIRECTORY_OPTIONAL_COLS`) until DTC adds them. This was confirmed to
      be a genuine absence (not a "blank on this row" artifact) via the view
      definition, not just a sample of populated rows.
    - Factory's "Customer Factory ID" is a distinct field NOT used per spec
      (which maps the literal "[Factory] Code" -> directory_id); left
      unmapped, available in `data_json` if ever needed.

Match key: BeProduct's Directory record is keyed by **`name` + `type`**
together (clarified 2026-08-28 by the project team) — not `name` alone, and
not `id`/`directory_id` (despite those columns existing). This means the SAME
name is perfectly fine across DIFFERENT partner types (e.g. "SUPPLIER ASPGAR"
can legitimately exist as both a SUPPLIER record and a FACTORY record — the
same physical entity acting in two roles — 19/34 real Supplier rows in UAT
have exactly this cross-type name match with a Factory row). The upsert MERGE
(built in `beproduct/p0_xts_master_to_directory_upsert.py`) therefore matches
`ON tgt.name = src.name AND tgt.partner_type = src.partner_type`.

A genuine collision only happens when the SAME (name, partner_type) pair
appears more than once (e.g. a duplicate row within one sheet) —
find_duplicate_keys/dedupe_by_key operate on that composite key.
"""

from typing import Any, Dict, List, Optional, Tuple

# request_reference (exact match only - "(BACKUP)"-named siblings and "XTS
# Mill Master" are excluded) -> {partner_type, view_name}
XTS_REQUESTS: Dict[str, Dict[str, str]] = {
    "XTS Supplier Master": {"partner_type": "SUPPLIER", "view_name": "Supplier"},
    "XTS Factory Master":  {"partner_type": "FACTORY",  "view_name": "Factory"},
}

# partner_type -> DTC column names (None = field does not exist in DTC for
# this partner type; always leave the mapped output column as None).
FIELD_MAP: Dict[str, Dict[str, Optional[str]]] = {
    "SUPPLIER": {"name": "Supplier Name", "code": "Supplier Code", "country": None},
    "FACTORY":  {"name": "Factory Name",  "code": "Factory Code",  "country": "Production Country"},
}

# partner_type -> the sheet's "Type" column value that marks a row as a
# BRAND-level access-sharing config entry (not a real company) to be
# EXCLUDED from the Directory extraction entirely. Factory has no "Type"
# column at all (None = no exclusion filter applies / nothing to exclude).
# Live-verified 2026-08-28: Supplier sheet mixes Type="Supplier" (real, kept)
# with Type="Brand" (excluded).
EXCLUDE_TYPE_VALUES: Dict[str, Optional[str]] = {
    "SUPPLIER": "Brand",
    "FACTORY":  None,
}

# Columns beproduct_directory has that XTS Master has no source field for at
# all (not merely blank-on-some-rows) - always None for XTS-sourced rows.
DIRECTORY_OPTIONAL_COLS: Tuple[str, ...] = (
    "address", "state", "zip", "city", "phone", "fax", "website", "notes",
)


def norm(v: Optional[Any]) -> Optional[str]:
    """Trim to a non-empty string, or None."""
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def is_brand_row(partner_type: str, row: Dict[str, Any]) -> bool:
    """True if `row` is a brand-level access-sharing config entry (NOT a real
    company) that must be excluded from the Directory extraction - see
    EXCLUDE_TYPE_VALUES and the module docstring's "CRITICAL" note."""
    exclude_value = EXCLUDE_TYPE_VALUES.get(partner_type)
    if exclude_value is None:
        return False
    return norm(row.get("Type")) == exclude_value


def extract_directory_row(
    partner_type: str,
    row: Dict[str, Any],
    *,
    request_id: str,
    request_reference: str,
) -> Optional[Dict[str, Any]]:
    """Build one normalized beproduct_directory-shaped dict from a raw XTS
    Master DTC sheet row, or None when the row should not become a Directory
    record: either it has no name (a name is required - it's part of the
    Directory match key), or it is a brand-level config row rather than a
    real company (see is_brand_row / EXCLUDE_TYPE_VALUES).

    :partner_type: one of "SUPPLIER" | "FACTORY" (see XTS_REQUESTS)
    :row: raw DTC sheetData row dict (as returned by DTCConnector.get_sheet)
    :request_id/request_reference: carried through for traceability/logging
    :returns: dict with name, directory_id, partner_type, country, the
              always-None optional columns, plus row_id/row_index/
              request_id/request_reference - or None if unnamed/excluded.
    """
    spec = FIELD_MAP.get(partner_type)
    if spec is None:
        raise ValueError(
            f"Unknown XTS partner_type {partner_type!r}; expected one of "
            f"{sorted(FIELD_MAP)}"
        )

    if is_brand_row(partner_type, row):
        return None

    name = norm(row.get(spec["name"]))
    if not name:
        return None

    code_col = spec["code"]
    country_col = spec["country"]

    out: Dict[str, Any] = {
        "name": name,
        "directory_id": norm(row.get(code_col)) if code_col else None,
        "partner_type": partner_type,
        "country": norm(row.get(country_col)) if country_col else None,
        "row_id": row.get("rowId"),
        "row_index": (int(row["rowIndex"]) if row.get("rowIndex") is not None else None),
        "request_id": request_id,
        "request_reference": request_reference,
    }
    for col in DIRECTORY_OPTIONAL_COLS:
        out[col] = None
    return out


def _key(r: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """The real BeProduct Directory match key: (name, partner_type)."""
    return (r.get("name"), r.get("partner_type"))


def find_duplicate_keys(
    rows: List[Dict[str, Any]],
) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    """{(name, partner_type): [row, ...]} for every (name, partner_type) pair
    that appears on 2+ extracted rows.

    BeProduct's real Directory match key is the (name, partner_type) PAIR —
    the SAME name across DIFFERENT partner types is expected and fine (e.g.
    the same company as both a SUPPLIER and a FACTORY record), so that is
    NOT reported here. Only a true collision - the identical pair repeated -
    means the upsert cannot deterministically tell which source row should
    own that BeProduct record; callers must flag it (see dedupe_by_key),
    never silently let one arbitrary row win without logging.
    """
    by_key: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for r in rows:
        name, ptype = _key(r)
        if not name or not ptype:
            continue
        by_key.setdefault((name, ptype), []).append(r)
    return {k: rs for k, rs in by_key.items() if len(rs) > 1}


def dedupe_by_key(
    rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[Tuple[str, str], List[Dict[str, Any]]]]:
    """Collapse `rows` to at most one row per (name, partner_type) for the
    upsert MERGE source, plus the full duplicate map (for logging - see
    find_duplicate_keys) so nothing is silently dropped without a trace.

    Winner selection when a (name, partner_type) pair collides (stable,
    deterministic - never arbitrary iteration order):
      1. Prefer a row with a non-null `directory_id` (has a real code) over
         one without.
      2. Then prefer the lower `row_index` (stable tie-break).

    :returns: (winners, duplicates) - `winners` has exactly one row per
              distinct (name, partner_type); `duplicates` is the
              find_duplicate_keys() map (empty dict when there were no
              collisions at all).
    """
    duplicates = find_duplicate_keys(rows)

    def _sort_key(r: Dict[str, Any]) -> Tuple[int, int]:
        has_code = 0 if r.get("directory_id") else 1
        row_index = r.get("row_index") if r.get("row_index") is not None else 10**9
        return (has_code, row_index)

    winners_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in rows:
        name, ptype = _key(r)
        if not name or not ptype:
            continue
        k = (name, ptype)
        current = winners_by_key.get(k)
        if current is None or _sort_key(r) < _sort_key(current):
            winners_by_key[k] = r

    # Preserve first-seen order for determinism/readability.
    seen_order: List[Tuple[str, str]] = []
    for r in rows:
        name, ptype = _key(r)
        if name and ptype and (name, ptype) not in seen_order:
            seen_order.append((name, ptype))

    winners = [winners_by_key[k] for k in seen_order]
    return winners, duplicates
