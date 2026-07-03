"""
Phase 1 BeProduct -> DTC upsert core (pure Python, no Spark / no HTTP).

This module holds the deterministic, unit-testable logic for the Phase 1
workflow described in dtc/PHASE1_WORKFLOW.md:

  * parse / scope-check a DTC request reference  ("KTB FW26 Wrangler")
  * map BeProduct (transformed/denormalized) staging rows to the real DTC
    WIP_ITS_USE column names, EXCLUDING "Style Image"
  * upsert BeProduct rows onto the current DTC rows using the in-request row
    key (BP Style#, Color / Wash) -> UPDATE existing / INSERT new
  * assign sparse-aware rowIndex for inserts, partitioned by (season, brand)
  * surface exceptions (mismatched scope, duplicate keys, unmapped data)

Phase 6 structural update (2026-06-26):
  - Match key changed from ("LF Style#", "Color / Wash") to
    ("BP Style#", "Color / Wash"). BeProduct's header_number field was renamed
    from "LF Style Number" to "BP Style Number" in BeProduct; the staging column
    is now bp_style_number (was lf_style_number).
  - Composite key for request routing: [Customer, BP Style#, SeasonCode, Brand]
    where Brand comes from brand_hk (single-value field, not brands_multi).
  - DTC "LF Style#" column is now OPTIONAL BeProduct->DTC (from the new separate
    lf_style_number field); it is no longer a match key.
  - DTC "Legacy Code" column is now OPTIONAL BeProduct->DTC, populated from
    BP's customer_style_number. Previously it was DTC->BP (that role moved to the
    new DTC "Customer Style#" column, handled in phase2.py).
  - New DTC "BP STYLE#" column (informational copy of the BP Style# match key)
    is also written BeProduct->DTC.

Why (BP Style#, Color / Wash) as the in-request key
----------------------------------------------------
DTC identifies a season as (Customer, SeasonCode) and the project guarantees
exactly ONE brand per request, agreeing with the request name. So within a
single request the (Seasoncode, Brand) part of the composite key is constant.
The denormalization step explodes each style into one row per colorway, so the
value that actually distinguishes rows inside a request is the colorway. Hence
the row identity used for matching is (BP Style#, Color / Wash). RowIndex is
therefore numbered per request (= per season+brand).

Phase 1 pushes only BeProduct-OWNED columns (Product Status, Style Description,
Class, Sub Class, Division, Brand, Garment Finish, Tech Pack Stage, Fabric Group,
Placement, Gender, BP Style#, LF Style# (optional), Legacy Code (optional);
Supplier (default-fill: "Supplier" only when DTC cell is blank);
Style Image excluded). The DTC-owned columns (Lot#,
Main Vendor (Sampling), Main Factory (Sampling), Main Factory Customer ID) flow
the other way (DTC -> BeProduct) and live in sync/phase2.py; they are NOT in
FIELD_MAPPING so a field is never synced in both directions.

All API I/O lives in connectors.dtc.DTCConnector; this module never calls it.
The DTC column names below were validated live against the WIP_ITS_USE view
definition on 2026-06-17 (GET /v1/views/{viewId} -> 178 dynamicFields). The
column is "Division" (an earlier "Division?" with a trailing '?' was renamed).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Column constants (validated against the live WIP_ITS_USE view)
# ---------------------------------------------------------------------------

STYLE_IMAGE_COL = "Style Image"  # never written in Phase 1 (requirement 3a)

# In-request row identity (see module docstring).
# Phase 6: changed from ("LF Style#", "Color / Wash") to ("BP Style#", "Color / Wash").
MATCH_KEY_COLS: Tuple[str, str] = ("BP Style#", "Color / Wash")

# BeProduct staging column -> DTC WIP_ITS_USE column display name.
# Only columns that actually exist in the view are mapped; anything else would
# make the PATCH fail with "'<col>' is not found in the mapping."
FIELD_MAPPING: Dict[str, str] = {
    # --- match-key / required ---
    "bp_style_number": "BP Style#",    # Phase 6: was lf_style_number->"LF Style#"
    "color": "Color / Wash",
    "brand": "Brand",                  # Phase 6: from brand_hk (single-value)
    # --- optional BeProduct-owned fields (pushed when non-blank) ---
    "lf_style_number": "LF Style#",    # Phase 6: now optional (new separate BP field)
    "customer_style_number": "Legacy Code",  # Phase 6: now BP->DTC optional (was DTC->BP)
    # --- updatable non-key fields (BeProduct-owned, pushed BeProduct -> DTC) ---
    "product_status": "Product Status",
    "description": "Style Description",
    "product_category": "Class",
    "product_sub_category": "Sub Class",
    "division": "Division",            # was "Division?"; the '?' column was renamed
    "garment_finish": "Garment Finish",
    "techpack_stage": "Tech Pack Stage",
    "fabric_group": "Fabric Group",
    "placement": "Placement",
    "gender": "Gender",                # Phase 6: new field (pending DTC column creation)
    # --- default-fill (only written when DTC cell is blank; see DEFAULT_FILL_COLS) ---
    "supplier": "Supplier",            # Phase 6: new DTC column; default = "Supplier"
    # --- image (mapped for reference but EXCLUDED from every push) ---
    "front_image_url": STYLE_IMAGE_COL,
}

# DTC-owned columns: these are written DTC -> BeProduct in Phase 2 and are
# therefore deliberately NOT in FIELD_MAPPING (we never push them BeProduct -> DTC,
# to keep each field one-directional and avoid sync loops). See sync/phase2.py.
#   "Lot#", "Main Vendor (Sampling)", "Main Factory (Sampling)",
#   "Main Factory Customer ID"
# Note: "Legacy Code" IS in FIELD_MAPPING (BP->DTC); "Customer Style#" is NOT
# created as a DTC column.

# Columns that are only filled when the DTC cell is currently blank — existing
# non-blank DTC values are NEVER overwritten (write-once default fill).
# Used in diff_updatable_fields() and respected for UPDATE ops; INSERT always fills.
DEFAULT_FILL_COLS: frozenset = frozenset({"Supplier"})

# Sentinel written to a stale DTC row's "Product Status" when the BeProduct style
# behind it has moved to a different request (key change). It is intentionally NOT
# a valid BeProduct status, so it signals the DTC user that the row is orphaned.
REMOVED_STATUS = "(removed)"

# DTC columns that form the key and must not be treated as "updatable" diffs.
# Brand is constant per request so it is key-like for update purposes.
KEY_DTC_COLS = set(MATCH_KEY_COLS) | {"Brand"}

# Sentinel source values treated as "no value".
_NULLISH = {"", "n/a", "na", "none", "null", "nan"}


# ---------------------------------------------------------------------------
# Normalisation & parsing
# ---------------------------------------------------------------------------

def norm(value: Any) -> Optional[str]:
    """
    Normalise a cell value for matching / equality comparison.

    - None -> None
    - trims, collapses internal runs of whitespace to a single space
    - common null sentinels ("N/A", "none", ...) -> None

    Whitespace is collapsed because the live DTC data contains values like
    ' WMG-R808-263 002' (leading space) and 'Body ' (trailing space). Case and
    dash-vs-space are intentionally preserved (e.g. 'WMG-J876-263-001' vs
    'WMG-J876-263 001' are NOT merged) to avoid collapsing distinct styles.
    """
    if value is None:
        return None
    s = re.sub(r"\s+", " ", str(value)).strip()
    if s.lower() in _NULLISH:
        return None
    return s


def parse_request_reference(reference: str) -> Dict[str, str]:
    """
    Parse a DTC request reference of the form "<customer> <seasonCode> <brand>".

    Example: "KTB FW26 Wrangler Western"
             -> {"customer": "KTB", "season_code": "FW26",
                 "brand": "Wrangler Western"}

    Raises:
        ValueError: if the reference does not have at least 3 tokens or the
                    second token is not a season code (2 letters + 2 digits).
    """
    if reference is None:
        raise ValueError("request reference is None")
    parts = str(reference).strip().split()
    if len(parts) < 3:
        raise ValueError(
            f"Request reference {reference!r} does not match "
            "'<customer> <seasonCode> <brand>'"
        )
    customer, season_code = parts[0], parts[1]
    brand = " ".join(parts[2:])
    if not re.fullmatch(r"[A-Za-z]{2}\d{2}", season_code):
        raise ValueError(
            f"Request reference {reference!r}: token {season_code!r} is not a "
            "valid seasonCode (expected 2 letters + 2 digits, e.g. 'FW26')"
        )
    return {"customer": customer, "season_code": season_code, "brand": brand}


def is_in_scope(reference: str, customer: str) -> bool:
    """
    True if a request reference is in scope for the given customer.

    In scope == reference parses cleanly AND its customer token matches the
    target customer (case-insensitive). E.g. with customer='KTB',
    'KTB FW26 Wrangler' is in scope while 'KON FW26 Wrangler' (developer test
    data) is not.
    """
    try:
        parsed = parse_request_reference(reference)
    except ValueError:
        return False
    return parsed["customer"].strip().upper() == str(customer).strip().upper()


# ---------------------------------------------------------------------------
# Payload building
# ---------------------------------------------------------------------------

def build_target_payload(
    bp_row: Dict[str, Any],
    allowed_cols: Optional[set] = None,
    include_keys: bool = True,
) -> Dict[str, str]:
    """
    Map a BeProduct staging row to a DTC {column: value} payload.

    - applies FIELD_MAPPING
    - ALWAYS excludes "Style Image" (requirement 3a)
    - drops null/blank values (norm() -> None)
    - if allowed_cols is given, drops any column not present in the live view
      (so the PATCH never trips "not found in the mapping")
    - if include_keys is False, drops the match-key/Brand columns (used to build
      the "updatable fields only" set for UPDATE operations)
    """
    payload: Dict[str, str] = {}
    for src_col, dtc_col in FIELD_MAPPING.items():
        if dtc_col == STYLE_IMAGE_COL:
            continue  # never push the image
        if not include_keys and dtc_col in KEY_DTC_COLS:
            continue
        if allowed_cols is not None and dtc_col not in allowed_cols:
            continue
        v = norm(bp_row.get(src_col))
        if v is None:
            continue
        payload[dtc_col] = v
    return payload


def diff_updatable_fields(
    dtc_row: Dict[str, Any],
    bp_row: Dict[str, Any],
    allowed_cols: Optional[set] = None,
) -> Dict[str, str]:
    """
    Return the updatable (non-key, non-image) DTC fields whose normalised value
    in the BeProduct row differs from the current DTC row.

    Only fields present in the BeProduct payload are considered (Phase 1 sets
    indicated fields; it does not blank out fields BeProduct has no value for).

    DEFAULT_FILL_COLS (e.g. "Supplier") are skipped for UPDATE when the DTC row
    already has a non-blank value — they are write-once defaults, never overwritten.
    """
    target = build_target_payload(bp_row, allowed_cols=allowed_cols, include_keys=False)
    changed: Dict[str, str] = {}
    for col, new_val in target.items():
        if col in DEFAULT_FILL_COLS and norm(dtc_row.get(col)) is not None:
            continue  # DTC already has a value; never overwrite a default-fill col
        if norm(dtc_row.get(col)) != norm(new_val):
            changed[col] = new_val
    return changed


# ---------------------------------------------------------------------------
# Upsert computation
# ---------------------------------------------------------------------------

@dataclass
class UpsertOp:
    """A single resolved operation for one BeProduct row."""
    op: str                       # 'UPDATE' | 'INSERT' | 'NOOP'
    match_key: Tuple[Optional[str], Optional[str]]
    fields: Dict[str, str] = field(default_factory=dict)
    row_id: Optional[str] = None       # set for UPDATE
    row_index: Optional[int] = None    # set for INSERT


@dataclass
class UpsertException:
    """A BeProduct row that could not be processed, with a reason."""
    reason: str
    match_key: Tuple[Optional[str], Optional[str]]
    detail: str = ""


@dataclass
class UpsertPlan:
    updates: List[UpsertOp] = field(default_factory=list)
    inserts: List[UpsertOp] = field(default_factory=list)
    noops: List[UpsertOp] = field(default_factory=list)
    exceptions: List[UpsertException] = field(default_factory=list)

    def summary(self) -> Dict[str, int]:
        return {
            "updates": len(self.updates),
            "inserts": len(self.inserts),
            "noops": len(self.noops),
            "exceptions": len(self.exceptions),
        }


def _match_key(row: Dict[str, Any], lf_col: str, color_col: str) -> Tuple[Optional[str], Optional[str]]:
    return (norm(row.get(lf_col)), norm(row.get(color_col)))


def max_row_index(dtc_rows: List[Dict[str, Any]]) -> int:
    """Sparse-aware max rowIndex over the given rows (0 if none)."""
    idxs = [r.get("rowIndex") for r in dtc_rows if r.get("rowIndex") is not None]
    return max(idxs) if idxs else 0


def compute_upsert(
    request_scope: Dict[str, Any],
    dtc_rows: List[Dict[str, Any]],
    bp_rows: List[Dict[str, Any]],
    allowed_cols: Optional[set] = None,
    enforce_scope: bool = True,
) -> UpsertPlan:
    """
    Compute the INSERT/UPDATE/NOOP plan for one request.

    Args:
        request_scope: dict with at least 'season_code' and 'brand' (e.g. from
                       DTCConnector.get_request_scope()).
        dtc_rows:      current DTC rows from the WIP_ITS_USE view; each must have
                       'rowId' and 'rowIndex' plus DTC column display names.
        bp_rows:       BeProduct staging rows (denormalized) targeting THIS
                       request. Keys are BeProduct staging column names.
        allowed_cols:  set of column names defined in the live view; payloads
                       are filtered to these. Pass the result of
                       DTCConnector.get_view_column_names(), which reads the view
                       DEFINITION (all view columns, not just populated cells).
        enforce_scope: if True, bp rows whose season/brand disagree with the
                       request are recorded as exceptions instead of processed.

    Returns:
        UpsertPlan with updates/inserts/noops/exceptions. rowIndex values for
        inserts are assigned sparsely from max(existing rowIndex)+1 upward
        (partition = this request = one season+brand).
    """
    plan = UpsertPlan()

    req_season = norm(request_scope.get("season_code"))
    req_brand = norm(request_scope.get("brand"))

    # Index current DTC rows by (LF Style#, Color / Wash).
    lf_col, color_col = MATCH_KEY_COLS
    dtc_index: Dict[Tuple, Dict[str, Any]] = {}
    for r in dtc_rows:
        key = _match_key(r, lf_col, color_col)
        if key == (None, None):
            continue  # skip fully-empty rows (e.g. pre-created blanks)
        # Last one wins if the sheet itself has dupes; flagged below.
        dtc_index[key] = r

    next_index = max_row_index(dtc_rows)
    seen_bp_keys: set = set()

    for bp in bp_rows:
        key = (norm(bp.get("bp_style_number")), norm(bp.get("color")))

        # required key present?
        if key[0] is None:
            plan.exceptions.append(UpsertException(
                "missing_bp_style", key, "BeProduct row has no bp_style_number"))
            continue

        # scope check: brand & season must agree with the request
        if enforce_scope:
            bp_season = norm(bp.get("season_code"))
            bp_brand = norm(bp.get("brand"))
            if req_season is not None and bp_season is not None and bp_season != req_season:
                plan.exceptions.append(UpsertException(
                    "season_mismatch", key,
                    f"bp season_code={bp_season!r} != request {req_season!r}"))
                continue
            if req_brand is not None and bp_brand is not None and norm_brand(bp_brand) != norm_brand(req_brand):
                plan.exceptions.append(UpsertException(
                    "brand_mismatch", key,
                    f"bp brand={bp_brand!r} != request brand {req_brand!r}"))
                continue

        # duplicate BeProduct rows for the same in-request key
        if key in seen_bp_keys:
            plan.exceptions.append(UpsertException(
                "duplicate_bp_key", key,
                "multiple BeProduct rows share (lf_style_number, color)"))
            continue
        seen_bp_keys.add(key)

        existing = dtc_index.get(key)
        if existing is not None:
            row_id = existing.get("rowId")
            if not row_id:
                plan.exceptions.append(UpsertException(
                    "missing_row_id", key,
                    "matched DTC row has no rowId; cannot UPDATE"))
                continue
            changed = diff_updatable_fields(existing, bp, allowed_cols=allowed_cols)
            if changed:
                plan.updates.append(UpsertOp(
                    op="UPDATE", match_key=key, fields=changed, row_id=row_id,
                    row_index=existing.get("rowIndex")))
            else:
                plan.noops.append(UpsertOp(
                    op="NOOP", match_key=key, row_id=row_id,
                    row_index=existing.get("rowIndex")))
        else:
            payload = build_target_payload(bp, allowed_cols=allowed_cols, include_keys=True)
            if not payload:
                plan.exceptions.append(UpsertException(
                    "empty_payload", key,
                    "no mappable values to insert"))
                continue
            next_index += 1
            plan.inserts.append(UpsertOp(
                op="INSERT", match_key=key, fields=payload, row_index=next_index))

    return plan


def norm_brand(value: Any) -> Optional[str]:
    """
    Brand comparison is case-insensitive (request brand vs sheet/BeProduct
    brand). Returns an upper-cased normalised brand or None.
    """
    v = norm(value)
    return v.upper() if v is not None else None


# ---------------------------------------------------------------------------
# Orphan / moved-key handling (requirement: point 1)
# ---------------------------------------------------------------------------

def compute_orphan_marks(
    dtc_rows: List[Dict[str, Any]],
    bp_keys_this_request: set,
    moved_elsewhere_keys: set,
) -> List[UpsertOp]:
    """
    Find DTC rows in THIS request whose BeProduct style has moved to a DIFFERENT
    request (its key field - BP Style#, brand or season - changed in BeProduct),
    and produce UPDATE ops that set "Product Status" = REMOVED_STATUS ("(removed)").

    This does NOT delete rows: it flags the stale row so the DTC user sees an
    invalid status and knows the row migrated to another request. We only mark a
    row when its (BP Style#, Color / Wash) key:
      * is NOT present in this request's current BeProduct rows, AND
      * IS present in BeProduct under a different request (moved_elsewhere_keys).
    Rows that are simply user-entered / unrelated (key not seen anywhere in
    BeProduct) are left untouched, and rows already marked are NOOP-skipped.

    Args:
        dtc_rows:             current DTC rows for this request (need rowId/rowIndex,
                              "BP Style#", "Color / Wash", "Product Status").
        bp_keys_this_request: set of (bp_style, color) keys present in BeProduct for THIS
                              request (already norm()'d).
        moved_elsewhere_keys: set of (bp_style, color) keys present in BeProduct under a
                              DIFFERENT request (already norm()'d).

    Returns:
        List of UpsertOp(op="UPDATE", fields={"Product Status": "(removed)"}, row_id=...)
    """
    lf_col, color_col = MATCH_KEY_COLS
    ops: List[UpsertOp] = []
    for r in dtc_rows:
        key = _match_key(r, lf_col, color_col)
        if key == (None, None):
            continue
        if key in bp_keys_this_request:
            continue  # still belongs here
        if key not in moved_elsewhere_keys:
            continue  # not a BeProduct-driven move; leave it alone
        if norm(r.get("Product Status")) == norm(REMOVED_STATUS):
            continue  # already flagged
        row_id = r.get("rowId")
        if not row_id:
            continue
        ops.append(UpsertOp(
            op="UPDATE", match_key=key,
            fields={"Product Status": REMOVED_STATUS},
            row_id=row_id, row_index=r.get("rowIndex")))
    return ops


# ---------------------------------------------------------------------------
# Push payload assembly (connector-ready sheetData)
# ---------------------------------------------------------------------------

def update_sheet_data(plan: UpsertPlan) -> List[Dict[str, Any]]:
    """UPDATE rows as connector sheetData: {**changed_fields, 'rowId': <id>}."""
    return [{**op.fields, "rowId": op.row_id} for op in plan.updates]


def insert_sheet_data(plan: UpsertPlan) -> List[Dict[str, Any]]:
    """INSERT rows as connector sheetData: {**all_fields, 'rowIndex': <n>}."""
    return [{**op.fields, "rowIndex": op.row_index} for op in plan.inserts]


def to_sheet_data(plan: UpsertPlan) -> List[Dict[str, Any]]:
    """
    Convenience for inspection/tests: updates followed by inserts.

    NOTE: the DTC API rejects a PATCH that mixes rowId and rowIndex in one call.
    For pushing, send update_sheet_data(plan) and insert_sheet_data(plan) as
    SEPARATE patch_rows() calls. NOOPs and exceptions are excluded.
    """
    return update_sheet_data(plan) + insert_sheet_data(plan)


def chunked(items: List[Any], size: int) -> List[List[Any]]:
    """Split a list into chunks of at most `size` (for batched PATCH calls)."""
    if size <= 0:
        return [items] if items else []
    return [items[i:i + size] for i in range(0, len(items), size)]
