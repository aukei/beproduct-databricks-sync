"""
Phase 2 DTC -> BeProduct pushback core (pure Python, no Spark / no HTTP).

Phase 2 is the reverse of Phase 1: a small set of DTC-OWNED columns are written
back from DTC into the corresponding BeProduct style. Per the agreed direction
partition these columns are NEVER pushed BeProduct -> DTC (see sync/phase1.py):

    DTC column                  BeProduct target                       level
    --------------------------  -------------------------------------  --------
    Legacy Code                 fieldId "customer_style_number"        header
    Main Vendor (Sampling)      fieldId "parent_vendor"                header
    Main Factory (Sampling)     fieldId "factory"                      header
    Lot#                        fieldId "drawing_number_walmart"       colorway
    Main Factory Customer ID    (no BeProduct field yet -> SKIPPED)    -

The actual write uses the BeProduct SDK in one call per style:

    api.style.attributes_update(
        header_id=<beproduct_style_id>,
        fields={<header fieldId>: value, ...},
        colorways=[{"id": <colorway_id>, "fields": {"drawing_number_walmart": <lot>}}],
    )

This module only computes WHAT to write (deterministic, unit-testable). All SDK
I/O lives in the Phase 2 notebook (dtc/notebooks/05_push_dtc_to_beproduct.py).

Input contract (one dict per DTC row, already joined to BeProduct identity):
    {
        "beproduct_style_id": "<style header id>",   # required
        "colorway_id":        "<colorway id>" | None, # required only for Lot#
        "lf_style_number":    "...", "color": "...",  # for logging / exceptions
        "dtc": { "<DTC column>": value, ... },        # incoming DTC values
        "bp":  { "<DTC column>": value, ... },        # current BeProduct values
                                                      #   (optional; enables NOOP diff)
    }

Header fields are STYLE-level: all colorway rows of a style must agree; a
disagreement is reported as a conflict (the first non-null value is kept).
Lot# is COLORWAY-level: it needs the row's colorway_id.

By default DTC blanks do NOT clear BeProduct (push_blanks=False) - a blank DTC
cell usually means "not entered yet", not "erase". Set push_blanks=True to mirror
clears.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .phase1 import norm  # reuse the same normalisation rules

# ---------------------------------------------------------------------------
# Reverse mapping (DTC column -> BeProduct fieldId), validated 2026-06-17
# ---------------------------------------------------------------------------

REVERSE_HEADER_FIELDS: Dict[str, str] = {
    "Legacy Code": "customer_style_number",
    "Main Vendor (Sampling)": "parent_vendor",
    "Main Factory (Sampling)": "factory",
}

# Colorway-level fields: DTC column -> colorway fieldId.
REVERSE_COLORWAY_FIELDS: Dict[str, str] = {
    "Lot#": "drawing_number_walmart",
}

# DTC columns with no BeProduct target yet: never written, reported if non-blank.
UNSUPPORTED_FIELDS = ("Main Factory Customer ID",)

ALL_PHASE2_COLUMNS = (
    tuple(REVERSE_HEADER_FIELDS) + tuple(REVERSE_COLORWAY_FIELDS) + UNSUPPORTED_FIELDS
)


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class StyleUpdate:
    """Accumulated BeProduct update for a single style (header + colorways)."""
    style_id: str
    fields: Dict[str, str] = field(default_factory=dict)          # {fieldId: value}
    colorways: Dict[str, Dict[str, str]] = field(default_factory=dict)  # {cwId: {fieldId: value}}

    def is_empty(self) -> bool:
        return not self.fields and not self.colorways


@dataclass
class Phase2Exception:
    reason: str
    style_id: Optional[str]
    detail: str = ""
    key: Tuple[Optional[str], Optional[str]] = (None, None)


@dataclass
class Phase2Plan:
    updates: Dict[str, StyleUpdate] = field(default_factory=dict)
    noops: int = 0
    skipped_unsupported: int = 0
    exceptions: List[Phase2Exception] = field(default_factory=list)

    def summary(self) -> Dict[str, int]:
        return {
            "styles": len([u for u in self.updates.values() if not u.is_empty()]),
            "header_field_changes": sum(len(u.fields) for u in self.updates.values()),
            "colorway_changes": sum(len(c) for u in self.updates.values()
                                    for c in u.colorways.values()),
            "noops": self.noops,
            "skipped_unsupported": self.skipped_unsupported,
            "exceptions": len(self.exceptions),
        }


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def build_beproduct_updates(
    rows: List[Dict[str, Any]],
    push_blanks: bool = False,
) -> Phase2Plan:
    """
    Compute the per-style BeProduct update plan from joined DTC rows.

    Args:
        rows:        list of joined-row dicts (see module docstring).
        push_blanks: if True, a blank/None DTC value clears the BeProduct field;
                     if False (default) blanks are ignored (no overwrite).

    Returns:
        Phase2Plan with one StyleUpdate per style that has changes, plus
        noop/skip counts and exceptions.
    """
    plan = Phase2Plan()

    for r in rows:
        style_id = r.get("beproduct_style_id")
        key = (norm(r.get("lf_style_number")), norm(r.get("color")))
        if not style_id:
            plan.exceptions.append(Phase2Exception(
                "missing_style_id", None,
                "joined row has no beproduct_style_id", key))
            continue

        dtc = r.get("dtc", {}) or {}
        bp = r.get("bp", {}) or {}
        has_bp = "bp" in r and r["bp"] is not None

        su = plan.updates.get(style_id) or StyleUpdate(style_id=style_id)

        # --- unsupported (no BeProduct target) ---
        for col in UNSUPPORTED_FIELDS:
            if norm(dtc.get(col)) is not None:
                plan.skipped_unsupported += 1
                plan.exceptions.append(Phase2Exception(
                    "unsupported_field", style_id,
                    f"{col!r} has a DTC value but no BeProduct target field; skipped",
                    key))

        # --- header fields (style-level) ---
        for col, fid in REVERSE_HEADER_FIELDS.items():
            new_val = norm(dtc.get(col))
            if new_val is None and not push_blanks:
                continue
            cur_val = norm(bp.get(col)) if has_bp else None
            if has_bp and cur_val == new_val:
                plan.noops += 1
                continue
            payload_val = "" if new_val is None else new_val
            if fid in su.fields and su.fields[fid] != payload_val:
                # different colorway rows of the same style disagree on a
                # style-level field -> keep first, flag conflict.
                plan.exceptions.append(Phase2Exception(
                    "header_value_conflict", style_id,
                    f"{col!r}: '{su.fields[fid]}' vs '{payload_val}' within one style",
                    key))
                continue
            su.fields[fid] = payload_val

        # --- colorway fields (Lot#) ---
        for col, fid in REVERSE_COLORWAY_FIELDS.items():
            new_val = norm(dtc.get(col))
            if new_val is None and not push_blanks:
                continue
            cur_val = norm(bp.get(col)) if has_bp else None
            if has_bp and cur_val == new_val:
                plan.noops += 1
                continue
            cw_id = r.get("colorway_id")
            if not cw_id:
                plan.exceptions.append(Phase2Exception(
                    "missing_colorway_id", style_id,
                    f"{col!r} change needs a colorway_id (none on joined row)", key))
                continue
            payload_val = "" if new_val is None else new_val
            su.colorways.setdefault(cw_id, {})[fid] = payload_val

        if not su.is_empty():
            plan.updates[style_id] = su

    return plan


def to_sdk_calls(plan: Phase2Plan) -> List[Dict[str, Any]]:
    """
    Convert a Phase2Plan into BeProduct SDK call arguments, one per style:

        {"header_id": <style_id>,
         "fields": {fieldId: value, ...},
         "colorways": [{"id": cwId, "fields": {fieldId: value}}, ...]}

    Feed each item to api.style.attributes_update(**item).
    """
    calls = []
    for style_id, su in plan.updates.items():
        if su.is_empty():
            continue
        calls.append({
            "header_id": style_id,
            "fields": dict(su.fields),
            "colorways": [
                {"id": cw_id, "fields": dict(fields)}
                for cw_id, fields in su.colorways.items()
            ],
        })
    return calls
