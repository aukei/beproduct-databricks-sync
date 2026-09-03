"""
Phase 2 DTC -> BeProduct pushback core (pure Python, no Spark / no HTTP).

Phase 2 is the reverse of Phase 1: a small set of DTC-OWNED columns are written
back from DTC into the corresponding BeProduct style. Per the agreed direction
partition these columns are NEVER pushed BeProduct -> DTC (see sync/phase1.py):

    DTC column                  BeProduct target                       level
    --------------------------  -------------------------------------  --------
    Customer Style#             fieldId "customer_style_number"        header
    Main Vendor (Sampling)      fieldId "parent_vendor"                header
    Main Factory (Sampling)     fieldId "factory"                      header
    Main Factory Customer ID    fieldId "customer_factory_code"        header
    Lot#                        fieldId "drawing_number_walmart"       colorway

Phase 6 update (2026-06-26):
    "Legacy Code" DTC column was REMOVED from REVERSE_HEADER_FIELDS. It is now
    a BeProduct->DTC field (populated from BP's customer_style_number in Phase 1).
    The new DTC column "Customer Style#" takes the DTC->BP role for
    customer_style_number.

"Main Factory Customer ID" wired up 2026-09-03 (owner spec) — previously
UNSUPPORTED (no BeProduct target had been identified). Live-confirmed the
same day: BeProduct's `customer_factory_code` ("Customer Factory Code",
`fieldType: "Text"`, tooltip "Customer Factory SAP Code") is a real,
WRITABLE header field via `api.style.attributes_update` — its
`LockField: true` UI property does NOT block API writes (live-tested:
write succeeded and persisted, then reverted), the same pattern already
seen with DTC's unreliable `isReadOnly` flag. Present in both the "KTB" and
"TEST KTB" folders.

NOTE: on the DTC SIDE, "Main Factory Customer ID" is itself `type: "lookup"`
(computed from the "XTS Factory Master" request via the row's selected
"Main Factory (Sampling)", NOT a plain user-editable column — the same
data Phase 0 already syncs into `beproduct_directory`). This module only
READS whatever value DTC has already computed there and forwards it —
mechanically identical to `parent_vendor`/`factory` above — so this is
unaffected either way; it only means a genuine non-null value can't be
manufactured by hand (a direct PATCH to a DTC lookup field is silently
ignored) and only appears once a row's factory has a populated "Customer
Factory ID" in real XTS Factory Master data. See AGENTS.md decisions log.

The actual write uses the BeProduct SDK in one call per style:

    api.style.attributes_update(
        header_id=<beproduct_style_id>,
        fields={<header fieldId>: value, ...},
        colorways=[{"id": <colorway_id>, "fields": {"drawing_number_walmart": <lot>}}],
    )

This module only computes WHAT to write (deterministic, unit-testable). All SDK
I/O lives in the Phase 2 notebook (dtc/notebooks/p2_push_dtc_to_beproduct.py).

Input contract (one dict per DTC row, already joined to BeProduct identity):
    {
        "beproduct_style_id": "<style header id>",   # required
        "colorway_id":        "<colorway id>" | None, # required only for Lot#
        "bp_style_number":    "...", "color": "...",  # for logging / exceptions
                                                      # Phase 6: was lf_style_number
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
    # Phase 6: "Legacy Code" removed (now BP->DTC in phase1.FIELD_MAPPING).
    # "Customer Style#" DTC column decided NOT to be created; no DTC->BP for
    # customer_style_number. Flow is single-direction: BP customer_style_number
    # -> DTC "Legacy Code" only.
    "Main Vendor (Sampling)": "parent_vendor",
    "Main Factory (Sampling)": "factory",
    # Wired up 2026-09-03 (owner spec) -- previously UNSUPPORTED. Live-confirmed
    # writable via attributes_update; see module docstring.
    "Main Factory Customer ID": "customer_factory_code",
}

# Colorway-level fields: DTC column -> colorway fieldId.
REVERSE_COLORWAY_FIELDS: Dict[str, str] = {
    "Lot#": "drawing_number_walmart",
}

# DTC columns with no BeProduct target yet: never written, reported if non-blank.
# Empty as of 2026-09-03 (Main Factory Customer ID wired up above) -- kept as a
# tuple (not removed) so future unsupported columns have an obvious place to land.
UNSUPPORTED_FIELDS: Tuple[str, ...] = ()

# NOTE: "Legacy Code" and "Customer Style#" are intentionally absent from all
# Phase 2 dicts. "Legacy Code" is BeProduct->DTC only (see phase1.FIELD_MAPPING),
# populated from BP customer_style_number. "Customer Style#" DTC column is not
# being created; there is no DTC->BP path for customer_style_number.

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
        key = (norm(r.get("bp_style_number")), norm(r.get("color")))  # Phase 6: was lf_style_number
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
