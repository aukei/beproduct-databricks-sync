"""
Phase 7 — BeProduct sample-app submit history → DTC status columns
==================================================================

BeProduct stores, per style, up to 6 SAMPLE applications (Proto / PreLine / SMS /
Fit / PP / TOP), each of type ``SampleRequestMulti``. ``p1p7_beproduct_style_sync``
already extracts each app's **submit × size** records into a raw JSON-array column
on ``ktb_styles`` (``{prefix}_sample_json``), e.g. ``preline_sample_json``.

Phase 7 turns that raw history into a compact per-app status string that is pushed
BeProduct → DTC (Phase 1). For each app we emit the **complete list of submits**,
one triple per submit taken from that submit's **first size**:

    [submit_name, sizes[0].submitStatus, sizes[0].submitStatusDate]

serialized as a compact JSON array of arrays, e.g.::

    [["1ST Submit","Approved with Corrections","2026-05-11T11:39:48.528Z"]]

Empty history → ``""`` (so the value is dropped by phase1.norm and never pushed).

All 6 apps are now mapped to DTC (confirmed 2026-07-07):

    BeProduct app   staging column            DTC column
    ─────────────   ───────────────────────   ────────────────────────────────
    Proto Sample    proto_sample_status     →  "Proto Sample - Sample Status"
    PreLine Sample  preline_sample_status   →  "Pre-line Sample - Status"
    SMS Sample      sms_sample_status       →  "SMS - Sample Status"
    Fit Sample      fit_sample_status       →  "1st Fit Sample Approval Status"
    PP Sample       pp_sample_status        →  "2nd Fit Sample Approval Status"
    TOP Sample      top_sample_status       →  "TOP Sample Approval Status"

    All 6 DTC columns confirmed present in the 198-field WIP_ITS_USE view (2026-07-07).

The DTC column mapping (staging → DTC) lives in phase1.FIELD_MAPPING; this module
owns the raw-column names and the deterministic formatter so it can be unit-tested
without Spark. The notebook wraps ``format_sample_field`` in a Spark UDF.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

__all__ = [
    "SAMPLE_SUBMIT_FIELDS",
    "format_sample_field",
]

# Phase 7 mappings — all 6 sample apps.
# Keys are the ktb_styles RAW column names (``{prefix}_json`` from
# p1p7_beproduct_style_sync.SAMPLE_APPS); each entry gives the derived staging column
# and the DTC column it is pushed to (via phase1.FIELD_MAPPING).
#
# DTC column presence (198-field WIP_ITS_USE view, confirmed 2026-07-07):
#   ✓ Proto Sample - Sample Status        (confirmed)
#   ✓ Pre-line Sample - Status            (confirmed; note lowercase 'l' and dash)
#   ✓ SMS - Sample Status                 (confirmed)
#   ✓ 1st Fit Sample Approval Status      (confirmed)
#   ✓ 2nd Fit Sample Approval Status      (confirmed)
#   ✓ TOP Sample Approval Status          (confirmed)
#
#   raw ktb_styles column   →   (staging column,          DTC column)
SAMPLE_SUBMIT_FIELDS: Dict[str, Dict[str, str]] = {
    "proto_sample_json": {
        "staging": "proto_sample_status",
        "dtc": "Proto Sample - Sample Status",
    },
    "preline_sample_json": {
        "staging": "preline_sample_status",
        "dtc": "Pre-line Sample - Status",         # note: lowercase 'l', dash separator
    },
    "sms_sample_json": {
        "staging": "sms_sample_status",
        "dtc": "SMS - Sample Status",
    },
    "fit_sample_json": {
        "staging": "fit_sample_status",
        "dtc": "1st Fit Sample Approval Status",
    },
    "pp_sample_json": {
        "staging": "pp_sample_status",
        "dtc": "2nd Fit Sample Approval Status",
    },
    "top_sample_json": {
        "staging": "top_sample_status",
        "dtc": "TOP Sample Approval Status",
    },
}


def _load_records(raw: Any) -> List[Dict[str, Any]]:
    """Parse the raw ``{prefix}_sample_json`` value into a list of record dicts.

    Accepts a JSON string (as stored in ktb_styles) or an already-parsed list.
    Anything else / malformed → empty list.
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        return [r for r in raw if isinstance(r, dict)]
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        try:
            parsed = json.loads(s)
        except (ValueError, TypeError):
            return []
        return [r for r in parsed if isinstance(r, dict)] if isinstance(parsed, list) else []
    return []


def format_sample_field(raw: Any) -> str:
    """
    Turn a raw sample-app JSON array (flattened submit × size records) into the
    Phase 7 DTC field string: the complete list of submits, one triple each from
    the submit's FIRST size.

    Input records (from p1p7_beproduct_style_sync.extract_sample_submits) carry:
      submit_id, submit_name, size, submit_status, submit_status_date, ...
    They are ordered submit-by-submit, size-by-size, so the first record seen for
    a given submit_id corresponds to that submit's first size.

    Returns a compact JSON array of ``[name, submitStatus, submitStatusDate]``
    triples, or ``""`` when there is no submit history.

    Example:
        '[["1ST Submit","Requested","2026-05-14T16:18:10.194Z"]]'
    """
    records = _load_records(raw)
    if not records:
        return ""

    first_by_submit: Dict[Any, Dict[str, Any]] = {}
    order: List[Any] = []
    for r in records:
        # Key on submit_id; fall back to submit_name so records without an id
        # still group sensibly (one triple per distinct submit).
        sid = r.get("submit_id")
        if sid is None:
            sid = ("name", r.get("submit_name"))
        if sid not in first_by_submit:
            first_by_submit[sid] = r
            order.append(sid)

    triples: List[List[Optional[str]]] = []
    for sid in order:
        r = first_by_submit[sid]
        triples.append([
            r.get("submit_name"),
            r.get("submit_status"),
            r.get("submit_status_date"),
        ])

    if not triples:
        return ""
    # Compact separators: no incidental whitespace, so phase1.norm() leaves the
    # value untouched and round-trip diffing against DTC is stable.
    return json.dumps(triples, separators=(",", ":"), default=str)
