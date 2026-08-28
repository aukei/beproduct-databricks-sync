"""
Phase 7 — BeProduct sample-app submit history → DTC status columns
==================================================================

BeProduct stores, per style, up to 6 SAMPLE applications (Proto / PreLine / SMS /
Fit / PP / TOP), each of type ``SampleRequestMulti``. ``p1p7_beproduct_style_sync``
already extracts each app's **submit × size** records into a raw JSON-array column
on ``ktb_styles`` (``{prefix}_sample_json``), e.g. ``preline_sample_json``.

Phase 7 turns that raw history into a compact per-app status string that is pushed
BeProduct → DTC (Phase 1). For each app we emit the **complete list of submits**,
one line per submit taken from that submit's **first size**:

    "submit_name","submitStatus","submitStatusDate"

with multiple submits on their own line, separated by a newline (changed
2026-08-28 - see below), e.g.::

    one submit:
        "1ST Submit","Approved with Corrections","2026-05-11T11:39:48.528Z"

    two submits:
        "1ST Submit","Requested","2026-05-14T00:00:00Z"
        "2ND Submit","Approved","2026-06-20T00:00:00Z"

This is a plain quoted/comma-separated line format, NOT a JSON array - there
are no enclosing ``[`` ``]`` brackets at all (changed 2026-08-28, superseding
the earlier flat-JSON-array format, itself a same-day fix of the original
nested array-of-arrays that always showed a doubled ``[[``/``]]`` for the
common single-submit case). Each value is always double-quoted (empty quotes
``""`` for a missing status/date); an embedded double-quote in a value is
escaped by doubling it (CSV-style: ``"`` → ``""``), never by backslash-escaping.

Empty history → ``""`` (so the value is dropped by phase1.norm and never pushed).

All 6 apps are now mapped to DTC (confirmed 2026-08-28, after a DTC WIP doc
restructure changed the Fit/PP destination columns from what was previously
confirmed 2026-07-07):

    BeProduct app   staging column            DTC column
    ─────────────   ───────────────────────   ────────────────────────────────────
    Proto Sample    proto_sample_status     →  "Proto Sample - Sample Status"
    PreLine Sample  preline_sample_status   →  "Pre-line Sample - Status"
    SMS Sample      sms_sample_status       →  "SMS - Sample Status"
    Fit Sample      fit_sample_status       →  "2nd Fit Sample Approval Status"    (was "1st Fit ...")
    PP Sample       pp_sample_status        →  "PP Sample Submission Approval Status"  (was "2nd Fit ...")
    TOP Sample      top_sample_status       →  "TOP Sample Approval Status"

    All 6 DTC columns confirmed present in the 204-field WIP_ITS_USE view (2026-08-28).
    "PP Sample Submission Approval Status" confirmed correct by the project team
    2026-08-28 (the originally requested "PP Sample Approval Status" does not exist
    as a field in the live view).

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
# DTC column presence (204-field WIP_ITS_USE view, confirmed 2026-08-28 after a
# DTC WIP doc restructure - Fit/PP destinations changed from the 2026-07-07
# mapping; both old AND new Fit columns ("1st Fit ..." and "2nd Fit ...") still
# exist side by side, only which ONE we push to changed). "PP Sample Submission
# Approval Status" confirmed correct by the project team 2026-08-28.
#   ✓ Proto Sample - Sample Status              (confirmed, unchanged)
#   ✓ Pre-line Sample - Status                  (confirmed, unchanged; note lowercase 'l' and dash)
#   ✓ SMS - Sample Status                       (confirmed, unchanged)
#   ✓ 2nd Fit Sample Approval Status            (confirmed; Fit now maps here, was "1st Fit ...")
#   ✓ PP Sample Submission Approval Status      (confirmed; PP now maps here, was "2nd Fit ...")
#   ✓ TOP Sample Approval Status                (confirmed, unchanged)
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
        "dtc": "2nd Fit Sample Approval Status",   # changed 2026-08-28, was "1st Fit Sample Approval Status"
    },
    "pp_sample_json": {
        "staging": "pp_sample_status",
        "dtc": "PP Sample Submission Approval Status",  # confirmed 2026-08-28, was "2nd Fit Sample Approval Status"
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


def _quote(value: Optional[Any]) -> str:
    """Double-quote a value for one line's comma-separated fields.

    ``None`` becomes empty quotes (``""``), never the literal text ``None``.
    An embedded double-quote is escaped by doubling it (CSV-style), so the
    output is always safe to split back on quoted-comma boundaries even
    though this is a plain display string, not JSON.
    """
    s = "" if value is None else str(value)
    return f'"{s.replace(chr(34), chr(34) * 2)}"'


def format_sample_field(raw: Any) -> str:
    """
    Turn a raw sample-app JSON array (flattened submit × size records) into the
    Phase 7 DTC field string: the complete list of submits, one LINE each from
    the submit's FIRST size, formatted as::

        "submit_name","submitStatus","submitStatusDate"

    Multiple submits are separated by a newline, one submit per line - never
    JSON array brackets (changed 2026-08-28; see module docstring).

    Input records (from p1p7_beproduct_style_sync.extract_sample_submits) carry:
      submit_id, submit_name, size, submit_status, submit_status_date, ...
    They are ordered submit-by-submit, size-by-size, so the first record seen for
    a given submit_id corresponds to that submit's first size.

    Returns the formatted multi-line string, or ``""`` when there is no submit
    history.

    Examples:
        one submit:
            '"1ST Submit","Requested","2026-05-14T16:18:10.194Z"'
        two submits:
            '"1ST Submit","Requested","2026-05-14T00:00:00Z"\\n'
            '"2ND Submit","Approved","2026-06-20T00:00:00Z"'
    """
    records = _load_records(raw)
    if not records:
        return ""

    first_by_submit: Dict[Any, Dict[str, Any]] = {}
    order: List[Any] = []
    for r in records:
        # Key on submit_id; fall back to submit_name so records without an id
        # still group sensibly (one line per distinct submit).
        sid = r.get("submit_id")
        if sid is None:
            sid = ("name", r.get("submit_name"))
        if sid not in first_by_submit:
            first_by_submit[sid] = r
            order.append(sid)

    lines: List[str] = []
    for sid in order:
        r = first_by_submit[sid]
        triple = [r.get("submit_name"), r.get("submit_status"), r.get("submit_status_date")]
        lines.append(",".join(_quote(v) for v in triple))

    if not lines:
        return ""
    return "\n".join(lines)
