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
    Fit Sample      fit_1st_sample_status   →  "1st Fit Sample Approval Status"   (submit 1 only; 2026-09-30)
                    fit_2nd_sample_status   →  "2nd Fit Sample Approval Status"   (submit 2 only; 2026-09-30)
    PP Sample       pp_sample_status        →  "PP Sample Submission Approval Status"  (was "2nd Fit ...")
    TOP Sample      top_sample_status       →  "TOP Sample Approval Status"

    All 6 DTC columns confirmed present in the 204-field WIP_ITS_USE view (2026-08-28).
    "PP Sample Submission Approval Status" confirmed correct by the project team
    2026-08-28 (the originally requested "PP Sample Approval Status" does not exist
    as a field in the live view).

**Fit is different (owner spec 2026-09-30, see FIT_SUBMIT_FIELDS):** it has at
most 2 submits, and each goes to its OWN column holding only that submit's
status and timestamp -- ``"submitStatus","submitStatusDate"``, no submit name.
Submit 1 -> "1st Fit Sample Approval Status", submit 2 -> "2nd Fit Sample
Approval Status", any further submit is ignored. The status text can change, so
both columns follow BeProduct on every run -- including being CLEARED when the
submit no longer exists (phase1.CLEAR_WHEN_BLANK_COLS).

The DTC column mapping (staging → DTC) lives in phase1.FIELD_MAPPING; this module
owns the raw-column names and the deterministic formatter so it can be unit-tested
without Spark. The notebook wraps ``format_sample_field`` in a Spark UDF.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

__all__ = [
    "SAMPLE_SUBMIT_FIELDS",
    "FIT_SUBMIT_FIELDS",
    "FIT_MAX_SUBMITS",
    "format_sample_field",
    "format_submit_status",
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
    # Fit is NOT here since 2026-09-30 -- it splits per submit, see FIT_SUBMIT_FIELDS.
    "pp_sample_json": {
        "staging": "pp_sample_status",
        "dtc": "PP Sample Submission Approval Status",  # confirmed 2026-08-28, was "2nd Fit Sample Approval Status"
    },
    "top_sample_json": {
        "staging": "top_sample_status",
        "dtc": "TOP Sample Approval Status",
    },
}


# Fit sample (owner spec 2026-09-30): at most 2 submits, one DTC column each,
# holding only that submit's status + timestamp. Submits beyond FIT_MAX_SUBMITS
# are ignored. Both DTC columns were confirmed present in the view 2026-08-28.
FIT_MAX_SUBMITS = 2
#   staging column    ->   (raw ktb_styles column, submit number (1-based), DTC column)
FIT_SUBMIT_FIELDS: Dict[str, Dict[str, Any]] = {
    "fit_1st_sample_status": {
        "raw": "fit_sample_json", "submit": 1, "dtc": "1st Fit Sample Approval Status",
    },
    "fit_2nd_sample_status": {
        "raw": "fit_sample_json", "submit": 2, "dtc": "2nd Fit Sample Approval Status",
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
    lines: List[str] = []
    for r in _first_record_per_submit(raw):
        triple = [r.get("submit_name"), r.get("submit_status"), r.get("submit_status_date")]
        lines.append(",".join(_quote(v) for v in triple))

    if not lines:
        return ""
    return "\n".join(lines)


def _first_record_per_submit(raw: Any) -> List[Dict[str, Any]]:
    """One record per distinct submit (its FIRST size), in BeProduct's order.

    Records are ordered submit-by-submit, size-by-size (see
    p1p7_beproduct_style_sync.extract_sample_submits), so the first record per
    submit_id is that submit's first size.
    """
    first_by_submit: Dict[Any, Dict[str, Any]] = {}
    order: List[Any] = []
    for r in _load_records(raw):
        # Key on submit_id; fall back to submit_name so records without an id
        # still group sensibly (one line per distinct submit).
        sid = r.get("submit_id")
        if sid is None:
            sid = ("name", r.get("submit_name"))
        if sid not in first_by_submit:
            first_by_submit[sid] = r
            order.append(sid)
    return [first_by_submit[sid] for sid in order]


def format_submit_status(raw: Any, submit_no: int) -> str:
    """
    ONE submit's status and timestamp, for the per-submit Fit columns
    (owner spec 2026-09-30)::

        "submitStatus","submitStatusDate"

    `submit_no` is 1-based, in BeProduct's submit order (first size of each
    submit, as format_sample_field uses). A submit that does not exist -> ``""``,
    which phase1 turns into a CLEAR of the DTC cell for these columns, so a
    removed submit does not leave stale text behind.

    Examples:
        submit 1 of two:  '"Requested","2026-09-23T11:16:14.37Z"'
        submit 3 of two:  ''
    """
    if submit_no < 1:
        return ""
    submits = _first_record_per_submit(raw)
    if submit_no > len(submits):
        return ""
    r = submits[submit_no - 1]
    return ",".join(_quote(v) for v in (r.get("submit_status"), r.get("submit_status_date")))
