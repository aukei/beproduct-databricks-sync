"""
Product Status ledger (owner spec 2026-11-11).

Before wip_push changes a row's "Product Status", the row AS IT IS NOW is
appended to the request "<master request> Snaps" in document
"KTB WIP HISTORY", view "Full". Pure logic only; the notebook does the I/O.

Live-verified 2026-11-11 against "KTB FW28 Collaborations Snaps": a full
WIP_ITS_USE row copied into "Full" round-trips unchanged, and "Log Date"
(type date) accepts an ISO timestamp. Since 2026-11-19 "Full" holds all 211
master columns (+ Log Date) as plain columns, so lookups are kept as their last
saved value. "Style Image" is not in the append body: the notebook copies its
pointer afterwards with DTCConnector.copy_row_image (no re-upload).
"""
from typing import Any, Dict, Iterable, List, Mapping

from sync.phase1 import norm

HISTORY_DOCUMENT = "KTB WIP HISTORY"
HISTORY_VIEW = "Full"                 # the view we write through (allowInsertRow=Y)
HISTORY_SHARE_VIEW = "full - readonly"  # what "Kontoor Project Team" is given
STATUS_COL = "Product Status"
LOG_DATE_COL = "Log Date"
_LOCATORS = {"rowId", "rowIndex"}


def snaps_request_name(master_request_name: str) -> str:
    return f"{master_request_name} Snaps"


def status_change_updates(updates: Iterable[Any]) -> List[Any]:
    """Planned UPDATEs that change a NON-BLANK Product Status.

    A blank current status means the row never had one, so there is nothing
    to keep (a style's first push). Inserts never qualify: nothing existed."""
    out = []
    for op in updates:
        if STATUS_COL not in op.fields:
            continue
        cur = norm(op.current.get(STATUS_COL))
        if cur and cur != norm(op.fields[STATUS_COL]):
            out.append(op)
    return out


def snapshot_row(current: Mapping[str, Any], history_cols: Mapping[str, str],
                 log_date_iso: str) -> Dict[str, Any]:
    """The append body for one row: its live values, restricted to the history
    view's columns, minus locators and image ("contact") cells, plus Log Date."""
    row = {k: v for k, v in current.items()
           if k in history_cols and k not in _LOCATORS
           and history_cols[k] != "contact" and v not in (None, "", [])}
    row[LOG_DATE_COL] = log_date_iso
    return row


def pick_history_request(found: Iterable[Mapping[str, Any]], name: str) -> List[Mapping[str, Any]]:
    """Active requests named exactly `name`. Caller refuses on more than one."""
    return [r for r in found
            if (r.get("requestReference") or "") == name
            and str(r.get("requestIsActive") or "").upper() == "Y"]
