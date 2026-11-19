#!/usr/bin/env python3
"""
Unit tests for sync/wip_history.py -- the Product Status ledger.

    python3 dtc/tests/test_wip_history.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from sync import wip_history as wh  # noqa: E402
from sync.wip_plan import PlannedRow  # noqa: E402

_failures = []


def check(cond, msg):
    print(f"  {'✅' if cond else '❌'} {msg}")
    if not cond:
        _failures.append(msg)


def upd(rid, fields, current):
    return PlannedRow(handle=rid, kind="update", match_key=("S", "C"), row_id=rid,
                      fields=fields, current=current)


print("[1] which updates are status changes")
ops = [
    upd("a", {"Product Status": "SMS"}, {"Product Status": "Proto"}),        # change
    upd("b", {"Product Status": "SMS"}, {"Product Status": None}),           # first status
    upd("c", {"Sub Class": "Jacket"}, {"Product Status": "Proto"}),          # no status field
    upd("d", {"Product Status": "Proto "}, {"Product Status": "Proto"}),     # same after norm
]
got = [o.row_id for o in wh.status_change_updates(ops)]
check(got == ["a"], f"only the real change qualifies: {got}")

print("[2] snapshot row")
hist = {"Product Status": "string", "Style Image": "contact", "Log Date": "date",
        "BP Style#": "string", "Vendor 1": "dropdown"}
cur = {"rowId": "a", "rowIndex": 3, "Product Status": "Proto", "Style Image": "http://x",
       "BP Style#": "KTB-1", "Vendor 1": None, "Main Vendor Customer ID": "V1"}
row = wh.snapshot_row(cur, hist, "2026-11-11T03:04:05.000Z")
check(row == {"Product Status": "Proto", "BP Style#": "KTB-1",
              "Log Date": "2026-11-11T03:04:05.000Z"},
      f"locators, image, blanks and non-history columns dropped: {row}")

print("[3] naming + request pick")
check(wh.snaps_request_name("KTB FW28 Collaborations") == "KTB FW28 Collaborations Snaps",
      "Snaps name")
found = [{"requestReference": "KTB FW28 Collaborations Snaps", "requestIsActive": "Y"},
         {"requestReference": "KTB FW28 Collaborations Snaps", "requestIsActive": "N"},
         {"requestReference": "KTB FW28 Collaborations Snaps 2", "requestIsActive": "Y"}]
check(len(wh.pick_history_request(found, "KTB FW28 Collaborations Snaps")) == 1,
      "exact name, active only")

if _failures:
    print(f"\n❌ {len(_failures)} failure(s)")
    sys.exit(1)
print("\n✅ all passed")
