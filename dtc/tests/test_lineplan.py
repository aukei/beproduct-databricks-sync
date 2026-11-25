#!/usr/bin/env python3
"""
Unit tests for the LinePlan join-key names (dtc/python/sync/lineplan.py).

Pure-Python, no Spark, no network. Run:
    python3 dtc/tests/test_lineplan.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from sync.lineplan import LINEPLAN_REF_COLS, PROJECTED_VOLUME_COLS, first_value, lineplan_ref

_failures = []


def check(cond, msg):
    print(f"  {'✅' if cond else '❌'} {msg}")
    if not cond:
        _failures.append(msg)


print("=" * 60)
print("LinePlan ref tests")
print("=" * 60)

print("\n[1] names")
check(LINEPLAN_REF_COLS[0] == "LINEPLAN #", "current LinePlan name first (renamed 2026-11-25)")
check("LinePlan ref#" in LINEPLAN_REF_COLS, "WIP's current name kept")
check("Lineplan Ref #" in LINEPLAN_REF_COLS, "pre-rename name kept as fallback")

print("\n[2] lineplan_ref()")
check(lineplan_ref({"LinePlan ref#": "WC-S8001"}) == "WC-S8001", "new name")
check(lineplan_ref({"Lineplan Ref #": "WC-S8001"}) == "WC-S8001", "old name still joins")
check(lineplan_ref({"LinePlan ref#": " WC-S8001 "}) == "WC-S8001", "stripped")
check(lineplan_ref({"LinePlan ref#": "NEW", "Lineplan Ref #": "OLD"}) == "NEW", "current name wins")
check(lineplan_ref({"LinePlan ref#": "  ", "Lineplan Ref #": "OLD"}) == "OLD",
      "blank current name falls through")
check(lineplan_ref({}) is None, "absent -> None")
check(lineplan_ref({"LinePlan ref#": ""}) is None, "blank -> None, never ''")
check(lineplan_ref({"LINEPLAN #": "WC-S8001"}) == "WC-S8001", "LinePlan sheet name (2026-11-25)")

print("\n[3] projected volume")
check(first_value({"PROJECTED VOLUME (SEASON)": 1500}, PROJECTED_VOLUME_COLS) == "1500", "new name")
check(first_value({"PROJECTED VOLUME (season)": "4000"}, PROJECTED_VOLUME_COLS) == "4000", "old name still read")
check(first_value({"PROJECTED VOLUME (SEASON)": 0}, PROJECTED_VOLUME_COLS) == "0", "zero is a value, not blank")
check(first_value({"PROJECTED VOLUME (ANNUAL)": 9}, PROJECTED_VOLUME_COLS) is None, "ANNUAL is not SEASON")

print()
if _failures:
    print(f"❌ {len(_failures)} failure(s)")
    sys.exit(1)
print("✅ all passed")
