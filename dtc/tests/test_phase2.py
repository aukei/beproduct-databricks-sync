#!/usr/bin/env python3
"""
Unit tests for the Phase 2 DTC -> BeProduct pushback core (sync/phase2.py).

Pure-Python, no Spark, no network. Run:
    python3 dtc/tests/test_phase2.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from sync import phase2
from sync.phase2 import build_beproduct_updates, to_sdk_calls

_failures = []


def check(cond, msg):
    print(f"  {'✅' if cond else '❌'} {msg}")
    if not cond:
        _failures.append(msg)


print("\n[1] header + colorway changes, NOOP diff, blank handling")
rows = [
    {  # style A, colorway c1: Legacy/Vendor/Factory changed; Lot# changed
        "beproduct_style_id": "A", "colorway_id": "c1",
        "lf_style_number": "S1", "color": "Black",
        "dtc": {"Legacy Code": "LEG1", "Main Vendor (Sampling)": "V1",
                "Main Factory (Sampling)": "F1", "Lot#": "100",
                "Main Factory Customer ID": "CUST9"},
        "bp": {"Legacy Code": "OLD", "Main Vendor (Sampling)": "V1",  # vendor unchanged -> noop
               "Main Factory (Sampling)": None, "Lot#": None},
    },
    {  # style A, colorway c2: only Lot# (header repeats same values -> no conflict)
        "beproduct_style_id": "A", "colorway_id": "c2",
        "lf_style_number": "S1", "color": "Blue",
        "dtc": {"Legacy Code": "LEG1", "Lot#": "200"},
        "bp": {"Legacy Code": "OLD", "Lot#": "200"},  # lot unchanged -> noop; legacy changes
    },
]
plan = build_beproduct_updates(rows)
s = plan.summary()
print("   summary:", s)
A = plan.updates["A"]
check(A.fields.get("customer_style_number") == "LEG1", "Legacy Code -> customer_style_number")
check(A.fields.get("factory") == "F1", "Main Factory -> factory")
check("parent_vendor" not in A.fields, "unchanged vendor is a NOOP (not in payload)")
check(A.colorways.get("c1", {}).get("drawing_number_walmart") == "100", "Lot# c1 -> colorway field")
check("c2" not in A.colorways, "unchanged Lot# c2 is a NOOP")
check(s["skipped_unsupported"] == 1, "Main Factory Customer ID skipped (no BeProduct target)")
check(any(e.reason == "unsupported_field" for e in plan.exceptions), "unsupported logged")

print("\n[2] header value conflict within one style")
rows2 = [
    {"beproduct_style_id": "B", "colorway_id": "c1", "lf_style_number": "S2", "color": "Red",
     "dtc": {"Legacy Code": "X"}},
    {"beproduct_style_id": "B", "colorway_id": "c2", "lf_style_number": "S2", "color": "Green",
     "dtc": {"Legacy Code": "Y"}},  # disagrees with sibling colorway
]
plan2 = build_beproduct_updates(rows2)
check(plan2.updates["B"].fields.get("customer_style_number") == "X", "first non-null header value kept")
check(any(e.reason == "header_value_conflict" for e in plan2.exceptions), "conflict flagged")

print("\n[3] missing identity -> exceptions")
rows3 = [
    {"beproduct_style_id": None, "dtc": {"Legacy Code": "Z"}},                 # no style id
    {"beproduct_style_id": "C", "colorway_id": None, "dtc": {"Lot#": "5"}},    # lot needs cw id
]
plan3 = build_beproduct_updates(rows3)
reasons = {e.reason for e in plan3.exceptions}
check("missing_style_id" in reasons, "missing style id flagged")
check("missing_colorway_id" in reasons, "Lot# without colorway_id flagged")
check("C" not in plan3.updates, "no payload built for the colorway-id-less style")

print("\n[4] blanks ignored by default, cleared when push_blanks=True")
rows4 = [{"beproduct_style_id": "D", "colorway_id": "c1", "lf_style_number": "S",
          "color": "k", "dtc": {"Legacy Code": None, "Lot#": ""},
          "bp": {"Legacy Code": "keep", "Lot#": "keeplot"}}]
check(build_beproduct_updates(rows4).summary()["styles"] == 0, "blank DTC -> no overwrite by default")
plan4b = build_beproduct_updates(rows4, push_blanks=True)
check(plan4b.updates["D"].fields.get("customer_style_number") == "", "push_blanks clears header field")
check(plan4b.updates["D"].colorways["c1"]["drawing_number_walmart"] == "", "push_blanks clears Lot#")

print("\n[5] to_sdk_calls() shape")
calls = to_sdk_calls(plan)
call_a = next(c for c in calls if c["header_id"] == "A")
check(set(call_a.keys()) == {"header_id", "fields", "colorways"}, "call has header_id/fields/colorways")
check(isinstance(call_a["colorways"], list) and call_a["colorways"][0]["id"] in ("c1", "c2"),
      "colorways is a list of {id, fields}")
check(all("fields" in cw for cw in call_a["colorways"]), "each colorway entry carries fields")

print("\n" + "=" * 70)
if _failures:
    print(f"❌ {len(_failures)} FAILURE(S):")
    for f in _failures:
        print("   -", f)
    sys.exit(1)
print("✅ ALL PHASE 2 CORE UNIT TESTS PASSED")
