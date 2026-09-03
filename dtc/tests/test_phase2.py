#!/usr/bin/env python3
"""
Unit tests for the Phase 2 DTC -> BeProduct pushback core (sync/phase2.py).

Pure-Python, no Spark, no network. Run:
    python3 dtc/tests/test_phase2.py

Phase 6 update (2026-07-02):
  - "Legacy Code" DTC column removed from REVERSE_HEADER_FIELDS (now BP->DTC in Phase 1).
  - "Customer Style#" DTC column decided NOT to create; removed from REVERSE_HEADER_FIELDS.
  - No DTC->BP path for customer_style_number.
  - DTC->BP header fields are now only: Main Vendor (Sampling), Main Factory (Sampling).
  - "Lot#" (colorway) unchanged.

2026-09-03 update:
  - "Main Factory Customer ID" wired up to BeProduct fieldId "customer_factory_code"
    (was UNSUPPORTED). UNSUPPORTED_FIELDS is now empty.
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
# Phase 6: DTC->BP header fields = Main Vendor (Sampling), Main Factory (Sampling).
# "Customer Style#" and "Legacy Code" are NOT in REVERSE_HEADER_FIELDS.
rows = [
    {  # style A, colorway c1: Vendor/Factory changed; Lot# changed
        "beproduct_style_id": "A", "colorway_id": "c1",
        "bp_style_number": "S1", "color": "Black",
        "dtc": {"Main Vendor (Sampling)": "V1",
                "Main Factory (Sampling)": "F1", "Lot#": "100",
                "Main Factory Customer ID": "CUST9"},
        "bp": {"Main Vendor (Sampling)": "V1",  # vendor unchanged -> noop
               "Main Factory (Sampling)": None, "Lot#": None},
    },
    {  # style A, colorway c2: only Lot# (header same values -> no conflict)
        "beproduct_style_id": "A", "colorway_id": "c2",
        "bp_style_number": "S1", "color": "Blue",
        "dtc": {"Main Vendor (Sampling)": "V1", "Lot#": "200"},
        "bp": {"Main Vendor (Sampling)": "V1", "Lot#": "200"},  # both unchanged -> noop
    },
]
plan = build_beproduct_updates(rows)
s = plan.summary()
print("   summary:", s)
A = plan.updates["A"]
check(A.fields.get("factory") == "F1", "Main Factory -> factory")
check("parent_vendor" not in A.fields, "unchanged vendor is a NOOP (not in payload)")
check(A.colorways.get("c1", {}).get("drawing_number_walmart") == "100", "Lot# c1 -> colorway field")
check("c2" not in A.colorways, "unchanged Lot# c2 is a NOOP")
check(A.fields.get("customer_factory_code") == "CUST9",
      "Main Factory Customer ID -> customer_factory_code (wired up 2026-09-03, was unsupported)")
check(s["skipped_unsupported"] == 0, "UNSUPPORTED_FIELDS is now empty -- nothing skipped")
check(not any(e.reason == "unsupported_field" for e in plan.exceptions), "no unsupported-field exceptions logged")

print("\n[2] header value conflict within one style")
rows2 = [
    {"beproduct_style_id": "B", "colorway_id": "c1", "bp_style_number": "S2", "color": "Red",
     "dtc": {"Main Vendor (Sampling)": "V_X"}},
    {"beproduct_style_id": "B", "colorway_id": "c2", "bp_style_number": "S2", "color": "Green",
     "dtc": {"Main Vendor (Sampling)": "V_Y"}},  # disagrees with sibling colorway
]
plan2 = build_beproduct_updates(rows2)
check(plan2.updates["B"].fields.get("parent_vendor") == "V_X", "first non-null header value kept")
check(any(e.reason == "header_value_conflict" for e in plan2.exceptions), "conflict flagged")

print("\n[3] missing identity -> exceptions")
rows3 = [
    {"beproduct_style_id": None, "dtc": {"Main Vendor (Sampling)": "Z"}},   # no style id
    {"beproduct_style_id": "C", "colorway_id": None, "dtc": {"Lot#": "5"}}, # lot needs cw id
]
plan3 = build_beproduct_updates(rows3)
reasons = {e.reason for e in plan3.exceptions}
check("missing_style_id" in reasons, "missing style id flagged")
check("missing_colorway_id" in reasons, "Lot# without colorway_id flagged")
check("C" not in plan3.updates, "no payload built for the colorway-id-less style")

print("\n[4] blanks ignored by default, cleared when push_blanks=True")
rows4 = [{"beproduct_style_id": "D", "colorway_id": "c1", "bp_style_number": "S",
          "color": "k", "dtc": {"Main Vendor (Sampling)": None, "Lot#": ""},
          "bp": {"Main Vendor (Sampling)": "keep", "Lot#": "keeplot"}}]
check(build_beproduct_updates(rows4).summary()["styles"] == 0, "blank DTC -> no overwrite by default")
plan4b = build_beproduct_updates(rows4, push_blanks=True)
check(plan4b.updates["D"].fields.get("parent_vendor") == "", "push_blanks clears header field")
check(plan4b.updates["D"].colorways["c1"]["drawing_number_walmart"] == "", "push_blanks clears Lot#")

print("\n[5] to_sdk_calls() shape")
calls = to_sdk_calls(plan)
call_a = next(c for c in calls if c["header_id"] == "A")
check(set(call_a.keys()) == {"header_id", "fields", "colorways"}, "call has header_id/fields/colorways")
check(isinstance(call_a["colorways"], list) and call_a["colorways"][0]["id"] in ("c1", "c2"),
      "colorways is a list of {id, fields}")
check(all("fields" in cw for cw in call_a["colorways"]), "each colorway entry carries fields")

print("\n[6] Legacy Code and Customer Style# NOT in Phase 2 (Phase 6 decisions)")
check("Legacy Code" not in phase2.REVERSE_HEADER_FIELDS,
      "'Legacy Code' removed from REVERSE_HEADER_FIELDS (Phase 6: now BP->DTC)")
check("Customer Style#" not in phase2.REVERSE_HEADER_FIELDS,
      "'Customer Style#' NOT in REVERSE_HEADER_FIELDS (decided not to create DTC column)")

print("\n[7] Legacy Code and Customer Style# in DTC data must NOT trigger Phase 2 writes")
rows5 = [
    {"beproduct_style_id": "E", "colorway_id": "c1", "bp_style_number": "S5",
     "color": "red",
     # Neither "Legacy Code" nor "Customer Style#" is in REVERSE_HEADER_FIELDS
     "dtc": {"Legacy Code": "some_legacy_value", "Customer Style#": "CS123"}},
]
plan5 = build_beproduct_updates(rows5)
check(plan5.summary()["styles"] == 0,
      "'Legacy Code'/'Customer Style#' in DTC data do NOT trigger Phase 2 writes")

print("\n" + "=" * 70)
if _failures:
    print(f"❌ {len(_failures)} FAILURE(S):")
    for f in _failures:
        print("   -", f)
    sys.exit(1)
print("✅ ALL PHASE 2 CORE UNIT TESTS PASSED")
