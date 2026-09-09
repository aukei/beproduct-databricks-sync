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
from sync.phase2 import build_beproduct_updates, to_sdk_calls, resolve_coo_country_name

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

print("\n[8] resolve_coo_country_name() -- COO code -> name lookup (2026-09-09)")
CODE_TO_NAME = {"US": "United States", "BD": "Bangladesh", "IN": "India"}
check(resolve_coo_country_name("US", CODE_TO_NAME) == "United States", "exact-case code match")
check(resolve_coo_country_name("us", CODE_TO_NAME) == "United States", "lowercase code normalized to uppercase")
check(resolve_coo_country_name(" bd ", CODE_TO_NAME) == "Bangladesh", "whitespace stripped before lookup")
check(resolve_coo_country_name(None, CODE_TO_NAME) is None, "None -> None (never raises)")
check(resolve_coo_country_name("", CODE_TO_NAME) is None, "blank string -> None")
check(resolve_coo_country_name("ZZ", CODE_TO_NAME) is None,
      "unrecognized code -> None (never falls back to the raw code)")

print("\n[9] 'Factory Production Country for Main Factory' wired up (2026-09-09)")
check(phase2.REVERSE_HEADER_FIELDS.get("Factory Production Country for Main Factory")
      == "country_of_origin",
      "DTC column -> BeProduct fieldId 'country_of_origin' (COO)")

print("\n[10] build_beproduct_updates() value_transforms -- COO end-to-end")
COO_COL = "Factory Production Country for Main Factory"
coo_transform = {COO_COL: lambda v: resolve_coo_country_name(v, CODE_TO_NAME)}

rows6 = [{  # style F: raw DTC code "US" must be transformed to "United States" before diff/write
    "beproduct_style_id": "F", "colorway_id": "c1", "bp_style_number": "S6", "color": "navy",
    "dtc": {COO_COL: "US"},
    "bp": {COO_COL: None},
}]
plan6 = build_beproduct_updates(rows6, value_transforms=coo_transform)
check(plan6.updates["F"].fields.get("country_of_origin") == "United States",
      "raw code 'US' transformed to 'United States' before being written")

print("  [10b] NOOP diff compares the TRANSFORMED value against BeProduct's current value")
rows6b = [{
    "beproduct_style_id": "G", "colorway_id": "c1", "bp_style_number": "S7", "color": "navy",
    "dtc": {COO_COL: "us"},                       # raw code, different case
    "bp": {COO_COL: "United States"},             # BeProduct already has the resolved name
}]
plan6b = build_beproduct_updates(rows6b, value_transforms=coo_transform)
check(plan6b.summary()["styles"] == 0,
      "code 'us' transforms to the SAME name BeProduct already has -> correctly a NOOP, not a spurious re-push")

print("  [10c] unmatched code -> transform returns None -> treated as blank (no push, respects push_blanks)")
rows6c = [{
    "beproduct_style_id": "H", "colorway_id": "c1", "bp_style_number": "S8", "color": "navy",
    "dtc": {COO_COL: "ZZ"},   # not in CODE_TO_NAME
    "bp": {COO_COL: "Somewhere Else"},
}]
check(build_beproduct_updates(rows6c, value_transforms=coo_transform).summary()["styles"] == 0,
      "unresolved code is treated as blank -- never overwrites BeProduct with nothing/garbage")

print("  [10d] fields WITHOUT a value_transforms entry are unaffected (existing behavior preserved)")
rows6d = [{
    "beproduct_style_id": "I", "colorway_id": "c1", "bp_style_number": "S9", "color": "navy",
    "dtc": {"Main Vendor (Sampling)": "V1", COO_COL: "US"},
    "bp": {"Main Vendor (Sampling)": None, COO_COL: None},
}]
plan6d = build_beproduct_updates(rows6d, value_transforms=coo_transform)
check(plan6d.updates["I"].fields.get("parent_vendor") == "V1",
      "a field with no transform entry is copied through exactly as before")
check(plan6d.updates["I"].fields.get("country_of_origin") == "United States",
      "the transformed field is still applied correctly alongside untransformed fields")

print("  [10e] value_transforms defaults to None -- zero regression for every existing caller")
rows6e = [{
    "beproduct_style_id": "J", "colorway_id": "c1", "bp_style_number": "S10", "color": "navy",
    "dtc": {COO_COL: "US"},   # no transform passed -> raw code goes through untouched
}]
plan6e = build_beproduct_updates(rows6e)  # no value_transforms kwarg at all
check(plan6e.updates["J"].fields.get("country_of_origin") == "US",
      "without value_transforms, COO is just copied raw like any other field (backward compatible)")

print("\n" + "=" * 70)
if _failures:
    print(f"❌ {len(_failures)} FAILURE(S):")
    for f in _failures:
        print("   -", f)
    sys.exit(1)
print("✅ ALL PHASE 2 CORE UNIT TESTS PASSED")
