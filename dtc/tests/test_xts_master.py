#!/usr/bin/env python3
"""
Unit tests for the pure (Spark-free) XTS Master helpers (sync/xts_master.py).

No Spark, no network. Run:
    python3 dtc/tests/test_xts_master.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from sync import xts_master as xm

_failures = []


def check(cond, msg):
    print(f"  {'✅' if cond else '❌'} {msg}")
    if not cond:
        _failures.append(msg)


print("\n[1] extract_directory_row - SUPPLIER: name + code mapped, no country field")
row = xm.extract_directory_row(
    "SUPPLIER",
    {"rowId": "r1", "rowIndex": 1, "Supplier Name": "Acme Supplier", "Supplier Code": "SUP001",
     "Type": "Supplier"},
    request_id="req1", request_reference="XTS Supplier Master",
)
check(row is not None, "row extracted")
check(row["name"] == "Acme Supplier", "name mapped from 'Supplier Name'")
check(row["directory_id"] == "SUP001", "directory_id mapped from 'Supplier Code'")
check(row["partner_type"] == "SUPPLIER", "partner_type stamped from arg, not 'Type' cell")
check(row["country"] is None, "country always None for SUPPLIER (no such DTC field)")
for c in xm.DIRECTORY_OPTIONAL_COLS:
    check(row[c] is None, f"optional col {c} is None")

print("\n[2] extract_directory_row - FACTORY: name + code + country all mapped")
row = xm.extract_directory_row(
    "FACTORY",
    {"rowId": "r3", "rowIndex": 1, "Factory Name": "Acme Factory", "Factory Code": "FAC001",
     "Customer Factory ID": "CUST999", "Production Country": "VN"},
    request_id="req3", request_reference="XTS Factory Master",
)
check(row["name"] == "Acme Factory", "name mapped from 'Factory Name'")
check(row["directory_id"] == "FAC001", "directory_id mapped from 'Factory Code' (not Customer Factory ID)")
check(row["country"] == "VN", "country mapped from 'Production Country'")

print("\n[3] extract_directory_row - blank name -> None (skipped)")
check(xm.extract_directory_row(
    "SUPPLIER", {"Supplier Name": "  ", "Supplier Code": "X"},
    request_id="r", request_reference="ref") is None, "blank/whitespace name skipped")
check(xm.extract_directory_row(
    "SUPPLIER", {"Supplier Code": "X"},
    request_id="r", request_reference="ref") is None, "missing name key skipped")

print("\n[4] extract_directory_row - unknown partner_type raises")
try:
    xm.extract_directory_row("BOGUS", {}, request_id="r", request_reference="ref")
    check(False, "should have raised ValueError")
except ValueError:
    check(True, "raises ValueError for unknown partner_type")

print("\n[5] extract_directory_row - MILL is out of scope (not a recognized partner_type)")
try:
    xm.extract_directory_row("MILL", {"Mill": "Acme Mill"}, request_id="r", request_reference="ref")
    check(False, "MILL should raise - out of scope per 2026-08-28 decision")
except ValueError:
    check(True, "MILL correctly raises ValueError - only SUPPLIER/FACTORY are in scope")

print("\n[6] norm() trims and blanks-to-None")
check(xm.norm("  Acme  ") == "Acme", "trims whitespace")
check(xm.norm("") is None, "empty string -> None")
check(xm.norm(None) is None, "None -> None")
check(xm.norm(123) == "123", "non-string coerced to str")

print("\n[7] find_duplicate_keys - SAME name across DIFFERENT partner_type is NOT a collision")
rows = [
    {"name": "SUPPLIER ASPGAR", "partner_type": "SUPPLIER", "directory_id": "ASPGAR"},
    {"name": "SUPPLIER ASPGAR", "partner_type": "FACTORY", "directory_id": "ASPGAR"},
]
dups = xm.find_duplicate_keys(rows)
check(dups == {}, "same name, different partner_type -> NOT flagged (composite key, both records are valid)")

print("\n[8] find_duplicate_keys - SAME (name, partner_type) pair repeated IS a collision")
rows = [
    {"name": "Acme Co", "partner_type": "SUPPLIER", "directory_id": "A1"},
    {"name": "Acme Co", "partner_type": "SUPPLIER", "directory_id": "A2"},
    {"name": "Unique Co", "partner_type": "FACTORY", "directory_id": "U1"},
]
dups = xm.find_duplicate_keys(rows)
check(list(dups.keys()) == [("Acme Co", "SUPPLIER")], "only the truly colliding (name, type) pair is flagged")
check(len(dups[("Acme Co", "SUPPLIER")]) == 2, "both colliding rows returned")

check(xm.find_duplicate_keys([{"name": "A", "partner_type": "SUPPLIER"},
                              {"name": "B", "partner_type": "FACTORY"}]) == {},
      "no collisions -> empty dict")
check(xm.find_duplicate_keys([{"name": None, "partner_type": "SUPPLIER"},
                              {"name": "", "partner_type": "FACTORY"},
                              {"name": "X", "partner_type": None}]) == {},
      "rows missing name or partner_type are ignored")

print("\n[9] dedupe_by_key - same name/different type: BOTH kept as separate winners")
rows = [
    {"name": "SUPPLIER ASPGAR", "partner_type": "SUPPLIER", "directory_id": "ASPGAR", "row_index": 1},
    {"name": "SUPPLIER ASPGAR", "partner_type": "FACTORY", "directory_id": "ASPGAR", "row_index": 1},
]
winners, dups = xm.dedupe_by_key(rows)
check(len(winners) == 2, "both rows kept - different partner_type means different key")
check(dups == {}, "no duplicate map entries - this was never a collision")

print("\n[10] dedupe_by_key - true collision: prefers row with a non-null directory_id")
rows = [
    {"name": "Acme Co", "partner_type": "SUPPLIER", "directory_id": None, "row_index": 1},
    {"name": "Acme Co", "partner_type": "SUPPLIER", "directory_id": "SUP1", "row_index": 5},
]
winners, dups = xm.dedupe_by_key(rows)
check(len(winners) == 1, "collapsed to exactly one winner")
check(winners[0]["directory_id"] == "SUP1", "winner has the non-null directory_id")
check(("Acme Co", "SUPPLIER") in dups and len(dups[("Acme Co", "SUPPLIER")]) == 2,
      "duplicate map still reports both rows")

print("\n[11] dedupe_by_key - true collision tie-break by row_index when code presence ties")
rows = [
    {"name": "Acme Co", "partner_type": "FACTORY", "directory_id": None, "row_index": 9},
    {"name": "Acme Co", "partner_type": "FACTORY", "directory_id": None, "row_index": 2},
]
winners, _ = xm.dedupe_by_key(rows)
check(winners[0]["row_index"] == 2, "lower row_index wins the tie-break")

print("\n[12] dedupe_by_key - no collisions -> all rows pass through unchanged, empty dup map")
rows = [
    {"name": "A", "partner_type": "SUPPLIER", "directory_id": "S1", "row_index": 1},
    {"name": "B", "partner_type": "FACTORY", "directory_id": "F1", "row_index": 1},
]
winners, dups = xm.dedupe_by_key(rows)
check(len(winners) == 2 and dups == {}, "no collisions -> both rows kept, empty duplicate map")

print("\n[13] dedupe_by_key - preserves first-seen order for readability")
rows = [
    {"name": "Zeta", "partner_type": "SUPPLIER", "directory_id": "Z1", "row_index": 1},
    {"name": "Alpha", "partner_type": "SUPPLIER", "directory_id": "A1", "row_index": 1},
]
winners, _ = xm.dedupe_by_key(rows)
check([w["name"] for w in winners] == ["Zeta", "Alpha"], "output order matches first-seen input order")

print("\n[14a] is_brand_row / extract_directory_row - Supplier: Type='Brand' is excluded")
brand_row = {"Supplier Name": "Wrangler", "Supplier Code": "", "Type": "Brand"}
check(xm.is_brand_row("SUPPLIER", brand_row) is True, "Type='Brand' flagged as brand row")
check(xm.extract_directory_row("SUPPLIER", brand_row, request_id="r", request_reference="ref") is None,
      "brand row produces no Directory record even though it has a name")

real_supplier_row = {"Supplier Name": "SUPPLIER ASPGAR", "Supplier Code": "ASPGAR", "Type": "Supplier"}
check(xm.is_brand_row("SUPPLIER", real_supplier_row) is False, "Type='Supplier' is NOT a brand row")
check(xm.extract_directory_row("SUPPLIER", real_supplier_row, request_id="r", request_reference="ref") is not None,
      "real supplier row (Type='Supplier') IS extracted")

print("\n[14b] is_brand_row - Factory has no Type filter (EXCLUDE_TYPE_VALUES['FACTORY'] is None)")
check(xm.is_brand_row("FACTORY", {"Factory Name": "Whatever", "Type": "Brand"}) is False,
      "Factory rows are never excluded by is_brand_row (no Type column exists in DTC for Factory)")
check(xm.extract_directory_row(
    "FACTORY", {"Factory Name": "Acme Factory", "Factory Code": "F1", "Production Country": "VN"},
    request_id="r", request_reference="ref") is not None, "factory row still extracts normally")

print("\n[15] XTS_REQUESTS / FIELD_MAP consistency - SUPPLIER + FACTORY only (Mill out of scope)")
check(set(xm.XTS_REQUESTS.keys()) == {"XTS Supplier Master", "XTS Factory Master"},
      "exactly the 2 in-scope exact request names are configured (no Mill)")
check({v["partner_type"] for v in xm.XTS_REQUESTS.values()} == {"SUPPLIER", "FACTORY"},
      "only SUPPLIER and FACTORY partner types are represented")
check(set(xm.FIELD_MAP.keys()) == {"SUPPLIER", "FACTORY"}, "FIELD_MAP covers only SUPPLIER/FACTORY")
check("MILL" not in xm.FIELD_MAP and "MILL" not in xm.XTS_REQUESTS and "MILL" not in xm.EXCLUDE_TYPE_VALUES,
      "MILL is fully absent from all XTS Master config maps")
check(xm.FIELD_MAP["SUPPLIER"]["country"] is None, "SUPPLIER has no country column (documented live finding)")
check(xm.FIELD_MAP["FACTORY"]["code"] == "Factory Code", "FACTORY code column is 'Factory Code'")
check(set(xm.EXCLUDE_TYPE_VALUES.keys()) == {"SUPPLIER", "FACTORY"},
      "EXCLUDE_TYPE_VALUES covers only SUPPLIER/FACTORY")
check(xm.EXCLUDE_TYPE_VALUES["SUPPLIER"] == "Brand", "Supplier brand-row marker is 'Brand'")
check(xm.EXCLUDE_TYPE_VALUES["FACTORY"] is None, "Factory has no brand-row filter (no Type column)")

print("\n" + "=" * 70)
if _failures:
    print(f"❌ {len(_failures)} XTS MASTER TEST(S) FAILED")
    sys.exit(1)
print("✅ ALL XTS MASTER PURE-FUNCTION TESTS PASSED")
