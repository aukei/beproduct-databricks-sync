#!/usr/bin/env python3
"""
Unit tests for the Phase 1 BeProduct -> DTC upsert core (dtc/python/sync/phase1.py).

Pure-Python, no Spark, no network. Run:
    python3 dtc/tests/test_phase1.py
or with pytest:
    pytest dtc/tests/test_phase1.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from sync import phase1
from sync.phase1 import (
    norm, parse_request_reference, is_in_scope, build_target_payload,
    diff_updatable_fields, compute_upsert, to_sheet_data, max_row_index,
    STYLE_IMAGE_COL,
)

_failures = []


def check(cond, msg):
    if cond:
        print(f"  ✅ {msg}")
    else:
        print(f"  ❌ {msg}")
        _failures.append(msg)


# ---------------------------------------------------------------------------
print("\n[1] norm()")
check(norm("  WMG-J876-263   001 ") == "WMG-J876-263 001", "collapses/trims whitespace")
check(norm("N/A") is None and norm("") is None and norm(None) is None, "null sentinels -> None")
check(norm("Body ") == "Body", "trailing space trimmed")
check(norm("WMG-J876-263-001") != norm("WMG-J876-263 001"), "dash vs space NOT merged")

print("\n[2] parse_request_reference() / is_in_scope()")
p = parse_request_reference("KTB FW26 Wrangler Western")
check(p == {"customer": "KTB", "season_code": "FW26", "brand": "Wrangler Western"},
      "parses customer/season/multi-word brand")
check(is_in_scope("KTB FW26 Wrangler", "KTB") is True, "KTB request in scope for KTB")
check(is_in_scope("KON FW26 Wrangler", "KTB") is False, "KON request out of scope for KTB")
for bad in ["KTB Wrangler", "KTB SPRING Wrangler", ""]:
    try:
        parse_request_reference(bad); check(False, f"{bad!r} should raise")
    except ValueError:
        check(True, f"invalid reference {bad!r} raises")

print("\n[3] build_target_payload() - Style Image excluded, view-def filtering")
bp = {
    "lf_style_number": "WMG-J876-263 001", "color": "Croc print", "brands": "Wrangler",
    "product_status": "Production", "description": "DESC", "division": "Modern Global",
    "front_image_url": "http://img", "garment_finish": "X", "techpack_stage": "Y",
    "customer_style_number": "LEG123", "parent_vendor": "VEND",
}
# All of these now exist in the live WIP_ITS_USE view definition.
allowed = {"LF Style#", "Color / Wash", "Brand", "Product Status",
           "Style Description", "Division", "Garment Finish", "Tech Pack Stage",
           "Legacy Code", "Main Vendor (Sampling)", STYLE_IMAGE_COL}
pl = build_target_payload(bp, allowed_cols=allowed, include_keys=True)
check(STYLE_IMAGE_COL not in pl, "Style Image excluded from payload")
check(pl.get("Division") == "Modern Global", "division -> 'Division' (renamed, no '?')")
check(pl.get("Garment Finish") == "X" and pl.get("Tech Pack Stage") == "Y",
      "Garment Finish / Tech Pack Stage now mapped & included")
check(pl.get("Legacy Code") == "LEG123" and pl.get("Main Vendor (Sampling)") == "VEND",
      "Legacy Code / Main Vendor (Sampling) mapped & included")
# allowed_cols still filters out columns absent from a given view.
pl_filtered = build_target_payload(
    bp, allowed_cols={"LF Style#", "Color / Wash", "Brand", "Product Status"},
    include_keys=True)
check("Division" not in pl_filtered and "Garment Finish" not in pl_filtered,
      "cols not in allowed view are dropped (no 400)")
pl_nokey = build_target_payload(bp, allowed_cols=allowed, include_keys=False)
check("LF Style#" not in pl_nokey and "Brand" not in pl_nokey,
      "include_keys=False drops key+brand cols")

print("\n[4] diff_updatable_fields()")
dtc_row = {"LF Style#": "WMG-J876-263 001", "Color / Wash": "Croc print",
           "Brand": "Wrangler", "Product Status": "Proto", "Style Description": "DESC"}
changed = diff_updatable_fields(dtc_row, bp, allowed_cols=allowed)
check(changed.get("Product Status") == "Production", "detects changed Product Status")
check("Style Description" not in changed, "unchanged field not in diff")
check("LF Style#" not in changed and "Brand" not in changed, "keys never in diff")

print("\n[5] compute_upsert() - UPDATE / INSERT / NOOP + sparse rowIndex")
scope = {"season_code": "FW26", "brand": "Wrangler"}
dtc_rows = [
    {"rowId": "r1", "rowIndex": 1, "LF Style#": "S1", "Color / Wash": "Black",
     "Brand": "Wrangler", "Product Status": "Proto"},
    {"rowId": "r2", "rowIndex": 5, "LF Style#": "S1", "Color / Wash": "Blue",
     "Brand": "Wrangler", "Product Status": "Production"},   # sparse gap 2..4
]
bp_rows = [
    # update S1/Black (status changes Proto->Production)
    {"lf_style_number": "S1", "color": "Black", "brands": "Wrangler",
     "season_code": "FW26", "product_status": "Production"},
    # noop S1/Blue (already Production)
    {"lf_style_number": "S1", "color": "Blue", "brands": "Wrangler",
     "season_code": "FW26", "product_status": "Production"},
    # insert S2/Red
    {"lf_style_number": "S2", "color": "Red", "brands": "Wrangler",
     "season_code": "FW26", "product_status": "Proto"},
    # out-of-scope brand -> exception
    {"lf_style_number": "S3", "color": "Green", "brands": "Lee",
     "season_code": "FW26", "product_status": "Proto"},
]
allowed2 = {"LF Style#", "Color / Wash", "Brand", "Product Status"}
plan = compute_upsert(scope, dtc_rows, bp_rows, allowed_cols=allowed2)
print("   summary:", plan.summary())
check(plan.summary() == {"updates": 1, "inserts": 1, "noops": 1, "exceptions": 1},
      "1 update / 1 insert / 1 noop / 1 exception")
check(plan.updates[0].row_id == "r1", "update targets matched rowId r1")
check(plan.updates[0].row_index == 1, "update preserves original rowIndex 1")
check(plan.inserts[0].row_index == 6, "insert rowIndex = max(5)+1 = 6 (sparse-aware)")
check(plan.exceptions[0].reason == "brand_mismatch", "Lee row flagged brand_mismatch")

print("\n[6] duplicate BeProduct key -> exception")
dup = compute_upsert(scope, [], [
    {"lf_style_number": "S9", "color": "Black", "brands": "Wrangler", "season_code": "FW26"},
    {"lf_style_number": "S9", "color": "Black", "brands": "Wrangler", "season_code": "FW26"},
], allowed_cols=allowed2)
check(dup.summary()["inserts"] == 1 and dup.summary()["exceptions"] == 1,
      "second duplicate row -> exception")

print("\n[7] to_sheet_data() shape for connector")
sd = to_sheet_data(plan)
upd = [r for r in sd if "rowId" in r]
ins = [r for r in sd if "rowIndex" in r]
check(len(upd) == 1 and "rowId" in upd[0], "UPDATE row carries rowId")
check(len(ins) == 1 and ins[0]["rowIndex"] == 6, "INSERT row carries rowIndex")
check(all(("rowId" in r) ^ ("rowIndex" in r) for r in sd), "each row has exactly one of rowId/rowIndex")

print("\n[8] max_row_index() sparse-aware")
check(max_row_index([{"rowIndex": 3}, {"rowIndex": 9}, {"rowIndex": None}]) == 9, "uses max not count")
check(max_row_index([]) == 0, "empty -> 0")

print("\n[9] split helpers + chunked + connector mix-guard")
usd = phase1.update_sheet_data(plan)
isd = phase1.insert_sheet_data(plan)
check(all("rowId" in r and "rowIndex" not in r for r in usd), "update_sheet_data: rowId only")
check(all("rowIndex" in r and "rowId" not in r for r in isd), "insert_sheet_data: rowIndex only")
check(phase1.chunked([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]], "chunked splits correctly")
check(phase1.chunked([], 10) == [], "chunked empty -> []")
# connector mix-guard (pure validation, no network)
sys.path.insert(0, str(Path(__file__).parent.parent / "python"))
from connectors.dtc import DTCConnector
_c = DTCConnector(api_key="x", environment="uat")
try:
    _c.patch_rows("s", "v", [{"rowId": "a", "X": "1"}, {"rowIndex": 9, "X": "2"}])
    check(False, "patch_rows should reject mixed rowId/rowIndex")
except ValueError:
    check(True, "patch_rows rejects mixed rowId/rowIndex batch")

print("\n" + "=" * 70)
if _failures:
    print(f"❌ {len(_failures)} FAILURE(S):")
    for f in _failures:
        print("   -", f)
    sys.exit(1)
print("✅ ALL PHASE 1 CORE UNIT TESTS PASSED")
