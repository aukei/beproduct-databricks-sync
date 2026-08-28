#!/usr/bin/env python3
"""
Unit tests for the Phase 1 BeProduct -> DTC upsert core (dtc/python/sync/phase1.py).

Pure-Python, no Spark, no network. Run:
    python3 dtc/tests/test_phase1.py
or with pytest:
    pytest dtc/tests/test_phase1.py

Phase 6 update (2026-06-26):
  - Match key changed from ("LF Style#", "Color / Wash") to ("BP Style#", "Color / Wash").
  - Staging key column renamed: lf_style_number -> bp_style_number.
  - brand (brand_hk, single-value) replaces brands (brands_multi list).
  - "LF Style#" DTC column is now optional BP->DTC (from lf_style_number field).
  - "Legacy Code" DTC column is now optional BP->DTC (from customer_style_number).
  - "Customer Style#" is the new DTC->BP column (handled in phase2).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from sync import phase1
from sync.phase1 import (
    norm, parse_request_reference, is_in_scope, build_target_payload,
    diff_updatable_fields, compute_upsert, to_sheet_data, max_row_index,
    STYLE_IMAGE_COL, DEFAULT_FILL_COLS,
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
check(norm('"A","B","C"\n"D","E","F"') == '"A","B","C"\n"D","E","F"',
      "embedded newline is PRESERVED, not collapsed to a space - critical for "
      "sync.samples.format_sample_field's multi-submit output to actually reach "
      "DTC as separate lines (build_target_payload pushes norm(value) verbatim)")
check(norm("A   \t  B\nC") == "A B\nC",
      "non-newline whitespace (spaces/tabs) around a newline still collapses normally")

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

print("\n[3] build_target_payload() - Style Image excluded, DTC-owned fields not pushed")
bp = {
    # Phase 6: bp_style_number replaces lf_style_number as the match key column.
    "bp_style_number": "WMG-J876-263 001",
    "lf_style_number": "LF-OLD-123",        # optional BP->DTC (new separate field)
    "color": "Croc print",
    "brand": "Wrangler",                     # Phase 6: brand_hk single-value
    "product_status": "Production", "description": "DESC", "division": "Modern Global",
    "front_image_url": "http://img", "garment_finish": "X", "techpack_stage": "Y",
    "gender": "Ladies",                      # Phase 6: new field
    "supplier": "Supplier",                  # Phase 6: default-fill constant
    # Optional BP->DTC: customer_style_number -> "Legacy Code" (Phase 6)
    "customer_style_number": "LEG123",
    # Phase 2 (DTC->BP) source columns - must NOT be pushed BeProduct -> DTC:
    "parent_vendor": "VEND", "lot_code": "L9", "factory": "FAC",
}
allowed = {"BP Style#", "LF Style#", "Color / Wash", "Brand", "Product Status",
           "Style Description", "Division", "Garment Finish", "Tech Pack Stage",
           "Legacy Code", "Gender", "Supplier",
           "Main Vendor (Sampling)", "Lot#", "Main Factory (Sampling)",
           STYLE_IMAGE_COL}
pl = build_target_payload(bp, allowed_cols=allowed, include_keys=True)
check(STYLE_IMAGE_COL not in pl, "Style Image excluded from payload")
check(pl.get("BP Style#") == "WMG-J876-263 001", "bp_style_number -> 'BP Style#' (new match key)")
check(pl.get("LF Style#") == "LF-OLD-123", "lf_style_number -> 'LF Style#' (optional)")
check(pl.get("Legacy Code") == "LEG123", "customer_style_number -> 'Legacy Code' (Phase 6 BP->DTC)")
check(pl.get("Gender") == "Ladies", "gender -> 'Gender'")
check(pl.get("Supplier") == "Supplier", "supplier -> 'Supplier' (default-fill constant)")
check(pl.get("Division") == "Modern Global", "division -> 'Division' (renamed, no '?')")
check(pl.get("Garment Finish") == "X" and pl.get("Tech Pack Stage") == "Y",
      "BeProduct-owned Garment Finish / Tech Pack Stage included")
check(not any(c in pl for c in ("Customer Style#", "Main Vendor (Sampling)", "Lot#",
              "Main Factory (Sampling)")),
      "DTC-owned / removed Phase 2 fields NOT pushed BeProduct->DTC")
# allowed_cols still filters out columns absent from a given view.
pl_filtered = build_target_payload(
    bp, allowed_cols={"BP Style#", "Color / Wash", "Brand", "Product Status"},
    include_keys=True)
check("Division" not in pl_filtered and "Garment Finish" not in pl_filtered,
      "cols not in allowed view are dropped (no 400)")
pl_nokey = build_target_payload(bp, allowed_cols=allowed, include_keys=False)
check("BP Style#" not in pl_nokey and "Brand" not in pl_nokey,
      "include_keys=False drops key+brand cols")

print("\n[4] diff_updatable_fields()")
# Phase 6: DTC rows now use "BP Style#" (not "LF Style#") as the match key column.
dtc_row = {"BP Style#": "WMG-J876-263 001", "Color / Wash": "Croc print",
           "Brand": "Wrangler", "Product Status": "Proto", "Style Description": "DESC"}
changed = diff_updatable_fields(dtc_row, bp, allowed_cols=allowed)
check(changed.get("Product Status") == "Production", "detects changed Product Status")
check("Style Description" not in changed, "unchanged field not in diff")
check("BP Style#" not in changed and "Brand" not in changed, "keys never in diff")

print("\n[5] compute_upsert() - UPDATE / INSERT / NOOP + sparse rowIndex")
scope = {"season_code": "FW26", "brand": "Wrangler"}
# Phase 6: DTC rows now have "BP Style#" column (not "LF Style#").
dtc_rows = [
    {"rowId": "r1", "rowIndex": 1, "BP Style#": "S1", "Color / Wash": "Black",
     "Brand": "Wrangler", "Product Status": "Proto"},
    {"rowId": "r2", "rowIndex": 5, "BP Style#": "S1", "Color / Wash": "Blue",
     "Brand": "Wrangler", "Product Status": "Production"},   # sparse gap 2..4
]
bp_rows = [
    # update S1/Black (status changes Proto->Production)
    # Phase 6: use bp_style_number (not lf_style_number) and brand (not brands)
    {"bp_style_number": "S1", "color": "Black", "brand": "Wrangler",
     "season_code": "FW26", "product_status": "Production"},
    # noop S1/Blue (already Production)
    {"bp_style_number": "S1", "color": "Blue", "brand": "Wrangler",
     "season_code": "FW26", "product_status": "Production"},
    # insert S2/Red
    {"bp_style_number": "S2", "color": "Red", "brand": "Wrangler",
     "season_code": "FW26", "product_status": "Proto"},
    # out-of-scope brand -> exception
    {"bp_style_number": "S3", "color": "Green", "brand": "Lee",
     "season_code": "FW26", "product_status": "Proto"},
]
allowed2 = {"BP Style#", "Color / Wash", "Brand", "Product Status"}
plan = compute_upsert(scope, dtc_rows, bp_rows, allowed_cols=allowed2)
print("   summary:", plan.summary())
check(plan.summary() == {"updates": 1, "inserts": 1, "noops": 1, "exceptions": 1},
      "1 update / 1 insert / 1 noop / 1 exception")
check(plan.updates[0].row_id == "r1", "update targets matched rowId r1")
check(plan.updates[0].row_index == 1, "update preserves original rowIndex 1")
check(plan.inserts[0].row_index == 6, "insert rowIndex = max(5)+1 = 6 (sparse-aware)")
check(plan.exceptions[0].reason == "brand_mismatch", "Lee row flagged brand_mismatch")

print("\n[6] missing bp_style_number -> exception (Phase 6: was missing_lf_style)")
missing_key = compute_upsert(scope, [], [
    {"bp_style_number": None, "color": "Black", "brand": "Wrangler", "season_code": "FW26"},
], allowed_cols=allowed2)
check(missing_key.summary()["exceptions"] == 1 and
      missing_key.exceptions[0].reason == "missing_bp_style",
      "None bp_style_number -> missing_bp_style exception")

print("\n[7] duplicate BeProduct key -> exception")
dup = compute_upsert(scope, [], [
    {"bp_style_number": "S9", "color": "Black", "brand": "Wrangler", "season_code": "FW26"},
    {"bp_style_number": "S9", "color": "Black", "brand": "Wrangler", "season_code": "FW26"},
], allowed_cols=allowed2)
check(dup.summary()["inserts"] == 1 and dup.summary()["exceptions"] == 1,
      "second duplicate row -> exception")

print("\n[8] to_sheet_data() shape for connector")
sd = to_sheet_data(plan)
upd = [r for r in sd if "rowId" in r]
ins = [r for r in sd if "rowIndex" in r]
check(len(upd) == 1 and "rowId" in upd[0], "UPDATE row carries rowId")
check(len(ins) == 1 and ins[0]["rowIndex"] == 6, "INSERT row carries rowIndex")
check(all(("rowId" in r) ^ ("rowIndex" in r) for r in sd), "each row has exactly one of rowId/rowIndex")

print("\n[9] max_row_index() sparse-aware")
check(max_row_index([{"rowIndex": 3}, {"rowIndex": 9}, {"rowIndex": None}]) == 9, "uses max not count")
check(max_row_index([]) == 0, "empty -> 0")

print("\n[10] split helpers + chunked + connector mix-guard")
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

print("\n[11] compute_orphan_marks() - moved-key rows flagged '(removed)'")
# Phase 6: DTC rows now use "BP Style#" as the match-key column.
dtc_rows_o = [
    # stale: key moved to a different request (in moved_elsewhere) -> mark
    {"rowId": "o1", "rowIndex": 2, "BP Style#": "S1", "Color / Wash": "Black",
     "Product Status": "Production"},
    # still in this request -> leave
    {"rowId": "o2", "rowIndex": 3, "BP Style#": "S2", "Color / Wash": "Blue",
     "Product Status": "Proto"},
    # user-entered / unknown key (not in BeProduct anywhere) -> leave
    {"rowId": "o3", "rowIndex": 4, "BP Style#": "USER", "Color / Wash": "Red",
     "Product Status": "Proto"},
    # already flagged -> skip (NOOP)
    {"rowId": "o4", "rowIndex": 5, "BP Style#": "S9", "Color / Wash": "Green",
     "Product Status": "(removed)"},
]
bp_here = {("S2", "Blue")}
moved = {("S1", "Black"), ("S9", "Green")}
omarks = phase1.compute_orphan_marks(dtc_rows_o, bp_here, moved)
check(len(omarks) == 1 and omarks[0].row_id == "o1", "only the moved, unflagged row is marked")
check(omarks[0].fields == {"Product Status": phase1.REMOVED_STATUS},
      "mark sets Product Status='(removed)'")

print("\n[12] DEFAULT_FILL_COLS — Supplier never overwrites existing DTC value")
dtc_with_supplier    = {"BP Style#": "S1", "Color / Wash": "Black", "Supplier": "ActualVendor"}
dtc_blank_supplier   = {"BP Style#": "S1", "Color / Wash": "Black"}
bp_supplier_row      = {"bp_style_number": "S1", "color": "Black",
                        "brand": "Wrangler", "supplier": "Supplier"}
allowed_sup = {"BP Style#", "Color / Wash", "Brand", "Supplier"}
# When DTC already has a value -> Supplier must NOT appear in diff
diff_existing = diff_updatable_fields(dtc_with_supplier, bp_supplier_row, allowed_cols=allowed_sup)
check("Supplier" not in diff_existing,
      "DEFAULT_FILL_COLS: Supplier NOT in diff when DTC already has a value")
# When DTC cell is blank -> Supplier SHOULD appear in diff
diff_blank = diff_updatable_fields(dtc_blank_supplier, bp_supplier_row, allowed_cols=allowed_sup)
check(diff_blank.get("Supplier") == "Supplier",
      "DEFAULT_FILL_COLS: Supplier IS in diff when DTC cell is blank")
# INSERT payload always includes Supplier (new rows start blank)
ins_payload = build_target_payload(bp_supplier_row, allowed_cols=allowed_sup, include_keys=True)
check(ins_payload.get("Supplier") == "Supplier",
      "Supplier included in INSERT payload (new rows are always blank)")

print("\n[13] MATCH_KEY_COLS / KEY_DTC_COLS / DEFAULT_FILL_COLS reflect Phase 6 names")
check(phase1.MATCH_KEY_COLS == ("BP Style#", "Color / Wash"),
      "MATCH_KEY_COLS is ('BP Style#', 'Color / Wash')")
check("BP Style#" in phase1.KEY_DTC_COLS, "'BP Style#' in KEY_DTC_COLS")
check("LF Style#" not in phase1.KEY_DTC_COLS, "'LF Style#' NOT in KEY_DTC_COLS (optional now)")
check("Supplier" in phase1.DEFAULT_FILL_COLS, "'Supplier' in DEFAULT_FILL_COLS")
check("Gender" not in phase1.DEFAULT_FILL_COLS, "'Gender' NOT in DEFAULT_FILL_COLS (full overwrite)")

print("\n" + "=" * 70)
if _failures:
    print(f"❌ {len(_failures)} FAILURE(S):")
    for f in _failures:
        print("   -", f)
    sys.exit(1)
print("✅ ALL PHASE 1 CORE UNIT TESTS PASSED")
