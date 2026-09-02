#!/usr/bin/env python3
"""
Unit tests for the Phase 10 BOM enrichment core (dtc/python/sync/bom.py).

Pure-Python, no Spark, no network. Run:
    python3 dtc/tests/test_bom.py
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from sync import bom
from sync.bom import (
    build_style_season, parse_bom_segments, extract_enrichment_fields,
    to_wip_fields, style_already_enriched, plan_row_enrichment,
    plan_style_enrichment, PLACEHOLDER_FABRIC_GROUP,
    WIP_FIELD_FABRIC_GROUP, WIP_FIELD_PLACEMENT, WIP_FIELD_MILL_FABRIC_ARTICLE,
)

_failures = []


def check(cond, msg):
    if cond:
        print(f"  ✅ {msg}")
    else:
        print(f"  ❌ {msg}")
        _failures.append(msg)


# ---------------------------------------------------------------------------
print("\n[1] build_style_season()")
check(build_style_season("Spring", "2028") == "Spring - 2028",
      "builds the exact live-confirmed join value")
check(build_style_season(None, "2028") is None, "blank season -> None")
check(build_style_season("Spring", None) is None, "blank year -> None")
check(build_style_season("  Spring  ", " 2028 ") == "Spring - 2028",
      "surrounding whitespace is trimmed")
check(build_style_season("", "") is None, "both blank -> None")

# ---------------------------------------------------------------------------
print("\n[2] parse_bom_segments() — real KTB-00023 data (Main Fabric + 1 Fabric segment)")
REAL_BOM_KTB00023 = json.dumps([{
    "part": "BOM",
    "details": [
        {"bom_detail_name": "Main Fabric", "material_type": "2/2 Twill",
         "material_name": "100% Recycled Nylon Shell", "material_no": "WV-0063",
         "mill_supplier": "BEPRODUCT VENDOR", "usage": "BODICE", "placement": "BODICE"},
        {"bom_detail_name": "Fabric", "material_type": "Tulle",
         "material_name": "LINING FABRIC (100% POLYESTER): AVAILABLE LINING",
         "material_no": "WV-0047", "usage": "Body Front", "placement": "Body Front"},
        {"bom_detail_name": "Stitch/Seam", "material_name": "TOPS THREAD T105"},
        {"bom_detail_name": "Trim", "material_name": "16L PLASTIC BUTTON DTM"},
        {"bom_detail_name": "Label", "material_name": "WOMEN Label"},
    ],
    "column_header": ["**BomHeader"],
}])

segs = parse_bom_segments(REAL_BOM_KTB00023)
check(segs.main_fabric is not None and segs.main_fabric["material_no"] == "WV-0063",
      "Main Fabric segment extracted")
check(len(segs.fabric_list) == 1 and segs.fabric_list[0]["material_no"] == "WV-0047",
      "exactly one Fabric segment extracted")
check(not segs.is_empty(), "non-empty when segments found")

print("\n[2b] parse_bom_segments() — Main Fabric only (no Fabric segment; e.g. KTB-00016)")
REAL_BOM_KTB00016 = json.dumps([{"part": "BOM", "details": [
    {"bom_detail_name": "Main Fabric", "material_name": "123455 - 97%Cotton 3%Spandex",
     "material_no": "WV-0064", "placement": "bodice"},
    {"bom_detail_name": "Trim", "material_name": "x"},
]}])
segs16 = parse_bom_segments(REAL_BOM_KTB00016)
check(segs16.main_fabric is not None, "Main Fabric present")
check(segs16.fabric_list == [], "no Fabric segments -> empty list")

print("\n[2c] parse_bom_segments() — edge cases")
check(parse_bom_segments(None).is_empty(), "None -> empty")
check(parse_bom_segments("").is_empty(), "empty string -> empty")
check(parse_bom_segments("not json{{{").is_empty(), "malformed JSON -> empty (never raises)")
check(parse_bom_segments('{"part": "BOM"}').is_empty(), "a bare dict (not a list) -> empty")
check(parse_bom_segments(json.dumps([{"part": "BOM", "details": [
    {"bom_detail_name": "Trim", "material_name": "x"},
]}])).is_empty(), "BOM with only uninteresting segments -> empty")
check(parse_bom_segments([{"part": "BOM", "details": [
    {"bom_detail_name": "Main Fabric", "material_name": "A"},
]}]).main_fabric["material_name"] == "A",
      "accepts an already-parsed list too (not just a JSON string)")

print("\n[2d] parse_bom_segments() — multiple Fabric segments (spec allows 0+; not yet seen live)")
MULTI_FABRIC_BOM = [{"part": "BOM", "details": [
    {"bom_detail_name": "Main Fabric", "material_no": "MN-001", "placement": "bodice"},
    {"bom_detail_name": "Fabric", "material_no": "FB-001", "placement": "sleeve"},
    {"bom_detail_name": "Fabric", "material_no": "FB-002", "placement": "collar"},
]}]
segs_multi = parse_bom_segments(MULTI_FABRIC_BOM)
check(len(segs_multi.fabric_list) == 2, "both Fabric segments collected, in document order")
check([d["material_no"] for d in segs_multi.fabric_list] == ["FB-001", "FB-002"],
      "order preserved")

print("\n[2e] parse_bom_segments() — duplicate Main Fabric, first wins")
DUP_BOM = [{"part": "BOM", "details": [
    {"bom_detail_name": "Main Fabric", "material_no": "FIRST"},
    {"bom_detail_name": "Main Fabric", "material_no": "SECOND"},
]}]
check(parse_bom_segments(DUP_BOM).main_fabric["material_no"] == "FIRST",
      "first occurrence of a repeated Main Fabric wins")

# ---------------------------------------------------------------------------
print("\n[3] extract_enrichment_fields() / to_wip_fields()")
fields = extract_enrichment_fields(segs.main_fabric)
check(fields == {
    "fabric_group": "Main Fabric",     # bom_detail_name, NOT material_name (corrected 2026-09-02)
    "placement": "BODICE",
    "mill_fabric_article": "WV-0063",
}, "fabric_group = bom_detail_name (NOT material_name); placement/mill_fabric_article unaffected")

fabric_fields = extract_enrichment_fields(segs.fabric_list[0])
check(fabric_fields["fabric_group"] == "Fabric",
      "a 'Fabric' segment's fabric_group is literally 'Fabric'")

wip_fields = to_wip_fields(fields)
check(wip_fields == {
    WIP_FIELD_FABRIC_GROUP: "Main Fabric",
    WIP_FIELD_PLACEMENT: "BODICE",
    WIP_FIELD_MILL_FABRIC_ARTICLE: "WV-0063",
}, "maps to the exact live-confirmed raw DTC field names")
check(WIP_FIELD_FABRIC_GROUP == "Fabric Group"
      and WIP_FIELD_PLACEMENT == "Placement"
      and WIP_FIELD_MILL_FABRIC_ARTICLE == "Mill Fabric Article #",
      "raw field name constants match the live WIP view definition")

# ---------------------------------------------------------------------------
print("\n[4] style_already_enriched()")
check(style_already_enriched([PLACEHOLDER_FABRIC_GROUP]) is False,
      "only the placeholder present -> not yet enriched")
check(style_already_enriched([None, ""]) is False, "blank/None values -> not yet enriched")
check(style_already_enriched([PLACEHOLDER_FABRIC_GROUP, "Real Fabric Data"]) is True,
      "ANY row with real data short-circuits the WHOLE style to already-enriched")
check(style_already_enriched([]) is False, "no rows at all -> not enriched (nothing to check)")

# ---------------------------------------------------------------------------
print("\n[5] plan_row_enrichment()")
plan_main_and_fabric = plan_row_enrichment(segs)
check(plan_main_and_fabric.update_fields["fabric_group"] == "Main Fabric",
      "Main Fabric present -> update_fields set")
check(len(plan_main_and_fabric.duplicate_fields_list) == 1
      and plan_main_and_fabric.duplicate_fields_list[0]["mill_fabric_article"] == "WV-0047",
      "one Fabric segment -> one duplicate plan entry")

plan_main_only = plan_row_enrichment(segs16)
check(plan_main_only.update_fields is not None
      and plan_main_only.duplicate_fields_list == [],
      "Main Fabric only -> update, no duplicates")

plan_multi = plan_row_enrichment(segs_multi)
check(len(plan_multi.duplicate_fields_list) == 2, "two Fabric segments -> two duplicate plan entries")

plan_none = plan_row_enrichment(bom.ParsedBomSegments())
check(plan_none.is_empty(), "no segments at all -> empty plan")

fabric_only_segs = bom.ParsedBomSegments(main_fabric=None, fabric_list=[{"bom_detail_name": "Fabric", "material_no": "X"}])
plan_fabric_only = plan_row_enrichment(fabric_only_segs)
check(plan_fabric_only.update_fields is None and len(plan_fabric_only.duplicate_fields_list) == 1,
      "Fabric segment(s) with NO Main Fabric -> no update, but duplicates still planned")

# ---------------------------------------------------------------------------
print("\n[6] plan_style_enrichment() — full integration")

print("  [6a] style already enriched -> no-op")
actions = plan_style_enrichment(
    existing_rows=[{"row_id": "r1", "fabric_group": "Real Data"}],
    bom_unified=REAL_BOM_KTB00023,
)
check(actions == [], "any real Fabric Group value short-circuits to []")

print("  [6b] no existing WIP rows -> no-op")
check(plan_style_enrichment([], REAL_BOM_KTB00023) == [], "empty existing_rows -> []")

print("  [6c] single row, Main Fabric only (KTB-00016-like) -> one UPDATE, no INSERT")
actions = plan_style_enrichment(
    existing_rows=[{"row_id": "r1", "fabric_group": PLACEHOLDER_FABRIC_GROUP}],
    bom_unified=REAL_BOM_KTB00016,
)
check(len(actions) == 1, "exactly one action")
check(actions[0].kind == "update" and actions[0].row_id == "r1",
      "single UPDATE targeting the existing row_id")
check(actions[0].wip_fields[WIP_FIELD_FABRIC_GROUP] == "Main Fabric",
      "UPDATE's Fabric Group is the literal segment name 'Main Fabric'")

print("  [6d] single row, Main Fabric + 1 Fabric segment (KTB-00023-like) -> one UPDATE + one INSERT")
actions = plan_style_enrichment(
    existing_rows=[{"row_id": "r1", "fabric_group": PLACEHOLDER_FABRIC_GROUP, "color": "RedGingham"}],
    bom_unified=REAL_BOM_KTB00023,
)
check(len(actions) == 2, "exactly two actions (update + insert)")
kinds = sorted(a.kind for a in actions)
check(kinds == ["insert", "update"], "one update, one insert")
update_action = next(a for a in actions if a.kind == "update")
insert_action = next(a for a in actions if a.kind == "insert")
check(update_action.wip_fields[WIP_FIELD_FABRIC_GROUP] == "Main Fabric",
      "the UPDATE's Fabric Group = 'Main Fabric'")
check(insert_action.wip_fields[WIP_FIELD_FABRIC_GROUP] == "Fabric",
      "the INSERT's Fabric Group = 'Fabric'")
check(insert_action.wip_fields[WIP_FIELD_MILL_FABRIC_ARTICLE] == "WV-0047",
      "the INSERT carries the Fabric segment's own material_no")
check(insert_action.base_row == {"row_id": "r1", "fabric_group": PLACEHOLDER_FABRIC_GROUP, "color": "RedGingham"},
      "the INSERT's base_row is the full original row dict, for copying all other fields")

print("  [6e] multi-row style (colorways), Main Fabric + 2 Fabric segments -> N updates + N*M inserts")
actions = plan_style_enrichment(
    existing_rows=[
        {"row_id": "r1", "fabric_group": PLACEHOLDER_FABRIC_GROUP, "color": "Black"},
        {"row_id": "r2", "fabric_group": PLACEHOLDER_FABRIC_GROUP, "color": "White"},
    ],
    bom_unified=MULTI_FABRIC_BOM,
)
check(len(actions) == 6, "2 rows x (1 update + 2 inserts) = 6 actions")
check(sorted(a.kind for a in actions).count("update") == 2, "two updates (one per row)")
check(sorted(a.kind for a in actions).count("insert") == 4, "four inserts (2 rows x 2 Fabric segments)")
update_row_ids = sorted(a.row_id for a in actions if a.kind == "update")
check(update_row_ids == ["r1", "r2"], "both existing rows get updated")
insert_base_colors = sorted(a.base_row["color"] for a in actions if a.kind == "insert")
check(insert_base_colors == ["Black", "Black", "White", "White"],
      "each colorway gets one duplicated row PER Fabric segment")

print("  [6f] BOM with no relevant segments -> no-op even though row is on placeholder")
no_seg_bom = json.dumps([{"part": "BOM", "details": [
    {"bom_detail_name": "Trim", "material_name": "x"},
]}])
check(plan_style_enrichment(
    [{"row_id": "r1", "fabric_group": PLACEHOLDER_FABRIC_GROUP}], no_seg_bom
) == [], "no Main Fabric/Fabric segments -> no-op")

print("  [6g] only Fabric segment(s), no Main Fabric -> inserts only, existing row untouched")
fabric_only_bom = [{"part": "BOM", "details": [
    {"bom_detail_name": "Fabric", "material_no": "FB-999", "placement": "yoke"},
]}]
actions = plan_style_enrichment(
    [{"row_id": "r1", "fabric_group": PLACEHOLDER_FABRIC_GROUP}], fabric_only_bom
)
check(len(actions) == 1 and actions[0].kind == "insert",
      "no Main Fabric -> no UPDATE, but the Fabric segment still produces an INSERT")

# ---------------------------------------------------------------------------
print(f"\n{'='*60}")
if _failures:
    print(f"❌ {len(_failures)} check(s) failed:")
    for f in _failures:
        print(f"   - {f}")
    sys.exit(1)
else:
    print("✅ All checks passed")
