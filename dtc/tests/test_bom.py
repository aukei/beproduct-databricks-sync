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
    to_wip_fields, segment_key, is_unenriched, build_target_segments,
    plan_style_enrichment, PLACEHOLDER_FABRIC_GROUP,
    WIP_FIELD_FABRIC_GROUP, WIP_FIELD_PLACEMENT, WIP_FIELD_MILL_FABRIC_ARTICLE,
    build_insert_row_payload, INSERT_EXCLUDE_COLS, compute_non_writable_cols,
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
print("\n[4] segment_key() / is_unenriched()")
check(segment_key({"fabric_group": "Main Fabric", "mill_fabric_article": "WV-0063"})
      == ("Main Fabric", "WV-0063"), "normal pair -> normalized tuple")
check(segment_key({"fabric_group": " Main Fabric ", "mill_fabric_article": "WV-0063"})
      == ("Main Fabric", "WV-0063"), "whitespace stripped")
check(segment_key({"fabric_group": None, "mill_fabric_article": ""})
      == (None, None), "blank/None values normalize to (None, None)")
check(segment_key({"fabric_group": "Fabric", "mill_fabric_article": "X"})
      != segment_key({"fabric_group": "Main Fabric", "mill_fabric_article": "X"}),
      "different Fabric Group -> different key even with same article #")

check(is_unenriched(PLACEHOLDER_FABRIC_GROUP) is True, "placeholder -> unenriched")
check(is_unenriched(None) is True, "None -> unenriched")
check(is_unenriched("") is True, "blank string -> unenriched")
check(is_unenriched("Main Fabric") is False, "real value -> NOT unenriched")

# ---------------------------------------------------------------------------
print("\n[5] build_target_segments()")
targets = build_target_segments(REAL_BOM_KTB00023)
check(targets is not None and len(targets) == 2, "Main Fabric + 1 Fabric segment -> 2 targets")
check(targets[0]["fabric_group"] == "Main Fabric", "target[0] is always Main Fabric")
check(targets[1]["fabric_group"] == "Fabric" and targets[1]["mill_fabric_article"] == "WV-0047",
      "target[1] is the Fabric segment")

targets16 = build_target_segments(REAL_BOM_KTB00016)
check(targets16 is not None and len(targets16) == 1, "Main Fabric only -> 1 target")

check(build_target_segments(None) is None, "blank bom_unified -> None (nothing to upsert)")
check(build_target_segments(json.dumps([{"part": "BOM", "details": [
    {"bom_detail_name": "Trim", "material_name": "x"},
]}])) is None, "no Main Fabric/Fabric segments at all -> None")
check(build_target_segments([{"part": "BOM", "details": [
    {"bom_detail_name": "Fabric", "material_no": "FB-999", "placement": "yoke"},
]}]) is None, "Fabric segment(s) present but NO Main Fabric -> None (never just insert-only)")

# ---------------------------------------------------------------------------
print("\n[6] plan_style_enrichment() — full integration (upsert semantics)")

print("  [6a] no existing WIP rows -> no-op")
check(plan_style_enrichment([], REAL_BOM_KTB00023) == [], "empty existing_rows -> []")

print("  [6b] first-time enrichment: single row, Main Fabric only (KTB-00016-like) -> one full UPDATE")
actions = plan_style_enrichment(
    existing_rows=[{"row_id": "r1", "fabric_group": PLACEHOLDER_FABRIC_GROUP,
                     "mill_fabric_article": None, "placement": None}],
    bom_unified=REAL_BOM_KTB00016,
)
check(len(actions) == 1, "exactly one action")
check(actions[0].kind == "update" and actions[0].row_id == "r1",
      "single UPDATE targeting the existing row_id")
check(actions[0].wip_fields[WIP_FIELD_FABRIC_GROUP] == "Main Fabric",
      "UPDATE's Fabric Group is the literal segment name 'Main Fabric'")
check(WIP_FIELD_MILL_FABRIC_ARTICLE in actions[0].wip_fields,
      "first-time enrichment writes the FULL field set (not just Placement)")

print("  [6c] first-time enrichment: single row, Main Fabric + 1 Fabric segment -> one UPDATE + one INSERT")
actions = plan_style_enrichment(
    existing_rows=[{"row_id": "r1", "fabric_group": PLACEHOLDER_FABRIC_GROUP,
                     "mill_fabric_article": None, "placement": None, "color": "RedGingham"}],
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
check(insert_action.base_row["color"] == "RedGingham",
      "the INSERT's base_row is the full original row dict, for copying all other fields")

print("  [6d] multi-row style (colorways), first-time, Main Fabric + 2 Fabric segments -> N updates + N*M inserts")
actions = plan_style_enrichment(
    existing_rows=[
        {"row_id": "r1", "fabric_group": PLACEHOLDER_FABRIC_GROUP, "mill_fabric_article": None,
         "placement": None, "color": "Black"},
        {"row_id": "r2", "fabric_group": PLACEHOLDER_FABRIC_GROUP, "mill_fabric_article": None,
         "placement": None, "color": "White"},
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

print("  [6e] BOM missing entirely this run -> ZERO actions, never revert existing enrichment")
already_enriched_row = {"row_id": "r1", "fabric_group": "Main Fabric",
                         "mill_fabric_article": "WV-0064", "placement": "bodice"}
check(plan_style_enrichment([already_enriched_row], None) == [],
      "bom_unified=None (e.g. missing from customer_teckpack_style_latest) -> no-op, row untouched")
check(plan_style_enrichment([already_enriched_row], json.dumps([{"part": "BOM", "details": [
    {"bom_detail_name": "Trim", "material_name": "x"},
]}])) == [], "no Main Fabric/Fabric segments at all -> no-op, row untouched")

print("  [6f] Fabric segment(s) present but NO Main Fabric -> ZERO actions (not insert-only anymore)")
fabric_only_bom = [{"part": "BOM", "details": [
    {"bom_detail_name": "Fabric", "material_no": "FB-999", "placement": "yoke"},
]}]
actions = plan_style_enrichment(
    [{"row_id": "r1", "fabric_group": PLACEHOLDER_FABRIC_GROUP, "mill_fabric_article": None, "placement": None}],
    fabric_only_bom
)
check(actions == [], "no Main Fabric -> zero actions at all, even for a placeholder row")

print("  [6g] upsert: row already matches a segment by (Fabric Group, Mill Fabric Article #) -> Placement-only update")
row_matches_main = {"row_id": "r1", "fabric_group": "Main Fabric",
                     "mill_fabric_article": "WV-0064", "placement": "WRONG PLACEMENT"}
actions = plan_style_enrichment([row_matches_main], REAL_BOM_KTB00016)
check(len(actions) == 1 and actions[0].kind == "update", "exactly one Placement-fix update")
check(actions[0].wip_fields == {WIP_FIELD_PLACEMENT: "bodice"},
      "ONLY Placement is in the payload -- Fabric Group/Mill Fabric Article # never re-written")

print("  [6h] upsert: row already matches AND Placement already correct -> no-op (idempotent)")
row_fully_correct = {"row_id": "r1", "fabric_group": "Main Fabric",
                      "mill_fabric_article": "WV-0064", "placement": "bodice"}
check(plan_style_enrichment([row_fully_correct], REAL_BOM_KTB00016) == [],
      "already fully matching -> no PATCH issued at all")

print("  [6i] never-revert: row holds a real, unrecognized (Fabric Group, Article#) combo not in current BOM -> untouched")
row_vanished_segment = {"row_id": "r1", "fabric_group": "Fabric",
                         "mill_fabric_article": "OLD-ARTICLE-NO-LONGER-IN-BOM", "placement": "yoke"}
check(plan_style_enrichment([row_vanished_segment], REAL_BOM_KTB00016) == [],
      "row's real data isn't Main Fabric's key and isn't unenriched -> left completely untouched")

print("  [6j] never-insert-duplicate: a Fabric segment already represented by an existing row -> no re-insert")
existing_with_fabric_segment = [
    {"row_id": "r1", "fabric_group": "Main Fabric", "mill_fabric_article": "WV-0063", "placement": "BODICE"},
    {"row_id": "r2", "fabric_group": "Fabric", "mill_fabric_article": "WV-0047", "placement": "Body Front"},
]
check(plan_style_enrichment(existing_with_fabric_segment, REAL_BOM_KTB00023) == [],
      "both segments already correctly represented -> zero actions, no duplicate insert")

# ---------------------------------------------------------------------------
print("\n[7] build_insert_row_payload() — Style Image must never be copied forward")
base_fields = {
    "rowId": "r1", "rowIndex": 3, "BP Style#": "KTB-00023",
    "Color / Wash": "Indigo", "Style Image": "https://cdn.example/img.jpg",
    "Fabric Group": "MAIN MATERIAL CONTENT",
}
wip = {"Fabric Group": "Fabric", "Placement": "yoke", "Mill Fabric Article #": "FB-999"}
payload = build_insert_row_payload(base_fields, wip)
check("Style Image" not in payload,
      "Style Image excluded from INSERT payload (DTC rejects image data on INSERT)")
check("rowId" not in payload and "rowIndex" not in payload,
      "rowId/rowIndex identity fields excluded from INSERT payload")
check(payload["BP Style#"] == "KTB-00023" and payload["Color / Wash"] == "Indigo",
      "non-excluded original fields still copied forward")
check(payload["Fabric Group"] == "Fabric" and payload["Placement"] == "yoke"
      and payload["Mill Fabric Article #"] == "FB-999",
      "wip_fields override applied on top of the copied row")
check(INSERT_EXCLUDE_COLS == frozenset({"rowId", "rowIndex", "Style Image"}),
      "INSERT_EXCLUDE_COLS is exactly the identity fields + Style Image")

# ---------------------------------------------------------------------------
print("\n[8] compute_non_writable_cols() — isReadOnly is unreliable; type/formula are the real signals")
dynamic_fields = [
    {"fieldName": "Style Image", "type": "contact", "isReadOnly": False},
    {"fieldName": "Fabric Article", "type": "string", "formula": "{69f029a4052cf39ce40da5ad}", "isReadOnly": False},
    {"fieldName": "Fabric Mill", "type": "string", "formula": "{69f029a4052cf39ce40da5ae}", "isReadOnly": False},
    {"fieldName": "Proto Sample - Target Sample Ready Date", "type": "date", "formula": "{x}"},
    {"fieldName": "BP Style#", "type": "string", "isReadOnly": False},
    {"fieldName": "Mill Fabric Article #", "type": "string", "formula": "", "isReadOnly": False},
    {"fieldName": "Color / Wash", "type": "string"},
]
non_writable = compute_non_writable_cols(dynamic_fields)
check(non_writable == frozenset({
    "Style Image", "Fabric Article", "Fabric Mill",
    "Proto Sample - Target Sample Ready Date",
}), "type=contact + truthy formula fields flagged; isReadOnly=False ignored (unreliable)")
check("BP Style#" not in non_writable and "Color / Wash" not in non_writable,
      "plain writable string fields NOT flagged")
check("Mill Fabric Article #" not in non_writable,
      "empty-string formula ('') is falsy -> NOT flagged (only a real formula expression counts)")

full_payload = build_insert_row_payload(
    base_fields, wip, exclude_cols=INSERT_EXCLUDE_COLS | non_writable)
check("Fabric Article" not in full_payload,
      "combining INSERT_EXCLUDE_COLS with compute_non_writable_cols excludes formula fields too")

# ---------------------------------------------------------------------------
print(f"\n{'='*60}")
if _failures:
    print(f"❌ {len(_failures)} check(s) failed:")
    for f in _failures:
        print(f"   - {f}")
    sys.exit(1)
else:
    print("✅ All checks passed")
