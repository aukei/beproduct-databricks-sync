#!/usr/bin/env python3
"""
Unit tests for the Phase 10 BOM enrichment core (dtc/python/sync/bom.py).

Pure-Python, no Spark, no network. Run:
    python3 dtc/tests/test_bom.py

SOURCE CHANGED 2026-09-09 ("2nd revision", owner spec): this module now
parses `customer_teckpack_style_log.custom_fields` (path: custom_fields ->
xts_data -> TECH_PACK_EXTRACTION -> Table[Type="BOM"] -> ColumnHeader/Data),
NOT `bom_unified`. See bom.py's module docstring for the full history and
live-confirmed real-data evidence (2,411 real non-KTB rows already have this
structure; the owner-supplied example below matches it).
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from sync import bom
from sync.bom import (
    build_style_season, parse_bom_segments, extract_enrichment_fields,
    to_wip_fields, segment_key, is_unenriched, build_target_segments,
    plan_style_enrichment, PLACEHOLDER_FABRIC_GROUP, RowAction,
    WIP_FIELD_FABRIC_GROUP, WIP_FIELD_PLACEMENT, WIP_FIELD_MILL_FABRIC_ARTICLE,
    WIP_FIELD_CONTENT,
    build_insert_row_payload, INSERT_EXCLUDE_COLS, compute_non_writable_cols,
)

_failures = []


def check(cond, msg):
    if cond:
        print(f"  ✅ {msg}")
    else:
        print(f"  ❌ {msg}")
        _failures.append(msg)


def bom_table(column_header, data, extra_tables=None, table_type="BOM", seq=2):
    """Build a full custom_fields dict wrapping one BOM Table entry (plus any
    extra_tables, e.g. POM/Colorway, that should be ignored)."""
    tables = list(extra_tables or [])
    tables.append({"Seq": seq, "Type": table_type, "Name": "", "Header": {},
                    "ColumnHeader": column_header, "Data": data})
    return {"xts_data": {"ACTION_CODE": "NEW_TECHPACK",
                          "TECH_PACK_EXTRACTION": {"Table": tables},
                          "DATE": ""}}


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
# Owner-supplied example (2026-09-09), live-shape-confirmed against 2,411
# real non-KTB rows (e.g. "Etam Lingerie") already using this structure.
COLUMN_HEADER = [
    "**BomHeader", "**MaterialCategory", "**MaterialCode", "**MaterialType",
    "**MaterialDescription", "**Quantity", "**MaterialContent",
    "**MaterialConstruction", "**MaterialCuttableWidth", "**Placement",
    "**Size", "**RefNo", "**Section", "**InternalName", "**SupplierName",
    "**SupplierCop", "**SupplierRefNo", "**MainMaterial", "**UOM",
    "**CountryOfOrigin", "**CostFob", "**CostCif", "**WeightBeforeWash",
    "**WeightAfterWash", "**WeightUOM", "**Comments",
    {"Colorway": ["CAD-12314 RedGingham", "12-1505 TCX Rose Bisque"]},
    {"Color": ["CAD-12314 RedGingham", "12-1505 TCX Rose Bisque"]},
    "COMPONENT", "MATERIAL/FABRIC WEIGHT",
]
DATA_ROWS_00023 = [
    ["SLEEVELESS SHIRT", "Main Fabric", "LF-BD26-000002--SH", "Sheeting",
     "WV-0003", "", "Cotton 100%", "", "", "BODICE", "", "", "", "",
     "AKIJ TEXTILE MILLS LTD.", "", "WV-0003", "", "", "", "", "", "", "",
     "", "", {"Colorway": ["CAD-12314 RedGingham", "12-1505 TCX Rose Bisque"]},
     {"Color": ["DTM", "DTM"]}, "", ""],
    ["SLEEVELESS SHIRT", "Fabric", "LF-BD26-000004--PN", "Poplin", "WV-0061",
     "", "Cotton 100%", "", "", "HEM", "", "", "", "", "BEPRODUCT VENDOR",
     "", "WV-0061", "", "", "", "", "", "", "", "", "",
     {"Colorway": ["CAD-12314 RedGingham", "12-1505 TCX Rose Bisque"]},
     {"Color": ["DTM", "DTM"]}, "", ""],
    ["SLEEVELESS SHIRT", "Fabric", "LF-CN26-001537--JE", "Jersey",
     "WG24-01706", "", "Cotton 65%, Modal 28%, Spandex 7%", "", "", "LINING",
     "", "", "", "", "WINGS GLORY CO., LIMITED", "", "WG24-01706", "", "",
     "", "", "", "", "", "", "",
     {"Colorway": ["CAD-12314 RedGingham", "12-1505 TCX Rose Bisque"]},
     {"Color": ["DTM", "DTM"]}, "", ""],
]
REAL_CUSTOM_FIELDS_KTB00023 = bom_table(COLUMN_HEADER, DATA_ROWS_00023)

print("\n[2] parse_bom_segments() — Main Fabric + 2 Fabric segments (owner example)")
segs = parse_bom_segments(REAL_CUSTOM_FIELDS_KTB00023)
check(segs.main_fabric is not None and segs.main_fabric["material_name"] == "WV-0003",
      "Main Fabric segment extracted, mill article # <- **SupplierRefNo")
check(segs.main_fabric["content"] == "Cotton 100%",
      "Main Fabric content <- **MaterialContent")
check(segs.main_fabric["placement"] == "BODICE", "Main Fabric placement <- **Placement")
check(len(segs.fabric_list) == 2, "exactly two Fabric segments extracted")
check(segs.fabric_list[0]["material_name"] == "WV-0061"
      and segs.fabric_list[1]["material_name"] == "WG24-01706",
      "Fabric segments in document order, each with its own SupplierRefNo")
check(segs.fabric_list[1]["content"] == "Cotton 65%, Modal 28%, Spandex 7%",
      "a Fabric segment's own MaterialContent is captured correctly")
check(not segs.is_empty(), "non-empty when segments found")

print("\n[3] parse_bom_segments() — path/shape edge cases (all -> empty, never raise)")
check(parse_bom_segments(None).is_empty(), "None -> empty")
check(parse_bom_segments("").is_empty(), "empty string -> empty")
check(parse_bom_segments("not json{{{").is_empty(), "malformed JSON -> empty")
check(parse_bom_segments({}).is_empty(), "empty dict -> empty")
check(parse_bom_segments({"xts_data": {}}).is_empty(),
      "xts_data present but no TECH_PACK_EXTRACTION key -> empty "
      "(this WAS wrongly believed to be every KTB style's live state -- "
      "corrected 2026-09-09: all 16 actually DO have this data; kept as a "
      "pure structural edge case)")
check(parse_bom_segments({"xts_data": {"TECH_PACK_EXTRACTION": {}}}).is_empty(),
      "TECH_PACK_EXTRACTION present but no Table key -> empty")
check(parse_bom_segments({"xts_data": {"TECH_PACK_EXTRACTION": {"Table": []}}}).is_empty(),
      "empty Table array -> empty")
check(parse_bom_segments({"xts_data": {"TECH_PACK_EXTRACTION": {"Table": [
    {"Seq": 1, "Type": "POM", "ColumnHeader": [], "Data": []},
    {"Seq": 3, "Type": "Colorway", "ColumnHeader": [], "Data": []},
]}}}).is_empty(), "Table present but no Type=='BOM' entry -> empty")
check(parse_bom_segments(json.dumps(REAL_CUSTOM_FIELDS_KTB00023)).main_fabric is not None,
      "accepts a JSON STRING (not just an already-parsed dict) too")
check(parse_bom_segments([1, 2, 3]).is_empty(), "a bare list (not a dict) -> empty")
check(parse_bom_segments({"xts_data": "not a dict"}).is_empty(),
      "xts_data present but not a dict -> empty")

print("\n[4] parse_bom_segments() — BOM table with no interesting MaterialCategory rows")
no_interesting = bom_table(COLUMN_HEADER, [
    ["SLEEVELESS SHIRT", "", "", "", "AOP", "", "", "", "", "", "", "", "",
     "", "", "", "", "", "", "", "", "", "", "", "", "", {"Colorway": []},
     {"Color": []}, "", ""],
])
check(parse_bom_segments(no_interesting).is_empty(),
      "blank/other MaterialCategory values (e.g. real Etam Lingerie rows) -> empty")

print("\n[5] parse_bom_segments() — multiple Type=='BOM' table entries are concatenated")
extra_bom_data = [
    ["SLEEVELESS SHIRT", "Fabric", "LF-XX", "X", "WV-9999", "", "50% Wool",
     "", "", "COLLAR", "", "", "", "", "", "", "WV-9999", "", "", "", "",
     "", "", "", "", "", {"Colorway": []}, {"Color": []}, "", ""],
]
multi_bom_tables = {"xts_data": {"TECH_PACK_EXTRACTION": {"Table": [
    {"Seq": 2, "Type": "BOM", "ColumnHeader": COLUMN_HEADER, "Data": DATA_ROWS_00023},
    {"Seq": 4, "Type": "BOM", "ColumnHeader": COLUMN_HEADER, "Data": extra_bom_data},
]}}}
segs_multi = parse_bom_segments(multi_bom_tables)
check(len(segs_multi.fabric_list) == 3,
      "Fabric segments from BOTH Type=='BOM' table entries are collected (2 + 1)")

print("\n[6] parse_bom_segments() — missing ColumnHeader entry for one of our 4 targets")
partial_cols = ["**MaterialCategory", "**Placement"]  # no MaterialContent/SupplierRefNo at all
partial_data = [["Main Fabric", "BODICE"]]
segs_partial = parse_bom_segments(bom_table(partial_cols, partial_data))
check(segs_partial.main_fabric == {
    "bom_detail_name": "Main Fabric", "material_name": None,
    "content": None, "placement": "BODICE",
}, "missing target columns resolve to None, never crash")

print("\n[7] parse_bom_segments() — duplicate Main Fabric row, first wins")
dup_data = [
    ["H", "Main Fabric", "C1", "T", "D", "Q", "First Content", "", "",
     "P1", "", "", "", "", "", "", "REF-FIRST", "", "", "", "", "", "", "",
     "", "", {"Colorway": []}, {"Color": []}, "", ""],
    ["H", "Main Fabric", "C2", "T", "D", "Q", "Second Content", "", "",
     "P2", "", "", "", "", "", "", "REF-SECOND", "", "", "", "", "", "", "",
     "", "", {"Colorway": []}, {"Color": []}, "", ""],
]
segs_dup = parse_bom_segments(bom_table(COLUMN_HEADER, dup_data))
check(segs_dup.main_fabric["material_name"] == "REF-FIRST",
      "first occurrence of a repeated Main Fabric wins")

# ---------------------------------------------------------------------------
print("\n[8] extract_enrichment_fields() / to_wip_fields()")
fields = extract_enrichment_fields(segs.main_fabric)
check(fields == {
    "fabric_group": "Main Fabric",
    "placement": "BODICE",
    "mill_fabric_article": "WV-0003",
    "content": "Cotton 100%",
}, "fabric_group<-MaterialCategory, placement<-Placement, "
   "mill_fabric_article<-SupplierRefNo, content<-MaterialContent")

wip_fields = to_wip_fields(fields)
check(wip_fields == {
    WIP_FIELD_FABRIC_GROUP: "Main Fabric",
    WIP_FIELD_PLACEMENT: "BODICE",
    WIP_FIELD_MILL_FABRIC_ARTICLE: "WV-0003",
    WIP_FIELD_CONTENT: "Cotton 100%",
}, "maps to the exact live-confirmed raw DTC field names, INCLUDING Content "
   "(reinstated 2026-09-09)")
check(WIP_FIELD_FABRIC_GROUP == "Fabric Group"
      and WIP_FIELD_PLACEMENT == "Placement"
      and WIP_FIELD_MILL_FABRIC_ARTICLE == "Mill Fabric Article #"
      and WIP_FIELD_CONTENT == "Content",
      "raw field name constants match the live WIP view definition")

# ---------------------------------------------------------------------------
print("\n[9] segment_key() / is_unenriched() -- unchanged (Content/Placement excluded from key)")
check(segment_key({"fabric_group": "Main Fabric", "mill_fabric_article": "WV-0003"})
      == ("Main Fabric", "WV-0003"), "normal pair -> normalized tuple")
check(segment_key({"fabric_group": " Main Fabric ", "mill_fabric_article": "WV-0003"})
      == ("Main Fabric", "WV-0003"), "whitespace stripped")
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
print("\n[10] build_target_segments()")
targets = build_target_segments(REAL_CUSTOM_FIELDS_KTB00023)
check(targets is not None and len(targets) == 3, "Main Fabric + 2 Fabric segments -> 3 targets")
check(targets[0]["fabric_group"] == "Main Fabric", "target[0] is always Main Fabric")
check(targets[1]["mill_fabric_article"] == "WV-0061" and targets[2]["mill_fabric_article"] == "WG24-01706",
      "targets[1:] are the Fabric segments, in order")

check(build_target_segments(None) is None, "blank custom_fields -> None (nothing to upsert)")
check(build_target_segments({"xts_data": {}}) is None,
      "no TECH_PACK_EXTRACTION at all -> None (a pure structural edge case -- "
      "NOT the live KTB state; all 16 KTB styles actually have this "
      "populated, corrected 2026-09-09)")
check(build_target_segments(no_interesting) is None,
      "BOM table present but no Main Fabric/Fabric rows -> None")
fabric_only = bom_table(COLUMN_HEADER, [DATA_ROWS_00023[1]])  # only the first Fabric row
check(build_target_segments(fabric_only) is None,
      "Fabric segment(s) present but NO Main Fabric -> None (never just insert-only)")

# ---------------------------------------------------------------------------
print("\n[11] plan_style_enrichment() — full integration (upsert semantics)")

print("  [11a] no existing WIP rows -> no-op")
check(plan_style_enrichment([], REAL_CUSTOM_FIELDS_KTB00023) == [], "empty existing_rows -> []")

SINGLE_MAIN_ONLY = bom_table(COLUMN_HEADER, [DATA_ROWS_00023[0]])  # just the Main Fabric row

print("  [11b] first-time enrichment: single row, Main Fabric only -> one full UPDATE (incl. Content)")
actions = plan_style_enrichment(
    existing_rows=[{"row_id": "r1", "fabric_group": PLACEHOLDER_FABRIC_GROUP,
                     "mill_fabric_article": None, "placement": None, "content": None}],
    custom_fields=SINGLE_MAIN_ONLY,
)
check(len(actions) == 1, "exactly one action")
check(actions[0].kind == "update" and actions[0].row_id == "r1",
      "single UPDATE targeting the existing row_id")
check(actions[0].wip_fields[WIP_FIELD_FABRIC_GROUP] == "Main Fabric",
      "UPDATE's Fabric Group is the literal MaterialCategory 'Main Fabric'")
check(actions[0].wip_fields[WIP_FIELD_MILL_FABRIC_ARTICLE] == "WV-0003",
      "first-time enrichment writes the FULL field set including Mill Fabric Article #")
check(actions[0].wip_fields[WIP_FIELD_CONTENT] == "Cotton 100%",
      "first-time enrichment writes Content too (reinstated 2026-09-09)")

print("  [11c] first-time enrichment: single row, Main Fabric + 2 Fabric segments -> 1 UPDATE + 2 INSERTs")
actions = plan_style_enrichment(
    existing_rows=[{"row_id": "r1", "fabric_group": PLACEHOLDER_FABRIC_GROUP,
                     "mill_fabric_article": None, "placement": None, "content": None,
                     "color": "RedGingham"}],
    custom_fields=REAL_CUSTOM_FIELDS_KTB00023,
)
check(len(actions) == 3, "exactly three actions (1 update + 2 inserts)")
kinds = sorted(a.kind for a in actions)
check(kinds == ["insert", "insert", "update"], "one update, two inserts")
update_action = next(a for a in actions if a.kind == "update")
insert_actions = [a for a in actions if a.kind == "insert"]
check(update_action.wip_fields[WIP_FIELD_FABRIC_GROUP] == "Main Fabric",
      "the UPDATE's Fabric Group = 'Main Fabric'")
check({a.wip_fields[WIP_FIELD_MILL_FABRIC_ARTICLE] for a in insert_actions} == {"WV-0061", "WG24-01706"},
      "the two INSERTs carry each Fabric segment's own SupplierRefNo")
check(all(a.wip_fields.get(WIP_FIELD_CONTENT) for a in insert_actions),
      "each INSERT also carries its own Content value")
check(all(a.base_row["color"] == "RedGingham" for a in insert_actions),
      "each INSERT's base_row is the full original row dict")

print("  [11d] multi-row style (colorways), first-time -> N updates + N*M inserts")
multi_fabric_cf = bom_table(COLUMN_HEADER, [
    DATA_ROWS_00023[0],  # Main Fabric
    DATA_ROWS_00023[1],  # Fabric #1
    DATA_ROWS_00023[2],  # Fabric #2
])
actions = plan_style_enrichment(
    existing_rows=[
        {"row_id": "r1", "fabric_group": PLACEHOLDER_FABRIC_GROUP, "mill_fabric_article": None,
         "placement": None, "content": None, "color": "Black"},
        {"row_id": "r2", "fabric_group": PLACEHOLDER_FABRIC_GROUP, "mill_fabric_article": None,
         "placement": None, "content": None, "color": "White"},
    ],
    custom_fields=multi_fabric_cf,
)
check(len(actions) == 6, "2 rows x (1 update + 2 inserts) = 6 actions")
check(sorted(a.kind for a in actions).count("update") == 2, "two updates (one per row)")
check(sorted(a.kind for a in actions).count("insert") == 4, "four inserts (2 rows x 2 Fabric segments)")

print("  [11e] custom_fields has no BOM data this run -> ZERO actions, never revert existing enrichment")
already_enriched_row = {"row_id": "r1", "fabric_group": "Main Fabric",
                         "mill_fabric_article": "WV-0003", "placement": "BODICE",
                         "content": "Cotton 100%"}
check(plan_style_enrichment([already_enriched_row], None) == [],
      "custom_fields=None -> no-op, row untouched")
check(plan_style_enrichment([already_enriched_row], {"xts_data": {}}) == [],
      "no TECH_PACK_EXTRACTION at all (the CURRENT live state for every KTB "
      "test style) -> no-op, row untouched")
check(plan_style_enrichment([already_enriched_row], no_interesting) == [],
      "BOM table present but no Main Fabric/Fabric segments -> no-op")

print("  [11f] Fabric segment(s) present but NO Main Fabric -> ZERO actions (not insert-only)")
actions = plan_style_enrichment(
    [{"row_id": "r1", "fabric_group": PLACEHOLDER_FABRIC_GROUP, "mill_fabric_article": None,
      "placement": None, "content": None}],
    fabric_only,
)
check(actions == [], "no Main Fabric -> zero actions at all, even for a placeholder row")

print("  [11g] upsert: row already matches by (Fabric Group, Mill Fabric Article #) -> "
      "Placement AND Content fixed independently")
row_matches_main = {"row_id": "r1", "fabric_group": "Main Fabric",
                     "mill_fabric_article": "WV-0003", "placement": "WRONG PLACEMENT",
                     "content": "Wrong Content"}
actions = plan_style_enrichment([row_matches_main], SINGLE_MAIN_ONLY)
check(len(actions) == 1 and actions[0].kind == "update", "exactly one update")
check(actions[0].wip_fields == {WIP_FIELD_PLACEMENT: "BODICE", WIP_FIELD_CONTENT: "Cotton 100%"},
      "BOTH Placement and Content are fixed -- Fabric Group/Mill Fabric Article # "
      "never re-written once matched")

print("  [11h] upsert: only Content drifted (Placement already correct) -> Content-only update")
row_content_only_wrong = {"row_id": "r1", "fabric_group": "Main Fabric",
                           "mill_fabric_article": "WV-0003", "placement": "BODICE",
                           "content": "Stale Content"}
actions = plan_style_enrichment([row_content_only_wrong], SINGLE_MAIN_ONLY)
check(len(actions) == 1 and actions[0].wip_fields == {WIP_FIELD_CONTENT: "Cotton 100%"},
      "ONLY Content is included in the PATCH when only Content changed (lean PATCH body)")

print("  [11i] upsert: row already matches AND both fields already correct -> no-op (idempotent)")
row_fully_correct = {"row_id": "r1", "fabric_group": "Main Fabric",
                      "mill_fabric_article": "WV-0003", "placement": "BODICE",
                      "content": "Cotton 100%"}
check(plan_style_enrichment([row_fully_correct], SINGLE_MAIN_ONLY) == [],
      "already fully matching -> no PATCH issued at all")

print("  [11j] never-revert: row holds a real, unrecognized (Fabric Group, Article#) combo -> untouched")
row_vanished_segment = {"row_id": "r1", "fabric_group": "Fabric",
                         "mill_fabric_article": "OLD-ARTICLE-NO-LONGER-IN-BOM",
                         "placement": "yoke", "content": "Old Content"}
check(plan_style_enrichment([row_vanished_segment], SINGLE_MAIN_ONLY) == [],
      "row's real data isn't Main Fabric's key and isn't unenriched -> left completely untouched")

print("  [11k] never-insert-duplicate: a Fabric segment already represented -> no re-insert")
existing_with_fabric_segments = [
    {"row_id": "r1", "fabric_group": "Main Fabric", "mill_fabric_article": "WV-0003",
     "placement": "BODICE", "content": "Cotton 100%"},
    {"row_id": "r2", "fabric_group": "Fabric", "mill_fabric_article": "WV-0061",
     "placement": "HEM", "content": "Cotton 100%"},
    {"row_id": "r3", "fabric_group": "Fabric", "mill_fabric_article": "WG24-01706",
     "placement": "LINING", "content": "Cotton 65%, Modal 28%, Spandex 7%"},
]
check(plan_style_enrichment(existing_with_fabric_segments, REAL_CUSTOM_FIELDS_KTB00023) == [],
      "all three segments already correctly represented -> zero actions, no duplicate insert")

print("  [11l] blank Mill Fabric Article # backfill (added 2026-09-10 -- live KTB-00025/legacy 112358013 case)")
# Real-world trigger: a row was first-enriched while **SupplierRefNo was still
# blank at the source (Content/Placement/Fabric Group got set, but Mill Fabric
# Article # had nothing to fill in); the source was later updated with real
# SupplierRefNo values. Before the fix: these rows were permanently stuck with
# a blank Mill Fabric Article #, AND every real segment got wastefully
# re-inserted as a brand-new duplicate row on top (since the row's blank-article
# key never matched anything). After the fix: blank-article rows are backfilled
# in place; only genuinely-unrepresented segments still trigger inserts.
CF_00025 = bom_table(
    ["**MaterialCategory", "**SupplierRefNo", "**MaterialContent", "**Placement"],
    [
        ["Main Fabric", "WV-0003", "Cotton 100%", "BODICE"],
        ["Fabric", "LTCL6080", "Polyester 96%, Spandex 4%", "LINING"],
        ["Fabric", "fb01art", "Rayon 70%, Tencel 30%", "HEM"],
    ],
)
existing_00025 = [
    {"row_id": "r1", "fabric_group": "Main Fabric", "mill_fabric_article": None,
     "placement": "BODICE", "content": "Cotton 100%"},
    {"row_id": "r2", "fabric_group": "Main Fabric", "mill_fabric_article": None,
     "placement": "BODICE", "content": "Cotton 100%"},
    {"row_id": "r3", "fabric_group": "Fabric", "mill_fabric_article": None,
     "placement": "LINING", "content": "Polyester 96%, Spandex 4%"},
    {"row_id": "r4", "fabric_group": "Fabric", "mill_fabric_article": None,
     "placement": "LINING", "content": "Polyester 96%, Spandex 4%"},
]
actions_00025 = plan_style_enrichment(existing_00025, CF_00025)
updates_00025 = {a.row_id: a.wip_fields for a in actions_00025 if a.kind == "update"}
inserts_00025 = [a for a in actions_00025 if a.kind == "insert"]
check(updates_00025 == {
    "r1": {WIP_FIELD_MILL_FABRIC_ARTICLE: "WV-0003"},
    "r2": {WIP_FIELD_MILL_FABRIC_ARTICLE: "WV-0003"},
    "r3": {WIP_FIELD_MILL_FABRIC_ARTICLE: "LTCL6080"},
    "r4": {WIP_FIELD_MILL_FABRIC_ARTICLE: "LTCL6080"},
}, "all 4 blank-article rows backfilled in place -- Main Fabric rows unambiguous "
   "(single target), Fabric/LINING rows disambiguated by Placement")
check(len(inserts_00025) == 4 and all(
    a.wip_fields[WIP_FIELD_MILL_FABRIC_ARTICLE] == "fb01art" for a in inserts_00025),
    "ONLY the genuinely-unrepresented 'fb01art'/HEM segment triggers inserts "
    "(one per existing row) -- the LTCL6080/LINING segment is NOT also "
    "re-inserted now that it's been correctly backfilled instead")

print("  [11m] blank Mill Fabric Article # backfill -- ambiguous case (2+ candidates share Fabric Group AND Placement) -> no guess")
CF_ambiguous = bom_table(
    ["**MaterialCategory", "**SupplierRefNo", "**MaterialContent", "**Placement"],
    [
        ["Main Fabric", "MN-001", "Content A", "BODICE"],
        ["Fabric", "FB-001", "Content B", "LINING"],
        ["Fabric", "FB-002", "Content C", "LINING"],  # same Fabric Group AND Placement as FB-001
    ],
)
ambiguous_row = [{"row_id": "r1", "fabric_group": "Fabric", "mill_fabric_article": None,
                   "placement": "LINING", "content": "Content B"}]
actions_ambiguous = plan_style_enrichment(ambiguous_row, CF_ambiguous)
check(all(a.kind != "update" for a in actions_ambiguous),
      "two Fabric candidates share BOTH Fabric Group and Placement -- "
      "genuinely ambiguous, so NO backfill UPDATE is guessed for r1 (never "
      "guess wrong); r1 itself is left with a blank Mill Fabric Article #")
check(sorted(a.wip_fields[WIP_FIELD_MILL_FABRIC_ARTICLE] for a in actions_ambiguous
             if a.kind == "insert") == ["FB-001", "FB-002"],
      "both genuinely-unrepresented Fabric segments still correctly fan out as "
      "inserts (pre-existing behavior, unrelated to the backfill fix) -- an "
      "ambiguous backfill just means r1 doesn't claim either target first")

print("  [11n] blank Mill Fabric Article # backfill -- one-way only (never overwrites a REAL, different article #)")
real_article_row = [{"row_id": "r1", "fabric_group": "Fabric", "mill_fabric_article": "OLD-REAL-VALUE",
                      "placement": "LINING", "content": "Polyester 96%, Spandex 4%"}]
actions_real = plan_style_enrichment(real_article_row, CF_00025)
check(all(a.kind != "update" for a in actions_real),
      "a row with a REAL (non-blank) but non-matching Mill Fabric Article # "
      "never gets an UPDATE from the backfill path -- only a currently-BLANK "
      "article # is eligible for backfill, exactly as before for real-value "
      "mismatches (the row itself is left completely untouched, matching the "
      "pre-existing 'unrecognized real value' rule)")

print("\n[11o] segment coverage is scoped PER COLOR (fixed 2026-09-10 -- live "
      "KTB-00029/LF Style# LFBP-1WTP0002 case: 2 colors, BOM = Main Fabric + 2 "
      "Fabric; expected 2x3=6 DTC rows, only got 4 -- the 2nd color stuck at 1)")
CF_00029 = bom_table(
    ["**MaterialCategory", "**SupplierRefNo", "**MaterialContent", "**Placement"],
    [
        ["Main Fabric", "LTCL6080", "Polyester 96%, Spandex 4%", ""],
        ["Fabric", "WV-0064", "Cotton 97%, Spandex 3%", ""],
        ["Fabric", "WV-0047", "Polyester 100%", ""],
    ],
)
# "Earthy Hours" already fully enriched (3 rows); "Early Hours" only has its
# Main Fabric row -- exactly the real live pre-fix state.
multi_color_rows = [
    {"row_id": "earthy_main", "color": "Earthy Hours", "fabric_group": "Main Fabric",
     "mill_fabric_article": "LTCL6080", "placement": None, "content": "Polyester 96%, Spandex 4%"},
    {"row_id": "earthy_fab1", "color": "Earthy Hours", "fabric_group": "Fabric",
     "mill_fabric_article": "WV-0064", "placement": None, "content": "Cotton 97%, Spandex 3%"},
    {"row_id": "earthy_fab2", "color": "Earthy Hours", "fabric_group": "Fabric",
     "mill_fabric_article": "WV-0047", "placement": None, "content": "Polyester 100%"},
    {"row_id": "early_main", "color": "Early Hours", "fabric_group": "Main Fabric",
     "mill_fabric_article": "LTCL6080", "placement": None, "content": "Polyester 96%, Spandex 4%"},
]
actions_multi_color = plan_style_enrichment(multi_color_rows, CF_00029)
check(actions_multi_color == [
    RowAction(kind="insert",
              base_row=multi_color_rows[3],
              wip_fields={WIP_FIELD_FABRIC_GROUP: "Fabric", WIP_FIELD_PLACEMENT: "",
                          WIP_FIELD_MILL_FABRIC_ARTICLE: "WV-0064", WIP_FIELD_CONTENT: "Cotton 97%, Spandex 3%"}),
    RowAction(kind="insert",
              base_row=multi_color_rows[3],
              wip_fields={WIP_FIELD_FABRIC_GROUP: "Fabric", WIP_FIELD_PLACEMENT: "",
                          WIP_FIELD_MILL_FABRIC_ARTICLE: "WV-0047", WIP_FIELD_CONTENT: "Polyester 100%"}),
], "'Earthy Hours' already fully covered (3/3 matched exactly, zero actions "
   "for it) -- 'Early Hours' correctly gets its OWN 2 missing Fabric segment "
   "inserts, even though 'Earthy Hours' already 'claimed' those same two "
   "segment keys; before the fix, those global claims would have wrongly "
   "suppressed inserting them for 'Early Hours' too")

print("  [11p] rows without a color_key value (or all-one-color) collapse to a single "
      "implicit group -- identical to pre-2026-09-10 behavior")
no_color_rows = [{k: v for k, v in r.items() if k != "color"} for r in multi_color_rows]
check(plan_style_enrichment(no_color_rows, CF_00029) == [],
      "WITHOUT color info, all 4 rows collapse into ONE group -- both Fabric "
      "segments are already represented somewhere in that single group, so "
      "(matching pre-fix behavior for callers that don't track color) no "
      "inserts happen at all -- this is exactly the bug being fixed, "
      "confirming color_key is what unlocks the correct per-color behavior")

print("\n[11q] blank-vs-blank Placement/Content never triggers a spurious PATCH "
      "(fixed 2026-09-10 alongside the color-scoping fix)")
blank_placement_row = [{"row_id": "r1", "color": "C1", "fabric_group": "Main Fabric",
                         "mill_fabric_article": "LTCL6080", "placement": None,
                         "content": "Polyester 96%, Spandex 4%"}]
actions_blank_pl = plan_style_enrichment(blank_placement_row, CF_00029)
check(not any(a.kind == "update" for a in actions_blank_pl),
      "DTC's current Placement=None vs. the source's own blank Placement=''  "
      "-- both blank, so NOT treated as a diff -- no spurious update PATCH "
      "for r1 itself (it's otherwise fully matched and up to date); the 2 "
      "Fabric segments still correctly insert since this style has only the "
      "one Main Fabric row so far")

print("\n[11r] REAL Content is NEVER overwritten by a BLANK target value, "
      "even on an exact-key match (added 2026-09-10, owner spec: 'make sure "
      "no steps incidentally overwrite <blank> on content field') -- fixes a "
      "live-confirmed real bug: KTB-00024/KTB-00026's Main Fabric rows carry "
      "a real manually-entered Content value in DTC, while their source's "
      "own **MaterialContent for that exact segment is genuinely blank")
CF_BLANK_MAIN_CONTENT = bom_table(
    ["**MaterialCategory", "**SupplierRefNo", "**MaterialContent", "**Placement"],
    [
        ["Main Fabric", "WV-0063", "", "BODICE"],  # blank content at the source -- real live shape
        ["Fabric", "WV-0061", "Cotton 100%", "HEM"],
    ],
)
row_with_real_content = [{"row_id": "r1", "color": "Black", "fabric_group": "Main Fabric",
                           "mill_fabric_article": "WV-0063", "placement": None,
                           "content": "100% test from ML"}]  # REAL value, matches live DTC exactly
actions_real_content = plan_style_enrichment(row_with_real_content, CF_BLANK_MAIN_CONTENT)
update_for_r1 = next((a for a in actions_real_content if a.kind == "update" and a.row_id == "r1"), None)
check(update_for_r1 is not None and update_for_r1.wip_fields == {WIP_FIELD_PLACEMENT: "BODICE"},
      "r1 exactly matches (Fabric Group, Mill Fabric Article #) and gets its "
      "blank Placement legitimately filled in ('BODICE'), but 'Content' is "
      "COMPLETELY ABSENT from the PATCH -- the source's blank content is "
      "never pushed, so the real 'Content' value already in DTC is left "
      "completely untouched")

print("\n[11s] Same one-way guard applies to the FIRST-TIME ENRICHMENT branch too "
      "-- a still-unenriched row (blank Fabric Group) can independently "
      "already carry a real Content/Placement value")
CF_BLANK_MAIN_CONTENT_AND_PLACEMENT = bom_table(
    ["**MaterialCategory", "**SupplierRefNo", "**MaterialContent", "**Placement"],
    [
        ["Main Fabric", "WV-0063", "", ""],  # BOTH content and placement blank at the source
        ["Fabric", "WV-0061", "Cotton 100%", "HEM"],
    ],
)
unenriched_row_with_real_content = [{"row_id": "r1", "fabric_group": PLACEHOLDER_FABRIC_GROUP,
                                      "mill_fabric_article": None, "placement": "PRE-SET PLACEMENT",
                                      "content": "PRE-SET REAL CONTENT"}]
actions_unenriched = plan_style_enrichment(unenriched_row_with_real_content, CF_BLANK_MAIN_CONTENT_AND_PLACEMENT)
update_action = next(a for a in actions_unenriched if a.kind == "update")
check(update_action.wip_fields.get(WIP_FIELD_FABRIC_GROUP) == "Main Fabric"
      and update_action.wip_fields.get(WIP_FIELD_MILL_FABRIC_ARTICLE) == "WV-0063",
      "Fabric Group / Mill Fabric Article # are still written normally -- "
      "that's the whole point of first-time enrichment")
check(WIP_FIELD_CONTENT not in update_action.wip_fields
      and WIP_FIELD_PLACEMENT not in update_action.wip_fields,
      "but Content/Placement are BOTH excluded from the PATCH -- the row's "
      "pre-existing real values for those two fields are left completely "
      "untouched, since the target's own Placement/Content are BOTH blank "
      "at the source for this Main Fabric segment")

print("\n[11t] ...but a NON-BLANK target Placement/Content in the first-time "
      "branch is still written normally alongside the blank-guarded one")
_actions_11t = plan_style_enrichment(
    [{"row_id": "r1", "fabric_group": PLACEHOLDER_FABRIC_GROUP, "mill_fabric_article": None,
      "placement": "PRE-SET PLACEMENT", "content": "PRE-SET REAL CONTENT"}],
    CF_BLANK_MAIN_CONTENT)
_update_11t = next(a for a in _actions_11t if a.kind == "update")
check(_update_11t.wip_fields == {WIP_FIELD_FABRIC_GROUP: "Main Fabric",
                                  WIP_FIELD_PLACEMENT: "BODICE",  # non-blank target -> written normally
                                  WIP_FIELD_MILL_FABRIC_ARTICLE: "WV-0063"},
      "Placement legitimately updates to 'BODICE' (non-blank target) while "
      "Content stays excluded (blank target) -- the guard is per-field, not "
      "all-or-nothing")

# ---------------------------------------------------------------------------
print("\n[12] build_insert_row_payload() — Style Image must never be copied forward")
base_fields = {
    "rowId": "r1", "rowIndex": 3, "BP Style#": "KTB-00023",
    "Color / Wash": "Indigo", "Style Image": "https://cdn.example/img.jpg",
    "Fabric Group": "MAIN MATERIAL CONTENT",
}
wip = {"Fabric Group": "Fabric", "Placement": "yoke", "Mill Fabric Article #": "FB-999",
       "Content": "50% Wool, 50% Nylon"}
payload = build_insert_row_payload(base_fields, wip)
check("Style Image" not in payload,
      "Style Image excluded from INSERT payload (DTC rejects image data on INSERT)")
check("rowId" not in payload and "rowIndex" not in payload,
      "rowId/rowIndex identity fields excluded from INSERT payload")
check(payload["BP Style#"] == "KTB-00023" and payload["Color / Wash"] == "Indigo",
      "non-excluded original fields still copied forward")
check(payload["Fabric Group"] == "Fabric" and payload["Placement"] == "yoke"
      and payload["Mill Fabric Article #"] == "FB-999" and payload["Content"] == "50% Wool, 50% Nylon",
      "wip_fields override applied on top of the copied row, including Content")
check(INSERT_EXCLUDE_COLS == frozenset({"rowId", "rowIndex", "Style Image"}),
      "INSERT_EXCLUDE_COLS is exactly the identity fields + Style Image")

# ---------------------------------------------------------------------------
print("\n[13] compute_non_writable_cols() — isReadOnly is unreliable; type/formula are the real signals")
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
