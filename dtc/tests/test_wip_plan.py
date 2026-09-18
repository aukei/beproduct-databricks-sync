#!/usr/bin/env python3
"""
Unit tests for sync/wip_plan.py -- the v2 single-write-window composition layer.

Pure Python: no Spark, no network, no dbutils.

    python3 dtc/tests/test_wip_plan.py

What these tests are actually protecting
----------------------------------------
v2 merges three previously-separate DTC write passes into one. That removes the
natural blast radius each pass used to have, so the properties that used to be
guaranteed by "these are separate tasks" now have to be guaranteed here:

  [1]  zero diff  => zero PATCH calls          (the cadence-critical invariant)
  [2]  allow-list is never violated             (ground rule #6)
  [3]  a failing contribution degrades, never aborts
  [4]  material owns its 4 columns; style only default-fills them
  [5]  duty lands on the RIGHT physical row (style x color x material)
  [6]  rowIndex is unique across style inserts AND material fan-out
  [7]  provenance is recorded for every planned field
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))

from sync import bom, duty, phase1, wip_plan  # noqa: E402

FG = bom.WIP_FIELD_FABRIC_GROUP           # "Fabric Group"
PL = bom.WIP_FIELD_PLACEMENT              # "Placement"
MA = bom.WIP_FIELD_MILL_FABRIC_ARTICLE    # "Mill Fabric Article #"
CT = bom.WIP_FIELD_CONTENT                # "Content"
STYLE_COL, COLOR_COL = phase1.MATCH_KEY_COLS

SCOPE = {"season_code": "FW26", "brand": "Wrangler"}

# A live view that contains every column we might legitimately write.
ALLOWED = set(wip_plan.allowed_patch_columns())

_passed = 0
_failed = []


def check(label, cond, detail=""):
    global _passed
    if cond:
        _passed += 1
        print(f"  ✓ {label}")
    else:
        _failed.append(label)
        print(f"  ✗ {label}")
        if detail:
            print(f"      {detail}")


def bp_row(style="KTB-1", color="Blue", **over):
    """
    A staging row (style x color x material grain).

    Keys are the STAGING column names `phase1.FIELD_MAPPING` reads -- note
    `description` / `product_status`, not the BeProduct field names.

    `fabric_group` / `mill_fabric_article` carry the "NO TPM BOM" sentinels
    because the TRANSFORM stages them as literal constants (v1:
    p1p7_beproduct_to_dtc_transform; v2: v2_build_wip_staging). phase1 has no
    BOM data of its own and never invents them -- so any fixture that omits
    them silently tests a different scenario than production.
    """
    row = {
        "bp_style_number": style,
        "color": color,
        "season_code": "FW26",
        "brand": "Wrangler",
        "product_status": "Development",
        "description": "A Shirt",
        "fabric_group": bom.DUMMY_FABRIC_GROUP,
        "mill_fabric_article": bom.DUMMY_FABRIC_ARTICLE,
    }
    row.update(over)
    return row


def dtc_row(row_id, row_index, style="KTB-1", color="Blue", **over):
    """A live DTC WIP row as `get_sheet()` returns it."""
    row = {
        "rowId": row_id,
        "rowIndex": row_index,
        STYLE_COL: style,
        COLOR_COL: color,
        "Product Status": "Development",
        "Style Description": "A Shirt",
        "Brand": "Wrangler",
    }
    row.update(over)
    return row


def bom_json(segments):
    """Minimal `custom_fields` payload with the given (category, article, placement, content)."""
    header = ["**MaterialCategory", "**SupplierRefNo", "**Placement", "**MaterialContent"]
    return {
        "xts_data": {
            "TECH_PACK_EXTRACTION": {
                "Table": [{"Type": "BOM", "ColumnHeader": header,
                           "Data": [list(s) for s in segments]}]
            }
        }
    }


MAIN_ONLY = bom_json([("Main Fabric", "WV-0003", "BODICE", "Cotton 100%")])
MAIN_PLUS_ONE = bom_json([
    ("Main Fabric", "WV-0003", "BODICE", "Cotton 100%"),
    ("Fabric", "WV-0061", "HEM", "Poly 100%"),
])


# ---------------------------------------------------------------------------
print("\n[1] THE invariant: a run that changes nothing writes nothing")
# ---------------------------------------------------------------------------

# A row that is already fully correct in DTC, with BOM data that also already
# matches. This is what EVERY run looks like on a quiet day -- and at 12
# runs/day it must produce no API calls at all, or users get invalidated for
# nothing.
settled = dtc_row("r1", 1, **{FG: "Main Fabric", MA: "WV-0003",
                              PL: "BODICE", CT: "Cotton 100%"})
p = wip_plan.compute_request_plan(
    SCOPE, [settled], [bp_row()],
    bom_by_style={"KTB-1": MAIN_ONLY}, allowed_cols=ALLOWED)

check("[1a] settled state => is_empty()", p.is_empty(), p.explain())
check("[1b] settled state => 0 PATCH calls", p.summary()["patch_calls"] == 0)
check("[1c] settled state => the row is a NOOP, not an empty UPDATE",
      len(p.noops) == 1 and not p.updates)
check("[1d] no allow-list violations", not p.violations, p.violations)

# Blank-vs-blank must not be a diff: DTC None vs source "" is the same absence.
blankish = dtc_row("r1", 1, **{FG: "Main Fabric", MA: "WV-0003", PL: None, CT: None})
p_blank = wip_plan.compute_request_plan(
    SCOPE, [blankish], [bp_row()],
    bom_by_style={"KTB-1": bom_json([("Main Fabric", "WV-0003", "", "")])},
    allowed_cols=ALLOWED)
check("[1e] blank(None) vs blank('') is not a diff", p_blank.is_empty(), p_blank.explain())

# Type mismatch must not be a diff either: costing_chart yields floats, DTC
# returns strings. v1's p9b2 used a plain `!=` and would have re-pushed forever.
typed = dtc_row("r1", 1, **{FG: "Main Fabric", MA: "WV-0003", PL: "BODICE",
                            CT: "Cotton 100%", "Main Factory Duty Rate (US)": "0.16"})
p_typed = wip_plan.compute_request_plan(
    SCOPE, [typed], [bp_row()], bom_by_style={"KTB-1": MAIN_ONLY},
    duty_rows=[{"bp_style_no": "KTB-1", "color_name": "Blue", "material_no": "WV-0003",
                "supplier_type": "Main", "duty_rate_us": 0.16}],
    allowed_cols=ALLOWED)
check("[1f] float 0.16 vs string '0.16' is not a diff", p_typed.is_empty(), p_typed.explain())

check("[1g] values_equal: None vs ''", wip_plan.values_equal(None, ""))
check("[1h] values_equal: blank vs real is a diff", not wip_plan.values_equal(None, "X"))
check("[1i] values_equal: 'n/a' normalizes to blank", wip_plan.values_equal("n/a", None))


# ---------------------------------------------------------------------------
print("\n[2] Allow-list enforcement (ground rule #6)")
# ---------------------------------------------------------------------------

allow = wip_plan.allowed_patch_columns()
check("[2a] Style Image is NEVER writable via sheetData",
      phase1.STYLE_IMAGE_COL not in allow)
check("[2b] the 4 material columns are in the allow-list",
      wip_plan.MATERIAL_OWNED_COLS <= allow)
check("[2c] per-slot HTS + duty columns are in the allow-list",
      set(duty.WIP_HTS_COL.values()) <= allow
      and set(duty.WIP_DUTY_COL["Main"].values()) <= allow)
check("[2d] per-slot tariff columns are in the allow-list (all 4 slots live "
      "2026-09-17; the WIP_TARIFF_COLS_LIVE switch was removed)",
      set(duty.WIP_TARIFF_COL.values()) <= allow
      and len(duty.WIP_TARIFF_COL) == 4)
check("[2e] every phase1 target column except the image is allowed",
      {c for c in phase1.FIELD_MAPPING.values() if c != phase1.STYLE_IMAGE_COL} <= allow)

# A rogue column must be dropped AND recorded -- never silently passed through,
# and never allowed to abort the push.
rogue = dtc_row("r1", 1, **{FG: "Main Fabric", MA: "WV-0003",
                            PL: "BODICE", CT: "Cotton 100%"})
p_rogue = wip_plan.compute_request_plan(
    SCOPE, [rogue], [bp_row()], bom_by_style={"KTB-1": MAIN_ONLY},
    allowed_cols=ALLOWED)
# Inject through the plan's own guarantee path by asking for a column the live
# view does not have.
p_view = wip_plan.compute_request_plan(
    SCOPE, [dtc_row("r1", 1, **{FG: "Main Fabric", MA: "WV-0003",
                                PL: "BODICE", CT: "Cotton 100%"})],
    [bp_row(description="Changed Name")],
    bom_by_style={"KTB-1": MAIN_ONLY},
    allowed_cols=ALLOWED - {"Style Description"})
check("[2f] a column absent from the live view is dropped",
      all("Style Description" not in r.fields for r in p_view.updates),
      p_view.explain())

# Style Image can never reach a PATCH body, even via a fan-out row copy that
# carries it forward from the row being duplicated.
p_img = wip_plan.compute_request_plan(
    SCOPE,
    [dtc_row("r1", 1, **{FG: "Main Fabric", MA: "WV-0003", PL: "BODICE",
                         CT: "Cotton 100%", phase1.STYLE_IMAGE_COL: "http://img"})],
    [bp_row()], bom_by_style={"KTB-1": MAIN_PLUS_ONE}, allowed_cols=ALLOWED)
check("[2g] Style Image never copied into a fan-out INSERT",
      all(phase1.STYLE_IMAGE_COL not in r.fields
          for r in p_img.updates + p_img.inserts),
      p_img.explain())
check("[2h] no violations recorded on a clean plan", not p_img.violations, p_img.violations)


# ---------------------------------------------------------------------------
print("\n[3] Degrade, never abort")
# ---------------------------------------------------------------------------

# Malformed BOM for one style must not take out that style's STYLE fields,
# nor any other style.
p_bad = wip_plan.compute_request_plan(
    SCOPE,
    [dtc_row("r1", 1, style="KTB-1"), dtc_row("r2", 2, style="KTB-2")],
    [bp_row(style="KTB-1", description="New Name"),
     bp_row(style="KTB-2", description="Also New")],
    bom_by_style={"KTB-1": {"xts_data": {"TECH_PACK_EXTRACTION": {"Table": "not-a-list"}}},
                  "KTB-2": MAIN_ONLY},
    allowed_cols=ALLOWED)
style_updates = [r for r in p_bad.updates if "Style Description" in r.fields]
check("[3a] malformed BOM does not abort the plan", len(p_bad.updates) >= 1, p_bad.explain())
check("[3b] style fields still pushed for BOTH styles despite bad BOM",
      len(style_updates) == 2, p_bad.explain())

# An unknown vendor slot makes duty.build_wip_patch_fields raise; that must be
# caught, recorded, and the row's other fields must survive.
p_duty_bad = wip_plan.compute_request_plan(
    SCOPE, [dtc_row("r1", 1, **{FG: "Main Fabric", MA: "WV-0003"})],
    [bp_row(description="New Name")],
    bom_by_style={"KTB-1": MAIN_ONLY},
    duty_rows=[{"bp_style_no": "KTB-1", "color_name": "Blue", "material_no": "WV-0003",
                "supplier_type": "NOT-A-SLOT", "duty_rate_us": 0.16}],
    allowed_cols=ALLOWED)
check("[3c] bad vendor slot is caught and recorded",
      any(d.startswith("duty:") for d in p_duty_bad.degraded), p_duty_bad.degraded)
check("[3d] the row's style fields survive a duty failure",
      any("Style Description" in r.fields for r in p_duty_bad.updates),
      p_duty_bad.explain())
check("[3e] no duty column leaked from the failed contribution",
      all(not (set(r.fields) & wip_plan.duty_columns()) for r in p_duty_bad.updates))


# ---------------------------------------------------------------------------
print("\n[4] Ownership: material owns its columns, style only default-fills")
# ---------------------------------------------------------------------------

# Brand-new style x color with BOM available. The style contribution inserts
# with "NO TPM BOM" placeholders; material must override them IN THE SAME RUN
# -- this is precisely what v1 needed repull_dtc to achieve.
p_new = wip_plan.compute_request_plan(
    SCOPE, [], [bp_row()], bom_by_style={"KTB-1": MAIN_ONLY}, allowed_cols=ALLOWED)
check("[4a] brand-new style x color produces exactly one INSERT",
      len(p_new.inserts) == 1 and not p_new.updates, p_new.explain())
ins = p_new.inserts[0]
check("[4b] material overrode the NO TPM BOM placeholder in the same run",
      ins.fields.get(FG) == "Main Fabric" and ins.fields.get(MA) == "WV-0003",
      ins.fields)
check("[4c] the override is recorded for debugging",
      any("style -> material" in o for o in ins.overrides), ins.overrides)
check("[4d] material columns attributed to the material contribution",
      ins.sources.get(FG) == wip_plan.SOURCE_MATERIAL
      and ins.sources.get(MA) == wip_plan.SOURCE_MATERIAL, ins.sources)
check("[4e] style columns still attributed to the style contribution",
      ins.sources.get("Style Description") == wip_plan.SOURCE_STYLE, ins.sources)

# No BOM for the style: the placeholder must survive so the row still reaches
# DTC, and nothing is reverted.
p_nobom = wip_plan.compute_request_plan(
    SCOPE, [], [bp_row()], bom_by_style={}, allowed_cols=ALLOWED)
check("[4f] BOM-less style still inserts, keeping the placeholder",
      len(p_nobom.inserts) == 1
      and p_nobom.inserts[0].fields.get(FG) == bom.DUMMY_FABRIC_GROUP,
      p_nobom.explain())

# "Never revert": a row holding a real, unrecognized material value is left
# completely alone when this run's BOM no longer contains it.
p_revert = wip_plan.compute_request_plan(
    SCOPE,
    [dtc_row("r1", 1, **{FG: "Fabric", MA: "GONE-999",
                         PL: "SLEEVE", CT: "Wool 100%"})],
    [bp_row()], bom_by_style={"KTB-1": MAIN_ONLY}, allowed_cols=ALLOWED)
touched = [r for r in p_revert.updates if set(r.fields) & wip_plan.MATERIAL_OWNED_COLS]
check("[4g] a vanished segment's row is never reverted or blanked",
      not touched, p_revert.explain())


# ---------------------------------------------------------------------------
print("\n[5] Duty lands on the right physical row")
# ---------------------------------------------------------------------------

# One style x color, TWO material rows. Duty for WV-0061 must land ONLY on the
# WV-0061 row. Joining on style+color alone would silently pick one row.
rows_two_materials = [
    dtc_row("rA", 1, **{FG: "Main Fabric", MA: "WV-0003", PL: "BODICE", CT: "Cotton 100%"}),
    dtc_row("rB", 2, **{FG: "Fabric", MA: "WV-0061", PL: "HEM", CT: "Poly 100%"}),
]
p_duty = wip_plan.compute_request_plan(
    SCOPE, rows_two_materials, [bp_row()],
    bom_by_style={"KTB-1": MAIN_PLUS_ONE},
    duty_rows=[{"bp_style_no": "KTB-1", "color_name": "Blue", "material_no": "WV-0061",
                "supplier_type": "Main", "hts_code": "6205.20", "duty_rate_us": 0.195}],
    allowed_cols=ALLOWED)
by_id = {r.row_id: r for r in p_duty.updates}
check("[5a] duty applied to the matching material row only",
      "rB" in by_id and by_id["rB"].fields.get("Main Factory HTS Code") == "6205.20",
      p_duty.explain())
check("[5b] the other material row got no duty fields",
      "rA" not in by_id or not (set(by_id["rA"].fields) & wip_plan.duty_columns()),
      p_duty.explain())

# Several costing_chart rows (one per vendor slot) target the SAME WIP row and
# MUST merge into one object: two sheetData entries sharing a rowId in one call
# is rejected with 400 "Duplicate rowId found" (live-confirmed 2026-09-01).
p_slots = wip_plan.compute_request_plan(
    SCOPE, [dtc_row("rA", 1, **{FG: "Main Fabric", MA: "WV-0003",
                                PL: "BODICE", CT: "Cotton 100%"})],
    [bp_row()], bom_by_style={"KTB-1": MAIN_ONLY},
    duty_rows=[
        {"bp_style_no": "KTB-1", "color_name": "Blue", "material_no": "WV-0003",
         "supplier_type": "Main", "hts_code": "6205.20"},
        {"bp_style_no": "KTB-1", "color_name": "Blue", "material_no": "WV-0003",
         "supplier_type": "1", "hts_code": "6205.30"},
    ],
    allowed_cols=ALLOWED)
sheet = p_slots.update_sheet_data()
row_ids = [o["rowId"] for o in sheet]
check("[5c] multiple vendor slots merge into ONE object per rowId",
      len(row_ids) == len(set(row_ids)) == 1, sheet)
check("[5d] both slots' columns present in that one object",
      sheet and "Main Factory HTS Code" in sheet[0]
      and "Factory 1 - HTS code" in sheet[0], sheet)

# tariff_rate IS written now -- all 4 slot columns were live-verified against
# the view definition 2026-09-17 and the WIP_TARIFF_COLS_LIVE switch removed.
# It used to be the one duty field with nowhere to go; this asserts the
# reversal, since a silent regression here would look exactly like the old
# (correct-at-the-time) behaviour.
p_tariff = wip_plan.compute_request_plan(
    SCOPE, [dtc_row("rA", 1, **{FG: "Main Fabric", MA: "WV-0003",
                                PL: "BODICE", CT: "Cotton 100%"})],
    [bp_row()], bom_by_style={"KTB-1": MAIN_ONLY},
    duty_rows=[{"bp_style_no": "KTB-1", "color_name": "Blue", "material_no": "WV-0003",
                "supplier_type": "Main", "tariff_rate": 0.25}],
    allowed_cols=ALLOWED)
check("[5e] tariff_rate IS written, to the live 'Main Factory Tariff' column",
      (not p_tariff.is_empty())
      and p_tariff.update_sheet_data()[0].get("Main Factory Tariff") == 0.25
      and p_tariff.counts.get("duty_values_not_writable", 0) == 0,
      p_tariff.summary())

# A value already equal in DTC must still produce no write -- tariff joins the
# zero-diff-zero-write invariant like every other column, so enabling it does
# not open a write window on rows that already agree.
p_tariff_same = wip_plan.compute_request_plan(
    SCOPE, [dtc_row("rA", 1, **{FG: "Main Fabric", MA: "WV-0003",
                                PL: "BODICE", CT: "Cotton 100%",
                                "Main Factory Tariff": "0.25"})],
    [bp_row()], bom_by_style={"KTB-1": MAIN_ONLY},
    duty_rows=[{"bp_style_no": "KTB-1", "color_name": "Blue", "material_no": "WV-0003",
                "supplier_type": "Main", "tariff_rate": 0.25}],
    allowed_cols=ALLOWED)
check("[5e2] a tariff DTC already holds is not rewritten (0.25 vs '0.25')",
      p_tariff_same.is_empty(), p_tariff_same.summary())

# EMPTY REQUEST (a freshly created / cleared sheet): every row is an INSERT,
# so a fan-out duplicate has no existing DTC row to copy identity from -- its
# style fields live only in the SAME run's planned writes. Live-confirmed
# 2026-09-18 on the recreated "KTB SS28 Collaborations": 25 of 39 rows landed
# with BP Style# and Color / Wash NULL, which also made every fan-out collapse
# into one indistinguishable group. Invisible on an established sheet, because
# there `current` is populated.
p_fresh = wip_plan.compute_request_plan(
    SCOPE, [], [bp_row(style="KTB-1", color="Blue")],
    bom_by_style={"KTB-1": MAIN_PLUS_ONE}, allowed_cols=ALLOWED)
_fresh_ins = p_fresh.insert_sheet_data()
check("[5f] empty request -> one row per segment (1 style INSERT + 1 fan-out)",
      len(_fresh_ins) == 2, p_fresh.summary())
check("[5f2] EVERY inserted row carries BP Style# and Color / Wash",
      all(o.get(STYLE_COL) == "KTB-1" and o.get(COLOR_COL) == "Blue"
          for o in _fresh_ins),
      [{k: v for k, v in o.items() if k in (STYLE_COL, COLOR_COL, FG, MA)}
       for o in _fresh_ins])
check("[5f3] the fan-out row carries its own material identity",
      sorted(str(o.get(MA)) for o in _fresh_ins) == ["WV-0003", "WV-0061"],
      [o.get(MA) for o in _fresh_ins])

# duty_rows for a row that does not exist yet must not invent one.
p_orphan_duty = wip_plan.compute_request_plan(
    SCOPE, [], [], bom_by_style={},
    duty_rows=[{"bp_style_no": "NOPE", "color_name": "X", "material_no": "Y",
                "supplier_type": "Main", "hts_code": "1"}],
    allowed_cols=ALLOWED)
check("[5f] unmatched duty row creates nothing", p_orphan_duty.is_empty())


# ---------------------------------------------------------------------------
print("\n[6] rowIndex uniqueness across style inserts and material fan-out")
# ---------------------------------------------------------------------------

# Two brand-new colours, each needing a Main Fabric + one Fabric segment.
# Style contributes 2 inserts; material fans out 2 more. All 4 rowIndexes must
# be distinct and clear of the existing rows.
p_fan = wip_plan.compute_request_plan(
    SCOPE,
    [dtc_row("r9", 7, style="KTB-9", color="Existing")],
    [bp_row(color="Blue"), bp_row(color="Red")],
    bom_by_style={"KTB-1": MAIN_PLUS_ONE}, allowed_cols=ALLOWED)
idxs = [r.row_index for r in p_fan.inserts]
check("[6a] style inserts + material fan-out produce 4 rows",
      len(p_fan.inserts) == 4, p_fan.explain())
check("[6b] every rowIndex is unique", len(idxs) == len(set(idxs)), idxs)
check("[6c] no rowIndex collides with an existing row",
      all(i > 7 for i in idxs), idxs)
check("[6d] per-colorway coverage: each colour gets both segments",
      sorted(r.match_key[1] for r in p_fan.inserts) == ["Blue", "Blue", "Red", "Red"],
      [r.match_key for r in p_fan.inserts])

# Updates and inserts must never be mixed in one call.
check("[6e] update bodies are keyed by rowId only",
      all("rowId" in o and "rowIndex" not in o for o in p_fan.update_sheet_data()))
check("[6f] insert bodies are keyed by rowIndex only",
      all("rowIndex" in o and "rowId" not in o for o in p_fan.insert_sheet_data()))
check("[6g] at most 2 PATCH calls per request",
      p_fan.summary()["patch_calls"] <= 2, p_fan.summary())


# ---------------------------------------------------------------------------
print("\n[7] Provenance, flags and diagnostics")
# ---------------------------------------------------------------------------

p_full = wip_plan.compute_request_plan(
    SCOPE, [dtc_row("rA", 1, **{FG: "Main Fabric", MA: "WV-0003",
                                PL: "BODICE", CT: "Cotton 100%"})],
    [bp_row(description="Renamed")], bom_by_style={"KTB-1": MAIN_ONLY},
    duty_rows=[{"bp_style_no": "KTB-1", "color_name": "Blue", "material_no": "WV-0003",
                "supplier_type": "Main", "hts_code": "6205.20"}],
    allowed_cols=ALLOWED, request_name="KTB FW26 Wrangler")
check("[7a] every planned field has a recorded source",
      all(set(r.fields) <= set(r.sources) for r in p_full.updates + p_full.inserts))
check("[7b] summary is JSON-safe and complete",
      set(p_full.summary()) >= {"request", "updates", "inserts", "noops",
                                "patch_calls", "fields_by_source", "diagnostics",
                                "degraded", "violations", "empty"}, p_full.summary())
check("[7b-2] fields_by_source holds ONLY contribution labels",
      set(p_full.summary()["fields_by_source"]) <= set(wip_plan.ALL_SOURCES),
      p_full.summary()["fields_by_source"])
check("[7b-3] non-field counters are separated into diagnostics",
      "duty_rows_matched" in p_full.summary()["diagnostics"],
      p_full.summary()["diagnostics"])
check("[7b-4] columns_changed names the actual columns and row counts",
      p_full.summary()["columns_changed"].get("Main Factory HTS Code") == 1
      and p_full.summary()["columns_changed"].get("Style Description") == 1,
      p_full.summary()["columns_changed"])
check("[7b-5] an empty plan reports no changed columns",
      p.summary()["columns_changed"] == {}, p.summary()["columns_changed"])
check("[7c] explain() renders without error", isinstance(p_full.explain(), str))
check("[7d] fields_by_source counts both contributions",
      p_full.counts.get("style", 0) >= 1 and p_full.counts.get("duty", 0) >= 1,
      p_full.counts)

# run_bom=false / run_duty_push=false must disable only their contribution.
p_nomat = wip_plan.compute_request_plan(
    SCOPE, [], [bp_row()], bom_by_style={"KTB-1": MAIN_ONLY},
    enable_material=False, allowed_cols=ALLOWED)
check("[7e] enable_material=False keeps the placeholder, still inserts",
      len(p_nomat.inserts) == 1
      and p_nomat.inserts[0].fields.get(FG) == bom.DUMMY_FABRIC_GROUP,
      p_nomat.explain())

p_noduty = wip_plan.compute_request_plan(
    SCOPE, [dtc_row("rA", 1, **{FG: "Main Fabric", MA: "WV-0003",
                                PL: "BODICE", CT: "Cotton 100%"})],
    [bp_row()], bom_by_style={"KTB-1": MAIN_ONLY},
    duty_rows=[{"bp_style_no": "KTB-1", "color_name": "Blue", "material_no": "WV-0003",
                "supplier_type": "Main", "hts_code": "6205.20"}],
    enable_duty=False, allowed_cols=ALLOWED)
check("[7f] enable_duty=False writes no duty columns", p_noduty.is_empty(),
      p_noduty.explain())

# Exceptions from the style contribution must be surfaced, not swallowed.
p_exc = wip_plan.compute_request_plan(
    SCOPE, [], [bp_row(bp_style_number=None)], bom_by_style={}, allowed_cols=ALLOWED)
check("[7g] style exceptions are surfaced on the plan",
      any(e.reason == "missing_bp_style" for e in p_exc.exceptions),
      [e.reason for e in p_exc.exceptions])
check("[7h] a request with only exceptions still writes nothing", p_exc.is_empty())


# ---------------------------------------------------------------------------
print("\n[7i] Orphan marks ride the SAME write window")
# ---------------------------------------------------------------------------

# A row whose style has moved to a DIFFERENT request (a BeProduct key field
# changed) is marked "(removed)", never deleted. In v1 this was a THIRD PATCH
# call against the old request; merging it here is most of the point of v2 --
# a key change must cost ONE write window, not two.
stale = dtc_row("rZ", 5, style="KTB-MOVED", color="Blue")
p_orph = wip_plan.compute_request_plan(
    SCOPE, [stale], [], bom_by_style={},
    bp_keys_this_request=set(),
    moved_elsewhere_keys={("KTB-MOVED", "Blue")},
    allowed_cols=ALLOWED)
check("[7i-1] stale row is marked (removed)",
      any(r.fields.get("Product Status") == phase1.REMOVED_STATUS
          for r in p_orph.updates), p_orph.explain())
check("[7i-2] the mark is attributed to the orphan contribution",
      any(r.sources.get("Product Status") == wip_plan.SOURCE_ORPHAN
          for r in p_orph.updates), p_orph.explain())
check("[7i-3] orphan marks still cost at most 1 PATCH call",
      p_orph.summary()["patch_calls"] == 1, p_orph.summary())

# Already-flagged rows must contribute nothing -- otherwise every subsequent
# run would re-mark them and invalidate sessions forever.
already = dtc_row("rZ", 5, style="KTB-MOVED", color="Blue",
                  **{"Product Status": phase1.REMOVED_STATUS})
p_orph2 = wip_plan.compute_request_plan(
    SCOPE, [already], [], bom_by_style={},
    bp_keys_this_request=set(),
    moved_elsewhere_keys={("KTB-MOVED", "Blue")},
    allowed_cols=ALLOWED)
check("[7i-4] an already-marked row is idempotent (writes nothing)",
      p_orph2.is_empty(), p_orph2.explain())

# A row nobody claims anywhere in BeProduct is user data -- never touch it.
p_orph3 = wip_plan.compute_request_plan(
    SCOPE, [dtc_row("rU", 6, style="USER-ENTERED", color="X")], [],
    bom_by_style={}, bp_keys_this_request=set(), moved_elsewhere_keys=set(),
    allowed_cols=ALLOWED)
check("[7i-5] unrelated user-entered row is left alone", p_orph3.is_empty(),
      p_orph3.explain())

# Orphan marking is opt-in: omitting the key sets must not mark anything.
p_orph4 = wip_plan.compute_request_plan(
    SCOPE, [stale], [], bom_by_style={}, allowed_cols=ALLOWED)
check("[7i-6] orphan pass is skipped when key sets are not supplied",
      p_orph4.is_empty(), p_orph4.explain())


# ---------------------------------------------------------------------------
print("\n[7j] material_exclude_cols -- holding a contested column back")
# ---------------------------------------------------------------------------

# DTC's own Content trigger and the techpack BOM express the SAME fibre content
# in different notation ("97% Cotton / 3% Spandex" vs "Cotton 97%, Spandex 3%").
# Both are "correct", so each overwrites the other on every run -- a permanent
# write window at a 2-hourly cadence. Excluding the column must suppress the
# write WITHOUT disturbing anything else the material contribution does.
contested = dtc_row("r1", 1, **{FG: "Main Fabric", MA: "WV-0003",
                                PL: "BODICE", CT: "97% Cotton / 3% Spandex"})
boms_ct = {"KTB-1": bom_json([("Main Fabric", "WV-0003", "HEM", "Cotton 97%, Spandex 3%")])}

p_incl = wip_plan.compute_request_plan(
    SCOPE, [contested], [bp_row()], bom_by_style=boms_ct, allowed_cols=ALLOWED)
check("[7j-1] without the exclusion, Content is written",
      p_incl.summary()["columns_changed"].get(CT) == 1,
      p_incl.summary()["columns_changed"])

p_excl = wip_plan.compute_request_plan(
    SCOPE, [contested], [bp_row()], bom_by_style=boms_ct,
    material_exclude_cols=frozenset({CT}), allowed_cols=ALLOWED)
check("[7j-2] with the exclusion, Content is NOT written",
      CT not in p_excl.summary()["columns_changed"],
      p_excl.summary()["columns_changed"])
check("[7j-3] the excluded column is recorded as dropped, not silently lost",
      any(CT in r.dropped for r in p_excl.updates + p_excl.noops),
      [r.dropped for r in p_excl.updates + p_excl.noops])
check("[7j-4] Placement (also material) is still written",
      p_excl.summary()["columns_changed"].get(PL) == 1,
      p_excl.summary()["columns_changed"])

# With Content the ONLY difference, excluding it must yield a true no-op.
settled_ct = dtc_row("r1", 1, **{FG: "Main Fabric", MA: "WV-0003",
                                 PL: "HEM", CT: "97% Cotton / 3% Spandex"})
p_noop = wip_plan.compute_request_plan(
    SCOPE, [settled_ct], [bp_row()], bom_by_style=boms_ct,
    material_exclude_cols=frozenset({CT}), allowed_cols=ALLOWED)
check("[7j-5] excluding the only contested column restores zero-write",
      p_noop.is_empty(), p_noop.explain())


# ---------------------------------------------------------------------------
print("\n[7k] material_fill_if_blank_cols -- write-once material columns")
# ---------------------------------------------------------------------------

# Content is contested: DTC's own trigger rewrites whatever Phase 10 writes,
# and the two notations are semantically identical. Phase 10's write only has
# to make the cell NON-BLANK (Phase 9a's completeness gate). So: fill a blank,
# never touch a filled one -- otherwise the diff reappears every single run and
# opens a write window every time.
filled_other_notation = dtc_row("r1", 1, **{FG: "Main Fabric", MA: "WV-0003",
                                            PL: "HEM", CT: "97% Cotton / 3% Spandex"})
boms_ct = {"KTB-1": bom_json([("Main Fabric", "WV-0003", "HEM", "Cotton 97%, Spandex 3%")])}

p_once = wip_plan.compute_request_plan(
    SCOPE, [filled_other_notation], [bp_row()], bom_by_style=boms_ct,
    material_fill_if_blank_cols=frozenset({CT}), allowed_cols=ALLOWED)
check("[7k-1] a NON-BLANK Content is left alone (no ping-pong)",
      p_once.is_empty(), p_once.explain())
check("[7k-2] the skip is recorded, not silent",
      any("write-once" in v for r in p_once.updates + p_once.noops
          for v in r.dropped.values()),
      [r.dropped for r in p_once.updates + p_once.noops])

blank_content = dtc_row("r1", 1, **{FG: "Main Fabric", MA: "WV-0003",
                                    PL: "HEM", CT: None})
p_fill = wip_plan.compute_request_plan(
    SCOPE, [blank_content], [bp_row()], bom_by_style=boms_ct,
    material_fill_if_blank_cols=frozenset({CT}), allowed_cols=ALLOWED)
check("[7k-3] a BLANK Content IS filled (Phase 9a's gate needs non-blank)",
      p_fill.summary()["columns_changed"].get(CT) == 1,
      p_fill.summary()["columns_changed"])

# A brand-new row has no current value at all, so it must be filled.
p_new_ct = wip_plan.compute_request_plan(
    SCOPE, [], [bp_row()], bom_by_style=boms_ct,
    material_fill_if_blank_cols=frozenset({CT}), allowed_cols=ALLOWED)
check("[7k-4] a new INSERT still receives Content",
      p_new_ct.inserts and p_new_ct.inserts[0].fields.get(CT) == "Cotton 97%, Spandex 3%",
      p_new_ct.explain())
check("[7k-5] Placement is unaffected by the write-once rule",
      p_fill.summary()["columns_changed"].get(PL) is None
      or p_fill.summary()["columns_changed"].get(PL) == 1)


# ---------------------------------------------------------------------------
print("\n[7l] BOM source-agnosticism -- Lakebase payload OR prebuilt segments")
# ---------------------------------------------------------------------------

# The BeProduct PageBomVariation source hands wip_plan already-built segments
# instead of a raw Lakebase custom_fields payload. Both must produce the
# IDENTICAL plan -- that equivalence is what makes the migration diffable.
_prebuilt = bom.build_target_segments(MAIN_ONLY)
p_raw = wip_plan.compute_request_plan(
    SCOPE, [], [bp_row()], bom_by_style={"KTB-1": MAIN_ONLY}, allowed_cols=ALLOWED)
p_seg = wip_plan.compute_request_plan(
    SCOPE, [], [bp_row()], bom_by_style={"KTB-1": _prebuilt}, allowed_cols=ALLOWED)
check("[7l-1] prebuilt segments produce the same field set as the raw payload",
      [dict(r.fields) for r in p_seg.inserts] == [dict(r.fields) for r in p_raw.inserts],
      (p_seg.explain(), p_raw.explain()))
check("[7l-2] and the same provenance",
      [dict(r.sources) for r in p_seg.inserts] == [dict(r.sources) for r in p_raw.inserts])

# An empty segment list must mean "no Main Fabric" -> zero actions, never a revert.
p_none = wip_plan.compute_request_plan(
    SCOPE, [dtc_row("r1", 1, **{FG: "Fabric", MA: "GONE", PL: "X", CT: "Y"})],
    [bp_row()], bom_by_style={"KTB-1": []}, allowed_cols=ALLOWED)
check("[7l-3] an empty segment list never reverts an enriched row",
      not any(set(r.fields) & wip_plan.MATERIAL_OWNED_COLS for r in p_none.updates),
      p_none.explain())


# ---------------------------------------------------------------------------
print("\n[8] Idempotence: planning twice over the applied result is a no-op")
# ---------------------------------------------------------------------------

# Simulate a second run: apply run 1's writes to the live rows, then re-plan.
# This is the property that makes a 2-hourly cadence safe, and the thing most
# likely to regress silently.
live = [dtc_row("rA", 1, **{FG: bom.DUMMY_FABRIC_GROUP, MA: bom.DUMMY_FABRIC_ARTICLE})]
staging = [bp_row(description="Renamed")]
boms = {"KTB-1": MAIN_ONLY}
duties = [{"bp_style_no": "KTB-1", "color_name": "Blue", "material_no": "WV-0003",
           "supplier_type": "Main", "hts_code": "6205.20", "duty_rate_us": 0.16}]

run1 = wip_plan.compute_request_plan(SCOPE, live, staging, bom_by_style=boms,
                                     duty_rows=duties, allowed_cols=ALLOWED)
check("[8a] run 1 does real work", not run1.is_empty(), run1.explain())

applied = [dict(live[0])]
for r in run1.updates:
    applied[0].update(r.fields)
for r in run1.inserts:  # none expected here, but keep the simulation honest
    applied.append({**r.fields, "rowId": f"new-{r.row_index}", "rowIndex": r.row_index})

run2 = wip_plan.compute_request_plan(SCOPE, applied, staging, bom_by_style=boms,
                                     duty_rows=duties, allowed_cols=ALLOWED)
check("[8b] run 2 over the applied result is EMPTY (idempotent)",
      run2.is_empty(), run2.explain())
check("[8c] run 2 issues 0 PATCH calls", run2.summary()["patch_calls"] == 0)

# And a third pass, to catch anything that oscillates between two states.
run3 = wip_plan.compute_request_plan(SCOPE, applied, staging, bom_by_style=boms,
                                     duty_rows=duties, allowed_cols=ALLOWED)
check("[8d] run 3 is still empty (no oscillation)", run3.is_empty())


# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
total = _passed + len(_failed)
if _failed:
    print(f"  ❌ {len(_failed)}/{total} FAILED")
    for f in _failed:
        print(f"      - {f}")
    sys.exit(1)
print(f"  ✅ ALL {total} WIP_PLAN COMPOSITION TESTS PASSED")
print("=" * 70)
