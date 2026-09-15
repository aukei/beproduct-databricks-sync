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
check("[2d] tariff columns excluded while WIP_TARIFF_COLS_LIVE is False",
      (not duty.WIP_TARIFF_COLS_LIVE)
      and not (set(duty.WIP_TARIFF_COL.values()) & allow))
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

# tariff_rate has no live WIP column; it must be silently skipped, not written.
p_tariff = wip_plan.compute_request_plan(
    SCOPE, [dtc_row("rA", 1, **{FG: "Main Fabric", MA: "WV-0003",
                                PL: "BODICE", CT: "Cotton 100%"})],
    [bp_row()], bom_by_style={"KTB-1": MAIN_ONLY},
    duty_rows=[{"bp_style_no": "KTB-1", "color_name": "Blue", "material_no": "WV-0003",
                "supplier_type": "Main", "tariff_rate": 0.25}],
    allowed_cols=ALLOWED)
check("[5e] tariff_rate is not written while its column is not live",
      p_tariff.is_empty()
      and p_tariff.counts.get("duty_values_not_writable", 0) >= 1,
      p_tariff.summary())

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
                                "patch_calls", "fields_by_source", "degraded",
                                "violations", "empty"}, p_full.summary())
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
