#!/usr/bin/env python3
"""
Unit tests for sync/color_palette.py -- Phase 11, BeProduct palettes -> DTC.

    python3 dtc/tests/test_color_palette.py

The property that matters most is zero-diff-zero-write: a run against a sheet
that already matches BeProduct must plan NO write at all.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from sync import color_palette as cp  # noqa: E402

_failures = []


def check(cond, msg):
    if cond:
        print(f"  ✅ {msg}")
    else:
        print(f"  ❌ {msg}")
        _failures.append(msg)


PREFIXES = {"FALL": "FW", "SPRING": "SS"}   # live dtc_seasoncode_mapping for KTB


def flatten(palettes, prefixes=PREFIXES):
    return cp.flatten_palettes(palettes, prefixes)


def palette(number="APP-S32027-00005", name="Collaboration - FW28", season="Fall",
            year="2028", brands=("Collaborations",), active="Yes", colors=None,
            deleted=False, ptype="SEASONAL", category="Tops"):
    fields = [
        {"id": "header_number", "value": number}, {"id": "header_name", "value": name},
        {"id": "season", "value": season}, {"id": "year", "value": year},
        {"id": "brands_multi", "value": list(brands)}, {"id": "active", "value": active},
        {"id": "palette_type", "value": ptype}, {"id": "product_category", "value": category},
    ]
    if colors is None:
        colors = [color("13-2705 TCX", "Flushing Pink")]
    return {"colorPaletteNumber": number, "colorPaletteName": name, "isDeleted": deleted,
            "headerData": {"fields": fields, "colors": {"ASEurl": "", "colors": colors}}}


def color(number, name, category=None, reference=None, hex_=None):
    schema = {"e1_color_code": None, "el_color_name": None}
    if category is not None:
        schema["color_category"] = category
    return {"color_number": number, "color_name": name,
            "color_reference": reference if reference is not None else number,
            "hex": hex_, "Schema": schema}


def as_dtc(targets, start=1):
    """What the live sheet would return after the targets were written."""
    return [{"rowId": f"r{i}", "rowIndex": i,
             **{k: v for k, v in t.items() if v is not None and k in cp.OWNED_COLS}}
            for i, t in enumerate(targets, start)]


print("[1] helpers")
check(cp.season_code("Fall", "2028", PREFIXES) == "FW28", "Fall 2028 -> FW28")
check(cp.season_code("spring", "2027", PREFIXES) == "SS27", "season matched case-insensitively")
check(cp.season_code({"text": "Spring", "code": "S1"}, "2027", PREFIXES) == "SS27",
      "DropDown dict value read as its text")
check(cp.season_code("Summer", "2028", PREFIXES) is None, "unmapped season -> None, never invented")
check(cp.season_code("Fall", "N/A", PREFIXES) is None and cp.season_code("Fall", None, PREFIXES) is None,
      "non-4-digit year -> None")
check(cp.season_brand("FW28", "Collaborations") == "FW28 - Collaborations",
      "Season Brand = '<SeasonCode><YY> - <Brand>'")
check(cp.season_brand("FW28", None) == "FW28", "no brand -> season code only")
check(cp.legacy_season_brand("Fall", "2028", "Collaborations") == "Fall 2028 - Collaborations",
      "legacy format still derivable for the in-place rename")
check(cp.is_bp_active("Yes") and not cp.is_bp_active("No") and not cp.is_bp_active(None),
      "BeProduct 'Yes'/'No' -> bool")
check(cp.as_list(["Lee", " ", "Lee", "Wrangler"]) == ["Lee", "Wrangler"], "multiselect deduped, blanks dropped")

print("[2] flatten: grain is palette x color x BRAND")
t, rep = flatten([palette(brands=("Wrangler", "Lee"),
                                      colors=[color("A", "a"), color("B", "b")])])
check(len(t) == 4, "2 colors x 2 brands -> 4 rows")
check({r[cp.COL_SEASON_BRAND] for r in t} == {"FW28 - Wrangler", "FW28 - Lee"},
      "one Season Brand per brand")
check(all(r[cp.COL_ACTIVE] == "YES" for r in t), "active palette -> 'YES' (the only accepted value)")

t, _ = flatten([palette(active="No")])
check(t[0][cp.COL_ACTIVE] == "NO", "inactive palette -> 'NO'")

t, _ = flatten([palette(colors=[color("A", "a", category="Brights")])])
check(t[0][cp.COL_COLOR_CATEGORY] == "Brights", "Color Category from the color's Schema.color_category")
t, _ = flatten([palette(colors=[color("A", "a", category="")])])
check(t[0][cp.COL_COLOR_CATEGORY] is None, "blank color_category -> None")

t, _ = flatten([palette(colors=[color("A", "a", hex_="fcccdc")])])
check(t[0][cp.COL_HEX] == "fcccdc", "Hex from the color's top-level hex, passed through as-is")
t, _ = flatten([palette(colors=[color("A", "a", hex_="")])])
check(t[0][cp.COL_HEX] is None, "blank hex -> None")

t, rep = flatten([palette(deleted=True)])
check(t == [] and rep["palettes_deleted"] == 1, "deleted palette contributes no rows")

t, rep = flatten([palette(colors=[color(None, "Nameless"), color("A", "a")])])
check(len(t) == 1 and len(rep["skipped_no_color_number"]) == 1,
      "color with no number is reported, never written")

t, rep = flatten([palette(colors=[color("A", "a"), color("A", "a again")])])
check(len(t) == 1 and rep["duplicate_keys"], "duplicate key keeps first, reports the rest")

t, rep = flatten([palette(season="Summer"), palette(number="P2", season="Spring")])
check([r[cp.COL_SEASON_BRAND] for r in t] == ["SS28 - Collaborations"],
      "unmapped-season palette contributes no rows")
check(rep["held_palettes"] == ["APP-S32027-00005"] and len(rep["skipped_no_season_mapping"]) == 1,
      "...and is reported + held")

print("[3] plan: empty sheet -> all inserts, no locators")
targets, _ = flatten([palette(colors=[color("A", "a"), color("B", "b")])])
p = cp.plan_sync(targets, [])
check(len(p.inserts) == 2 and not p.updates, "2 inserts, 0 updates")
check(all(set(r) <= set(cp.OWNED_COLS) for r in p.inserts),
      "inserts carry only owned columns (no legacy alias, no locators)")
check(all("rowId" not in r and "rowIndex" not in r for r in p.inserts),
      "inserts carry no rowId/rowIndex (append_rows would reject them)")
check(all(v is not None for r in p.inserts for v in r.values()),
      "blank columns omitted from insert bodies")

print("[4] ZERO-DIFF-ZERO-WRITE")
p = cp.plan_sync(targets, as_dtc(targets))
check(not p.has_writes and p.noops == 2, "matching sheet -> no write at all")

print("[5] plan: changed value -> lean update")
dtc = as_dtc(targets)
dtc[0][cp.COL_COLOR_NAME] = "old name"
p = cp.plan_sync(targets, dtc)
check(p.updates == [{"rowId": "r1", cp.COL_COLOR_NAME: "a"}], "only the changed column is sent")
check(not p.inserts and p.noops == 1, "other row untouched")

dtc = as_dtc(targets)
t_hex, _ = flatten([palette(colors=[color("A", "a", hex_="fcccdc"), color("B", "b")])])
p = cp.plan_sync(t_hex, dtc)
check(p.updates == [{"rowId": "r1", cp.COL_HEX: "fcccdc"}],
      "Hex newly set in BeProduct -> lean update of Hex only (existing rows backfill)")

print("[6] plan: value blanked in BeProduct is CLEARED (sheet is BeProduct-owned)")
dtc = as_dtc(targets)
dtc[0][cp.COL_PRODUCT_CATEGORY] = "Bottoms"
t2, _ = flatten([palette(category=None, colors=[color("A", "a"), color("B", "b")])])
p = cp.plan_sync(t2, dtc)
check(all(u.get(cp.COL_PRODUCT_CATEGORY, "x") is None for u in p.updates) and len(p.updates) == 2,
      "Product Category -> null on both rows")

print("[7] plan: removed color is UNTICKED, never deleted")
t1, _ = flatten([palette(colors=[color("A", "a")])])
p = cp.plan_sync(t1, as_dtc(targets))
check(p.updates == [{"rowId": "r2", cp.COL_ACTIVE: "NO"}], "B -> Active 'NO'")
check(len(p.unticked) == 1 and not p.inserts, "reported as unticked, nothing inserted")

dtc = as_dtc(targets)
dtc[1][cp.COL_ACTIVE] = "NO"
p = cp.plan_sync(t1, dtc)
check(not p.has_writes, "already unticked -> no write (idempotent)")

print("[8] plan: color re-added after untick -> Active back to YES")
dtc = as_dtc(targets)
dtc[1][cp.COL_ACTIVE] = "NO"
p = cp.plan_sync(targets, dtc)
check(p.updates == [{"rowId": "r2", cp.COL_ACTIVE: "YES"}], "re-ticked in place, no insert")

print("[9] plan: palette moved to another season -> insert new key, untick old")
moved, _ = flatten([palette(season="Spring", colors=[color("A", "a"), color("B", "b")])])
p = cp.plan_sync(moved, as_dtc(targets))
check(len(p.inserts) == 2 and len(p.unticked) == 2, "2 inserted under the new Season Brand, 2 old unticked")

print("[10] plan: DTC rows it cannot key are left alone")
dtc = as_dtc(targets) + [{"rowId": "x9", cp.COL_COLOR_NAME: "typed by hand"}]
p = cp.plan_sync(targets, dtc)
check(not p.has_writes and len(p.unkeyed_dtc_rows) == 1, "unkeyed row reported, never unticked")

print("[11] plan: duplicate DTC rows for one key are ALL kept in sync")
dtc = as_dtc(targets) + [{"rowId": "dup", **{k: v for k, v in targets[0].items() if v is not None}}]
dtc[-1][cp.COL_COLOR_NAME] = "stale"
p = cp.plan_sync(targets, dtc)
check(p.updates == [{"rowId": "dup", cp.COL_COLOR_NAME: "a"}] and len(p.duplicate_dtc_keys) == 1,
      "the stale duplicate is updated and the duplicate reported")

print("[12] Season Brand format change: legacy rows RENAMED in place, not re-inserted")
legacy_sheet = as_dtc(targets)
for r in legacy_sheet:
    r[cp.COL_SEASON_BRAND] = "Fall 2028 - Collaborations"
p = cp.plan_sync(targets, legacy_sheet)
check(not p.inserts and not p.unticked, "no insert, no untick")
check(p.updates == [{"rowId": "r1", cp.COL_SEASON_BRAND: "FW28 - Collaborations"},
                    {"rowId": "r2", cp.COL_SEASON_BRAND: "FW28 - Collaborations"}],
      "only Season Brand changes, on the same rowIds")
p = cp.plan_sync(targets, legacy_sheet[:1] + as_dtc(targets)[1:])
check(len(p.updates) == 1 and not p.inserts, "mixed sheet: only the legacy row is renamed")
both = as_dtc(targets) + [dict(legacy_sheet[0], rowId="old")]
p = cp.plan_sync(targets, both)
check(p.updates == [{"rowId": "old", cp.COL_ACTIVE: "NO"}],
      "legacy row is NOT claimed when the new-format row already exists -> unticked")

print("[13] held palette: rows of an unmapped-season palette are never unticked")
t_none, rep = flatten([palette(season="Summer", colors=[color("A", "a"), color("B", "b")])])
p = cp.plan_sync(t_none, as_dtc(targets), rep["held_palettes"])
check(not p.has_writes and p.held == 2, "2 rows held, no write")
p = cp.plan_sync(t_none, as_dtc(targets))
check(len(p.unticked) == 2, "(without the hold they would have been unticked)")

print("[14] every planned key is in the owned allow-list")
p = cp.plan_sync(moved, as_dtc(targets))
keys = {k for u in p.updates for k in u} | {k for r in p.inserts for k in r}
check(keys <= set(cp.OWNED_COLS) | {"rowId"}, "no column outside OWNED_COLS")

print()
if _failures:
    print(f"❌ {len(_failures)} failure(s)")
    sys.exit(1)
print("✅ all color_palette tests passed")
