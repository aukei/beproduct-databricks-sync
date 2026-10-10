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


def color(number, name, category=None, reference=None):
    schema = {"e1_color_code": None, "el_color_name": None}
    if category is not None:
        schema["color_category"] = category
    return {"color_number": number, "color_name": name,
            "color_reference": reference if reference is not None else number,
            "Schema": schema}


def as_dtc(targets, start=1):
    """What the live sheet would return after the targets were written."""
    return [{"rowId": f"r{i}", "rowIndex": i, **{k: v for k, v in t.items() if v is not None}}
            for i, t in enumerate(targets, start)]


print("[1] helpers")
check(cp.season_brand("Fall", "2028", "Collaborations") == "Fall 2028 - Collaborations",
      "Season Brand = '<Season> <Year> - <Brand>'")
check(cp.season_brand("Fall", None, "Lee") == "Fall - Lee", "missing year dropped, not padded")
check(cp.season_brand("Fall", "2028", None) == "Fall 2028", "no brand -> season only")
check(cp.season_brand({"text": "Spring", "code": "S1"}, "2027", "Lee") == "Spring 2027 - Lee",
      "DropDown dict value read as its text")
check(cp.is_bp_active("Yes") and not cp.is_bp_active("No") and not cp.is_bp_active(None),
      "BeProduct 'Yes'/'No' -> bool")
check(cp.as_list(["Lee", " ", "Lee", "Wrangler"]) == ["Lee", "Wrangler"], "multiselect deduped, blanks dropped")

print("[2] flatten: grain is palette x color x BRAND")
t, rep = cp.flatten_palettes([palette(brands=("Wrangler", "Lee"),
                                      colors=[color("A", "a"), color("B", "b")])])
check(len(t) == 4, "2 colors x 2 brands -> 4 rows")
check({r[cp.COL_SEASON_BRAND] for r in t} == {"Fall 2028 - Wrangler", "Fall 2028 - Lee"},
      "one Season Brand per brand")
check(all(r[cp.COL_ACTIVE] == "YES" for r in t), "active palette -> 'YES' (the only accepted value)")

t, _ = cp.flatten_palettes([palette(active="No")])
check(t[0][cp.COL_ACTIVE] == "NO", "inactive palette -> 'NO'")

t, _ = cp.flatten_palettes([palette(colors=[color("A", "a", category="Brights")])])
check(t[0][cp.COL_COLOR_CATEGORY] == "Brights", "Color Category from the color's Schema.color_category")
t, _ = cp.flatten_palettes([palette(colors=[color("A", "a", category="")])])
check(t[0][cp.COL_COLOR_CATEGORY] is None, "blank color_category -> None")

t, rep = cp.flatten_palettes([palette(deleted=True)])
check(t == [] and rep["palettes_deleted"] == 1, "deleted palette contributes no rows")

t, rep = cp.flatten_palettes([palette(colors=[color(None, "Nameless"), color("A", "a")])])
check(len(t) == 1 and len(rep["skipped_no_color_number"]) == 1,
      "color with no number is reported, never written")

t, rep = cp.flatten_palettes([palette(colors=[color("A", "a"), color("A", "a again")])])
check(len(t) == 1 and rep["duplicate_keys"], "duplicate key keeps first, reports the rest")

print("[3] plan: empty sheet -> all inserts, no locators")
targets, _ = cp.flatten_palettes([palette(colors=[color("A", "a"), color("B", "b")])])
p = cp.plan_sync(targets, [])
check(len(p.inserts) == 2 and not p.updates, "2 inserts, 0 updates")
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

print("[6] plan: value blanked in BeProduct is CLEARED (sheet is BeProduct-owned)")
dtc = as_dtc(targets)
dtc[0][cp.COL_PRODUCT_CATEGORY] = "Bottoms"
t2, _ = cp.flatten_palettes([palette(category=None, colors=[color("A", "a"), color("B", "b")])])
p = cp.plan_sync(t2, dtc)
check(all(u.get(cp.COL_PRODUCT_CATEGORY, "x") is None for u in p.updates) and len(p.updates) == 2,
      "Product Category -> null on both rows")

print("[7] plan: removed color is UNTICKED, never deleted")
t1, _ = cp.flatten_palettes([palette(colors=[color("A", "a")])])
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
moved, _ = cp.flatten_palettes([palette(season="Spring", colors=[color("A", "a"), color("B", "b")])])
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

print("[12] every planned key is in the owned allow-list")
p = cp.plan_sync(moved, as_dtc(targets))
keys = {k for u in p.updates for k in u} | {k for r in p.inserts for k in r}
check(keys <= set(cp.OWNED_COLS) | {"rowId"}, "no column outside OWNED_COLS")

print()
if _failures:
    print(f"❌ {len(_failures)} failure(s)")
    sys.exit(1)
print("✅ all color_palette tests passed")
