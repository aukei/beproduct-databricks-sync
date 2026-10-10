"""
Phase 11 -- BeProduct color palettes -> DTC "KTB Color Palette" (pure logic).

One-way BeProduct -> DTC. DTC Fabric requests pick their colors (number + name)
from this sheet, so it must mirror BeProduct's palettes. Every column is
BeProduct-owned; nothing here ever reads a value back into BeProduct.

Source (live-verified 2026-10-07/08): ONE `api.color.attributes_list(folder_id)`
call returns every palette in the folder WITH its colors, so there is no
per-palette fetch. Each palette:

    colorPaletteNumber, colorPaletteName, isDeleted, modifiedAt,
    headerData.fields[]          season, year, brands_multi, palette_type,
                                 product_category, active, ...
    headerData.colors.colors[]   color_number, color_name, color_reference,
                                 Schema.color_category (per-color custom field)

Grain (owner decisions 2026-10-08): ONE DTC row per palette x color x BRAND.
A palette listing two brands yields each color twice.

    "Season Brand"   = "<Season> <Year> - <Brand>", e.g. "Fall 2028 - Collaborations"
    "Active"         = "YES" / "NO" -- the ONLY values DTC accepts for this
                       checkbox (400 "'Active' must be either YES or NO";
                       lowercase is rejected too)

Match key: (Palette Number, Color Number, Season Brand). There is no hidden id
column in the sheet, so the key is built from what the sheet shows. A change to
any of the three (a re-numbered color, a palette moved to another season or
brand) therefore looks like "old row gone, new row added": the new row is
INSERTED and the old one is set Active = "NO".

Each run:
  * a target with no DTC row         -> INSERT (append_rows; DTC assigns locators)
  * a target whose DTC row differs   -> UPDATE only the changed columns
  * a DTC row with no target         -> Active = "NO" ("untick", owner decision;
                                        never deleted)
  * nothing differs                  -> NO write at all (zero-diff-zero-write)

Because the sheet is wholly BeProduct-owned, a value BLANKED in BeProduct is
cleared in DTC (JSON null), unlike the WIP sheet where blank never overwrites.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sync.wip_plan import values_equal

# ── DTC columns (exact display names, view "WIP_ITS_USE" of the sample request)
COL_SEASON_BRAND = "Season Brand"
COL_PALETTE_NUMBER = "Palette Number"
COL_PALETTE_NAME = "Palette Name"
COL_PALETTE_TYPE = "Palette Type"
COL_PRODUCT_CATEGORY = "Product Category"
COL_ACTIVE = "Active"
COL_COLOR_NUMBER = "Color Number"
COL_COLOR_NAME = "Color Name"
COL_COLOR_CATEGORY = "Color Category"
COL_COLOR_REFERENCE = "Color Reference"

MATCH_KEY_COLS = (COL_PALETTE_NUMBER, COL_COLOR_NUMBER, COL_SEASON_BRAND)

# Every column this sync writes, and therefore the whole PATCH allow-list.
OWNED_COLS = (
    COL_SEASON_BRAND, COL_PALETTE_NUMBER, COL_PALETTE_NAME, COL_PALETTE_TYPE,
    COL_PRODUCT_CATEGORY, COL_ACTIVE, COL_COLOR_NUMBER, COL_COLOR_NAME,
    COL_COLOR_CATEGORY, COL_COLOR_REFERENCE,
)

ACTIVE_YES = "YES"
ACTIVE_NO = "NO"

# BeProduct fieldIds (folder_schema of the KTB color folder).
BP_SEASON = "season"
BP_YEAR = "year"
BP_BRANDS = "brands_multi"
BP_PALETTE_TYPE = "palette_type"
BP_PRODUCT_CATEGORY = "product_category"
BP_ACTIVE = "active"
BP_COLOR_CATEGORY = "color_category"   # inside each color's `Schema` block


def text(value: Any) -> Optional[str]:
    """A BeProduct field value as plain text, or None when blank.

    DropDown values can arrive as a dict ({'text','value','code'}); everything
    else is taken as-is. Lists are NOT handled here -- see `as_list`."""
    if isinstance(value, dict):
        value = value.get("text") or value.get("value")
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def as_list(value: Any) -> List[str]:
    """A MultiSelect value as a list of non-blank strings (order kept, deduped)."""
    if value is None:
        return []
    items = value if isinstance(value, (list, tuple)) else [value]
    out: List[str] = []
    for v in items:
        t = text(v)
        if t and t not in out:
            out.append(t)
    return out


def season_brand(season: Any, year: Any, brand: Optional[str]) -> Optional[str]:
    """"Fall 2028 - Collaborations". Missing parts are dropped, never padded."""
    when = " ".join(p for p in (text(season), text(year)) if p)
    brand = text(brand)
    if when and brand:
        return f"{when} - {brand}"
    return when or brand


def is_bp_active(value: Any) -> bool:
    """BeProduct TrueFalse field -> bool. Live values are 'Yes' / 'No'."""
    if isinstance(value, bool):
        return value
    return (text(value) or "").lower() in ("yes", "true", "y", "1")


def row_key(row: Dict[str, Any]) -> Tuple[Optional[str], ...]:
    return tuple(text(row.get(c)) for c in MATCH_KEY_COLS)


def _key_complete(key: Tuple[Optional[str], ...]) -> bool:
    return all(key)


def flatten_palettes(palettes: Iterable[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """BeProduct palettes -> target DTC rows (one per palette x color x brand).

    Returns (targets, report). A deleted palette contributes nothing, so its
    DTC rows get unticked. A color without a number cannot be keyed and is
    reported, never written. A key seen twice keeps its first occurrence.
    """
    targets: List[Dict[str, Any]] = []
    seen = set()
    report = {"palettes": 0, "palettes_deleted": 0, "colors": 0,
              "skipped_no_color_number": [], "duplicate_keys": []}

    for p in palettes:
        if p.get("isDeleted"):
            report["palettes_deleted"] += 1
            continue
        report["palettes"] += 1
        hd = p.get("headerData") or {}
        f = {x.get("id"): x.get("value") for x in hd.get("fields") or []}
        number = text(p.get("colorPaletteNumber")) or text(f.get("header_number"))
        name = text(p.get("colorPaletteName")) or text(f.get("header_name"))
        active = ACTIVE_YES if is_bp_active(f.get(BP_ACTIVE)) else ACTIVE_NO
        brands = as_list(f.get(BP_BRANDS)) or [None]
        colors = ((hd.get("colors") or {}).get("colors")) or []

        for c in colors:
            report["colors"] += 1
            color_number = text(c.get("color_number"))
            if not color_number:
                report["skipped_no_color_number"].append(
                    {"palette": number, "color_name": text(c.get("color_name"))})
                continue
            for brand in brands:
                row = {
                    COL_SEASON_BRAND: season_brand(f.get(BP_SEASON), f.get(BP_YEAR), brand),
                    COL_PALETTE_NUMBER: number,
                    COL_PALETTE_NAME: name,
                    COL_PALETTE_TYPE: text(f.get(BP_PALETTE_TYPE)),
                    COL_PRODUCT_CATEGORY: text(f.get(BP_PRODUCT_CATEGORY)),
                    COL_ACTIVE: active,
                    COL_COLOR_NUMBER: color_number,
                    COL_COLOR_NAME: text(c.get("color_name")),
                    COL_COLOR_CATEGORY: text((c.get("Schema") or {}).get(BP_COLOR_CATEGORY)),
                    COL_COLOR_REFERENCE: text(c.get("color_reference")),
                }
                key = row_key(row)
                if key in seen:
                    report["duplicate_keys"].append(list(key))
                    continue
                seen.add(key)
                targets.append(row)
    return targets, report


@dataclass
class PalettePlan:
    updates: List[Dict[str, Any]] = field(default_factory=list)   # {"rowId", <changed cols>}
    inserts: List[Dict[str, Any]] = field(default_factory=list)   # full rows, no locators
    unticked: List[Dict[str, Any]] = field(default_factory=list)  # subset of updates, for reporting
    noops: int = 0
    unkeyed_dtc_rows: List[Dict[str, Any]] = field(default_factory=list)
    duplicate_dtc_keys: List[List[Optional[str]]] = field(default_factory=list)

    def summary(self) -> Dict[str, Any]:
        cols: Dict[str, int] = {}
        for u in self.updates:
            for k in u:
                if k != "rowId":
                    cols[k] = cols.get(k, 0) + 1
        return {"inserts": len(self.inserts), "updates": len(self.updates),
                "unticked": len(self.unticked), "noops": self.noops,
                "columns_changed": cols,
                "unkeyed_dtc_rows": len(self.unkeyed_dtc_rows),
                "duplicate_dtc_keys": len(self.duplicate_dtc_keys)}

    @property
    def has_writes(self) -> bool:
        return bool(self.updates or self.inserts)


def plan_sync(targets: List[Dict[str, Any]], dtc_rows: List[Dict[str, Any]]) -> PalettePlan:
    """Diff target rows against the live DTC sheet.

    DTC rows sharing one key are ALL updated (none is singled out as "the" row)
    and reported, so a duplicate is never silently left stale. A DTC row with an
    incomplete key cannot be matched to anything: it is reported and left
    alone, never unticked, since it may be a row a user typed by hand.
    """
    plan = PalettePlan()
    by_key: Dict[Tuple, List[Dict[str, Any]]] = {}
    for r in dtc_rows:
        k = row_key(r)
        if not _key_complete(k):
            plan.unkeyed_dtc_rows.append({"rowId": r.get("rowId"), "key": list(k)})
            continue
        by_key.setdefault(k, []).append(r)
    plan.duplicate_dtc_keys = [list(k) for k, rs in by_key.items() if len(rs) > 1]

    target_keys = set()
    for t in targets:
        k = row_key(t)
        target_keys.add(k)
        existing = by_key.get(k)
        if not existing:
            plan.inserts.append({c: v for c, v in t.items() if v is not None})
            continue
        for r in existing:
            changed = {c: t.get(c) for c in OWNED_COLS
                       if not values_equal(r.get(c), t.get(c))}
            if changed:
                plan.updates.append({"rowId": r["rowId"], **changed})
            else:
                plan.noops += 1

    for k, rs in by_key.items():
        if k in target_keys:
            continue
        for r in rs:
            if text(r.get(COL_ACTIVE)) == ACTIVE_NO:
                plan.noops += 1
                continue
            u = {"rowId": r["rowId"], COL_ACTIVE: ACTIVE_NO}
            plan.updates.append(u)
            plan.unticked.append({"rowId": r["rowId"], "key": list(k)})
    return plan
