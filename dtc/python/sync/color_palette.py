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
    headerData.colors.colors[]   color_number, color_name, color_reference, hex,
                                 Schema.color_category (per-color custom field)

Grain (owner decisions 2026-10-08): ONE DTC row per palette x color x BRAND.
A palette listing two brands yields each color twice.

    "Season Brand"   = "<SeasonCode><YY> - <Brand>", e.g. "FW28 - Collaborations"
                       (owner change 2026-10-20). The season prefix comes from
                       `dtc_seasoncode_mapping` (FALL -> FW, SPRING -> SS), the
                       SAME table that builds the WIP request names, so the
                       palette and the WIP sheets agree on what "FW28" means.
                       A season with no mapping is skipped and reported, and
                       its palette's DTC rows are HELD (never unticked).
    "Active"         = "YES" / "NO" -- the ONLY values DTC accepts for this
                       checkbox (400 "'Active' must be either YES or NO";
                       lowercase is rejected too)

Match key: (Palette Number, Color Number, Season Brand). There is no hidden id
column in the sheet, so the key is built from what the sheet shows. A change to
any of the three (a re-numbered color, a palette moved to another season or
brand) therefore looks like "old row gone, new row added": the new row is
INSERTED and the old one is set Active = "NO".

The previous format ("Fall 2028 - Collaborations", 2026-10-08..20) is
recognised as an ALIAS: a DTC row still carrying it is renamed in place
(UPDATE of Season Brand) rather than unticked and re-inserted.

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
COL_HEX = "Hex"   # capital H in the view; BeProduct sends e.g. "fcccdc", no '#'

MATCH_KEY_COLS = (COL_PALETTE_NUMBER, COL_COLOR_NUMBER, COL_SEASON_BRAND)

# Every column this sync writes, and therefore the whole PATCH allow-list.
OWNED_COLS = (
    COL_SEASON_BRAND, COL_PALETTE_NUMBER, COL_PALETTE_NAME, COL_PALETTE_TYPE,
    COL_PRODUCT_CATEGORY, COL_ACTIVE, COL_COLOR_NUMBER, COL_COLOR_NAME,
    COL_COLOR_CATEGORY, COL_COLOR_REFERENCE, COL_HEX,
)

ACTIVE_YES = "YES"
ACTIVE_NO = "NO"

# BeProduct fieldIds (folder_schema of the KTB color folder).
BP_SEASON = "season"                   # DropDown text, e.g. "Fall"
BP_YEAR = "year"                       # DropDown text, e.g. "2028"
BP_BRANDS = "brands_multi"
BP_PALETTE_TYPE = "palette_type"
BP_PRODUCT_CATEGORY = "product_category"
BP_ACTIVE = "active"
BP_COLOR_CATEGORY = "color_category"   # inside each color's `Schema` block
BP_HEX = "hex"                         # top-level on each color, beside rgb_*/cmyk_*


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


# Not an owned column: carried on a target only so `plan_sync` can find a DTC
# row still holding the pre-2026-10-20 Season Brand. Never written to DTC.
LEGACY_SEASON_BRAND = "_legacy_season_brand"


def season_code(season: Any, year: Any, prefixes: Dict[str, str]) -> Optional[str]:
    """"FW28" from ("Fall", "2028") via the prefix map {"FALL": "FW", ...}.

    Same rule as the WIP transform: prefix + last 2 digits of the year. None
    when the season has no mapping or the year is not a 4-digit year (e.g.
    "N/A"), so the caller can report it instead of inventing a code."""
    prefix = prefixes.get((text(season) or "").upper())
    y = text(year) or ""
    if not prefix or not (len(y) == 4 and y.isdigit()):
        return None
    return f"{prefix}{y[-2:]}"


def season_brand(code: Optional[str], brand: Optional[str]) -> Optional[str]:
    """"FW28 - Collaborations". Missing parts are dropped, never padded."""
    code, brand = text(code), text(brand)
    if code and brand:
        return f"{code} - {brand}"
    return code or brand


def legacy_season_brand(season: Any, year: Any, brand: Optional[str]) -> Optional[str]:
    """The pre-2026-10-20 format, "Fall 2028 - Collaborations"."""
    return season_brand(" ".join(p for p in (text(season), text(year)) if p) or None, brand)


def is_bp_active(value: Any) -> bool:
    """BeProduct TrueFalse field -> bool. Live values are 'Yes' / 'No'."""
    if isinstance(value, bool):
        return value
    return (text(value) or "").lower() in ("yes", "true", "y", "1")


def row_key(row: Dict[str, Any]) -> Tuple[Optional[str], ...]:
    return tuple(text(row.get(c)) for c in MATCH_KEY_COLS)


def _key_complete(key: Tuple[Optional[str], ...]) -> bool:
    return all(key)


def flatten_palettes(palettes: Iterable[Dict[str, Any]],
                     season_prefixes: Dict[str, str]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """BeProduct palettes -> target DTC rows (one per palette x color x brand).

    `season_prefixes` maps an UPPERCASE BeProduct season to its DTC prefix
    ({"FALL": "FW", "SPRING": "SS"}).

    Returns (targets, report). A deleted palette contributes nothing, so its
    DTC rows get unticked. A palette whose season has no mapping contributes
    nothing either, but is listed in report["held_palettes"] so its DTC rows
    are left alone. A color without a number cannot be keyed and is reported,
    never written. A key seen twice keeps its first occurrence.
    """
    targets: List[Dict[str, Any]] = []
    seen = set()
    report = {"palettes": 0, "palettes_deleted": 0, "colors": 0,
              "skipped_no_color_number": [], "duplicate_keys": [],
              "skipped_no_season_mapping": [], "held_palettes": []}

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
        code = season_code(f.get(BP_SEASON), f.get(BP_YEAR), season_prefixes)
        if not code:
            report["skipped_no_season_mapping"].append(
                {"palette": number, "season": text(f.get(BP_SEASON)), "year": text(f.get(BP_YEAR))})
            if number:
                report["held_palettes"].append(number)
            continue
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
                    COL_SEASON_BRAND: season_brand(code, brand),
                    COL_PALETTE_NUMBER: number,
                    COL_PALETTE_NAME: name,
                    COL_PALETTE_TYPE: text(f.get(BP_PALETTE_TYPE)),
                    COL_PRODUCT_CATEGORY: text(f.get(BP_PRODUCT_CATEGORY)),
                    COL_ACTIVE: active,
                    COL_COLOR_NUMBER: color_number,
                    COL_COLOR_NAME: text(c.get("color_name")),
                    COL_COLOR_CATEGORY: text((c.get("Schema") or {}).get(BP_COLOR_CATEGORY)),
                    COL_COLOR_REFERENCE: text(c.get("color_reference")),
                    COL_HEX: text(c.get(BP_HEX)),
                    LEGACY_SEASON_BRAND: legacy_season_brand(f.get(BP_SEASON), f.get(BP_YEAR), brand),
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
    held: int = 0                                                  # rows of an unmapped-season palette
    unkeyed_dtc_rows: List[Dict[str, Any]] = field(default_factory=list)
    duplicate_dtc_keys: List[List[Optional[str]]] = field(default_factory=list)

    def summary(self) -> Dict[str, Any]:
        cols: Dict[str, int] = {}
        for u in self.updates:
            for k in u:
                if k != "rowId":
                    cols[k] = cols.get(k, 0) + 1
        return {"inserts": len(self.inserts), "updates": len(self.updates),
                "unticked": len(self.unticked), "noops": self.noops, "held": self.held,
                "columns_changed": cols,
                "unkeyed_dtc_rows": len(self.unkeyed_dtc_rows),
                "duplicate_dtc_keys": len(self.duplicate_dtc_keys)}

    @property
    def has_writes(self) -> bool:
        return bool(self.updates or self.inserts)


def plan_sync(targets: List[Dict[str, Any]], dtc_rows: List[Dict[str, Any]],
              held_palettes: Iterable[str] = ()) -> PalettePlan:
    """Diff target rows against the live DTC sheet.

    DTC rows sharing one key are ALL updated (none is singled out as "the" row)
    and reported, so a duplicate is never silently left stale. A DTC row with an
    incomplete key cannot be matched to anything: it is reported and left
    alone, never unticked, since it may be a row a user typed by hand.

    A target with no exact match may claim DTC rows keyed by its legacy Season
    Brand; they are renamed in place. Rows of a palette in `held_palettes`
    (season not mapped this run) are never unticked.
    """
    plan = PalettePlan()
    held = {text(h) for h in held_palettes if text(h)}
    by_key: Dict[Tuple, List[Dict[str, Any]]] = {}
    for r in dtc_rows:
        k = row_key(r)
        if not _key_complete(k):
            plan.unkeyed_dtc_rows.append({"rowId": r.get("rowId"), "key": list(k)})
            continue
        by_key.setdefault(k, []).append(r)
    plan.duplicate_dtc_keys = [list(k) for k, rs in by_key.items() if len(rs) > 1]

    target_keys = {row_key(t) for t in targets}
    claimed = set(target_keys)
    for t in targets:
        k = row_key(t)
        existing = by_key.get(k)
        if not existing:
            legacy = (k[0], k[1], text(t.get(LEGACY_SEASON_BRAND)))
            if _key_complete(legacy) and legacy not in claimed and by_key.get(legacy):
                existing = by_key[legacy]
                claimed.add(legacy)
        if not existing:
            plan.inserts.append({c: t.get(c) for c in OWNED_COLS if t.get(c) is not None})
            continue
        for r in existing:
            changed = {c: t.get(c) for c in OWNED_COLS
                       if not values_equal(r.get(c), t.get(c))}
            if changed:
                plan.updates.append({"rowId": r["rowId"], **changed})
            else:
                plan.noops += 1

    for k, rs in by_key.items():
        if k in claimed:
            continue
        for r in rs:
            if k[0] in held:
                plan.held += 1
                continue
            if text(r.get(COL_ACTIVE)) == ACTIVE_NO:
                plan.noops += 1
                continue
            u = {"rowId": r["rowId"], COL_ACTIVE: ACTIVE_NO}
            plan.updates.append(u)
            plan.unticked.append({"rowId": r["rowId"], "key": list(k)})
    return plan
