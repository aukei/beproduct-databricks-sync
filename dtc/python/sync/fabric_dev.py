"""
Stage 65 -- BeProduct style BOM -> DTC "KTB FABRIC" "<request> - DEV" lines,
one line per (fabric x colorway). Pure logic; the notebook is
`dtc/notebooks/v2_fabric_dev_sync.py`. Owner spec 2026-11-23.

Source: the BeProduct style BOM (PageBomVariation), NOT the techpack (TPM) BOM
that wip_push uses. Only BOM rows whose Group is "Main Fabric" or "Fabric".

Target: the KTB FABRIC request "<Customer> <SeasonCode> <Brand> - DEV", i.e. the
style's WIP request name plus " - DEV". The fabric lines there are created by
DTC users; this stage never creates a fabric, it only multiplexes an existing
line by colour:

  * a line is "the same fabric" when its "Mill Fabric Article #" equals the BOM
    row's `MILL FABRIC CODE/SUPPLIER ITEM CODE` (whitespace-normalised, case
    kept);
  * every colorway that uses the fabric needs one line whose "Color / Wash" is
    the colorway label "<COLOR NUMBER>-<COLOR NAME upper>", e.g.
    "17-1444 TCX-GINGER". Hidden colorways count too (owner spec);
  * a needed label already on a line -> nothing; a line holding the same colour
    in an older notation -- the bare NAME, or the WIP "Color / Wash" form
    "<name> <number>" (both seen live 2026-11-23) -> renamed in place; otherwise
    the article's first BLANK-colour line is claimed in place; otherwise the
    article's first line is copied with the new label (append_rows);
  * a line whose colour no BeProduct colorway needs is left alone, never
    deleted;
  * an article with no DTC line, or a style with no DEV request, is reported
    as a gap for the data-gaps dashboard, never created.

Zero-diff-zero-write: when every label is already present, the plan is empty.

Which colorways use a fabric: the BOM variation's own colorways
(`selectedVariationColorways`) unless it has `syncColorways`, in which case all
of the style's colorways.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sync import bom
from sync.phase1 import color_wash, norm

DEV_SUFFIX = " - DEV"
COL_ARTICLE = "Mill Fabric Article #"
COL_COLOR = "Color / Wash"
FABRIC_GROUPS = frozenset({bom.SEGMENT_MAIN_FABRIC, bom.SEGMENT_FABRIC})

# Never copied into a multiplexed line: DTC locators, DTC's own row key, and the
# provenance marker ("ML" on lines the material library made -- a copy is ours).
# Lookups are excluded per view in non_copyable_cols (DTC recomputes them).
COPY_EXCLUDE_COLS = frozenset({"rowId", "rowIndex", "ITS_Key", "sysSource"})

GAP_REQUEST_NOT_FOUND = "FABRIC_REQUEST_NOT_FOUND"
GAP_REQUEST_AMBIGUOUS = "FABRIC_REQUEST_AMBIGUOUS"
GAP_LINE_NOT_FOUND = "FABRIC_LINE_NOT_FOUND"
GAP_NO_ARTICLE = "BOM_FABRIC_NO_ARTICLE"


def dev_request_name(wip_request_name: Optional[str]) -> Optional[str]:
    return f"{wip_request_name}{DEV_SUFFIX}" if wip_request_name else None


def colorway_label(color_number: Any, color_name: Any) -> Optional[str]:
    """"17-1444 TCX", "Ginger" -> "17-1444 TCX-GINGER". Number kept as entered;
    name alone (upper) when the number is blank."""
    number, name = norm(color_number), norm(color_name)
    if not name:
        return number
    return f"{number}-{name.upper()}" if number else name.upper()


def _key(value: Any) -> Optional[str]:
    """Colour match key: normalised, case-insensitive."""
    v = norm(value)
    return v.upper() if v else None


def non_copyable_cols(dynamic_fields: List[Dict[str, Any]]) -> frozenset:
    """Columns a copied line must not carry: formula/image (DTC 400s on them,
    see bom.compute_non_writable_cols) and lookups."""
    lookups = {f.get("fieldName") for f in dynamic_fields or [] if f.get("type") == "lookup"}
    return bom.compute_non_writable_cols(dynamic_fields) | frozenset(lookups) | COPY_EXCLUDE_COLS


@dataclass
class StyleFabrics:
    """One style's fabric needs: article -> ordered [(label, older notations)]."""
    needs: Dict[str, List[Tuple[str, Tuple[str, ...]]]] = field(default_factory=dict)
    no_article: List[Dict[str, Any]] = field(default_factory=list)  # BOM rows with a blank article


def style_fabric_needs(colorways: Iterable[Dict[str, Any]],
                       variations: Iterable[Dict[str, Any]]) -> StyleFabrics:
    """
    `colorways`: the style record's `colorways` (BeProduct API shape: id,
    colorNumber, colorName, hideColorway) -- hidden ones INCLUDED on purpose.
    `variations`: the PageBomVariation GET bodies, each with `metadata` = the
    variation-list entry (syncColorways, selectedVariationColorways).
    """
    by_id: Dict[str, Tuple[str, Tuple[str, ...]]] = {}
    for cw in colorways or []:
        number, name = cw.get("colorNumber"), cw.get("colorName")
        label = colorway_label(number, name)
        if label and cw.get("id"):
            aliases = tuple(a for a in (norm(name), color_wash(name, number)) if a)
            by_id[cw["id"]] = (label, aliases)
    all_colors = list(by_id.values())

    out = StyleFabrics()
    for v in variations or []:
        meta = (v or {}).get("metadata") or {}
        if meta.get("syncColorways", True):
            colors = all_colors
        else:
            colors = [by_id[i] for i in meta.get("selectedVariationColorways") or [] if i in by_id]
        for row in (v or {}).get("rows") or []:
            group = norm(bom._bv_field(row, bom.BV_FIELD_GROUP))
            if group not in FABRIC_GROUPS:
                continue
            article = norm(bom._bv_field(row, bom.BV_FIELD_MILL_FABRIC_CODE))
            if not article:
                out.no_article.append({"group": group, "variation": meta.get("variationName"),
                                       "material": row.get("materialNumber") or bom._bv_field(row, "Material Name")})
                continue
            have = out.needs.setdefault(article, [])
            for c in colors:
                if c not in have:
                    have.append(c)
    return out


@dataclass
class FabricPlan:
    updates: List[Dict[str, Any]] = field(default_factory=list)   # {"rowId", COL_COLOR}
    inserts: List[Dict[str, Any]] = field(default_factory=list)   # full copied rows
    missing_articles: List[str] = field(default_factory=list)
    noops: int = 0

    @property
    def has_writes(self) -> bool:
        return bool(self.updates or self.inserts)

    def summary(self) -> Dict[str, int]:
        return {"updates": len(self.updates), "inserts": len(self.inserts),
                "noops": self.noops, "missing_articles": len(self.missing_articles)}


def plan_request(needs: Dict[str, List[Tuple[str, Tuple[str, ...]]]],
                 dtc_rows: List[Dict[str, Any]],
                 exclude_cols: frozenset = COPY_EXCLUDE_COLS) -> FabricPlan:
    """
    `needs`: article -> [(label, older notations)] for every style mapped to this
    request (merged by the caller). `dtc_rows`: the request's live sheetData.
    """
    plan = FabricPlan()
    rows_by_article: Dict[str, List[Dict[str, Any]]] = {}
    for r in sorted(dtc_rows or [], key=lambda r: r.get("rowIndex") or 0):
        a = norm(r.get(COL_ARTICLE))
        if a:
            rows_by_article.setdefault(a, []).append(r)

    for article in sorted(needs):
        rows = rows_by_article.get(article)
        if not rows:
            plan.missing_articles.append(article)
            continue
        current = {r["rowId"]: _key(r.get(COL_COLOR)) for r in rows}
        blanks = [r for r in rows if current[r["rowId"]] is None]
        for label, aliases in needs[article]:
            if _key(label) in current.values():
                plan.noops += 1
                continue
            # The same colour in an older notation -> rename that line in place.
            alias_keys = {_key(a) for a in aliases}
            target = next((r for r in rows if current[r["rowId"]] in alias_keys), None)
            if target is None and blanks:
                target = blanks.pop(0)
            if target is not None:
                plan.updates.append({"rowId": target["rowId"], COL_COLOR: label})
                current[target["rowId"]] = _key(label)
            else:
                plan.inserts.append(bom.build_insert_row_payload(
                    rows[0], {COL_COLOR: label}, exclude_cols=exclude_cols))
    return plan
