"""
bom_push -- DTC "Fabric Customer # or SAP #" -> BeProduct MATERIAL MASTER.

The only DTC -> BeProduct-material direction in this pipeline. Pure Python; no
Spark, no network. Unit-tested in dtc/tests/test_bom_push.py.

Why the target is the MATERIAL, not the BOM row
-----------------------------------------------
The obvious target would be the BOM row's `CUSTOMER MATERIAL CODE` field. It is
not writable there -- live-confirmed 2026-09-17:

    POST .../Variation/{v}/Update
    -> 400 Field [customer_material_code] is not editable on a material-linked row.

The BOM row only DISPLAYS that code, read through from the linked material
(owner confirmation: "BomVariation now lookup this code (via LF Material No) and
display inline"). Proven live: writing the material's own
`customer_material_code` made the BOM row show the new value immediately.

So the write goes to the material, via `material.attributes_update(material_id,
fields={"customer_material_code": value})`.

Resolution chain
----------------
    DTC row  --(Fabric Group, Mill Fabric Article #)-->  BOM segment
             --materialId-->                            material master

`(Fabric Group, Mill Fabric Article #)` is unique within a style (owner
confirmation), and is the SAME pair `bom.segment_key()` already uses for
enrichment -- so both directions agree on what "the same fabric assignment"
means.

**Two ways the segment yields a material, depending on the BOM source
(2026-09-22 walkback):**

  * PageBomVariation segments carry `material_id` directly. Preferred whenever
    present -- the GUID is stable under the planned reorganisation of material
    master into per-customer folders.
  * Lakebase segments carry only `lf_material_id` (`**MaterialCode`), the
    material's `headerNumber`. The caller resolves those to GUIDs and passes
    `material_id_by_code=`; this module never makes a network call. That route
    DOES depend on `headerNumber` staying globally unique across folders, which
    the GUID route did not -- a real cost of the walkback, recorded here so it
    is not rediscovered.

Either way the write itself always uses the GUID.

The hazard this module exists to handle
---------------------------------------
**Materials are SHARED across styles.** One material can be referenced by many
BOM rows in many styles, so two DTC rows can disagree about what its customer
code should be. Writing "last one wins" would silently corrupt a shared master
record. `plan_customer_code_push()` therefore groups by material and REFUSES to
write any material whose candidate values disagree, reporting the conflict
instead.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from . import bom, phase1

# The DTC WIP column that sources this push. Confirmed present in the
# WIP_ITS_USE view (Delta column `col_Fabric_Customer_or_SAP`), and blank on
# every row as of 2026-09-17 -- so a run today correctly does nothing.
DTC_CUSTOMER_CODE_COL = "Fabric Customer # or SAP #"

# The material-master header field to write. Live-verified id/name pair:
#   {"id": "customer_material_code", "name": "CUSTOMER MATERIAL CODE",
#    "type": "Text"}  in material headerData.fields
MATERIAL_CUSTOMER_CODE_FIELD = "customer_material_code"


@dataclass
class CodeWrite:
    """One material-master field write."""
    material_id: str
    value: str
    lf_material_id: Optional[str] = None
    current_value: Optional[str] = None
    # Every (style, colour) whose DTC row asked for this value -- so a
    # surprising write can be traced back to the cell that caused it.
    sources: List[Tuple[Optional[str], Optional[str]]] = field(default_factory=list)


@dataclass
class CustomerCodePlan:
    writes: List[CodeWrite] = field(default_factory=list)
    # Material wanted two different values from two different DTC rows.
    # NEVER written -- a shared master record must not be decided by row order.
    conflicts: List[Dict[str, Any]] = field(default_factory=list)
    # DTC row had a value but no BOM segment matched its (group, article).
    unmatched: List[Dict[str, Any]] = field(default_factory=list)
    # Matched, but the material already holds exactly this value.
    noops: List[str] = field(default_factory=list)
    # Matched to an ad-hoc BOM row, which has no linked material to write to.
    ad_hoc_skipped: List[Dict[str, Any]] = field(default_factory=list)
    # Matched a segment, but its LF material code could not be turned into
    # exactly one material GUID (blank code, no match, several matches, or the
    # lookup failed). Deliberately NOT folded into `unmatched` or
    # `ad_hoc_skipped`: the three demand completely different operator
    # responses -- "fix the DTC fabric assignment", "this row has no material
    # by design", and "fix or re-key the material master" -- and merging them
    # makes the summary unactionable.
    unresolved: List[Dict[str, Any]] = field(default_factory=list)

    def is_empty(self) -> bool:
        """True => issue no BeProduct writes at all."""
        return not self.writes

    def summary(self) -> Dict[str, Any]:
        return {
            "writes": len(self.writes),
            "conflicts": len(self.conflicts),
            "unmatched": len(self.unmatched),
            "noops": len(self.noops),
            "ad_hoc_skipped": len(self.ad_hoc_skipped),
            "unresolved": len(self.unresolved),
            "empty": self.is_empty(),
        }


def _segment_material(
    segment: Dict[str, Any],
    material_id_by_code: Optional[Dict[str, Any]],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Resolve one BOM segment to a material GUID.

    -> (material_id, bucket, reason). Exactly one of `material_id` / `bucket` is
    set. `bucket` is "ad_hoc_skipped" or "unresolved"; `reason` explains it.

    Order matters. `is_ad_hoc` is checked BEFORE the code lookup so an ad-hoc
    row -- which has no linked material by design -- keeps landing in its own
    bucket rather than being reported as a lookup failure.
    """
    material_id = segment.get("material_id")
    if material_id:
        return material_id, None, None          # PageBomVariation: GUID in hand

    if segment.get("is_ad_hoc"):
        return None, "ad_hoc_skipped", "ad-hoc BOM row has no linked material"

    if material_id_by_code is None:
        # No resolver offered. Preserves the pre-2026-09-22 behaviour exactly:
        # a segment with no material_id is reported as ad-hoc.
        return None, "ad_hoc_skipped", "ad-hoc BOM row has no linked material"

    code = phase1.norm(segment.get("lf_material_id"))
    if code is None:
        return None, "unresolved", "BOM row has no **MaterialCode"
    resolved = material_id_by_code.get(code)
    if not resolved:
        return None, "unresolved", f"no single material for header_number={code!r}"
    return resolved, None, None


def plan_customer_code_push(
    dtc_rows: List[Dict[str, Any]],
    segments_by_style: Dict[Optional[str], List[Dict[str, Any]]],
    current_by_material: Optional[Dict[str, Any]] = None,
    dtc_col: str = DTC_CUSTOMER_CODE_COL,
    material_id_by_code: Optional[Dict[str, Any]] = None,
) -> CustomerCodePlan:
    """
    Plan the DTC -> material-master customer-code writes.

    Args:
        dtc_rows: WIP rows, keyed by DTC column display names. Needs
            `BP Style#`, `Color / Wash`, `Fabric Group`,
            `Mill Fabric Article #` and `dtc_col`.
        segments_by_style: `{bp_style_number: [segment, ...]}` as written by
            Stage 20b, each segment carrying `material_id` / `lf_material_id` /
            `is_ad_hoc` (see `bom.extract_variation_row_fields`).
        current_by_material: `{material_id: current customer_material_code}`.
            Supply it to suppress writes that would change nothing -- the same
            lean-write principle the DTC side uses. Omit it and every matched
            row is treated as needing a write.
        dtc_col: the source DTC column.
        material_id_by_code: `{lf_material_id: material GUID}`, for segments
            that carry no `material_id` of their own -- i.e. the Lakebase
            source (2026-09-22 walkback). The CALLER performs the lookup and
            is responsible for having refused anything ambiguous; a code
            missing from this map, or mapped to a falsy value, lands in
            `unresolved`. Omit it entirely and behaviour is byte-identical to
            before the walkback.

    Returns:
        CustomerCodePlan. Check `is_empty()` before calling BeProduct.

    A BLANK DTC value is never pushed -- the owner spec is "if not blank/null,
    should push". That also means this push can never CLEAR a material code,
    only set or change one.
    """
    plan = CustomerCodePlan()
    style_col, color_col = phase1.MATCH_KEY_COLS

    # material_id -> {normalised value: [(style, colour), ...]}
    wanted: Dict[str, Dict[str, List[Tuple[Optional[str], Optional[str]]]]] = {}
    lf_by_material: Dict[str, Optional[str]] = {}

    for row in dtc_rows:
        value = phase1.norm(row.get(dtc_col))
        if value is None:
            continue                      # blank: nothing to push, never clear

        style = phase1.norm(row.get(style_col))
        colour = phase1.norm(row.get(color_col))
        key = bom.segment_key({
            "fabric_group": row.get(bom.WIP_FIELD_FABRIC_GROUP),
            "mill_fabric_article": row.get(bom.WIP_FIELD_MILL_FABRIC_ARTICLE),
        })

        segment = next(
            (s for s in (segments_by_style.get(style) or [])
             if bom.segment_key(s) == key), None)
        if segment is None:
            plan.unmatched.append({
                "bp_style_number": style, "color": colour,
                "fabric_group": key[0], "mill_fabric_article": key[1],
                "value": value,
                "reason": "no BOM segment matches this (Fabric Group, "
                          "Mill Fabric Article #) for the style",
            })
            continue

        material_id, bucket, reason = _segment_material(segment, material_id_by_code)
        if material_id is None:
            # Either an ad-hoc row (no linked material by design -- owner: KTB
            # does not use them, so this is a guard rather than an expected
            # path), or an LF code that did not resolve to exactly one
            # material. Both are refusals to write, reported, never guessed.
            getattr(plan, bucket).append({
                "bp_style_number": style, "color": colour,
                "fabric_group": key[0], "mill_fabric_article": key[1],
                "material_code": segment.get("lf_material_id"),
                "value": value, "reason": reason,
            })
            continue

        wanted.setdefault(material_id, {}).setdefault(value, []).append((style, colour))
        lf_by_material.setdefault(material_id, segment.get("lf_material_id"))

    current = current_by_material or {}
    for material_id, by_value in sorted(wanted.items()):
        if len(by_value) > 1:
            # Two DTC rows disagree about a SHARED master record. Refuse.
            plan.conflicts.append({
                "material_id": material_id,
                "lf_material_id": lf_by_material.get(material_id),
                "values": {v: [list(s) for s in srcs] for v, srcs in sorted(by_value.items())},
                "reason": "DTC rows disagree on the customer code for one shared "
                          "material; refusing to let row order decide",
            })
            continue

        value, sources = next(iter(by_value.items()))
        if material_id in current and phase1.norm(current.get(material_id)) == value:
            plan.noops.append(material_id)
            continue

        plan.writes.append(CodeWrite(
            material_id=material_id,
            value=value,
            lf_material_id=lf_by_material.get(material_id),
            current_value=current.get(material_id),
            sources=sources,
        ))

    return plan


def required_material_codes(
    dtc_rows: List[Dict[str, Any]],
    segments_by_style: Dict[Optional[str], List[Dict[str, Any]]],
    dtc_col: str = DTC_CUSTOMER_CODE_COL,
) -> List[str]:
    """
    The distinct LF material codes a caller must resolve to GUIDs before
    calling `plan_customer_code_push(..., material_id_by_code=...)`.

    Exists so the notebook can fetch exactly what it needs without first
    building a throwaway plan. Walks the same value -> style -> `segment_key()`
    path the planner does, and returns codes ONLY for segments that actually
    need one: a segment already carrying `material_id` (the PageBomVariation
    source) and an ad-hoc segment are both skipped.

    Because a blank DTC value short-circuits first -- exactly as in the planner
    -- this returns `[]` when no DTC row carries a customer code, which is the
    live state as of 2026-09-17. Lookup cost is therefore proportional to the
    WORK, not to the size of the BOM corpus.

    Sorted, so a caller's logging and any cache key are deterministic.
    """
    style_col = phase1.MATCH_KEY_COLS[0]
    codes = set()

    for row in dtc_rows:
        if phase1.norm(row.get(dtc_col)) is None:
            continue                      # blank: nothing to push, never clear

        key = bom.segment_key({
            "fabric_group": row.get(bom.WIP_FIELD_FABRIC_GROUP),
            "mill_fabric_article": row.get(bom.WIP_FIELD_MILL_FABRIC_ARTICLE),
        })
        style = phase1.norm(row.get(style_col))
        segment = next(
            (s for s in (segments_by_style.get(style) or [])
             if bom.segment_key(s) == key), None)
        if segment is None or segment.get("material_id") or segment.get("is_ad_hoc"):
            continue

        code = phase1.norm(segment.get("lf_material_id"))
        if code is not None:
            codes.add(code)

    return sorted(codes)
