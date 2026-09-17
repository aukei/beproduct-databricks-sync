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
means. `LF MATERIAL ID` (the material's `headerNumber`) is carried alongside for
logging; the GUID `materialId` is what the write actually uses, so a planned
reorganisation of material master into per-customer folders cannot break it.

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
            "empty": self.is_empty(),
        }


def plan_customer_code_push(
    dtc_rows: List[Dict[str, Any]],
    segments_by_style: Dict[Optional[str], List[Dict[str, Any]]],
    current_by_material: Optional[Dict[str, Any]] = None,
    dtc_col: str = DTC_CUSTOMER_CODE_COL,
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

        material_id = segment.get("material_id")
        if segment.get("is_ad_hoc") or not material_id:
            # An ad-hoc row has no linked material, so there is nothing to
            # write to. (Owner: KTB does not use ad-hoc BOM rows, so this is a
            # guard rather than an expected path.)
            plan.ad_hoc_skipped.append({
                "bp_style_number": style, "color": colour,
                "mill_fabric_article": key[1], "value": value,
                "reason": "ad-hoc BOM row has no linked material",
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
