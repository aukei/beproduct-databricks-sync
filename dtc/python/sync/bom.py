"""
Phase 10 — BOM enrichment from externally-processed techpack data (pure Python).

Fulfills a Phase 1 gap: BOM (Bill of Materials) data is not available from the
BeProduct API and instead relies on techpack extraction, processed by a
separate pipeline and landed in:

    alb_tpm_uat.public.customer_teckpack_style_log   (UAT)
    alb_tpm_prd.public.customer_teckpack_style_log   (PRD)

Both catalogs are live-confirmed reachable from this Databricks workspace's
metastore (`SHOW CATALOGS` lists them directly) — no federation/JDBC needed,
just `spark.table("alb_tpm_<env>.public.customer_teckpack_style_log")`.

Join (owner spec, live-validated 2026-09-02 against the KONTOOR/Wrangler test
data already used throughout Phase 9a/9b testing — KTB-00016..KTB-00023 all
appear in this table with style_season="Spring - 2028", matching
`lft.beproduct.ktb_styles.season="Spring"` + `.year="2028"`):

    ktb_styles.bp_style_number = customer_teckpack_style_log.style_no
    AND (ktb_styles.season || " - " || ktb_styles.year) = customer_teckpack_style_log.style_season

INNER JOIN only — a BeProduct style with no matching BOM row is simply not
processed by Phase 10 (not an error).

``bom_unified`` is a JSON string shaped like:

    [{"part": "BOM", "details": [
        {"bom_detail_name": "Main Fabric", "material_name": "...",
         "material_no": "...", "placement": "...", ...},
        {"bom_detail_name": "Fabric", ...},
        {"bom_detail_name": "Stitch/Seam", ...},
        {"bom_detail_name": "Trim", ...},
        ...
    ], "column_header": [...]}]

Only two ``bom_detail_name`` values matter here: "Main Fabric" and "Fabric"
(corrected 2026-09-02 — an earlier iteration of this spec used "Body" instead
of "Fabric"; live data across all 16 KONTOOR rows never has "Body" at all,
but 3/16 genuinely have a "Fabric" segment alongside "Main Fabric" — e.g.
KTB-00020, KTB-00023). By construction there is exactly ONE "Main Fabric" per
style, and ZERO OR MORE "Fabric" segments (never seen more than 1 in live
data, but the notebook handles any count). "Stitch/Seam", "Trim", "Label" are
also live-confirmed present and are NOT used.

Enrichment logic (owner spec, corrected 2026-09-02):
  * A style's WIP rows are enriched ONLY if NONE of them already carry real
    Fabric Group data — a single already-enriched row (Fabric Group !=
    the DTC placeholder ``"MAIN MATERIAL CONTENT"``) short-circuits the
    WHOLE style to a no-op (`style_already_enriched`).
  * Otherwise, every one of that style's WIP rows still on the placeholder
    gets its `Fabric Group` / `Placement` / `Mill Fabric Article #` set from
    the "Main Fabric" segment.
  * For EACH "Fabric" segment found (0 or more), each of those same rows is
    ALSO duplicated into a brand-new row carrying that "Fabric" segment's
    values instead — i.e. an N-colorway style with 1 "Main Fabric" + M
    "Fabric" segments produces N UPDATEs (from Main Fabric) + N*M INSERTs
    (one per colorway row, per Fabric segment).
  * IMPORTANT — the value written to `Fabric Group` is the segment's own
    `bom_detail_name` (i.e. literally "Main Fabric" or "Fabric"), NOT
    `material_name` (corrected 2026-09-02; an earlier iteration of this spec
    used `material_name`). `Placement` / `Mill Fabric Article #` are
    unaffected by this correction — still `placement` / `material_no`.
  * A BOM row with neither segment (e.g. only "Trim"/"Label"/"Stitch/Seam")
    is a no-op for that style.

This module holds only the deterministic decision logic (JSON parsing, the
no-op/update/duplicate decision, and mapping to the raw DTC field names for
the eventual PATCH). Notebook orchestration (Spark I/O, DTCConnector PATCH
calls) lives in ``dtc/notebooks/p10_pull_bom_and_enrich.py``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# The DTC placeholder value that means "no real BOM data yet" — confirmed
# live in dtc_wip_ktb (e.g. KTB-00016 before enrichment: Fabric Group =
# "MAIN MATERIAL CONTENT").
PLACEHOLDER_FABRIC_GROUP = "MAIN MATERIAL CONTENT"

# The only two bom_detail_name values Phase 10 cares about (corrected
# 2026-09-02 -- was "Main Fabric"/"Body"; live data never has "Body", but
# genuinely has "Fabric" segments in 3/16 KONTOOR rows). There is exactly ONE
# "Main Fabric" per style by construction; "Fabric" can be zero or more.
# Live-confirmed other values present in the source table (ignored):
# "Stitch/Seam", "Trim", "Label".
SEGMENT_MAIN_FABRIC = "Main Fabric"
SEGMENT_FABRIC = "Fabric"

# Raw DTC WIP field names (live-confirmed via GET /v1/views/{WIP_ITS_USE view id}
# dynamicFields, 2026-09-02) — NOT the Delta col_* normalized names.
WIP_FIELD_FABRIC_GROUP = "Fabric Group"
WIP_FIELD_PLACEMENT = "Placement"
WIP_FIELD_MILL_FABRIC_ARTICLE = "Mill Fabric Article #"


def _blank(v: Any) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    return s == "" or s.lower() in {"n/a", "na", "none", "null", "nan"}


# ---------------------------------------------------------------------------
# Join key
# ---------------------------------------------------------------------------

def build_style_season(season: Optional[str], year: Optional[str]) -> Optional[str]:
    """
    Build the join value matching `customer_teckpack_style_log.style_season`
    (e.g. "Spring - 2028") from BeProduct's separate `season` ("Spring") and
    `year` ("2028") fields (`ktb_styles.season` / `ktb_styles.year`).

    Returns None if either input is blank (no valid join value can be built).
    """
    if _blank(season) or _blank(year):
        return None
    return f"{str(season).strip()} - {str(year).strip()}"


# ---------------------------------------------------------------------------
# BOM JSON parsing
# ---------------------------------------------------------------------------

@dataclass
class ParsedBomSegments:
    """
    Result of `parse_bom_segments()`. `main_fabric` is at most ONE detail
    dict (first "Main Fabric" occurrence wins, though by construction there
    should only ever be one); `fabric_list` holds ALL "Fabric" segments found
    (zero or more), in document order.
    """
    main_fabric: Optional[Dict[str, Any]] = None
    fabric_list: List[Dict[str, Any]] = field(default_factory=list)

    def is_empty(self) -> bool:
        return self.main_fabric is None and not self.fabric_list


def parse_bom_segments(bom_unified: Any) -> ParsedBomSegments:
    """
    Parse `customer_teckpack_style_log.bom_unified` (a JSON string, or an
    already-parsed list/dict — accepted for testability) and return a
    `ParsedBomSegments` holding ONLY the "Main Fabric" / "Fabric" segments.

    Returns an empty `ParsedBomSegments` (never raises) on any parse
    failure, blank input, or a payload with no matching segments — callers
    treat that uniformly as "nothing to enrich for this style."
    """
    if _blank(bom_unified):
        return ParsedBomSegments()
    try:
        parts = json.loads(bom_unified) if isinstance(bom_unified, str) else bom_unified
    except (json.JSONDecodeError, TypeError, ValueError):
        return ParsedBomSegments()
    if not isinstance(parts, list):
        return ParsedBomSegments()

    main_fabric: Optional[Dict[str, Any]] = None
    fabric_list: List[Dict[str, Any]] = []
    for part in parts:
        if not isinstance(part, dict):
            continue
        for detail in part.get("details") or []:
            if not isinstance(detail, dict):
                continue
            name = detail.get("bom_detail_name")
            if name == SEGMENT_MAIN_FABRIC and main_fabric is None:
                main_fabric = detail
            elif name == SEGMENT_FABRIC:
                fabric_list.append(detail)
    return ParsedBomSegments(main_fabric=main_fabric, fabric_list=fabric_list)


def extract_enrichment_fields(detail: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """
    Map one BOM detail object ("Main Fabric" or a "Fabric" segment) to the
    three fields Phase 10 writes, using the module's own (Delta-agnostic)
    field names — see `to_wip_fields()` for the raw-DTC-field-name mapping
    used for the actual PATCH.

    IMPORTANT: `fabric_group` is the segment's own `bom_detail_name` (i.e.
    literally "Main Fabric" or "Fabric"), NOT `material_name` — corrected
    2026-09-02 per an explicit spec amendment.
    """
    return {
        "fabric_group": detail.get("bom_detail_name"),
        "placement": detail.get("placement"),
        "mill_fabric_article": detail.get("material_no"),
    }


def to_wip_fields(fields: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    """Map `extract_enrichment_fields()`'s output to raw DTC WIP field names,
    ready to merge into a `sheetData` PATCH/INSERT row object."""
    return {
        WIP_FIELD_FABRIC_GROUP: fields.get("fabric_group"),
        WIP_FIELD_PLACEMENT: fields.get("placement"),
        WIP_FIELD_MILL_FABRIC_ARTICLE: fields.get("mill_fabric_article"),
    }


# ---------------------------------------------------------------------------
# Per-style enrichment decision
# ---------------------------------------------------------------------------

def style_already_enriched(existing_fabric_groups: List[Optional[str]]) -> bool:
    """
    True if ANY of a style's existing WIP rows already carries real (non-
    blank, non-placeholder) Fabric Group data. Per spec this short-circuits
    the WHOLE style to a no-op — Phase 10 never partially re-enriches a style
    some of whose colorway rows already have real data, even if others are
    still on the placeholder (safer than guessing why they differ).
    """
    return any(
        not _blank(fg) and str(fg).strip() != PLACEHOLDER_FABRIC_GROUP
        for fg in existing_fabric_groups
    )


@dataclass
class RowEnrichmentPlan:
    """
    What to do to ONE existing WIP row for a style, given its BOM segments.

    update_fields: fields to PATCH onto the row itself, from "Main Fabric".
        None means no "Main Fabric" segment was found -> the existing row is
        left untouched (still no-op'd even if "Fabric" segments exist).
    duplicate_fields_list: fields for ZERO OR MORE new rows to be inserted as
        copies of this one, one per "Fabric" segment found (in document
        order). Empty list means no duplication needed.
    """
    update_fields: Optional[Dict[str, Optional[str]]] = None
    duplicate_fields_list: List[Dict[str, Optional[str]]] = field(default_factory=list)

    def is_empty(self) -> bool:
        return self.update_fields is None and not self.duplicate_fields_list


def plan_row_enrichment(segments: ParsedBomSegments) -> RowEnrichmentPlan:
    """
    Decide, from the segments found by `parse_bom_segments()`, what to do to
    ONE existing WIP row (the actual "for every currently-placeholder row of
    this style" fan-out happens in `plan_style_enrichment()`).
    """
    update_fields = (
        extract_enrichment_fields(segments.main_fabric)
        if segments.main_fabric is not None else None
    )
    duplicate_fields_list = [extract_enrichment_fields(d) for d in segments.fabric_list]
    return RowEnrichmentPlan(update_fields=update_fields, duplicate_fields_list=duplicate_fields_list)


# ---------------------------------------------------------------------------
# Whole-style planning (fans a style's WIP rows out into concrete actions)
# ---------------------------------------------------------------------------

@dataclass
class RowAction:
    """One concrete action for the notebook to execute against DTC/Delta."""
    kind: str                       # "update" | "insert"
    row_id: Optional[str] = None    # for "update": the existing WIP row_id
    base_row: Optional[Dict[str, Any]] = None  # for "insert": the row to copy from
    wip_fields: Dict[str, Optional[str]] = field(default_factory=dict)  # raw DTC field names -> values


def plan_style_enrichment(
    existing_rows: List[Dict[str, Any]],
    bom_unified: Any,
    fabric_group_key: str = "fabric_group",
    row_id_key: str = "row_id",
) -> List[RowAction]:
    """
    Plan every action needed to enrich ONE style's existing WIP rows from its
    BOM data. This is the top-level entry point the notebook calls once per
    style that has a matched BOM row.

    Args:
        existing_rows: the style's current WIP rows (one dict per colorway
            row), each containing at least `fabric_group_key` (current
            Fabric Group value) and `row_id_key` (its DTC rowId). Any other
            keys are passed through untouched into `RowAction.base_row` for
            "insert" actions, so the notebook can copy the FULL row when
            creating a genuinely new DTC row.
        bom_unified: the raw `bom_unified` JSON (string or parsed).
        fabric_group_key / row_id_key: lets the notebook pass its own dict
            shape without a separate translation step.

    Returns:
        [] if there's nothing to do (style already enriched, no WIP rows,
        or the BOM has neither "Main Fabric" nor any "Fabric" segments).
        Otherwise, per existing row: at most one `RowAction(kind="update")`
        (only if a "Main Fabric" segment exists) plus one
        `RowAction(kind="insert")` PER "Fabric" segment found (zero or more).
    """
    if not existing_rows:
        return []
    if style_already_enriched([r.get(fabric_group_key) for r in existing_rows]):
        return []

    segments = parse_bom_segments(bom_unified)
    plan = plan_row_enrichment(segments)
    if plan.is_empty():
        return []

    actions: List[RowAction] = []
    for row in existing_rows:
        if plan.update_fields:
            actions.append(RowAction(
                kind="update",
                row_id=row.get(row_id_key),
                wip_fields=to_wip_fields(plan.update_fields),
            ))
        for dup_fields in plan.duplicate_fields_list:
            actions.append(RowAction(
                kind="insert",
                base_row=row,
                wip_fields=to_wip_fields(dup_fields),
            ))
    return actions
