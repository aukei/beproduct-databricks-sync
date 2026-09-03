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

Enrichment logic -- UPSERT semantics (owner spec, revised 2026-09-03; see
AGENTS.md decisions log for the full history including the earlier
all-or-nothing `style_already_enriched` design this replaces):

  * The match key between a BOM segment and an existing DTC WIP row is the
    PAIR (Fabric Group, Mill Fabric Article #) — i.e. these two values
    together identify "this is the same fabric assignment" across runs.
    `Placement` is explicitly EXCLUDED from the match key because it is the
    one field expected to still legitimately change/correct itself over
    time for an otherwise-unchanged material assignment.
  * Per existing WIP row, per run:
      - If the row's current (Fabric Group, Mill Fabric Article #) matches
        one of this style's CURRENT BOM segments (Main Fabric or any
        Fabric segment) exactly: UPSERT — update `Placement` ONLY, and only
        if it actually changed. `Fabric Group`/`Mill Fabric Article #`
        are never blindly re-written once they already match.
      - Else if the row is still un-enriched (blank or the DTC placeholder
        `"MAIN MATERIAL CONTENT"`): apply the "Main Fabric" segment's full
        field set (first-time enrichment — unchanged from the original
        design).
      - Else (the row carries some OTHER real, recognized-as-real value not
        present in the CURRENT BOM data — e.g. a "Fabric" segment that has
        since disappeared from the techpack, or a material someone edited
        by hand in DTC): **leave it COMPLETELY UNTOUCHED.** Phase 10 NEVER
        reverts or blanks existing DTC data just because this run's BOM
        snapshot no longer contains a matching segment — see the next
        bullet for the even more common trigger of this rule.
  * If the style's `bom_unified` is entirely missing/blank THIS RUN, or its
    "Main Fabric" segment itself is absent (parses to no "Main Fabric" at
    all): treat the WHOLE STYLE as "nothing to upsert" and take ZERO
    actions — never revert. Live-confirmed real trigger (2026-09-03):
    switching the source table to `customer_teckpack_style_latest` (see
    below) left `bom_unified` NULL for some previously-BOM-bearing test
    styles (KTB-00016, KTB-00021) — this rule is what keeps their earlier,
    correct Phase 10 enrichment intact rather than silently wiping it.
  * For each "Fabric" segment (0 or more) whose (Fabric Group, Mill Fabric
    Article #) key is NOT already represented by ANY existing row for this
    style: it's genuinely new — duplicate every existing row once per such
    segment (unchanged fan-out shape: N colorway rows x each new segment
    produces N new INSERTs).
  * The value written to `Fabric Group` is the segment's own
    `bom_detail_name` (i.e. literally "Main Fabric" or "Fabric"), NOT
    `material_name`. `Placement` / `Mill Fabric Article #` map from
    `placement` / `material_no`. `Content` (added 2026-09-03) maps from
    `material_name` instead — see `WIP_FIELD_CONTENT`'s comment for why
    Phase 10 writes this itself rather than relying on DTC's own trigger.
  * A BOM segment list with neither "Main Fabric" nor "Fabric" (e.g. only
    "Trim"/"Label"/"Stitch/Seam") is equivalent to "no Main Fabric" above —
    zero actions, never revert.

Source table (changed 2026-09-03, owner spec): reads
`customer_teckpack_style_latest`, NOT `customer_teckpack_style_log` — the
"latest" table pre-resolves the multi-version-per-style history the "log"
table required this module to dedupe itself (`current_version` DESC /
`timestamp_lf_captured` tie-break), guaranteeing at most one row per
(`style_no`, `customer_name`, `customer_department`, `style_season`).
NOTE `customer_department` IS part of that uniqueness key even though it is
a constant, non-null value for KONTOOR ("Wrangler Collaborations") in this
environment — live-confirmed 0 duplicate groups for
`customer_name='KONTOOR'` on the full 4-column key (2026-09-03); the 3-column
key without `customer_department` is ALSO duplicate-free for KONTOOR
specifically today, but the 4-column key is used for correctness since the
column is genuinely part of the table's real uniqueness constraint and other
customers in this shared table are NOT constant on it.

This module holds only the deterministic decision logic (JSON parsing, the
upsert/no-op/insert decision, and mapping to the raw DTC field names for the
eventual PATCH). Notebook orchestration (Spark I/O, DTCConnector PATCH
calls) lives in ``dtc/notebooks/p10_pull_bom_and_enrich.py``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

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
# Added 2026-09-03 (owner spec): "Content" is normally populated by a
# DTC-internal trigger polling Mill Fabric Article #, but that trigger's
# timing/conditions in UAT are unreliable (live-confirmed: every KTB test
# row still blank days after Mill Fabric Article # was set) and was blocking
# Phase 9a's fabric-details completeness filter. For our purposes, Phase 10
# writes "Content" itself from the SAME BOM segment's `material_name` --
# removing that dependency on DTC's own trigger entirely. This is an
# intentional, accepted dual-write (DTC's trigger may also write the same
# cell independently) per explicit owner instruction, unlike the earlier
# Phase 1/Phase 10 Fabric Group conflict, which was an unintentional bug.
WIP_FIELD_CONTENT = "Content"


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
    four fields Phase 10 writes, using the module's own (Delta-agnostic)
    field names — see `to_wip_fields()` for the raw-DTC-field-name mapping
    used for the actual PATCH.

    IMPORTANT: `fabric_group` is the segment's own `bom_detail_name` (i.e.
    literally "Main Fabric" or "Fabric"), NOT `material_name` — corrected
    2026-09-02 per an explicit spec amendment.

    `content` (added 2026-09-03) IS the segment's `material_name` — see
    `WIP_FIELD_CONTENT`'s comment for why Phase 10 writes this itself rather
    than waiting on DTC's own trigger.
    """
    return {
        "fabric_group": detail.get("bom_detail_name"),
        "placement": detail.get("placement"),
        "mill_fabric_article": detail.get("material_no"),
        "content": detail.get("material_name"),
    }


def to_wip_fields(fields: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    """Map `extract_enrichment_fields()`'s output to raw DTC WIP field names,
    ready to merge into a `sheetData` PATCH/INSERT row object."""
    return {
        WIP_FIELD_FABRIC_GROUP: fields.get("fabric_group"),
        WIP_FIELD_PLACEMENT: fields.get("placement"),
        WIP_FIELD_MILL_FABRIC_ARTICLE: fields.get("mill_fabric_article"),
        WIP_FIELD_CONTENT: fields.get("content"),
    }


# ---------------------------------------------------------------------------
# Per-style enrichment decision (upsert semantics — see module docstring)
# ---------------------------------------------------------------------------

def _norm_key_part(v: Any) -> Optional[str]:
    return None if _blank(v) else str(v).strip()


def segment_key(fields: Dict[str, Optional[str]]) -> Tuple[Optional[str], Optional[str]]:
    """
    The (Fabric Group, Mill Fabric Article #) composite match key used to
    identify "the same fabric assignment" across runs. `Placement` is
    deliberately excluded — it's the one field expected to still change for
    an otherwise-unchanged assignment (see module docstring).
    """
    return (
        _norm_key_part(fields.get("fabric_group")),
        _norm_key_part(fields.get("mill_fabric_article")),
    )


def is_unenriched(fabric_group_value: Optional[str]) -> bool:
    """True if a WIP row's current Fabric Group means "never enriched yet""
    -- blank, or still the DTC placeholder."""
    return _blank(fabric_group_value) or str(fabric_group_value).strip() == PLACEHOLDER_FABRIC_GROUP


def build_target_segments(bom_unified: Any) -> Optional[List[Dict[str, Optional[str]]]]:
    """
    Build the ordered list of enrichment-field dicts (module field names —
    see `extract_enrichment_fields`) Phase 10 targets for one style:
    [Main Fabric fields] + [Fabric segment fields, ...] (0 or more).

    Returns None if there is no "Main Fabric" segment at all (BOM missing
    entirely this run, parse failure, or Main Fabric itself absent).
    Callers MUST treat None as "nothing to upsert for this style right
    now" — never as license to revert or blank already-enriched DTC rows.
    """
    segments = parse_bom_segments(bom_unified)
    if segments.main_fabric is None:
        return None
    return [extract_enrichment_fields(segments.main_fabric)] + [
        extract_enrichment_fields(d) for d in segments.fabric_list
    ]


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
    mill_fabric_article_key: str = "mill_fabric_article",
    placement_key: str = "placement",
    row_id_key: str = "row_id",
    content_key: Optional[str] = "content",
) -> List[RowAction]:
    """
    Plan every action needed to upsert ONE style's existing WIP rows from
    its current BOM data. This is the top-level entry point the notebook
    calls once per style that has a matched BOM row. See the module
    docstring for the full upsert-semantics spec; summary:

      - Row already matches a current segment by (Fabric Group, Mill
        Fabric Article #): update `Placement` only if it changed, PLUS
        backfill `Content` if it's currently blank there (added 2026-09-03
        — see below; never overwrites a real existing Content value).
      - Row is still un-enriched (blank/placeholder): apply "Main Fabric"'s
        full field set (first-time enrichment).
      - Row carries some OTHER real value not in the current BOM data
        (a vanished segment, or hand-edited DTC data): left COMPLETELY
        UNTOUCHED — never reverted.
      - No "Main Fabric" segment at all this run (BOM missing/vanished):
        ZERO actions for the whole style — never reverts existing data.
      - Each "Fabric" segment not yet represented by any existing row is
        genuinely new: duplicate every existing row once per such segment.

    Content backfill (added 2026-09-03): a row enriched by an EARLIER
    version of this notebook (before `Content` existed as a target field at
    all) already satisfies the "matches a current segment" branch above and
    would otherwise NEVER get `Content` populated — first-time enrichment
    (the `is_unenriched` branch) is the only OTHER place `Content` is
    written, and that branch is for un-enriched rows only. So the matched
    branch also fills `Content` whenever the row's current value (via
    `content_key`) is blank, sourced from the SAME matched segment's
    `content` field. Pass `content_key=None` to disable this check entirely
    if the caller doesn't track the row's current Content value.

    Args:
        existing_rows: the style's current WIP rows (one dict per colorway
            row), each containing at least `fabric_group_key` (current
            Fabric Group value), `mill_fabric_article_key` (current Mill
            Fabric Article # value), `placement_key` (current Placement
            value), `content_key` (current Content value, or omit/None to
            skip the backfill check), and `row_id_key` (its DTC rowId). Any
            other keys are passed through untouched into `RowAction.
            base_row` for "insert" actions, so the notebook can copy the
            FULL row when creating a genuinely new DTC row.
        bom_unified: the raw `bom_unified` JSON (string or parsed).

    Returns:
        [] if there's nothing to do (no existing rows, or no "Main Fabric"
        segment this run). Otherwise a mix of `RowAction(kind="update")`
        (Placement-only or full-field, per row) and `RowAction(kind=
        "insert")` (one per existing row, per genuinely-new "Fabric"
        segment).
    """
    if not existing_rows:
        return []

    target_segments = build_target_segments(bom_unified)
    if target_segments is None:
        # No Main Fabric this run (BOM missing entirely, or Main Fabric
        # itself vanished) -- never revert existing DTC data. No-op.
        return []
    main_target, fabric_targets = target_segments[0], target_segments[1:]

    def row_key(row: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        return segment_key({
            "fabric_group": row.get(fabric_group_key),
            "mill_fabric_article": row.get(mill_fabric_article_key),
        })

    actions: List[RowAction] = []
    for row in existing_rows:
        rkey = row_key(row)
        matched_target = next(
            (t for t in target_segments if segment_key(t) == rkey), None)
        if matched_target is not None:
            # Already represents this exact segment -- upsert ONLY the
            # fields expected to still legitimately drift or need
            # backfilling, never re-write Fabric Group/Mill Fabric Article #
            # (they're already correct, that's how we matched).
            upsert_fields: Dict[str, Optional[str]] = {}
            if row.get(placement_key) != matched_target.get("placement"):
                upsert_fields[WIP_FIELD_PLACEMENT] = matched_target.get("placement")
            # Content backfill (added 2026-09-03): rows enriched by an
            # earlier version of this notebook (before Content existed as a
            # target field at all) already satisfy the match above and would
            # otherwise NEVER get Content populated, since first-time
            # enrichment (the `is_unenriched` branch below) is the only
            # other place Content is written. Only fills a currently-blank
            # cell -- never overwrites a real existing Content value (e.g.
            # one DTC's own trigger already wrote independently).
            if content_key and _blank(row.get(content_key)):
                content_val = matched_target.get("content")
                if not _blank(content_val):
                    upsert_fields[WIP_FIELD_CONTENT] = content_val
            if upsert_fields:
                actions.append(RowAction(
                    kind="update",
                    row_id=row.get(row_id_key),
                    wip_fields=upsert_fields,
                ))
        elif is_unenriched(row.get(fabric_group_key)):
            # Never-enriched row -- first-time enrichment from Main Fabric.
            actions.append(RowAction(
                kind="update",
                row_id=row.get(row_id_key),
                wip_fields=to_wip_fields(main_target),
            ))
        # else: row carries some OTHER real, unrecognized (Fabric Group,
        # Mill Fabric Article #) combination -- e.g. a "Fabric" segment
        # that's since disappeared, or hand-edited DTC data. NEVER revert
        # or overwrite it; leave completely untouched.

    existing_keys = {row_key(row) for row in existing_rows}
    for target in fabric_targets:
        if segment_key(target) in existing_keys:
            continue  # already represented by some existing row -- no insert needed
        for row in existing_rows:
            actions.append(RowAction(
                kind="insert",
                base_row=row,
                wip_fields=to_wip_fields(target),
            ))

    return actions


# ---------------------------------------------------------------------------
# INSERT row-copy payload construction
# ---------------------------------------------------------------------------

# Columns that must NEVER be copied forward into a new (INSERT) row, on top
# of the always-excluded rowId/rowIndex identity fields.
#
# Live-discovered 2026-09-02: DTC's sheetData PATCH/INSERT endpoint rejects
# any value in an image-type column outright -- "'Style Image' is an image
# field and cannot have data added to it" (HTTP 400) -- even when merely
# copying an existing value forward from the row being duplicated. Images can
# ONLY be set via the separate multipart /images endpoint (Phase 3), never
# via sheetData; this mirrors Phase 1's own long-standing rule that
# STYLE_IMAGE_COL is "never written in Phase 1". New duplicate rows are
# simply created with a blank Style Image cell; `phase3_images` picks them up
# on its next run like any other blank-image row.
INSERT_EXCLUDE_COLS = frozenset({"rowId", "rowIndex", "Style Image"})


def build_insert_row_payload(
    base_fields: Dict[str, Any],
    wip_fields: Dict[str, Optional[str]],
    exclude_cols: Optional[frozenset] = None,
) -> Dict[str, Any]:
    """
    Build the sheetData INSERT payload for a duplicated row: a full copy of
    `base_fields` (the original row's parsed `data_json`), minus
    `exclude_cols` (identity fields that must never be copied, plus any
    write-rejected column like Style Image), with `wip_fields` (the new
    row's own Fabric Group/Placement/Mill Fabric Article # values) applied
    on top.

    `exclude_cols` should normally be `INSERT_EXCLUDE_COLS |
    compute_non_writable_cols(view_dynamic_fields)` (see that function) so
    every column DTC actually rejects a write to is excluded, not just the
    ones hardcoded here. `INSERT_EXCLUDE_COLS` alone is a safe minimum
    fallback (e.g. for unit tests / when view metadata isn't available).
    """
    cols = INSERT_EXCLUDE_COLS if exclude_cols is None else exclude_cols
    new_row = {k: v for k, v in base_fields.items() if k not in cols}
    new_row.update(wip_fields)
    return new_row


def compute_non_writable_cols(dynamic_fields: List[Dict[str, Any]]) -> frozenset:
    """
    Determine which WIP view columns must NEVER be written via the
    sheetData PATCH/INSERT endpoint, from a DTC view's `dynamicFields`
    metadata (`DTCConnector.get_view_definition(view_id)["dynamicFields"]`).

    Live-discovered 2026-09-02 (two separate 400s hit back-to-back while
    fixing the first): DTC's own `isReadOnly` flag is NOT a reliable signal
    for this -- live-confirmed `false` on every field that DTC itself then
    rejected a write to. The two signals that DID reliably predict a
    rejection, checked against the live KTB WIP_ITS_USE view (204 fields):
      - `type == "contact"` -- the view's one image-upload field ("Style
        Image"); images are binary and can ONLY be set via the separate
        multipart /images endpoint (Phase 3), never sheetData. DTC's error:
        "'Style Image' is an image field and cannot have data added to it."
      - a truthy `formula` key -- a computed/derived column. Live-confirmed
        6 such fields in this view: "Fabric Article", "Fabric Mill", and 4
        "<app> - Target Sample Ready Date" fields (Proto/Pre-line/SMS/
        Advertising). DTC's error: "'<field>' is a formula field and cannot
        have data added to it."
    Both error shapes are HTTP 400 from the SAME sheetData PATCH/INSERT
    endpoint, discovered when Phase 10 tried to copy a full existing row
    forward (as the base for a new "Fabric" segment duplicate row) and hit
    each one in turn as the prior one was excluded.
    """
    non_writable = set()
    for f in dynamic_fields:
        name = f.get("fieldName")
        if not name:
            continue
        if f.get("type") == "contact" or f.get("formula"):
            non_writable.add(name)
    return frozenset(non_writable)
