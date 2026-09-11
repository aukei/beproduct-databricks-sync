"""
Phase 10 — BOM enrichment from externally-processed techpack data (pure Python).

Fulfills a Phase 1 gap: BOM (Bill of Materials) data is not available from the
BeProduct API and instead relies on techpack extraction, processed by a
separate pipeline and landed in:

    alb_tpm_uat.public.customer_teckpack_style_log       (UAT)
    alb_tpm_prd.public.customer_teckpack_style_log       (PRD)
    alb_tpm_uat.public.customer_teckpack_style_latest    (UAT)
    alb_tpm_prd.public.customer_teckpack_style_latest    (PRD)

Both catalogs are live-confirmed reachable from this Databricks workspace's
metastore (`SHOW CATALOGS` lists them directly) — no federation/JDBC needed.

**SOURCE CHANGED 2026-09-09 ("2nd revision", owner spec) — supersedes
everything about `bom_unified` below (kept for history).** The BOM developer
pushed back on ever adding new fields to `customer_teckpack_style_latest`
again; new fields (including the ones this module now needs) are added ONLY
to the raw `customer_teckpack_style_log.custom_fields` JSON column. This
module's source of truth for Fabric Group/Placement/Mill Fabric Article #/
Content moved from `customer_teckpack_style_latest.bom_unified` to
`customer_teckpack_style_log.custom_fields` — this **overrides all fields
from BOM_UNIFIED entirely, no fallback** (a style with no populated
`custom_fields.xts_data.TECH_PACK_EXTRACTION.Table[Type="BOM"]` structure
yet gets ZERO enrichment actions this run, exactly like "BOM missing this
run" always meant before — see the "never revert" rule below; it is NOT a
special case). `customer_teckpack_style_latest` is STILL used, but now
ONLY to resolve, per style, WHICH specific log row is current
(`latest_techpack_style_log_id`, a foreign key onto
`customer_teckpack_style_log.teckpack_style_log_id` — live-confirmed exact
column names on both tables 2026-09-09). **Live-confirmed 2026-09-09
(CORRECTED same day — an initial check used the wrong path,
`custom_fields.TECH_PACK_EXTRACTION` instead of the real
`custom_fields.xts_data.TECH_PACK_EXTRACTION`, and wrongly concluded no KTB
style had this data at all):** all 16 current KTB/KONTOOR test styles
DO have `custom_fields.xts_data.TECH_PACK_EXTRACTION.Table` populated, and
14/16 have a real "Main Fabric" segment (`KTB-00016`/`KTB-00021` are the
two exceptions — no Main Fabric segment found, consistent with their
long-standing lack of BOM data under the old `bom_unified` source too).
`**SupplierRefNo` (`mill_fabric_article`) is genuinely BLANK for most of
these real rows (`KTB-00017`..`KTB-00027`) — only `KTB-00028`..`KTB-00031`
have a real, non-blank value there. 2,411 OTHER real rows across other
customers (e.g. "Etam Lingerie") also have this structure live, confirming
the shape below is real and not merely illustrative.

The join itself is UNCHANGED (still `ktb_styles.bp_style_number` /
`ktb_styles.season||" - "||year` against `style_no`/`style_season`, still
INNER JOIN, still pre-filtered by `customer_name` for scoping/perf only —
see `build_style_season()` and the notebook's Step 1): it now targets
`customer_teckpack_style_latest.style_no`/`.style_season` as before, PLUS a
second join hop through `latest_techpack_style_log_id` to fetch
`custom_fields` from `customer_teckpack_style_log`.

``custom_fields`` (on the LOG table, NOT the "latest" table) is a JSON
string/object shaped like (live-confirmed path and structure 2026-09-09,
via 2,411 real non-KTB rows plus the owner-supplied example below):

    {
      "xts_data": {
        "ACTION_CODE": "NEW_TECHPACK", ...,
        "TECH_PACK_EXTRACTION": {
          "Table": [
            {"Seq": 1, "Type": "POM", ...},
            {"Seq": 2, "Type": "BOM",
             "ColumnHeader": ["**BomHeader", "**MaterialCategory",
                 "**MaterialCode", "**MaterialType", "**MaterialDescription",
                 "**Quantity", "**MaterialContent", "**MaterialConstruction",
                 "**MaterialCuttableWidth", "**Placement", ..., "**SupplierRefNo",
                 ..., {"Colorway": [...]}, {"Color": [...]}, ...],
             "Data": [
               ["SLEEVELESS SHIRT", "Main Fabric", "LF-BD26-000002--SH",
                "Sheeting", "WV-0003", "", "Cotton 100%", "", "", "BODICE",
                ..., "WV-0003", ..., {"Colorway": [...]}, {"Color": [...]}, "", ""],
               ["SLEEVELESS SHIRT", "Fabric", "LF-BD26-000004--PN", "Poplin",
                "WV-0061", "", "Cotton 100%", "", "", "HEM", ..., "WV-0061", ...],
               ...
             ]},
            {"Seq": 3, "Type": "Colorway", ...}
          ]
        },
        "DATE": ""
      }
    }

Path: `custom_fields -> xts_data -> TECH_PACK_EXTRACTION -> Table[] ->
(entries where Type == "BOM") -> ColumnHeader (defines each Data row's
column order) + Data (list of row-arrays)`. `ColumnHeader` can ALSO contain
DICT entries (e.g. `{"Colorway": [...]}`, `{"Color": [...]}`) for
per-colorway-column breakdowns this module does not use — a plain-string
`.index()` lookup for our 4 target column names simply never matches those,
so no special-casing is required. If more than one `Type == "BOM"` table
entry exists, ALL of their `Data` rows are concatenated (defensive; only one
has ever been observed live, but nothing in the spec guarantees exactly one).

Only two ``**MaterialCategory`` values matter here: "Main Fabric" and
"Fabric" (UNCHANGED from the old `bom_detail_name` semantics — the OLD
`bom_unified` structure's `bom_detail_name` field and this NEW structure's
`**MaterialCategory` column play the exact same conceptual role: "Main
Fabric" appears exactly once per style by construction; "Fabric" is zero or
more). Other `**MaterialCategory` values (blank, or anything else) are
ignored, matching the old "Stitch/Seam"/"Trim"/"Label" ignore-list behavior.

Enrichment logic -- UPSERT semantics (owner spec, revised 2026-09-03,
mapping source revised again 2026-09-09; see AGENTS.md decisions log for the
full history including the earlier all-or-nothing `style_already_enriched`
design this replaces):

  * The match key between a BOM segment and an existing DTC WIP row is the
    PAIR (Fabric Group, Mill Fabric Article #) — i.e. these two values
    together identify "this is the same fabric assignment" across runs.
    `Placement`/`Content` are explicitly EXCLUDED from the match key because
    they are the fields expected to still legitimately change/correct
    themselves over time for an otherwise-unchanged material assignment.
  * Per existing WIP row, per run:
      - If the row's current (Fabric Group, Mill Fabric Article #) matches
        one of this style's CURRENT BOM segments (Main Fabric or any
        Fabric segment) exactly: UPSERT — update `Placement` and/or
        `Content`, each independently, ONLY if it actually changed.
        `Fabric Group`/`Mill Fabric Article #` are never blindly re-written
        once they already match.
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
  * If the style's `custom_fields` has no populated BOM table THIS RUN (the
    new structure is entirely missing, `xts_data`/`TECH_PACK_EXTRACTION`/
    `Table` is absent, or its "Main Fabric" row itself is absent): NO
    Fabric Group/Placement/Mill Fabric Article #/Content action is taken
    for the whole style — never revert. This is the CURRENT state for
    every existing KTB/KONTOOR test style as of 2026-09-09 (see above) —
    an accepted transition state, not a bug.
  * For each "Fabric" segment (0 or more) whose (Fabric Group, Mill Fabric
    Article #) key is NOT already represented by ANY existing row for this
    style: it's genuinely new — duplicate every existing row once per such
    segment (unchanged fan-out shape: N colorway rows x each new segment
    produces N new INSERTs).
  * **Field mapping (CORRECTED 2026-09-09, "2nd revision" — supersedes both
    the 2026-09-02 and 2026-09-09-morning mappings below):**
      - `Fabric Group`         <- `**MaterialCategory`   (e.g. "Main Fabric"/"Fabric")
      - `Placement`            <- `**Placement`          (e.g. "BODICE"/"HEM"/"LINING")
      - `Mill Fabric Article #`<- `**SupplierRefNo`      (e.g. "WV-0003" -- article-code-shaped)
      - `Content`              <- `**MaterialContent`    (e.g. "Cotton 100%" -- a REAL content
        description this time, unlike the false-start below) — **REINSTATED**
        2026-09-09: Phase 10 writes `Content` again, now from a genuinely
        reliable dedicated column instead of overloading `material_name`.
    (`**MaterialCode` is NOT used for anything — it corresponds to the OLD
    `bom_unified.material_no`, deliberately unused, same as before.)
  * A BOM segment list with neither "Main Fabric" nor "Fabric" (e.g. only
    other/blank `**MaterialCategory` values) is equivalent to "no Main
    Fabric" above — zero actions, never revert.

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

# DUMMY sentinel values that mean "no real BOM data yet" (owner spec,
# 2026-09-11 -- supersedes the old single "MAIN MATERIAL CONTENT" placeholder
# below, kept for history). Phase 1 (sync/phase1.py) stages these two
# constants directly at INSERT time via FIELD_MAPPING/DEFAULT_FILL_COLS
# (Phase 1 has no BOM data of its own to offer); Phase 10 upgrades them to
# real segment data in place the first time a "Main Fabric" segment resolves
# -- see `is_unenriched()` and `plan_style_enrichment()`'s first-time
# enrichment branch. No backward-compatibility concern: all pre-existing live
# DTC WIP data is being purged, so there is no legacy "MAIN MATERIAL CONTENT"
# value left to recognize.
DUMMY_FABRIC_GROUP = "NO TPM BOM"
DUMMY_FABRIC_ARTICLE = "NO TPM BOM"

# The only two **MaterialCategory (formerly bom_detail_name) values Phase 10
# cares about. There is exactly ONE "Main Fabric" per style by construction;
# "Fabric" can be zero or more. Any other value (blank, or anything else) is
# ignored -- matches the old "Stitch/Seam"/"Trim"/"Label" ignore-list
# behavior under the prior bom_unified-based source.
SEGMENT_MAIN_FABRIC = "Main Fabric"
SEGMENT_FABRIC = "Fabric"

# Raw DTC WIP field names (live-confirmed via GET /v1/views/{WIP_ITS_USE view id}
# dynamicFields, 2026-09-02) — NOT the Delta col_* normalized names.
WIP_FIELD_FABRIC_GROUP = "Fabric Group"
WIP_FIELD_PLACEMENT = "Placement"
# Sourced from **SupplierRefNo (2026-09-09 "2nd revision" -- see module
# docstring). Was `material_name` from bom_unified's per-detail dict before
# that (2026-09-09 morning), and `material_no` before that (2026-09-02).
WIP_FIELD_MILL_FABRIC_ARTICLE = "Mill Fabric Article #"
# REINSTATED 2026-09-09 ("2nd revision", owner spec) -- sourced from
# **MaterialContent, a genuinely reliable dedicated content-description
# column in the NEW custom_fields-based source (unlike the briefly-attempted
# 2026-09-03 mapping from bom_unified's overloaded `material_name`, reverted
# 2026-09-09 morning after it was found to push material-CODE-shaped garbage
# for some styles -- see AGENTS.md decisions log for that history). This is
# a genuine field again, not a removed one.
WIP_FIELD_CONTENT = "Content"

# ColumnHeader names in customer_teckpack_style_log.custom_fields's BOM Table
# entry (raw strings, prefixed "**" in the live schema -- see module
# docstring for the exact path and a full example).
COL_MATERIAL_CATEGORY = "**MaterialCategory"   # -> Fabric Group
COL_MATERIAL_CONTENT = "**MaterialContent"     # -> Content
COL_PLACEMENT = "**Placement"                  # -> Placement
COL_SUPPLIER_REF_NO = "**SupplierRefNo"        # -> Mill Fabric Article #


def _blank(v: Any) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    return s == "" or s.lower() in {"n/a", "na", "none", "null", "nan"}


def _values_differ(current: Any, new: Any) -> bool:
    """Diff helper for the Placement/Content upsert checks (added
    2026-09-10): two BLANK values (e.g. DTC's current `None` vs. the
    source's own blank `''`) are never considered "different", even though
    `None != ''` in a plain Python comparison -- this avoids a spurious
    lean-PATCH violation (Ground Rule #6) where every already-blank row
    would otherwise get a wasteful `{"Placement": ""}`-style PATCH every
    run just because the two blank REPRESENTATIONS differ, not their
    actual (both-blank) meaning."""
    if _blank(current) and _blank(new):
        return False
    return current != new


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


def _extract_bom_table_rows(custom_fields: Any) -> List[Dict[str, Optional[str]]]:
    """
    Parse `customer_teckpack_style_log.custom_fields` (a JSON string, or an
    already-parsed dict — accepted for testability) and return one
    normalized dict per raw BOM `Data` row, using the module's own
    (Delta-agnostic) field names:

        {"bom_detail_name": <**MaterialCategory>, "material_name": <**SupplierRefNo>,
         "content": <**MaterialContent>, "placement": <**Placement>}

    Path: `custom_fields -> xts_data -> TECH_PACK_EXTRACTION -> Table[] ->
    (entries where Type == "BOM") -> ColumnHeader + Data`. See the module
    docstring for the full structure and a real example. If more than one
    `Type == "BOM"` table entry exists, ALL of their `Data` rows are
    concatenated.

    Never raises -- any parse failure, blank input, or unexpected shape at
    any point in the path returns `[]` (treated uniformly as "nothing to
    enrich for this style", same as the old bom_unified parser).
    """
    if _blank(custom_fields):
        return []
    try:
        cf = json.loads(custom_fields) if isinstance(custom_fields, str) else custom_fields
    except (json.JSONDecodeError, TypeError, ValueError):
        return []
    if not isinstance(cf, dict):
        return []
    xts_data = cf.get("xts_data")
    if not isinstance(xts_data, dict):
        return []
    tpe = xts_data.get("TECH_PACK_EXTRACTION")
    if not isinstance(tpe, dict):
        return []
    tables = tpe.get("Table")
    if not isinstance(tables, list):
        return []

    def _col_index(cols: List[Any], name: str) -> Optional[int]:
        # cols can contain dict entries (e.g. {"Colorway": [...]}) alongside
        # plain strings; .index() on a string target simply skips those.
        try:
            return cols.index(name)
        except ValueError:
            return None

    def _cell(row: Any, idx: Optional[int]) -> Optional[str]:
        if idx is None or not isinstance(row, list) or idx >= len(row):
            return None
        v = row[idx]
        # Guard against a stray dict-typed cell (shouldn't happen for our 4
        # plain-string target columns, but never let it crash/propagate).
        return v if isinstance(v, str) else None

    rows: List[Dict[str, Optional[str]]] = []
    for t in tables:
        if not isinstance(t, dict) or t.get("Type") != "BOM":
            continue
        cols = t.get("ColumnHeader")
        data = t.get("Data")
        if not isinstance(cols, list) or not isinstance(data, list):
            continue
        i_cat = _col_index(cols, COL_MATERIAL_CATEGORY)
        i_content = _col_index(cols, COL_MATERIAL_CONTENT)
        i_place = _col_index(cols, COL_PLACEMENT)
        i_supref = _col_index(cols, COL_SUPPLIER_REF_NO)
        for row in data:
            rows.append({
                "bom_detail_name": _cell(row, i_cat),
                "material_name": _cell(row, i_supref),
                "content": _cell(row, i_content),
                "placement": _cell(row, i_place),
            })
    return rows


def parse_bom_segments(custom_fields: Any) -> ParsedBomSegments:
    """
    Parse `customer_teckpack_style_log.custom_fields` (see
    `_extract_bom_table_rows()` and the module docstring for the full path
    and structure) and return a `ParsedBomSegments` holding ONLY the
    "Main Fabric" / "Fabric" segments (by `**MaterialCategory`).

    Returns an empty `ParsedBomSegments` (never raises) on any parse
    failure, blank input, or a payload with no matching segments — callers
    treat that uniformly as "nothing to enrich for this style."
    """
    main_fabric: Optional[Dict[str, Any]] = None
    fabric_list: List[Dict[str, Any]] = []
    for detail in _extract_bom_table_rows(custom_fields):
        name = detail.get("bom_detail_name")
        if name == SEGMENT_MAIN_FABRIC and main_fabric is None:
            main_fabric = detail
        elif name == SEGMENT_FABRIC:
            fabric_list.append(detail)
    return ParsedBomSegments(main_fabric=main_fabric, fabric_list=fabric_list)


def extract_enrichment_fields(detail: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """
    Map one BOM detail dict (from `_extract_bom_table_rows()` — "Main
    Fabric" or a "Fabric" segment) to the four fields Phase 10 writes, using
    the module's own (Delta-agnostic) field names — see `to_wip_fields()`
    for the raw-DTC-field-name mapping used for the actual PATCH.

    `fabric_group` <- `**MaterialCategory` (via `bom_detail_name`), NOT the
    old bom_unified `material_name`. `mill_fabric_article` <-
    `**SupplierRefNo` (via `material_name`) — CORRECTED 2026-09-09 "2nd
    revision" (was `bom_unified.material_name` earlier the same day, and
    `bom_unified.material_no` before that). `content` <- `**MaterialContent`
    — REINSTATED 2026-09-09 (was removed entirely earlier the same day; see
    the module docstring's field-mapping section for the full history).
    """
    return {
        "fabric_group": detail.get("bom_detail_name"),
        "placement": detail.get("placement"),
        "mill_fabric_article": detail.get("material_name"),
        "content": detail.get("content"),
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
    -- blank, or still the DUMMY_FABRIC_GROUP sentinel Phase 1 stages at
    INSERT time (see module docstring)."""
    return _blank(fabric_group_value) or str(fabric_group_value).strip() == DUMMY_FABRIC_GROUP


def build_target_segments(custom_fields: Any) -> Optional[List[Dict[str, Optional[str]]]]:
    """
    Build the ordered list of enrichment-field dicts (module field names —
    see `extract_enrichment_fields`) Phase 10 targets for one style:
    [Main Fabric fields] + [Fabric segment fields, ...] (0 or more).

    `custom_fields` is `customer_teckpack_style_log.custom_fields` (see the
    module docstring for the full path/structure) — NOT `bom_unified`
    (source changed 2026-09-09, "2nd revision").

    Returns None if there is no "Main Fabric" segment at all (BOM missing
    entirely this run, parse failure, or Main Fabric itself absent).
    Callers MUST treat None as "nothing to upsert for this style right
    now" — never as license to revert or blank already-enriched DTC rows.
    """
    segments = parse_bom_segments(custom_fields)
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
    custom_fields: Any,
    fabric_group_key: str = "fabric_group",
    mill_fabric_article_key: str = "mill_fabric_article",
    placement_key: str = "placement",
    content_key: str = "content",
    row_id_key: str = "row_id",
    color_key: str = "color",
) -> List[RowAction]:
    """
    Plan every action needed to upsert ONE style's existing WIP rows from
    its current BOM data. This is the top-level entry point the notebook
    calls once per style that has a matched BOM row. See the module
    docstring for the full upsert-semantics spec; summary:

      - Row already matches a current segment by (Fabric Group, Mill
        Fabric Article #): update `Placement` and/or `Content`, each
        independently, only if it changed.
      - Row's Mill Fabric Article # is currently BLANK, but its Fabric
        Group uniquely identifies one current segment (see "Blank Mill
        Fabric Article # backfill" below): treated the same as an exact
        match, PLUS Mill Fabric Article # itself is backfilled.
      - Row is still un-enriched (blank/placeholder): apply "Main Fabric"'s
        full field set (first-time enrichment).
      - Row carries some OTHER real value not in the current BOM data
        (a vanished segment, or hand-edited DTC data): left COMPLETELY
        UNTOUCHED — never reverted.
      - No "Main Fabric" segment at all this run (BOM missing/vanished):
        ZERO actions for the whole style — never reverts existing data.
      - Each "Fabric" segment not yet represented (by an exact match OR a
        blank-article backfill match) by any existing row is genuinely new:
        duplicate every existing row once per such segment.

    `Content` is REINSTATED as a real, upsertable field (2026-09-09, "2nd
    revision" — reverses the same-day-earlier removal; see the module
    docstring's field-mapping section). It is treated exactly like
    `Placement`: excluded from the match KEY (so a Content-only change never
    looks like "a different segment"), but independently diffed/upserted on
    an already-matched row.

    **Blank Mill Fabric Article # backfill (added 2026-09-10, owner spec)**:
    fixes a live-confirmed gap (KTB-00025, legacy code 112358013) — a row
    first-enriched while the source's `**SupplierRefNo` was still blank has
    `mill_fabric_article=None` baked in permanently; once the source is
    later updated with a real `SupplierRefNo`, the exact-match key
    (`Fabric Group`, `None`) vs. (`Fabric Group`, `"WV-0003"`) never matches
    again, so the row was previously stuck in the "leave untouched" branch
    FOREVER, and the real segment would ALSO get wastefully re-inserted as a
    brand-new duplicate row (since it looked "unrepresented"). Fixed via a
    one-way (blank -> real only, NEVER real -> different) backfill: a row
    with a currently-blank `mill_fabric_article` is matched to a target
    sharing its exact `Fabric Group`, disambiguated by `Placement` when more
    than one target shares that `Fabric Group` (e.g. two "Fabric" segments);
    if the match is still ambiguous after that (multiple candidates share
    both Fabric Group AND Placement), NO backfill is attempted (never guess
    wrong) and the row falls through to "leave untouched" as before. A
    successfully backfilled target is also excluded from the "genuinely
    new -> insert" fan-out below, so it's fixed in place rather than both
    backfilled AND duplicated.

    **Segment-coverage decisions are scoped PER COLORWAY (fixed 2026-09-10,
    owner spec) — fixes a live-confirmed gap (KTB-00029, LF Style#
    LFBP-1WTP0002).** `existing_rows` may span MULTIPLE colorways of the
    same style (the notebook groups WIP rows by `bp_style_number` only, not
    by color). Before this fix, "is this Fabric segment already
    represented?" was checked GLOBALLY across every colorway's rows
    combined — so once ANY ONE colorway's rows happened to satisfy a
    segment (e.g. because that colorway existed and got enriched earlier),
    EVERY OTHER colorway was wrongly treated as "already covered" too, and
    never got its own copy of that segment inserted. Live-confirmed real
    trigger: `KTB-00029` has colors "Earthy Hours" (existed early, got all
    3 segments: Main Fabric + 2 Fabric) and "Early Hours" (added later, as
    a fresh placeholder row) — by the time "Early Hours" was first-time
    enriched from Main Fabric, both Fabric targets were already "claimed"
    by "Earthy Hours"'s rows, so "Early Hours" never got its own Fabric
    segment rows inserted (BeProduct: 2 colors x 3 materials = 6 expected
    DTC rows; actual: only 4 — "Early Hours" stuck at 1). Fixed by grouping
    `existing_rows` by `color_key` internally and running the ENTIRE
    per-row match/backfill/insert decision tree independently PER color
    group — each colorway now gets its own full segment coverage,
    regardless of what any other colorway already has. Rows without a
    `color_key` value (or when the style genuinely has only one color) all
    fall into a single implicit group — identical to the pre-fix behavior,
    so this is backward compatible for single-color styles/callers that
    don't track color.

    Args:
        existing_rows: the style's current WIP rows (one dict per colorway
            row, POSSIBLY SPANNING MULTIPLE COLORS), each containing at
            least `fabric_group_key` (current Fabric Group value),
            `mill_fabric_article_key` (current Mill Fabric Article # value),
            `placement_key` (current Placement value), `content_key`
            (current Content value), `color_key` (its colorway, used to
            scope segment-coverage decisions independently per color — see
            above), and `row_id_key` (its DTC rowId). Any other keys are
            passed through untouched into `RowAction.base_row` for "insert"
            actions, so the notebook can copy the FULL row when creating a
            genuinely new DTC row.
        custom_fields: `customer_teckpack_style_log.custom_fields` (raw
            JSON string or already-parsed) — NOT `bom_unified` (source
            changed 2026-09-09, "2nd revision"; see module docstring).

    Returns:
        [] if there's nothing to do (no existing rows, or no "Main Fabric"
        segment this run). Otherwise a mix of `RowAction(kind="update")`
        (Placement/Content-only or full-field, per row) and `RowAction(
        kind="insert")` (one per existing row, per genuinely-new "Fabric"
        segment).
    """
    if not existing_rows:
        return []

    target_segments = build_target_segments(custom_fields)
    if target_segments is None:
        # No Main Fabric this run (BOM missing entirely, or Main Fabric
        # itself vanished) -- never revert existing Fabric Group/Placement/
        # Mill Fabric Article #/Content data.
        return []
    main_target, fabric_targets = target_segments[0], target_segments[1:]

    def row_key(row: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        return segment_key({
            "fabric_group": row.get(fabric_group_key),
            "mill_fabric_article": row.get(mill_fabric_article_key),
        })

    def find_backfill_target(row: Dict[str, Any]) -> Optional[Dict[str, Optional[str]]]:
        """A row with a currently-blank Mill Fabric Article # is matched to
        the unique target sharing its Fabric Group, disambiguated by
        Placement if more than one target shares that Fabric Group. Returns
        None (no backfill) if there's no candidate, or if it's still
        ambiguous after the Placement tie-break -- never guess wrong."""
        if not _blank(row.get(mill_fabric_article_key)):
            return None
        row_fg = _norm_key_part(row.get(fabric_group_key))
        candidates = [t for t in target_segments
                      if _norm_key_part(t.get("fabric_group")) == row_fg]
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) > 1:
            row_pl = _norm_key_part(row.get(placement_key))
            placement_matches = [t for t in candidates
                                  if _norm_key_part(t.get("placement")) == row_pl]
            if len(placement_matches) == 1:
                return placement_matches[0]
        return None

    # Group existing rows by colorway so segment-coverage decisions (has this
    # Fabric segment already been inserted?) are scoped PER COLOR, not
    # globally across every color combined -- see docstring above. Rows
    # without a color_key value (or a style with only one color) all land in
    # a single implicit group (key None), identical to pre-fix behavior.
    rows_by_color: Dict[Any, List[Dict[str, Any]]] = {}
    for row in existing_rows:
        rows_by_color.setdefault(row.get(color_key), []).append(row)

    actions: List[RowAction] = []
    for rows_for_color in rows_by_color.values():
        claimed_target_keys: set = set()  # segment_key() of every target
                                           # already matched within THIS color
        for row in rows_for_color:
            rkey = row_key(row)
            matched_target = next(
                (t for t in target_segments if segment_key(t) == rkey), None)
            backfill_article: Optional[str] = None
            if matched_target is None:
                matched_target = find_backfill_target(row)
                if matched_target is not None:
                    backfill_article = matched_target.get("mill_fabric_article")

            if matched_target is not None:
                claimed_target_keys.add(segment_key(matched_target))
                # Already represents this segment (exactly, or via blank-
                # article backfill) -- upsert Placement/Content (the fields
                # expected to still legitimately drift) and, if this was a
                # backfill match, Mill Fabric Article # itself. Never
                # re-write Fabric Group.
                #
                # ONE-WAY ONLY for Placement/Content (added 2026-09-10, owner
                # spec: "make sure no steps incidentally overwrite <blank> on
                # content field") -- a BLANK target value is NEVER pushed,
                # even if it differs from the row's current (possibly REAL)
                # value. Without this guard, a genuinely-blank source segment
                # (live-confirmed real case: KTB-00024/KTB-00026's Main
                # Fabric `**MaterialContent` is `''` at the source, while
                # their DTC WIP cells hold a real manually-entered value)
                # would silently overwrite that real value with an empty
                # string on the very next Phase 10 run -- the exact
                # "never revert" violation this pipeline is designed to
                # avoid everywhere else (DEFAULT_FILL_COLS, Phase 2's
                # push_blanks default, the whole-style "no Main Fabric this
                # run -> zero actions" rule, etc.). A target value that IS
                # non-blank still upserts normally, including a real value
                # replacing a DIFFERENT real value.
                upsert_fields: Dict[str, Optional[str]] = {}
                _new_placement = matched_target.get("placement")
                _new_content = matched_target.get("content")
                if not _blank(_new_placement) and _values_differ(row.get(placement_key), _new_placement):
                    upsert_fields[WIP_FIELD_PLACEMENT] = _new_placement
                if not _blank(_new_content) and _values_differ(row.get(content_key), _new_content):
                    upsert_fields[WIP_FIELD_CONTENT] = _new_content
                if backfill_article is not None:
                    upsert_fields[WIP_FIELD_MILL_FABRIC_ARTICLE] = backfill_article
                if upsert_fields:
                    actions.append(RowAction(
                        kind="update",
                        row_id=row.get(row_id_key),
                        wip_fields=upsert_fields,
                    ))
            elif is_unenriched(row.get(fabric_group_key)):
                # Never-enriched row -- first-time enrichment from Main
                # Fabric. `Fabric Group`/`Mill Fabric Article #` are always
                # written here (that's the whole point of first-time
                # enrichment). `Placement`/`Content` get the SAME one-way
                # guard as the matched branch above (added 2026-09-10): even
                # though `Fabric Group` is still the placeholder, the row
                # could independently already carry a REAL Placement/Content
                # value (e.g. DTC's own trigger, or a manual edit made before
                # Phase 10 ever enriched Fabric Group) -- never let a blank
                # target value clobber that.
                _wip_fields = to_wip_fields(main_target)
                if _blank(main_target.get("content")) and not _blank(row.get(content_key)):
                    _wip_fields.pop(WIP_FIELD_CONTENT, None)
                if _blank(main_target.get("placement")) and not _blank(row.get(placement_key)):
                    _wip_fields.pop(WIP_FIELD_PLACEMENT, None)
                actions.append(RowAction(
                    kind="update",
                    row_id=row.get(row_id_key),
                    wip_fields=_wip_fields,
                ))
            # else: row carries some OTHER real, unrecognized (Fabric Group,
            # Mill Fabric Article #) combination -- e.g. a "Fabric" segment
            # that's since disappeared, or hand-edited DTC data. NEVER
            # revert or overwrite it; leave completely untouched.

        for target in fabric_targets:
            if segment_key(target) in claimed_target_keys:
                continue  # already represented within THIS color -- no insert needed
            for row in rows_for_color:
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
