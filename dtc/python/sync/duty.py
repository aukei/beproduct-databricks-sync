"""
Phase 9b — NT Orbit Duty Tools core (pure Python, no Spark / no HTTP).

Fills the ``hts_code`` / ``duty_rate_us`` / ``duty_rate_ca`` / ``duty_rate_mx`` /
``tariff_rate`` gaps on ``lft.beproduct.costing_chart`` (built by Phase 9a,
``dtc/notebooks/p9a_build_costing_chart.py``) by calling the NT Orbit Duty
Tools 3rd-party API (``connectors.nt_orbit.NTOrbitConnector``), then maps the
result back onto the per-vendor-slot DTC WIP columns for pushing via
``connectors.dtc.DTCConnector.patch_rows`` (same PATCH contract as Phase 1).

This module holds only the deterministic, unit-testable decision logic:

  * ``build_product_description`` — Style Description + Content + Gender +
    Class + Sub Class concatenation (costing_chart columns: style_description,
    fabric_content, gender, class_name, sub_class).
  * ``build_calc_request`` — the NT Orbit ``/calcuate/single/`` request body
    for one costing_chart row x one target market (US/CA/MX).
  * ``markets_needing_lookup`` — decides which of the up-to-3 per-row API
    calls (US/CA/MX) are actually needed, so Phase 9b never re-calls NT Orbit
    for a market that's already filled (cost/latency control + "with caching"
    per AGENTS.md).
  * ``extract_duty_fields`` — parses one NT Orbit response into
    {hts_code, duty_rate, tariff_rate}. ``duty_rate`` is the "General Duty"
    line's own rate (NOT ``data.duty_rate``, which is the combined
    duty+tariff+fee total — see module docstring section below for why).
  * ``merge_lookup_into_row`` — applies one market's extracted fields onto a
    costing_chart row dict, producing only the changed columns.
  * ``build_wip_patch_fields`` — maps a costing_chart row's factory_slot +
    filled fields to the corresponding DTC WIP per-slot column names, for the
    Phase 1-style PATCH push. Tariff Rate columns do not exist in the WIP view
    yet (AGENTS.md verified-discoveries log, 2026-07-17) so they are reported
    as skipped rather than silently dropped.

Why ``duty_rate_xx`` = the "General Duty" line's rate, not ``data.duty_rate``
--------------------------------------------------------------------------
The NT Orbit response's top-level ``data.duty_rate`` is the COMBINED rate
across every ``detailed_lines`` entry of type "duty" (General Duty + any
named tariff lines, e.g. Section 301/122) plus fees. Phase 9b keeps
``tariff_rate`` as its own separate costing_chart column (mirroring the DTC
WIP schema, which also has separate "Duty Rate" and "Tariff Rate" columns per
slot), so folding the tariff into ``duty_rate_xx`` as well would double-count
it. ``duty_rate_xx`` is therefore taken from the ``detailed_lines`` entry
named exactly "General Duty"; every additional type="duty" line (tariff line)
is summed separately into ``tariff_rate``.

Per the Phase 9b spec, ``tariff_rate`` is only meaningful for
``import_country_code == "US"`` (Section 301/122 tariffs are US-specific in
the examples given); CA/MX lookups therefore never touch the shared
``tariff_rate`` column, only their own ``duty_rate_ca`` / ``duty_rate_mx``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# costing_chart column -> import_country_code
MARKET_COLUMNS: Dict[str, str] = {
    "duty_rate_us": "US",
    "duty_rate_ca": "CA",
    "duty_rate_mx": "MX",
}

# costing_chart columns concatenated (in order) to build product_description.
# Spec: Style Description (C) + Content (I) + Gender (J) + Class (K) + Sub Class (L).
PRODUCT_DESCRIPTION_COLS: Tuple[str, ...] = (
    "style_description", "fabric_content", "gender", "class_name", "sub_class",
)

GENERAL_DUTY_LINE_NAME = "General Duty"

DEFAULT_DE_MINIMIS = False
DEFAULT_MODE_OF_TRANSPORT = "freight"

# WIP (DTC) column names per factory_slot ("Main" | "1" | "2" | "3"), confirmed
# live 2026-07-17 (see AGENTS.md / docs/costing_interested_fields.txt). Tariff
# Rate columns are listed for forward-compatibility but ARE NOT present in the
# live WIP_ITS_USE view yet — build_wip_patch_fields() reports them as skipped.
WIP_HTS_COL: Dict[str, str] = {
    "Main": "Main Factory HTS Code",
    "1": "Factory 1 - HTS code",
    "2": "Factory 2 - HTS code",
    "3": "Factory 3 - HTS code",
}

WIP_DUTY_COL: Dict[str, Dict[str, str]] = {
    "Main": {
        "US": "Main Factory Duty Rate (US)",
        "CA": "Main Factory Duty Rate (CA)",
        "MX": "Main Factory Duty Rate (MX)",
    },
    "1": {
        "US": "Factory 1 - Duty Rate (US)",
        "CA": "Factory 1 - Duty Rate (CA)",
        "MX": "Factory 1 - Duty Rate (MX)",
    },
    "2": {
        "US": "Factory 2 - Duty Rate (US)",
        "CA": "Factory 2 - Duty Rate (CA)",
        "MX": "Factory 2 - Duty Rate (MX)",
    },
    "3": {
        "US": "Factory 3 - Duty Rate (US)",
        "CA": "Factory 3 - Duty Rate (CA)",
        "MX": "Factory 3 - Duty Rate (MX)",
    },
}

# NOT present in the live WIP_ITS_USE view as of 2026-07-17 — kept here as the
# documented, forward-compatible target names so build_wip_patch_fields() can
# start writing to them the moment DTC adds the columns, without a code change
# beyond flipping WIP_TARIFF_COLS_LIVE to True.
WIP_TARIFF_COL: Dict[str, str] = {
    "Main": "Main Factory Tariff rate",
    "1": "Factory 1 - Tariff rate",
    "2": "Factory 2 - Tariff rate",
    "3": "Factory 3 - Tariff rate",
}
WIP_TARIFF_COLS_LIVE = False


def _blank(v: Any) -> bool:
    """True if a costing_chart cell is null/blank (mirrors phase1.norm's null check
    but avoids importing phase1 just for this one helper)."""
    if v is None:
        return True
    s = str(v).strip()
    return s == "" or s.lower() in {"n/a", "na", "none", "null", "nan"}


# ---------------------------------------------------------------------------
# Request building
# ---------------------------------------------------------------------------

def build_product_description(row: Dict[str, Any]) -> str:
    """
    Concatenate Style Description + Content + Gender + Class + Sub Class
    (costing_chart columns: style_description, fabric_content, gender,
    class_name, sub_class), skipping blank parts, joined by single spaces.
    """
    parts = []
    for col in PRODUCT_DESCRIPTION_COLS:
        v = row.get(col)
        if not _blank(v):
            parts.append(str(v).strip())
    return " ".join(parts)


def build_calc_request(
    row: Dict[str, Any],
    import_country_code: str,
    de_minimis: bool = DEFAULT_DE_MINIMIS,
    mode_of_transport: str = DEFAULT_MODE_OF_TRANSPORT,
) -> Dict[str, Any]:
    """
    Build the NT Orbit ``/calcuate/single/`` request body for one costing_chart
    row targeting one market.

    origin_country_code == export_country_code == costing_chart
    "production_country" (WIP column P), per the Phase 9b spec.
    """
    return {
        "product_description": build_product_description(row),
        "origin_country_code": row.get("production_country"),
        "import_country_code": import_country_code,
        "export_country_code": row.get("production_country"),
        "de_minimis": de_minimis,
        "mode_of_transport": mode_of_transport,
    }


def cache_key(row: Dict[str, Any], import_country_code: str) -> Tuple[str, Optional[str], str]:
    """
    Dedup key for caching NT Orbit calls across costing_chart rows that would
    produce an identical request (same product description + origin + target
    market — e.g. multiple colors of the same style/slot). Callers should keep
    an in-memory ``{cache_key(...): response}`` dict for the duration of one
    Phase 9b run.
    """
    return (
        build_product_description(row),
        row.get("production_country"),
        import_country_code,
    )


# ---------------------------------------------------------------------------
# Lookup-need decision
# ---------------------------------------------------------------------------

def markets_needing_lookup(row: Dict[str, Any]) -> List[str]:
    """
    Return the subset of ["US", "CA", "MX"] that still need an NT Orbit call
    for this costing_chart row: a market needs a call when its own duty_rate
    column is blank. Every NT Orbit response also returns ``hs_code``, so a
    blank ``hts_code`` gets filled as a side effect of whichever market call
    (if any) still needs to run — it does not, by itself, force an otherwise
    fully-filled market to be re-queried.

    A market is skipped entirely when the row has no production_country
    (origin/export country is required by the API and cannot be inferred).
    """
    if _blank(row.get("production_country")):
        return []
    needed = [
        country_code
        for duty_col, country_code in MARKET_COLUMNS.items()
        if _blank(row.get(duty_col))
    ]
    if not needed and _blank(row.get("hts_code")):
        # Rare edge case: every duty_rate_* column is already filled but
        # hts_code somehow still isn't (e.g. manually cleared). Make one US
        # call purely to backfill the HTS code as a side effect.
        needed = ["US"]
    return needed


def row_needs_any_lookup(row: Dict[str, Any]) -> bool:
    return len(markets_needing_lookup(row)) > 0


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

@dataclass
class DutyLookupResult:
    hts_code: Optional[str] = None
    duty_rate: Optional[float] = None      # "General Duty" line's rate only
    tariff_rate: Optional[float] = None    # sum of non-General-Duty "duty" lines
    classification_name: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)


def extract_duty_fields(response: Dict[str, Any]) -> DutyLookupResult:
    """
    Parse one NT Orbit ``/calcuate/single/`` response into
    (hts_code, general_duty_rate, tariff_rate).

    See module docstring for why ``duty_rate`` is the "General Duty" line's
    own rate rather than the response's top-level ``data.duty_rate`` (which is
    the combined duty+tariff+fee total).

    Raises:
        ValueError: if ``response["success"]`` is falsy, or ``data`` is missing.
    """
    if not response.get("success", False):
        raise ValueError(f"NT Orbit call was not successful: {response}")
    data = response.get("data") or {}
    if not data:
        raise ValueError(f"NT Orbit response has no 'data': {response}")

    hts_code = data.get("hs_code") or None
    classification_name = data.get("classification_name") or None

    general_duty_rate: Optional[float] = None
    tariff_rate_sum: Optional[float] = None
    for line in data.get("detailed_lines") or []:
        if line.get("type") != "duty":
            continue  # skip fees (e.g. Harbor Maintenance Fee)
        rate = line.get("rate")
        name = line.get("name") or ""
        if name == GENERAL_DUTY_LINE_NAME:
            general_duty_rate = rate
        else:
            tariff_rate_sum = (tariff_rate_sum or 0.0) + (rate or 0.0)

    return DutyLookupResult(
        hts_code=hts_code,
        duty_rate=general_duty_rate,
        tariff_rate=tariff_rate_sum,
        classification_name=classification_name,
        raw=data,
    )


# ---------------------------------------------------------------------------
# Applying a lookup result back onto a costing_chart row
# ---------------------------------------------------------------------------

def merge_lookup_into_row(
    row: Dict[str, Any],
    import_country_code: str,
    result: DutyLookupResult,
) -> Dict[str, Any]:
    """
    Compute the {column: value} updates for ONE market's lookup result, to be
    applied onto a costing_chart row (e.g. via a Delta MERGE UPDATE SET).

    Only fills columns that are currently blank on the row (never overwrites
    an existing value — mirrors phase1's write-once semantics for default-fill
    columns). ``tariff_rate`` is only ever set from a US lookup (see module
    docstring).

    Args:
        row: the current costing_chart row (dict).
        import_country_code: "US" | "CA" | "MX" — which market this result is for.
        result: parsed NT Orbit response (extract_duty_fields()).

    Returns:
        Dict of only the columns that should change (may be empty).
    """
    updates: Dict[str, Any] = {}

    if _blank(row.get("hts_code")) and result.hts_code:
        updates["hts_code"] = result.hts_code

    duty_col = next(
        (c for c, cc in MARKET_COLUMNS.items() if cc == import_country_code), None
    )
    if duty_col and _blank(row.get(duty_col)) and result.duty_rate is not None:
        updates[duty_col] = result.duty_rate

    if (
        import_country_code == "US"
        and _blank(row.get("tariff_rate"))
        and result.tariff_rate is not None
    ):
        updates["tariff_rate"] = result.tariff_rate

    return updates


# ---------------------------------------------------------------------------
# Mapping filled fields back onto DTC WIP per-slot columns (for the push step)
# ---------------------------------------------------------------------------

@dataclass
class WipPatchPlan:
    fields: Dict[str, Any] = field(default_factory=dict)
    skipped: List[str] = field(default_factory=list)  # columns that can't be written yet


def build_wip_patch_fields(
    factory_slot: str,
    filled_fields: Dict[str, Any],
) -> WipPatchPlan:
    """
    Map a costing_chart row's factory_slot ("Main" | "1" | "2" | "3") plus its
    NEWLY-filled fields (hts_code / duty_rate_us / duty_rate_ca / duty_rate_mx
    / tariff_rate — as produced by merge_lookup_into_row, across one or more
    market lookups) to the corresponding DTC WIP column names, ready to become
    one row object in a ``DTCConnector.patch_rows()`` UPDATE call (the WIP row
    already exists, so this is always an UPDATE keyed by rowId, never an
    INSERT).

    Args:
        factory_slot: "Main" | "1" | "2" | "3" (costing_chart column).
        filled_fields: dict that may contain any of
            {hts_code, duty_rate_us, duty_rate_ca, duty_rate_mx, tariff_rate}.

    Returns:
        WipPatchPlan(fields={<DTC column display name>: value, ...},
                     skipped=[<field names that couldn't be mapped, with reason>]).

    Raises:
        ValueError: if factory_slot is not one of the 4 known slots.
    """
    if factory_slot not in WIP_HTS_COL:
        raise ValueError(f"Unknown factory_slot {factory_slot!r}; expected Main/1/2/3")

    plan = WipPatchPlan()

    if "hts_code" in filled_fields:
        plan.fields[WIP_HTS_COL[factory_slot]] = filled_fields["hts_code"]

    for duty_col, country_code in MARKET_COLUMNS.items():
        if duty_col in filled_fields:
            plan.fields[WIP_DUTY_COL[factory_slot][country_code]] = filled_fields[duty_col]

    if "tariff_rate" in filled_fields:
        if WIP_TARIFF_COLS_LIVE:
            plan.fields[WIP_TARIFF_COL[factory_slot]] = filled_fields["tariff_rate"]
        else:
            plan.skipped.append(
                "tariff_rate: DTC WIP has no 'Tariff Rate' column yet "
                f"(would target {WIP_TARIFF_COL[factory_slot]!r}); value is kept "
                "in costing_chart only until the column exists."
            )

    return plan
