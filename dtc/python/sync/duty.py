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
  * ``build_calc_request`` — the NT Orbit ``/calculate/single/`` request body
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
  * ``build_wip_patch_fields`` — maps a costing_chart row's supplier_type
    (the "Main"|"1"|"2"|"3" slot flag, renamed from factory_slot 2026-09-01)
    + filled fields to the corresponding DTC WIP per-slot column names, for the
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
from datetime import timezone
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

# Every costing_chart column that can hold a duty VALUE to push to DTC WIP.
# Derived from MARKET_COLUMNS so the market list cannot drift between the
# lookup logic and the push. Promoted here 2026-09-14 (v2): this was a
# notebook-local constant in p9b2_push_duty_to_wip.py, and sync/wip_plan.py
# now needs the same list -- one definition, not two.
DUTY_VALUE_FIELDS: Tuple[str, ...] = (
    ("hts_code",) + tuple(MARKET_COLUMNS) + ("tariff_rate",)
)

# costing_chart columns concatenated (in order) to build product_description.
# Spec: Style Description (C) + Content (I) + Gender (J) + Class (K) + Sub Class (L).
# "color_name" added 2026-09-07 (owner spec) -- REVERSES the earlier design
# intent (see cache_key()'s docstring, which used to say "multiple colors of
# the same style/slot" should share one cache entry/API call, since color
# doesn't affect HS classification). Owner decision: a NEW row landing on
# costing_chart should only reuse a previous result when style_description,
# color_name, fabric_content, gender, class_name, AND sub_class all match
# exactly -- different colors of an otherwise-identical style now get their
# OWN NT Orbit call/cache entry, never share one. Since PRODUCT_DESCRIPTION_
# COLS feeds BOTH the actual NT Orbit request text (build_product_
# description) AND the persistent cache key (cache_key(), derived from the
# same description string), this single change achieves both at once.
PRODUCT_DESCRIPTION_COLS: Tuple[str, ...] = (
    "style_description", "color_name", "fabric_content", "gender", "class_name", "sub_class",
)

GENERAL_DUTY_LINE_NAME = "General Duty"

DEFAULT_DE_MINIMIS = False
DEFAULT_MODE_OF_TRANSPORT = "freight"

# Persistent cross-run cache key columns (see docstring below / the
# lft.beproduct.nt_orbit_duty_cache table used by
# dtc/notebooks/p9b_fill_duty_rates.py). Each NT Orbit call is ~30s and
# costing_chart is FULLY OVERWRITTEN by every Phase 9a run, so without a
# cache that survives ACROSS runs (not just within one), every daily run
# would re-look-up every row from scratch. Same (product_description,
# origin_country, import_country) -> same duty/tariff/HTS result, so this is
# cached indefinitely (subject to CACHE_TTL_DAYS below, since tariff policy
# can genuinely change over time, e.g. Section 301/122 rate changes).
DUTY_CACHE_KEY_COLS: Tuple[str, str, str] = (
    "product_description", "origin_country_code", "import_country_code",
)

# ``costing_chart``'s own MERGE/match key (moved here 2026-09-07 from a
# previously-duplicated local constant in p9b1_compute_duty_rates.py, so
# p9a_build_costing_chart.py can share the identical definition -- see next
# use). "material_no" (2026-09-03) disambiguates Phase 10's Main-Fabric +
# Fabric-segment duplicate rows; "supplier_type" ("Main"|"1"|"2"|"3",
# generated from WIP structure) disambiguates the 4 transposed vendor slots.
#
# **IMPORTANT for any SQL/Spark join or MERGE using this key (fixed
# 2026-09-10, live-confirmed real bug in both current call sites)**:
# `lf_style_no` (and in principle any other column here) can be genuinely
# NULL for a real style. Standard SQL/Spark equality (`t.c = s.c`, or
# PySpark's `.join(other, on=[col_list])` shorthand) treats `NULL = NULL`
# as NULL, never TRUE -- so a row with a NULL key column silently never
# matches its own counterpart, with no error and no log line pointing at
# it. Any join/MERGE keyed on `COSTING_KEY` MUST use NULL-safe equality
# (Spark SQL's `<=>` operator, or PySpark's `.eqNullSafe()`) instead of
# plain `=`/the `on=[list]` shorthand. `p9b2_push_duty_to_wip.py`'s own
# WIP-row lookup is unaffected by this class of bug because it uses a
# plain Python dict keyed on a tuple, where `None == None` is `True`.
COSTING_KEY: Tuple[str, ...] = (
    "customer", "season_code", "brand", "bp_style_no", "lf_style_no",
    "color_name", "lineplan_ref", "material_no", "supplier_type", "supplier", "factory",
)

# How long a cached lookup is trusted before being treated as stale and
# re-queried. Tariffs/duty rates DO change (trade policy shifts), so this is
# not cached forever - but a several-month TTL avoids re-paying the ~30s/call
# cost every single day for data that hasn't changed. Override via the
# notebook's `cache_ttl_days` widget.
DEFAULT_CACHE_TTL_DAYS = 180

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

# Tariff columns. NOT present in the live WIP_ITS_USE view as of 2026-07-17 —
# kept here as the documented, forward-compatible target names.
#
# **STALE AS OF 2026-09-17 — needs an owner decision before flipping.** DTC has
# since added a tariff column for the Main slot, but under a DIFFERENT name than
# assumed here:
#
#     live:     "Main Factory Tariff"        (Delta col_Main_Factory_Tariff)
#     assumed:  "Main Factory Tariff rate"   <- does not exist
#
# and for the Main slot ONLY — there is still no Factory 1/2/3 tariff column.
# Two of 60 WIP rows already carry a value in it, written by a human, not by
# this pipeline (we have never written the column). So enabling the push is NOT
# a pure no-op: it would start overwriting hand-entered values.
#
# To enable: correct "Main" below to "Main Factory Tariff", flip
# WIP_TARIFF_COLS_LIVE, and decide what should happen to the Factory 1/2/3
# slots, which must keep reporting as skipped because they have no column at
# all. Until then tariff_rate stays in costing_chart only — which is WHY it
# needs its own carry-forward in p9a Step 4b while hts_code/duty_rate_* do not:
# they have a live WIP column to be re-read from, and tariff_rate does not.
WIP_TARIFF_COL: Dict[str, str] = {
    "Main": "Main Factory Tariff rate",
    "1": "Factory 1 - Tariff rate",
    "2": "Factory 2 - Tariff rate",
    "3": "Factory 3 - Tariff rate",
}
WIP_TARIFF_COLS_LIVE = False


def is_blank(v: Any) -> bool:
    """True if a costing_chart cell is null/blank (mirrors phase1.norm's null check
    but avoids importing phase1 just for this one helper).

    Public because p9a_build_costing_chart.py's Step 4c needs the IDENTICAL
    notion of "was this cell already populated" to tell a forced overwrite
    apart from an ordinary blank-fill. A second definition there would be free
    to drift from this one, and the whole force_refresh_duty path is defined in
    terms of blank-vs-not.
    """
    if v is None:
        return True
    s = str(v).strip()
    return s == "" or s.lower() in {"n/a", "na", "none", "null", "nan"}


# Internal shorthand -- this module used `_blank` throughout before the helper
# was made public, and the short name reads better at its many call sites.
_blank = is_blank


def _same_value(current: Any, new: Any, numeric: bool) -> bool:
    """
    True if a stored costing_chart value and a freshly-fetched one are the same
    duty answer. Used only by `merge_lookup_into_row(force=True)`, so that a
    forced refresh still emits ONLY genuine changes.

    `numeric` must be True for `duty_rate_*`/`tariff_rate` and False for
    `hts_code`, because the two compare differently:

    * duty rates are DOUBLE in Delta but arrive from JSON, so 0.067 has to
      equal "0.067" and 0.32 has to equal 0.320 — a string compare would
      report spurious changes on every forced run.
    * an HTS code is a STRING whose leading zeros are significant ("0101210010"
      is chapter 1, not chapter 101). Comparing it as a float would treat
      "06206900040" and "6206900040" as the same code and silently suppress a
      real correction, so it is always compared as text.
    """
    if current is None or new is None:
        return current is new
    if numeric:
        try:
            return float(current) == float(new)
        except (TypeError, ValueError):
            pass
    return str(current).strip() == str(new).strip()


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
    Build the NT Orbit ``/calculate/single/`` request body for one costing_chart
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
    market). This is the SAME key used for both the in-run dict cache AND the
    persistent ``nt_orbit_duty_cache`` Delta table (its 3-column primary key
    is ``DUTY_CACHE_KEY_COLS``, in this same order) — the two are meant to be
    used together: seed the in-run cache from the persistent table at the
    start of a run, and write new/refreshed entries back to the persistent
    table at the end, so a cost-visible NT Orbit call is only ever made once
    per unique key, EVER (until it goes stale — see DEFAULT_CACHE_TTL_DAYS),
    not once per run.

    A NEW row reuses a previous result only when `style_description`,
    `color_name`, `fabric_content`, `gender`, `class_name`, AND `sub_class`
    all match exactly (owner spec, 2026-09-07 — REVERSES the earlier design
    intent, which deliberately shared one cache entry across "multiple
    colors of the same style/slot" since color doesn't affect HS
    classification; different colors of an otherwise-identical style now
    each get their OWN lookup/cache entry). See `PRODUCT_DESCRIPTION_COLS`.
    """
    return (
        build_product_description(row),
        row.get("production_country"),
        import_country_code,
    )


def _as_naive_utc(dt: Any) -> Any:
    """
    Normalize a datetime to naive UTC (strip tzinfo, converting first if it
    was aware). Both call sites here are conceptually always UTC (the
    notebook's `now = datetime.now(timezone.utc)`, and `looked_up_at` was
    written from that same `now`) — but Spark's TIMESTAMP columns come back
    as NAIVE datetimes via `.asDict()`/`collect()` (no tzinfo at all), while
    a fresh `datetime.now(timezone.utc)` is AWARE. Subtracting one aware and
    one naive datetime raises `TypeError: can't subtract offset-naive and
    offset-aware datetimes` (confirmed live 2026-09-01) — normalize both to
    naive UTC before comparing so it works regardless of which side (if
    either) happens to carry tzinfo.
    """
    if getattr(dt, "tzinfo", None) is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def is_cache_entry_stale(
    looked_up_at: Optional[Any],
    now: Any,
    ttl_days: int = DEFAULT_CACHE_TTL_DAYS,
    force: bool = False,
) -> bool:
    """
    True if a persistent cache entry is old enough to be re-queried rather
    than trusted (tariff/duty rates can genuinely change over time).

    Args:
        looked_up_at: a datetime (or None — treated as stale so a corrupt/
            missing timestamp always errs on the side of re-querying).
            May be naive or aware; normalized internally (see _as_naive_utc).
        now: a datetime to compare against (pass the run's `now`, not
            datetime.now(), so this is deterministic/testable). May be
            naive or aware.
        ttl_days: cache lifetime in days.
        force: `force_refresh_duty` — treat EVERY entry as stale, so the
            caller re-queries NT Orbit instead of trusting the cache. This is
            what makes a forced refresh actually reach the API; without it a
            forced run would re-apply the same cached answer it is trying to
            replace. See `markets_needing_lookup`.
    """
    if force:
        return True
    if looked_up_at is None:
        return True
    age = _as_naive_utc(now) - _as_naive_utc(looked_up_at)
    try:
        age_days = age.total_seconds() / 86400.0
    except AttributeError:
        return True
    return age_days > ttl_days


def build_cache_row(
    key: Tuple[str, Optional[str], str],
    result: "DutyLookupResult",
    looked_up_at: Any,
) -> Dict[str, Any]:
    """
    Build one row for the persistent ``nt_orbit_duty_cache`` table from a
    cache key + its NT Orbit lookup result, ready to write via a Spark
    MERGE (see dtc/notebooks/p9b_fill_duty_rates.py).
    """
    description, origin_country_code, import_country_code = key
    return {
        "product_description": description,
        "origin_country_code": origin_country_code,
        "import_country_code": import_country_code,
        "hts_code": result.hts_code,
        "duty_rate": result.duty_rate,
        "tariff_rate": result.tariff_rate,
        "classification_name": result.classification_name,
        "looked_up_at": looked_up_at,
    }


def cache_row_to_result(cache_row: Dict[str, Any]) -> "DutyLookupResult":
    """Reconstruct a DutyLookupResult from a persistent-cache row (dict)."""
    return DutyLookupResult(
        hts_code=cache_row.get("hts_code"),
        duty_rate=cache_row.get("duty_rate"),
        tariff_rate=cache_row.get("tariff_rate"),
        classification_name=cache_row.get("classification_name"),
        raw={},
    )


# ---------------------------------------------------------------------------
# Lookup-need decision
# ---------------------------------------------------------------------------

def markets_needing_lookup(row: Dict[str, Any], force: bool = False) -> List[str]:
    """
    Return the subset of ["US", "CA", "MX"] that still need an NT Orbit call
    for this costing_chart row: a market needs a call when its own duty_rate
    column is blank. Every NT Orbit response also returns ``hs_code``, so a
    blank ``hts_code`` gets filled as a side effect of whichever market call
    (if any) still needs to run — it does not, by itself, force an otherwise
    fully-filled market to be re-queried.

    A market is skipped entirely when the row has no production_country
    (origin/export country is required by the API and cannot be inferred).

    ``force`` (= the ``force_refresh_duty`` job parameter) returns EVERY
    market regardless of what the row already holds. This is the entry point
    of the whole forced-refresh path and exists because the blank-check here
    is what makes an outdated rate permanently invisible: a market that is
    already filled is never queried, so its cache entry is never even looked
    at, so `DEFAULT_CACHE_TTL_DAYS` can never expire for it. Live-diagnosed
    2026-09-17 — ageing every `looked_up_at` to a year ago produced zero API
    calls, precisely because no market was ever requested. Pair it with
    ``merge_lookup_into_row(force=True)``, or the fresh answer is fetched and
    then discarded by the blank-check there.

    US is ALSO independently re-queried when `tariff_rate` is still blank,
    even if `duty_rate_us` is already filled (fixed 2026-09-07,
    live-discovered bug): `tariff_rate` is only ever set from a US-market
    response, but before this fix a market was considered "done" purely
    based on its own `duty_rate_*` column. Since `p9a_build_costing_chart.py`
    re-adopts `duty_rate_us`/`hts_code` from the live WIP row as a
    "fallback" on every rebuild (so they're essentially NEVER blank once
    pushed there once), the US market was permanently treated as
    already-done and `tariff_rate` — which has no WIP fallback at all — could
    never be (re-)computed again, even when it was genuinely still blank.
    """
    if _blank(row.get("production_country")):
        return []
    if force:
        # Every market, unconditionally. Order matters only for determinism.
        return [cc for _, cc in MARKET_COLUMNS.items()]
    needed = [
        country_code
        for duty_col, country_code in MARKET_COLUMNS.items()
        if _blank(row.get(duty_col))
    ]
    if "US" not in needed and _blank(row.get("tariff_rate")):
        needed.append("US")
    if not needed and _blank(row.get("hts_code")):
        # Rare edge case: every duty_rate_* column is already filled but
        # hts_code somehow still isn't (e.g. manually cleared). Make one US
        # call purely to backfill the HTS code as a side effect.
        needed = ["US"]
    return needed


def row_needs_any_lookup(row: Dict[str, Any], force: bool = False) -> bool:
    return len(markets_needing_lookup(row, force=force)) > 0


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
    Parse one NT Orbit ``/calculate/single/`` response into
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
    force: bool = False,
) -> Dict[str, Any]:
    """
    Compute the {column: value} updates for ONE market's lookup result, to be
    applied onto a costing_chart row (e.g. via a Delta MERGE UPDATE SET).

    Only fills columns that are currently blank on the row (never overwrites
    an existing value — mirrors phase1's write-once semantics for default-fill
    columns). ``tariff_rate`` is only ever set from a US lookup (see module
    docstring).

    With ``force`` (= ``force_refresh_duty``) an existing value IS overwritten.
    All three fields are then governed by exactly one rule — "take the fetched
    value when there is one" — applied identically to hts_code, the market's
    duty_rate_* and tariff_rate. A forced refresh still never CLEARS a field:
    a None in the result means the API did not return that line, which is
    indistinguishable from a partial failure, so the old value stands. The one
    case this cannot express is a tariff that has been genuinely REMOVED
    upstream (no tariff line at all parses to None, not 0.0); clear
    `tariff_rate` by hand if that happens.

    Args:
        row: the current costing_chart row (dict).
        import_country_code: "US" | "CA" | "MX" — which market this result is for.
        result: parsed NT Orbit response (extract_duty_fields()).
        force: overwrite non-blank values instead of skipping them.

    Returns:
        Dict of only the columns that should change (may be empty).
    """
    updates: Dict[str, Any] = {}

    def _take(col: str, value: Any) -> None:
        """One rule for all three fields: write when we have something to
        write, and (unless forced) only into a blank cell."""
        if value is None or value == "":
            return
        if _blank(row.get(col)):
            updates[col] = value
        elif force and not _same_value(row.get(col), value, numeric=col != "hts_code"):
            # Forced, and it genuinely differs. Equal values are dropped so a
            # forced run still reports (and MERGEs) only real changes.
            updates[col] = value

    # hts_code is a SINGLE costing_chart column (and a single DTC WIP column,
    # "… Factory HTS Code") but every market returns its OWN code -- they are
    # different tariff schedules, and they genuinely differ. Live, 2026-09-17:
    #   US 6206403035 | CA 6206400000 | MX 61062099   (one product)
    # Without force, the first market in MARKET_COLUMNS order (US) fills it and
    # the blank-check makes CA/MX no-ops, so US wins by construction. Under
    # force that accident disappears and MX -- the last market applied -- would
    # silently win, pushing a Mexican code into the WIP HTS column. So US is
    # made the explicit owner: CA/MX may only fill a BLANK hts_code, which is
    # what still happens for a row that has no US lookup at all.
    if import_country_code == "US" or is_blank(row.get("hts_code")):
        _take("hts_code", result.hts_code)

    duty_col = next(
        (c for c, cc in MARKET_COLUMNS.items() if cc == import_country_code), None
    )
    if duty_col:
        _take(duty_col, result.duty_rate)

    # Still US-only: tariff_rate is only ever populated from a US response
    # (Section 301/122 are US-specific), NOT a different write rule.
    if import_country_code == "US":
        _take("tariff_rate", result.tariff_rate)

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
    Map a costing_chart row's slot value ("Main" | "1" | "2" | "3" — the
    costing_chart column is named ``supplier_type``, renamed from
    ``factory_slot`` 2026-09-01 per the original spec's "Supplier Type -
    Generated from Master Chart data"; this function's own parameter name is
    unaffected, it's purely an internal detail) plus its NEWLY-filled fields
    (hts_code / duty_rate_us / duty_rate_ca / duty_rate_mx / tariff_rate — as
    produced by merge_lookup_into_row, across one or more market lookups) to
    the corresponding DTC WIP column names, ready to become one row object in
    a ``DTCConnector.patch_rows()`` UPDATE call (the WIP row already exists,
    so this is always an UPDATE keyed by rowId, never an INSERT).

    Args:
        factory_slot: "Main" | "1" | "2" | "3" (from costing_chart's
            ``supplier_type`` column).
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
