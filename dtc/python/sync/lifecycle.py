"""
Style/WIP-row lifecycle gating (pure Python, no Spark / no HTTP)
==================================================================

Owner spec, 2026-09-08 — resolves a live-discovered bug found during a
full-pipeline conflict scan (see AGENTS.md decisions log): `EXCLUDED_STATUSES`
used to filter `ktb_styles`'s OWN write payload in `p1p7_beproduct_style_sync.py`,
so a style transitioning to a terminal Product Status actually DISAPPEARED
from `ktb_styles` entirely on the next full sync, rather than staying there
("full picture") and simply being excluded from DTC push once fully synced.

Two independent lifecycle signals, both DTC WIP-row-scoped:

1. **BeProduct `Product Status`** (mirrored onto DTC's own "Product Status"
   WIP column by Phase 1). Terminal values: `EXCLUDED_STATUSES` = `Finalized`,
   `Drop`. Behavior (owner spec):
     - While a style's Product Status is terminal AND the WIP row's OWN
       current "Product Status" does NOT YET match it (still an older/active
       value, or the WIP row doesn't exist at all) — SYNC IT ONE LAST TIME
       (so the terminal status itself gets pushed, along with anything else
       that may have changed) — see `should_include_in_staging()`.
     - Once the WIP row's "Product Status" matches the terminal BeProduct
       status, EVERY future sync leaves that WIP row alone entirely (no more
       Phase 1 pushes) — see `should_include_in_staging()` again returning
       `False` once caught up.
     - If a style REACTIVATES (BeProduct status moves back to a non-terminal
       value after having been Finalized/Drop), syncing resumes exactly like
       any other active style — no special-casing needed, since a
       non-terminal `bp_status` always returns `True` regardless of the
       WIP row's history.
     - NOTE (corrected 2026-09-08): `p9a_build_costing_chart.py` does NOT
       currently have any terminal-status filter -- an earlier draft of this
       docstring incorrectly claimed one existed there. A terminal-status
       WIP row can therefore still enter `costing_chart` today. This is a
       real, separate gap (not yet fixed) -- flag for a follow-up decision
       before relying on `costing_chart` excluding Finalized/Drop styles.

2. **DTC-only `Active / Dropped` WIP column** (NOT a BeProduct field at all —
   a purely DTC-side flag). If this literal value is `"Dropped"`
   (case-insensitive), the row receives ZERO further processing of ANY
   kind — no Phase 1 push, no Phase 2 pushback, no Phase 3 image upload, no
   Phase 9a/10 processing — see `is_wip_row_dropped()`. This is independent
   of and takes priority over the Product Status logic above.

NOTE: the exact DTC column name `"Active / Dropped"` was supplied by the
project team but could not be independently live-verified against the WIP
view schema at implementation time (`GET /v1/views/{id}` was returning a
transient 403 from the Azure Application Gateway fronting the DTC API for
an extended period — a previously-documented intermittent issue with this
specific endpoint; `GET /v1/sheets/{id}` continued to work fine throughout).
No row in the live KTB test data currently has this field populated either
(consistent with either it being genuinely blank everywhere in test data, or
a column-name mismatch). `is_wip_row_dropped()` is written defensively: a
missing/blank value is treated as "not dropped" (normal processing
continues) so an incorrect column name fails SAFE (this check simply never
fires) rather than accidentally blocking all rows. Re-verify the exact
column name against a live `get_view_definition()` call once that endpoint
is reachable again, before fully relying on this in production.
"""

from __future__ import annotations

from typing import Optional

# Mirrors the identically-named constant in beproduct/p1p7_beproduct_style_sync.py
# (that file still owns its own copy for now, since it's a plain notebook
# without an import path back to this package at the time it was written --
# keep both in sync if this ever changes).
EXCLUDED_STATUSES = frozenset({"Finalized", "Drop"})

# See module docstring's caveat -- name supplied by the project team, not
# independently live-verified against the WIP view schema (DTC API outage
# at implementation time).
WIP_FIELD_ACTIVE_DROPPED = "Active / Dropped"
DROPPED_VALUE = "dropped"


def _norm(value: Optional[str]) -> str:
    return (value or "").strip().lower()


def is_wip_row_dropped(active_dropped_value: Optional[str]) -> bool:
    """
    True if the DTC-only "Active / Dropped" WIP column literally (case-
    insensitively) reads "Dropped". A blank/missing value is NOT dropped
    (defensive default -- see module docstring).
    """
    return _norm(active_dropped_value) == DROPPED_VALUE


def should_include_in_staging(
    bp_status: Optional[str],
    wip_status: Optional[str],
    excluded_statuses: frozenset = EXCLUDED_STATUSES,
) -> bool:
    """
    Decide whether a style x color row should be included in this run's
    Phase 1 push staging, based on BeProduct's CURRENT Product Status vs.
    the DTC WIP row's OWN current Product Status value.

    Args:
        bp_status: BeProduct's current Product Status for this style
            (`ktb_styles.product_status`).
        wip_status: the DTC WIP row's OWN current "Product Status" value, or
            None if the WIP row doesn't exist yet at all (brand-new style)
            or genuinely has no value there yet.
        excluded_statuses: the terminal-status set (default `EXCLUDED_STATUSES`).

    Returns:
        True  -- include this row in staging (push it this run):
                   * bp_status is NOT terminal (normal active style, OR a
                     reactivated style moving back from Finalized/Drop --
                     no special-casing needed, this falls out naturally), or
                   * bp_status IS terminal but the WIP row hasn't caught up
                     yet (wip_status is None, or wip_status != bp_status) --
                     one last push to sync the terminal status through.
        False -- exclude (leave the WIP row alone): bp_status IS terminal
                   AND wip_status already equals it -- nothing left to sync.
    """
    if bp_status not in excluded_statuses:
        return True
    return wip_status is None or wip_status != bp_status
