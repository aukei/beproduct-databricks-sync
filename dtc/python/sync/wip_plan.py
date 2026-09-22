"""
wip_plan -- compose ONE DTC WIP write plan per request (v2, branch `v2`).

This module is the heart of v2. It COMPOSES the three existing contributions
into a single plan per request; it does NOT re-implement any of their decision
logic:

    style     sync.phase1.compute_upsert()        BeProduct style + sample fields
    material  sync.bom.plan_style_enrichment()    techpack BOM fields
    duty      sync.duty.build_wip_patch_fields()  NT Orbit HTS / duty rates

Why this exists
---------------
DTC uses permissive optimistic locking at REQUEST granularity: any successful
write moves the request's server-side `last_read`, and every browser session
that loaded earlier is then refused on save and silently loses in-progress
edits. Only writes move it; reads are free; the scope is the whole request, not
the view or the row (confirmed with the DTC developer 2026-09-14).

v1 wrote each request at up to 5 moments scattered across the whole DAG
(phase1_push updates/inserts/orphans, fill_bom_data updates/inserts,
push_duty_rates updates). At the target cadence -- a run every ~2 hours -- that
is 36 write moments a day and ~6-8 h/day of user-visible exposure. v2 writes
each request at exactly ONE point, in <=2 back-to-back calls (2 is the floor:
DTCConnector.patch_rows rejects a body mixing rowId and rowIndex).

See docs/PIPELINE.md (Stage 40) and docs/MIGRATION_V1_V2.md.

The invariant this module exists to guarantee
---------------------------------------------
**A run that changes nothing must write nothing.** Not an emergent property of
per-field diffing -- an asserted, tested property of `compute_request_plan()`.
`RequestPlan.is_empty()` is what the notebook checks before issuing ANY call.
At 12 runs/day this is the difference between safe and intolerable.

Three structural guarantees beyond that:

1. **Provenance.** Every planned field records WHICH contribution produced it
   (`PlannedRow.sources`) and every place a later contribution replaced an
   earlier one (`PlannedRow.overrides`). Three task run_ids collapse into one
   in v2, so this is the replacement for per-phase logs -- "why did this cell
   change?" must stay answerable.
2. **Degrade, never abort.** In v1 a BOM or duty bug could only corrupt its own
   fields. Merging removes that natural blast radius, so it is re-established
   here explicitly: a contribution that raises is caught, its keys are OMITTED,
   and the failure is recorded in `RequestPlan.degraded`. The style fields
   still go out. A failing contribution must never fail a row or a request.
3. **Allow-list enforcement.** Ground rule #6 requires every WIP sheetData PATCH
   body to be lean and drawn only from the canonical allow-list. v1 audited
   three separate call sites; v2 has one, so the check lives here. A field
   outside the allow-list is DROPPED and recorded in `RequestPlan.violations`
   (never silently passed through, and never allowed to abort a push).

Ownership boundary between `style` and `material`
-------------------------------------------------
`Fabric Group` / `Placement` / `Mill Fabric Article #` / `Content` are owned by
the MATERIAL contribution. The style contribution only default-fills them on
INSERT (`DUMMY_FABRIC_GROUP` / `DUMMY_FABRIC_ARTICLE` = "NO TPM BOM", blank
Placement) so a BOM-less style still reaches DTC. When both contribute a value
for the same column on the same row, material wins and the override is recorded.
This mirrors v1's ordering (Phase 10 ran after Phase 1) and
`phase1.DEFAULT_FILL_COLS` write-once behaviour, but here it is explicit rather
than a consequence of two tasks running in sequence.

Planning against INTENT, not a round-trip
-----------------------------------------
v1's Phase 10 could only enrich rows that already physically existed in DTC,
which is why `repull_dtc` had to run between the style push and the BOM push.
Here the material contribution runs against the PROJECTED row set -- existing
live rows PLUS the style contribution's planned inserts -- so a brand-new
style x color gets its material fan-out in the SAME run, with no re-pull.

Purity
------
No Spark, no network, no dbutils. Unit-tested in dtc/tests/test_wip_plan.py.
"""

from __future__ import annotations

import traceback
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from . import bom, duty, phase1

# ---------------------------------------------------------------------------
# Contribution names (used as provenance labels throughout)
# ---------------------------------------------------------------------------

SOURCE_STYLE = "style"
SOURCE_MATERIAL = "material"
SOURCE_DUTY = "duty"
# A stale row left behind when a BeProduct key field moved the style to a
# DIFFERENT request. Marked, never deleted -- see phase1.compute_orphan_marks.
SOURCE_ORPHAN = "orphan"

ALL_SOURCES = (SOURCE_STYLE, SOURCE_MATERIAL, SOURCE_DUTY, SOURCE_ORPHAN)

# Columns the MATERIAL contribution owns outright. When the style contribution
# also produced one of these (its INSERT-time default-fill), material wins.
MATERIAL_OWNED_COLS: frozenset = frozenset({
    bom.WIP_FIELD_FABRIC_GROUP,
    bom.WIP_FIELD_PLACEMENT,
    bom.WIP_FIELD_MILL_FABRIC_ARTICLE,
    bom.WIP_FIELD_CONTENT,
})


def duty_columns() -> frozenset:
    """Every DTC WIP column the duty contribution may write, across all slots.

    All THREE field families -- HTS, duty rate, tariff -- are included
    unconditionally. Tariff used to be gated behind `duty.WIP_TARIFF_COLS_LIVE`
    because the columns did not exist in the view; all four slots were
    live-verified on 2026-09-17 and the switch was removed (owner
    instruction), so tariff is now a first-class duty column like the others.
    """
    cols = set(duty.WIP_HTS_COL.values())
    for slot_map in duty.WIP_DUTY_COL.values():
        cols.update(slot_map.values())
    cols.update(duty.WIP_TARIFF_COL.values())
    return frozenset(cols)


def allowed_patch_columns() -> frozenset:
    """
    The canonical WIP sheetData PATCH allow-list (AGENTS.md ground rule #6,
    docs/SYNC_CONTRACT.md). Nothing outside this set may appear in any PATCH
    body from any contribution.

    Derived from the payload builders themselves rather than restated, so it
    cannot drift from them: add a field to `phase1.FIELD_MAPPING` (or to the
    material/duty column maps) and it is allowed here automatically.

    `Style Image` is explicitly excluded: image cells can ONLY be set through
    the separate multipart /images endpoint, and DTC rejects any sheetData
    write to that column outright (HTTP 400).
    """
    cols = {c for c in phase1.FIELD_MAPPING.values() if c != phase1.STYLE_IMAGE_COL}
    cols.update(MATERIAL_OWNED_COLS)
    cols.update(duty_columns())
    cols.discard(phase1.STYLE_IMAGE_COL)
    return frozenset(cols)


# ---------------------------------------------------------------------------
# Value comparison
# ---------------------------------------------------------------------------

def values_equal(current: Any, new: Any) -> bool:
    """
    True when writing `new` over `current` would be a no-op.

    Deliberately CONSERVATIVE -- when in doubt, treat as equal and do not
    write. Every suppressed write is one fewer reason to move a request's
    `last_read` and invalidate somebody's open session.

    Three rules, in order:
      1. Blank vs blank is never a diff. DTC's `None` and a source's `""` are
         the same absence. (bom fixed exactly this on 2026-09-10 -- without it
         every already-blank, otherwise-matched row generated a spurious
         `{"Placement": ""}` PATCH.)
      2. Exact equality.
      3. Normalized-string equality, so `0.16` (float, from costing_chart) and
         `"0.16"` (string, as DTC returns it) do not look like a change. This
         is stricter than v1's `p9b2_push_duty_to_wip`, which used a plain
         `!=` and would have re-pushed on every type mismatch.
    """
    cur_blank = bom._blank(current)
    new_blank = bom._blank(new)
    if cur_blank and new_blank:
        return True
    if cur_blank != new_blank:
        return False
    if current == new:
        return True
    return phase1.norm(str(current)) == phase1.norm(str(new))


# ---------------------------------------------------------------------------
# Plan data structures
# ---------------------------------------------------------------------------

@dataclass
class PlannedRow:
    """One physical DTC row this run intends to write."""

    handle: str                                  # stable internal id (see _handle_*)
    kind: str                                    # "update" | "insert"
    match_key: Tuple[Optional[str], Optional[str]]   # (BP Style#, Color / Wash)
    row_id: Optional[str] = None                 # updates only
    row_index: Optional[int] = None              # inserts only
    fields: Dict[str, Any] = field(default_factory=dict)
    sources: Dict[str, str] = field(default_factory=dict)   # column -> contribution
    overrides: List[str] = field(default_factory=list)      # "col: style -> material"
    current: Dict[str, Any] = field(default_factory=dict)   # live row snapshot (updates)
    base_row: Optional[Dict[str, Any]] = None    # inserts: the row copied from
    dropped: Dict[str, str] = field(default_factory=dict)   # column -> why dropped

    def material_article(self) -> Optional[str]:
        """
        This row's `Mill Fabric Article #` AFTER planning -- the planned value
        if one is being written this run, else its current value.

        This is the discriminator the duty contribution joins on: a style x
        color fans out to several physical rows and each has its OWN duty
        cells, so joining on style+color alone would silently collapse them
        and push one material's duty onto the wrong row.
        """
        col = bom.WIP_FIELD_MILL_FABRIC_ARTICLE
        if col in self.fields:
            return phase1.norm(self.fields[col])
        return phase1.norm(self.current.get(col))

    def set_field(self, col: str, value: Any, source: str) -> None:
        """Record a planned write, tracking provenance and any override."""
        if col in self.sources and self.sources[col] != source:
            self.overrides.append(f"{col}: {self.sources[col]} -> {source}")
        self.fields[col] = value
        self.sources[col] = source


@dataclass
class RequestPlan:
    """Everything this run intends to do to ONE DTC request."""

    request_name: Optional[str] = None
    updates: List[PlannedRow] = field(default_factory=list)
    inserts: List[PlannedRow] = field(default_factory=list)
    noops: List[PlannedRow] = field(default_factory=list)
    exceptions: List[phase1.UpsertException] = field(default_factory=list)
    # A contribution that raised: its keys were omitted, everything else went
    # out. "material:KTB-00025: KeyError(...)" etc.
    degraded: List[str] = field(default_factory=list)
    # Fields dropped for being outside the allow-list (ground rule #6) or
    # absent from the live view.
    violations: List[str] = field(default_factory=list)
    counts: Dict[str, int] = field(default_factory=dict)

    def is_empty(self) -> bool:
        """
        THE invariant. True => the notebook must issue ZERO API calls for this
        request: no PATCH, and not even the live GET-to-PATCH path.
        """
        return not self.updates and not self.inserts

    def update_sheet_data(self) -> List[Dict[str, Any]]:
        """UPDATE bodies, keyed by rowId. One object per row, all contributions merged."""
        return [{**r.fields, "rowId": r.row_id} for r in self.updates]

    def insert_sheet_data(self) -> List[Dict[str, Any]]:
        """INSERT bodies, keyed by rowIndex. Never mixed with updates in one call."""
        return [{**r.fields, "rowIndex": r.row_index} for r in self.inserts]

    def columns_changed(self) -> Dict[str, int]:
        """{column: number of rows writing it}, updates and inserts together."""
        hist: Dict[str, int] = {}
        for r in self.updates + self.inserts:
            for col in r.fields:
                hist[col] = hist.get(col, 0) + 1
        return dict(sorted(hist.items(), key=lambda kv: (-kv[1], kv[0])))

    def sample_changes(self, limit: int = 12) -> List[Dict[str, Any]]:
        """
        A few concrete before/after values, for the exit payload.

        Counts alone cannot tell a real correction from a diffing bug -- a
        column reported as "changing on every row" needs its old and new values
        side by side to judge. Serverless runs return no stdout, so explain()
        is invisible outside the UI and this has to travel in the JSON.
        """
        out: List[Dict[str, Any]] = []
        for r in self.updates + self.inserts:
            for col, new in r.fields.items():
                if len(out) >= limit:
                    return out
                out.append({
                    "kind": r.kind,
                    "key": list(r.match_key),
                    "column": col,
                    "current": r.current.get(col),
                    "new": new,
                    "source": r.sources.get(col),
                })
        return out

    def summary(self) -> Dict[str, Any]:
        """Compact, JSON-safe summary.

        The Jobs API does NOT return notebook stdout for serverless runs --
        only the `dbutils.notebook.exit` value comes back (verified 2026-09-14,
        run 66807905429726). So anything that must be readable outside the
        Databricks UI has to travel in a structure like this one.
        """
        # `fields_by_source` is kept strictly to the contribution labels: it
        # answers "which contribution changed cells here?" and nothing else.
        # Counters that are not field counts (duty rows matched, tariff values
        # with no live column, orphan marks) live under `diagnostics`, so the
        # two are never read as the same kind of number.
        return {
            "request": self.request_name,
            "updates": len(self.updates),
            "inserts": len(self.inserts),
            "noops": len(self.noops),
            "exceptions": len(self.exceptions),
            "patch_calls": (1 if self.updates else 0) + (1 if self.inserts else 0),
            "fields_by_source": {k: v for k, v in self.counts.items() if k in ALL_SOURCES},
            "diagnostics": {k: v for k, v in self.counts.items() if k not in ALL_SOURCES},
            # WHICH columns this run intends to write, and how many rows each.
            # Without this, a summary saying "14 updates" is unactionable --
            # and since serverless runs return no stdout, the explain() trace
            # is only visible by opening the run in the UI.
            "columns_changed": self.columns_changed(),
            "degraded": self.degraded,
            "violations": self.violations,
            "empty": self.is_empty(),
        }

    def explain(self, limit: int = 40) -> str:
        """
        Human-readable trace of every planned write, with provenance.

        Printed by the notebook for the Databricks UI. `limit` caps the row
        listing (a production run is ~250 styles, so an unbounded dump is not
        useful); the summary line above it is always complete.
        """
        out: List[str] = []
        s = self.summary()
        out.append(f"  request={s['request']!r}  updates={s['updates']} "
                   f"inserts={s['inserts']} noops={s['noops']} "
                   f"exceptions={s['exceptions']}  -> {s['patch_calls']} PATCH call(s)")
        out.append(f"  fields by source: {s['fields_by_source']}")
        if s["diagnostics"]:
            out.append(f"  diagnostics     : {s['diagnostics']}")
        if s["columns_changed"]:
            out.append(f"  columns changed : {s['columns_changed']}")
        if self.degraded:
            out.append(f"  ⚠ DEGRADED (keys omitted, rest still pushed): {self.degraded}")
        if self.violations:
            out.append(f"  ⚠ ALLOW-LIST VIOLATIONS (dropped): {self.violations}")
        for r in (self.updates + self.inserts)[:limit]:
            ident = f"rowId={r.row_id}" if r.kind == "update" else f"rowIndex={r.row_index}"
            out.append(f"    [{r.kind:6}] {r.match_key} {ident} "
                       f"article={r.material_article()!r}")
            for col in sorted(r.fields):
                out.append(f"        {col!r} = {r.fields[col]!r}   ({r.sources.get(col)})")
            for o in r.overrides:
                out.append(f"        override: {o}")
            for col, why in sorted(r.dropped.items()):
                out.append(f"        dropped {col!r}: {why}")
        extra = len(self.updates) + len(self.inserts) - limit
        if extra > 0:
            out.append(f"    … and {extra} more row(s)")
        return "\n".join(out)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _handle_existing(row_id: Any) -> str:
    return f"e:{row_id}"


def _handle_insert(n: int) -> str:
    return f"i:{n}"


def _projected_row(pr: PlannedRow) -> Dict[str, Any]:
    """
    Render a PlannedRow in the shape `bom.plan_style_enrichment()` expects.

    It reads four material columns plus an opaque row-id and a colour key. The
    values must reflect INTENT -- what the row will hold after this run's
    already-planned writes -- not just what DTC currently holds, so that a
    style contribution INSERT (carrying "NO TPM BOM") is correctly seen as
    un-enriched and gets first-time enrichment in the SAME run.

    `_handle` is passed as the row id: bom treats it as opaque and echoes it
    back on each RowAction, which is how actions are mapped to targets here.
    """
    def cur(col: str) -> Any:
        return pr.fields.get(col, pr.current.get(col))

    return {
        "_handle": pr.handle,
        "_color": pr.match_key[1],
        "fabric_group": cur(bom.WIP_FIELD_FABRIC_GROUP),
        "mill_fabric_article": cur(bom.WIP_FIELD_MILL_FABRIC_ARTICLE),
        "placement": cur(bom.WIP_FIELD_PLACEMENT),
        "content": cur(bom.WIP_FIELD_CONTENT),
        # Carried for build_insert_row_payload(): the full row a fan-out
        # duplicate is copied from. Like every other value here it must reflect
        # INTENT -- `pr.fields` (this run's planned writes) layered OVER what
        # DTC currently holds.
        #
        # `dict(pr.base_row or pr.current)` alone was wrong on an EMPTY request:
        # a style INSERT has no `current` and no `base_row` (there is nothing in
        # DTC yet), its values exist only in `pr.fields`, so every fan-out
        # duplicate copied `{}` and landed with NO BP Style# and NO
        # Color / Wash. Live-confirmed 2026-09-18 on the recreated "KTB SS28
        # Collaborations": 25 of 39 rows had a null style/colour. Invisible on
        # an established sheet, where `current` is populated -- which is why it
        # only surfaced on a fresh one.
        "_base_fields": {**dict(pr.base_row or pr.current), **pr.fields},
    }


def _style_rows_from_upsert(
    plan: phase1.UpsertPlan,
    dtc_rows: List[Dict[str, Any]],
) -> Tuple[List[PlannedRow], List[PlannedRow], Dict[str, Dict[str, Any]]]:
    """Convert a phase1 UpsertPlan into PlannedRows, preserving its decisions."""
    by_row_id = {r.get("rowId"): r for r in dtc_rows if r.get("rowId") is not None}

    updates: List[PlannedRow] = []
    for i, op in enumerate(plan.updates):
        current = by_row_id.get(op.row_id, {})
        pr = PlannedRow(
            handle=_handle_existing(op.row_id),
            kind="update",
            match_key=op.match_key,
            row_id=op.row_id,
            current=dict(current),
        )
        for col, val in op.fields.items():
            pr.set_field(col, val, SOURCE_STYLE)
        updates.append(pr)

    inserts: List[PlannedRow] = []
    for i, op in enumerate(plan.inserts):
        pr = PlannedRow(
            handle=_handle_insert(i),
            kind="insert",
            match_key=op.match_key,
            row_index=op.row_index,
        )
        for col, val in op.fields.items():
            pr.set_field(col, val, SOURCE_STYLE)
        inserts.append(pr)

    return updates, inserts, by_row_id


# ---------------------------------------------------------------------------
# The entry point
# ---------------------------------------------------------------------------

def compute_request_plan(
    request_scope: Dict[str, Any],
    dtc_rows: List[Dict[str, Any]],
    bp_rows: List[Dict[str, Any]],
    bom_by_style: Optional[Dict[Optional[str], Any]] = None,
    duty_rows: Optional[List[Dict[str, Any]]] = None,
    bp_keys_this_request: Optional[set] = None,
    moved_elsewhere_keys: Optional[set] = None,
    allowed_cols: Optional[set] = None,
    non_writable_cols: Optional[frozenset] = None,
    enforce_scope: bool = True,
    enable_material: bool = True,
    enable_duty: bool = True,
    material_exclude_cols: Optional[frozenset] = None,
    material_fill_if_blank_cols: Optional[frozenset] = None,
    request_name: Optional[str] = None,
) -> RequestPlan:
    """
    Compose ONE request's complete write plan from all three contributions.

    Args:
        request_scope:  {'season_code', 'brand'} for this request.
        dtc_rows:       the request's CURRENT live rows (one `get_sheet()` call
                        per request -- v2 plans against live for every
                        contribution, where v1 planned material and duty
                        against a stale Delta snapshot and needed two re-pulls).
        bp_rows:        staging rows targeting this request.
        bom_by_style:   {bp_style_number: custom_fields}. Absent style -> that
                        style takes zero material actions (never an error, and
                        never a revert).
        duty_rows:      costing_chart rows for this request. Each needs
                        bp_style_no / color_name / material_no / supplier_type
                        plus any of hts_code / duty_rate_us|ca|mx / tariff_rate.
        allowed_cols:   columns present in the LIVE view. Independent of the
                        contract allow-list: a column can be legitimate and
                        still be absent from this view.
        non_writable_cols: from `bom.compute_non_writable_cols(dynamicFields)`;
                        columns DTC rejects writes to (image / formula types).
        bp_keys_this_request / moved_elsewhere_keys: (BP Style#, Color) keys
                        for the orphan-mark pass. Both must be supplied for it
                        to run. A row whose key is absent here but present
                        under a DIFFERENT request is flagged
                        `Product Status = "(removed)"` -- never deleted.
                        Handled HERE rather than in the notebook so that ALL
                        writes to this request, orphan marks included, land in
                        the SAME single window and pass the same allow-list and
                        lean-PATCH checks.
        enable_material / enable_duty: `run_bom` / `run_duty_push`.
        material_exclude_cols: material columns to plan but NOT write. Exists
                        for a specific hazard: DTC's own Content trigger and the
                        techpack BOM express the same fibre content in DIFFERENT
                        NOTATION ("97% Cotton / 3% Spandex" vs "Cotton 97%,
                        Spandex 3%"). Both are "correct", so each would keep
                        overwriting the other -- a write on EVERY run, forever,
                        which at a 2-hourly cadence means a permanent write
                        window. Excluding the column is the safe holding
                        position until one notation is declared canonical.

    Returns:
        RequestPlan. Check `is_empty()` BEFORE issuing any call.
    """
    plan = RequestPlan(request_name=request_name)
    exclude_cols = bom.INSERT_EXCLUDE_COLS | (non_writable_cols or frozenset())

    # ── 1. Style contribution ───────────────────────────────────────────────
    # Not wrapped in try/except: without it there is no plan at all, so a
    # failure here is a genuine task failure rather than something to degrade.
    style_plan = phase1.compute_upsert(
        request_scope, dtc_rows, bp_rows,
        allowed_cols=allowed_cols, enforce_scope=enforce_scope,
    )
    plan.exceptions.extend(style_plan.exceptions)
    updates, inserts, by_row_id = _style_rows_from_upsert(style_plan, dtc_rows)

    # Every existing row participates in the material pass, not only the ones
    # the style contribution happened to touch -- a row whose style fields are
    # all current can still need enrichment.
    touched_row_ids = {u.row_id for u in updates}
    for row in dtc_rows:
        rid = row.get("rowId")
        if rid is None or rid in touched_row_ids:
            continue
        key = (phase1.norm(row.get(phase1.MATCH_KEY_COLS[0])),
               phase1.norm(row.get(phase1.MATCH_KEY_COLS[1])))
        if key == (None, None):
            continue  # pre-created blank row
        updates.append(PlannedRow(
            handle=_handle_existing(rid), kind="update",
            match_key=key, row_id=rid, current=dict(row),
        ))

    # ── 2. Material contribution ────────────────────────────────────────────
    if enable_material and bom_by_style:
        _apply_material(plan, updates, inserts, bom_by_style, exclude_cols, dtc_rows,
                        material_exclude_cols or frozenset(),
                        material_fill_if_blank_cols or frozenset())

    # ── 3. Duty contribution ────────────────────────────────────────────────
    if enable_duty and duty_rows:
        _apply_duty(plan, updates, inserts, duty_rows)

    # ── 4. Orphan marks (stale rows whose style moved to another request) ───
    if bp_keys_this_request is not None and moved_elsewhere_keys is not None:
        _apply_orphan_marks(plan, updates, dtc_rows,
                            bp_keys_this_request, moved_elsewhere_keys)

    # ── 5. Finalize ─────────────────────────────────────────────────────────
    _finalize(plan, updates, inserts, allowed_cols, exclude_cols)
    return plan


def _apply_orphan_marks(
    plan: RequestPlan,
    updates: List[PlannedRow],
    dtc_rows: List[Dict[str, Any]],
    bp_keys_this_request: set,
    moved_elsewhere_keys: set,
) -> None:
    """
    Fold `phase1.compute_orphan_marks()` into the same plan.

    In v1 these were a THIRD PATCH call against the old request, on top of its
    updates and inserts. Merging them here is most of the point of v2: a stale
    row is marked in the same window as everything else that request receives,
    so a key change costs one write window rather than two.

    `compute_orphan_marks` already skips rows that are already flagged, so this
    is idempotent and contributes nothing on a settled run.
    """
    try:
        ops = phase1.compute_orphan_marks(
            dtc_rows, bp_keys_this_request, moved_elsewhere_keys)
    except Exception as e:  # noqa: BLE001
        plan.degraded.append(f"{SOURCE_ORPHAN}: {type(e).__name__}: {e}")
        return

    by_row_id = {u.row_id: u for u in updates if u.row_id is not None}
    for op in ops:
        target = by_row_id.get(op.row_id)
        if target is None:
            current = next((r for r in dtc_rows if r.get("rowId") == op.row_id), {})
            target = PlannedRow(
                handle=_handle_existing(op.row_id), kind="update",
                match_key=op.match_key, row_id=op.row_id, current=dict(current),
            )
            updates.append(target)
            by_row_id[op.row_id] = target
        for col, val in op.fields.items():
            target.set_field(col, val, SOURCE_ORPHAN)
    plan.counts["orphan_marks"] = len(ops)


def _apply_material(
    plan: RequestPlan,
    updates: List[PlannedRow],
    inserts: List[PlannedRow],
    bom_by_style: Dict[Optional[str], Any],
    exclude_cols: frozenset,
    dtc_rows: List[Dict[str, Any]],
    material_exclude_cols: frozenset = frozenset(),
    material_fill_if_blank_cols: frozenset = frozenset(),
) -> None:
    """
    Run `bom.plan_style_enrichment()` per style against the PROJECTED rows
    (existing + this run's planned inserts), then fold its RowActions back in.

    Per style, wrapped in try/except: one style's malformed BOM JSON degrades
    that style's material fields only -- every other style, and every style
    field on this style, still goes out.
    """
    by_handle = {r.handle: r for r in updates + inserts}

    projected_by_style: Dict[Optional[str], List[PlannedRow]] = {}
    for pr in updates + inserts:
        projected_by_style.setdefault(pr.match_key[0], []).append(pr)

    # rowIndex for fan-out inserts continues past everything already assigned,
    # so a duplicate can never collide with a style INSERT or an existing row.
    next_index = max(
        [phase1.max_row_index(dtc_rows)] + [i.row_index or 0 for i in inserts]
    )
    fanout_seq = len(inserts)

    for style, rows in sorted(projected_by_style.items(), key=lambda kv: (kv[0] or "")):
        custom_fields = bom_by_style.get(style)
        if custom_fields is None:
            continue  # no BOM for this style this run -- zero actions, never a revert

        # `bom_by_style` may hold EITHER already-built segments (a list) or a
        # raw Lakebase `custom_fields` payload (a dict/str). Callers normally
        # hand in segments now -- `bom.segments_from_delta_value()` decodes
        # both shapes upstream -- but the raw path is kept because it costs
        # nothing and makes this function usable directly from a payload.
        # Source-agnosticism here is what made the 2026-09-16 switch and the
        # 2026-09-22 walkback each a one-notebook change; `test_wip_plan.py
        # [7l]` pins it by proving both inputs produce an identical plan.
        _segments = custom_fields if isinstance(custom_fields, list) else None
        try:
            actions = bom.plan_style_enrichment(
                [_projected_row(r) for r in rows],
                None if _segments is not None else custom_fields,
                row_id_key="_handle",
                color_key="_color",
                target_segments=_segments,
            )
        except Exception as e:  # noqa: BLE001
            plan.degraded.append(
                f"{SOURCE_MATERIAL}:{style}: {type(e).__name__}: {e} "
                f"| {traceback.format_exc(limit=1).strip().splitlines()[-1]}")
            continue

        for act in actions:
            if act.kind == "update":
                target = by_handle.get(act.row_id)
                if target is None:
                    plan.degraded.append(
                        f"{SOURCE_MATERIAL}:{style}: action referenced unknown "
                        f"handle {act.row_id!r}")
                    continue
                for col, val in act.wip_fields.items():
                    if col in material_exclude_cols:
                        target.dropped[col] = "material column excluded by parameter"
                        continue
                    if (col in material_fill_if_blank_cols
                            and not bom._blank(target.current.get(col))):
                        target.dropped[col] = (
                            "write-once material column; target already non-blank")
                        continue
                    target.set_field(col, val, SOURCE_MATERIAL)

            elif act.kind == "insert":
                base = dict((act.base_row or {}).get("_base_fields") or {})
                next_index += 1
                fanout_seq += 1
                pr = PlannedRow(
                    handle=_handle_insert(fanout_seq),
                    kind="insert",
                    match_key=(style, (act.base_row or {}).get("_color")),
                    row_index=next_index,
                    base_row=base,
                )
                payload = bom.build_insert_row_payload(
                    base, act.wip_fields, exclude_cols=exclude_cols)
                # The copied-forward columns come from an existing row, so they
                # are attributed to `style`; only the 4 BOM columns the fan-out
                # actually sets are attributed to `material`.
                for col, val in payload.items():
                    if col in material_exclude_cols and col in act.wip_fields:
                        # A fan-out INSERT still copies the base row's value
                        # forward; only the material contribution's OWN write to
                        # this column is suppressed.
                        pr.dropped[col] = "material column excluded by parameter"
                        continue
                    pr.set_field(col, val,
                                 SOURCE_MATERIAL if col in act.wip_fields else SOURCE_STYLE)
                inserts.append(pr)
                by_handle[pr.handle] = pr


def _apply_duty(
    plan: RequestPlan,
    updates: List[PlannedRow],
    inserts: List[PlannedRow],
    duty_rows: List[Dict[str, Any]],
) -> None:
    """
    Merge costing_chart duty values onto their target rows.

    Joined on (BP Style#, Color / Wash, Mill Fabric Article #) -- NOT style +
    colour, which would collapse a style's several material rows into one and
    push a material's duty onto the wrong physical row.

    Several costing_chart rows (one per vendor slot Main/1/2/3) map to the SAME
    WIP row, differing only in which COLUMNS they target. They must be merged
    into ONE object: two sheetData entries sharing a rowId in one call is
    rejected with 400 "Duplicate rowId found" (confirmed live 2026-09-01).
    Merging is safe because the slots address disjoint columns.
    """
    # Indexed as a LIST, not a single row. Two physical rows CAN legitimately
    # share (style, colour, article) -- e.g. the same material appearing under
    # two BOM segments -- and they are the same material, so the same duty
    # applies to both. v1's p9b2 used a plain dict here and silently kept only
    # the LAST such row; applying to all of them is both more correct and
    # deterministic (no dependence on row ordering).
    index: Dict[Tuple, List[PlannedRow]] = {}
    for pr in updates + inserts:
        index.setdefault(
            (pr.match_key[0], pr.match_key[1], pr.material_article()), []).append(pr)

    matched = skipped_reasons = 0
    for row in duty_rows:
        filled = {c: row.get(c) for c in duty.DUTY_VALUE_FIELDS
                  if row.get(c) is not None}
        if not filled:
            continue

        key = (phase1.norm(row.get("bp_style_no")),
               phase1.norm(row.get("color_name")),
               phase1.norm(row.get("material_no")))
        targets = index.get(key)
        if not targets:
            continue  # no matching physical row this run -- picked up next run

        try:
            wip = duty.build_wip_patch_fields(row.get("supplier_type"), filled)
        except Exception as e:  # noqa: BLE001
            plan.degraded.append(f"{SOURCE_DUTY}:{key}: {type(e).__name__}: {e}")
            continue

        skipped_reasons += len(wip.skipped)
        for target in targets:
            for col, val in wip.fields.items():
                target.set_field(col, val, SOURCE_DUTY)
        matched += 1

    plan.counts["duty_rows_matched"] = matched
    plan.counts["duty_values_not_writable"] = skipped_reasons


def _finalize(
    plan: RequestPlan,
    updates: List[PlannedRow],
    inserts: List[PlannedRow],
    allowed_cols: Optional[set],
    exclude_cols: frozenset,
) -> None:
    """
    Enforce the write contract, then partition into updates / inserts / noops.

    In order:
      1. Drop any field outside the canonical allow-list -> `violations`.
      2. Drop any field absent from the live view, or one DTC rejects writes
         to -> `violations`.
      3. On UPDATEs, drop any field whose value already matches the live cell.
         This is what makes the zero-diff-zero-write invariant hold even if a
         contribution is careless: the lean-PATCH guarantee does not depend on
         each contribution diffing correctly, only on this pass.
      4. An UPDATE left with no fields becomes a NOOP and issues no call.
         An INSERT is always kept -- a new row with only key fields is still a
         row that must exist.
    """
    allow = allowed_patch_columns()

    for pr in updates + inserts:
        for col in list(pr.fields):
            if col not in allow:
                pr.dropped[col] = "not in canonical PATCH allow-list (ground rule #6)"
                plan.violations.append(
                    f"{pr.kind}:{pr.match_key}:{col} (source={pr.sources.get(col)})")
                del pr.fields[col]
                pr.sources.pop(col, None)
                continue
            if col in exclude_cols:
                pr.dropped[col] = "DTC rejects writes to this column (image/formula)"
                del pr.fields[col]
                pr.sources.pop(col, None)
                continue
            if allowed_cols is not None and col not in allowed_cols:
                pr.dropped[col] = "not present in the live view definition"
                del pr.fields[col]
                pr.sources.pop(col, None)
                continue
            if pr.kind == "update" and values_equal(pr.current.get(col), pr.fields[col]):
                pr.dropped[col] = "unchanged (lean PATCH)"
                del pr.fields[col]
                pr.sources.pop(col, None)

    counts = {s: 0 for s in ALL_SOURCES}
    for pr in updates:
        if pr.fields:
            plan.updates.append(pr)
            for s in pr.sources.values():
                counts[s] = counts.get(s, 0) + 1
        else:
            plan.noops.append(pr)
    for pr in inserts:
        plan.inserts.append(pr)
        for s in pr.sources.values():
            counts[s] = counts.get(s, 0) + 1

    plan.counts.update(counts)
