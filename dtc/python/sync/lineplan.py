"""
LinePlan join-key column names -- the ONE place they are spelled.

The WIP <-> LinePlan join (Phase 9a / Stage 30) reads the ref BY DTC DISPLAY
NAME from both sheets, and DTC column names are edited by humans. A rename does
not fail anything: `get_json_object` / `dict.get` just return NULL, the INNER
JOIN drops every row, and `costing_chart` comes out EMPTY on a green run.

That happened on 2026-09-28: DTC renamed ``"Lineplan Ref #"`` to
``"LinePlan ref#"`` on BOTH the WIP and the LinePlan sheets (41/45 WIP rows and
36 LinePlan rows carried the new key, none the old), and costing silently
stopped.

So the ref is read from a list of accepted names, CURRENT FIRST, first
non-blank wins. The old name stays as a fallback so a sheet that has not been
renamed yet (or a Delta snapshot taken before the rename) still joins. Add a
new name at the FRONT when DTC renames again; never "tidy" the spelling -- a
name absent from the sheet silently reads as NULL.

`dashboards/data_gaps/v_data_gaps.sql` (`names` CTE) must list the same names.
"""
from typing import Any, Mapping, Optional

LINEPLAN_REF_COLS = ("LinePlan ref#", "Lineplan Ref #")


def lineplan_ref(row: Mapping[str, Any]) -> Optional[str]:
    """First non-blank ref across `LINEPLAN_REF_COLS`, stripped; else None."""
    for col in LINEPLAN_REF_COLS:
        v = row.get(col)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return None
