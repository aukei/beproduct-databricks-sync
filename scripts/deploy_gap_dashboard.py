#!/usr/bin/env python3
"""
Deploy the non-IT "Data gaps" dashboard.

Two objects, both idempotent:
  1. the views lft.beproduct.v_style_field_conflicts and lft.beproduct.v_data_gaps
     (dashboards/data_gaps/*.sql)
  2. the AI/BI (Lakeview) dashboard "BeProduct DTC - Data gaps" on top of it,
     created or updated in place, then published.

The dashboard only READS Delta tables the main job already writes. It opens no
DTC write window and touches neither DTC nor BeProduct.

Publishing uses EMBEDDED credentials, so a viewer needs access to the dashboard
only -- not UC grants on lft.beproduct. Share it from the Databricks UI
(Share -> add users/groups); this script never shares it with anyone.

Usage
-----
    python scripts/deploy_gap_dashboard.py --dry-run   # print the dashboard JSON only
    python scripts/deploy_gap_dashboard.py             # create/replace view + dashboard, publish
    python scripts/deploy_gap_dashboard.py --view-only

Requires DATABRICKS_HOST + DATABRICKS_PAT and DATABRICKS_HTTP_PATH (its
warehouse runs the view DDL and the dashboard queries) in .env.
"""

import argparse
import json
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).parent.parent
from dotenv import load_dotenv

load_dotenv(_ROOT / ".env")

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard
from databricks.sdk.service.sql import StatementState

# Deployed IN ORDER -- v_data_gaps reads v_style_field_conflicts.
VIEW_SQLS = [
    _ROOT / "dashboards" / "data_gaps" / "v_style_field_conflicts.sql",
    _ROOT / "dashboards" / "data_gaps" / "v_data_gaps.sql",
]
VIEW = "lft.beproduct.v_data_gaps"
CONFLICTS_VIEW = "lft.beproduct.v_style_field_conflicts"
DISPLAY_NAME = "BeProduct DTC - Data gaps"
FOLDER_NAME = "BeProduct DTC sync"

SEVERITY_ORDER = "CASE severity WHEN 'blocker' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END"


# ── dashboard definition ─────────────────────────────────────────────────────

def _q(dataset, fields, disaggregated=False, name="main_query"):
    return {"name": name, "query": {
        "datasetName": dataset,
        "fields": [{"name": n, "expression": e} for n, e in fields],
        "disaggregated": disaggregated}}


def _counter(name, title, dataset, expr, color_pos):
    return {"name": name,
            "queries": [_q(dataset, [("v", expr)])],
            "spec": {"version": 2, "widgetType": "counter",
                     "frame": {"showTitle": True, "title": title},
                     "encodings": {"value": {"fieldName": "v", "style": {
                         "bold": True, "fontSize": 28,
                         "rules": [{"condition": {"operand": {"type": "data-value", "value": "0"},
                                                  "operator": ">"},
                                    "color": {"themeColorType": "visualizationColors",
                                              "position": color_pos}}]}}}}}


def _filter(name, title, field, datasets=("gaps",)):
    """Multi-select filter on `field`; applies to every dataset listed."""
    queries, fields = [], []
    for ds in datasets:
        qname = f"filter_{ds}_{field}"
        queries.append(_q(ds, [(field, f"`{field}`"),
                               (f"{field}_associativity",
                                "COUNT_IF(`associative_filter_predicate_group`)")],
                          name=qname))
        fields.append({"fieldName": field, "displayName": title, "queryName": qname})
    return {"name": name, "queries": queries,
            "spec": {"version": 2, "widgetType": "filter-multi-select",
                     "frame": {"showTitle": True, "title": title},
                     "encodings": {"fields": fields}}}


def _bar(name, title, field):
    return {"name": name,
            "queries": [_q("gaps", [(field, f"`{field}`"), ("severity", "`severity`"),
                                    ("count(*)", "COUNT(`*`)")])],
            "spec": {"version": 3, "widgetType": "bar",
                     "frame": {"showTitle": True, "title": title},
                     "mark": {"layout": "stack"},
                     "encodings": {
                         "x": {"fieldName": "count(*)", "displayName": "Gaps",
                               "scale": {"type": "quantitative"}},
                         "y": {"fieldName": field, "scale": {"type": "categorical",
                                                             "sort": {"by": "x-reversed"}}},
                         "color": {"fieldName": "severity", "scale": {
                             "type": "categorical",
                             "mappings": [
                                 {"value": "blocker", "color": {"themeColorType": "visualizationColors", "position": 4}},
                                 {"value": "warning", "color": {"themeColorType": "visualizationColors", "position": 2}},
                                 {"value": "info", "color": {"themeColorType": "visualizationColors", "position": 1}}]},
                             "legend": {"position": "bottom"}},
                         "label": {"show": True}}}}


TABLE_COLS = [
    ("severity", "Severity"), ("owner", "Who fixes it"), ("area", "Area"),
    ("bp_style", "BP Style#"), ("color", "Colour"), ("article", "Mill Fabric Article #"),
    ("request", "DTC request"), ("action", "What to do"), ("detail", "Why"),
    ("gap_code", "Gap code"), ("runbook", "Runbook (IT)"),
]


CONFLICT_COLS = [
    ("bp_style", "BP Style#"), ("field", "Field"), ("n_values", "# values"),
    ("values_and_rows", "Values  <-  rows holding them (colour / fabric group)"),
    ("beproduct_now", "BeProduct now holds"), ("n_rows", "# rows"),
]


def _table(name, title, dataset, cols, freeze, extra=()):
    return {"name": name,
            "queries": [_q(dataset, [(c, f"`{c}`") for c, _ in cols] + [(e, f"`{e}`") for e in extra],
                           disaggregated=True)],
            "spec": {"version": 2, "widgetType": "table",
                     "frame": {"showTitle": True, "title": title},
                     "freezeUpToColumnNumber": freeze,
                     "encodings": {"columns": [
                         {"fieldName": c, "displayName": d} for c, d in cols]}}}


def _text(name, lines):
    return {"name": name, "multilineTextboxSpec": {"lines": lines}}


def build_dashboard():
    datasets = [
        {"name": "gaps", "displayName": "Data gaps",
         "queryLines": [
             "SELECT *,\n",
             f"  {SEVERITY_ORDER} AS sev_rank,\n",
             "  CASE WHEN severity = 'blocker' THEN 1 ELSE 0 END AS is_blocker,\n",
             "  CASE WHEN severity = 'warning' THEN 1 ELSE 0 END AS is_warning\n",
             f"FROM {VIEW}\n",
             "ORDER BY sev_rank, area, owner, bp_style, color"]},
        {"name": "conflicts", "displayName": "Style field conflicts",
         "queryLines": [f"SELECT * FROM {CONFLICTS_VIEW}\n", "ORDER BY bp_style, field"]},
        {"name": "fresh", "displayName": "Freshness",
         "queryLines": [
             "SELECT CAST(timestampdiff(MINUTE, max(extracted_at), current_timestamp()) AS INT) AS minutes_old\n",
             "FROM lft.beproduct.dtc_wip_ktb"]},
    ]
    intro = _text("intro", [
        "## BeProduct / DTC sync - data gaps\n",
        "Everything the sync could not do, and **who can fix it**. "
        "Filter by *Who fixes it* to see only your items, or by *BP Style#* to check one style.\n",
        "\n",
        "- **blocker** - something is not synced / no costing line / no duty rate until fixed\n",
        "- **warning** - synced, but something is wrong or still pending\n",
        "- **info** - by design; listed so you are not surprised\n",
        "\n",
        "Refreshed by every main run (every 8 min). A fix you make shows here after the **next** run, "
        "so allow up to ~20 min. *Costing* and *Duty* inputs (LinePlan ref#, vendor, factory) must be on the **Main Fabric** row.",
    ])
    layout = [
        (intro, 0, 0, 6, 3),
        (_filter("f_owner", "Who fixes it", "owner"), 0, 3, 2, 1),
        (_filter("f_style", "BP Style#", "bp_style", ("gaps", "conflicts")), 2, 3, 2, 1),
        (_filter("f_area", "Area", "area"), 4, 3, 1, 1),
        (_filter("f_sev", "Severity", "severity"), 5, 3, 1, 1),
        (_counter("c_block", "Blockers", "gaps", "SUM(`is_blocker`)", 4), 0, 4, 2, 2),
        (_counter("c_warn", "Warnings", "gaps", "SUM(`is_warning`)", 2), 2, 4, 1, 2),
        (_counter("c_styles", "Styles affected", "gaps", "COUNT(DISTINCT `bp_style`)", 1), 3, 4, 1, 2),
        (_counter("c_fresh", "Minutes since last sync", "fresh", "MAX(`minutes_old`)", 3), 4, 4, 2, 2),
        (_bar("b_owner", "Gaps by who fixes them", "owner"), 0, 6, 3, 6),
        (_bar("b_area", "Gaps by area", "area"), 3, 6, 3, 6),
        (_table("gap_table", "All gaps (most severe first)", "gaps", TABLE_COLS, 5,
                extra=("sev_rank",)), 0, 12, 6, 12),
        (_text("conflict_intro", [
            "### Style-level fields that disagree\n",
            "Main Vendor, Main Factory, Main Factory Customer ID and Production Country "
            "are **one value per style** in BeProduct, but DTC has a row per colour and material. "
            "When the rows disagree, BeProduct gets whichever row the sync reads first. "
            "Make the rows agree, or keep the value on one row and blank the rest.",
        ]), 0, 24, 4, 2),
        (_counter("c_conf", "Style fields in conflict", "conflicts", "COUNT(`*`)", 2), 4, 24, 2, 2),
        (_table("conflict_table", "Conflicts by style and field", "conflicts", CONFLICT_COLS, 2),
         0, 26, 6, 8),
    ]
    return {
        "datasets": datasets,
        "pages": [{"name": "gaps", "displayName": "Data gaps",
                   "layout": [{"widget": wd, "position": {"x": x, "y": y, "width": wd_, "height": h}}
                              for wd, x, y, wd_, h in layout]}],
    }


# ── deploy ───────────────────────────────────────────────────────────────────

def _warehouse_id():
    http_path = os.environ.get("DATABRICKS_HTTP_PATH", "")
    wid = http_path.rstrip("/").rsplit("/", 1)[-1]
    if not wid:
        sys.exit("DATABRICKS_HTTP_PATH is not set (needed for the warehouse id)")
    return wid


def deploy_view(w, warehouse_id):
    for path in VIEW_SQLS:
        r = w.statement_execution.execute_statement(
            statement=path.read_text(), warehouse_id=warehouse_id, wait_timeout="50s")
        if r.status.state != StatementState.SUCCEEDED:
            sys.exit(f"view DDL failed ({path.name}): {r.status.state} {r.status.error}")
        print(f"✅ view from {path.name} created/replaced")


def deploy_dashboard(w, warehouse_id, serialized):
    me = w.current_user.me().user_name
    parent = f"/Users/{me}/{FOLDER_NAME}"
    w.workspace.mkdirs(parent)

    # Found by WORKSPACE PATH, not lakeview.list(): the listing does not return
    # parent_path, so matching on it never finds the existing dashboard and a
    # second create fails with AlreadyExists.
    existing_id = None
    try:
        existing_id = w.workspace.get_status(f"{parent}/{DISPLAY_NAME}.lvdash.json").resource_id
    except Exception:  # noqa: BLE001 -- NotFound: first deploy
        pass

    if existing_id:
        cur = w.lakeview.get(existing_id)
        d = w.lakeview.update(existing_id, dashboard=Dashboard(
            display_name=DISPLAY_NAME, warehouse_id=warehouse_id,
            serialized_dashboard=serialized, etag=cur.etag))
        print(f"✅ dashboard updated: {d.dashboard_id}")
    else:
        d = w.lakeview.create(dashboard=Dashboard(
            display_name=DISPLAY_NAME, parent_path=parent,
            warehouse_id=warehouse_id, serialized_dashboard=serialized))
        print(f"✅ dashboard created: {d.dashboard_id}")

    w.lakeview.publish(d.dashboard_id, embed_credentials=True, warehouse_id=warehouse_id)
    host = w.config.host.rstrip("/")
    print(f"✅ published (embedded credentials, not shared with anyone)")
    print(f"   {host}/dashboardsv3/{d.dashboard_id}/published")
    return d.dashboard_id


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="print the dashboard JSON; deploy nothing")
    ap.add_argument("--view-only", action="store_true", help="create/replace the view only")
    args = ap.parse_args()

    serialized = json.dumps(build_dashboard())
    if args.dry_run:
        print(json.dumps(json.loads(serialized), indent=2))
        return

    w = WorkspaceClient()
    wid = _warehouse_id()
    deploy_view(w, wid)
    if not args.view_only:
        deploy_dashboard(w, wid, serialized)


if __name__ == "__main__":
    main()
