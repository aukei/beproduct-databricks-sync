#!/usr/bin/env python3
"""
DTC WIP_ITS_USE view column checker
====================================

Connects to DTC UAT, scans the WIP_ITS_USE view across all in-scope KTB requests,
and reports:
  1. Which Phase 6 columns already exist in the view (have data in ≥1 request)
  2. Which Phase 6 columns are MISSING and need to be added by the DTC admin

Run:
    python3 scripts/check_dtc_view.py

Requirements:
    - Proxy must be reachable: http://100.64.0.7:8888
    - dtc/python must be on the path (handled automatically below)
"""

import os
import sys
import time

# ── Proxy ──────────────────────────────────────────────────────────────────────
PROXY = "http://100.64.0.7:8888"
os.environ["https_proxy"] = PROXY
os.environ["HTTPS_PROXY"] = PROXY
os.environ["http_proxy"]  = PROXY
os.environ["HTTP_PROXY"]  = PROXY

# ── Path ───────────────────────────────────────────────────────────────────────
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, "dtc", "python"))

import requests
from connectors.dtc import DTCConnector

# ── Config ─────────────────────────────────────────────────────────────────────
DTC_API_KEY    = "49A127E0942071B4BD440DD00386C6B3"
DTC_ENV        = "uat"
DTC_WORKSPACE  = "KTB"
DTC_DOCUMENT   = "KTB WIP"

# WIP_ITS_USE view definition ID for the KTB WIP document (per-request view).
# Confirmed 2026-07-03: this view has 194 dynamicFields and is the one used for
# all sync PATCH operations.  The earlier ID 6a3907f6df772fd797ee5b7c belongs to
# the "XTS Master" document (only 8 fields) — do not use it.
DOC_VIEW_ID    = "69f04983501f3d9cf4fc379c"

# ── Phase 6 column requirements ────────────────────────────────────────────────
# Columns the sync pipeline writes to (BeProduct → DTC, Phase 1).
# Each entry: (DTC column name, description, phase6 status)
PHASE1_COLUMNS = [
    # existing, verified working
    ("Product Status",        "BeProduct product status",                 "existing"),
    ("Style Description",     "BeProduct style description",              "existing"),
    ("Color / Wash",          "Colorway name (match key)",                "existing"),
    ("Brand",                 "Brand from BP brand_hk field",             "existing"),
    ("LF Style#",             "LF style number (new BP lf_style_number)", "existing"),
    ("Class",                 "Product category",                         "existing"),
    ("Sub Class",             "Product sub-category",                     "existing"),
    ("Division",              "Division",                                 "existing"),
    ("Garment Finish",        "Garment finish",                           "existing"),
    ("Tech Pack Stage",       "Tech pack stage",                          "existing"),
    ("Fabric Group",          "Fabric group",                             "existing"),
    ("Placement",             "Placement",                                "existing"),
    ("Style Image",           "Front image (Phase 3, binary upload)",     "existing"),
    ("Legacy Code",           "Customer style number (BP → DTC)",         "existing"),
    # NEW columns DTC admin must add
    ("BP Style#",             "BP Style Number (new match key for sync)", "ADD REQUIRED"),
    ("Gender",                "Gender from BeProduct",                    "ADD REQUIRED"),
    ("Supplier",              "Supplier (default-fill: 'Supplier')",      "ADD REQUIRED"),
]

# Columns the sync reads FROM DTC (DTC → BeProduct, Phase 2).
PHASE2_COLUMNS = [
    ("Lot#",                   "Lot code → BP drawing_number_walmart",    "existing"),
    ("Main Vendor (Sampling)", "Main vendor → BP parent_vendor",          "existing"),
    ("Main Factory (Sampling)","Main factory → BP factory",               "existing"),
    ("Main Factory Customer ID","No BP target; logged and skipped",       "existing"),
]


# ─────────────────────────────────────────────────────────────────────────────
def hr(char="─", width=72):
    print(char * width)

def header(title):
    hr("═")
    print(f"  {title}")
    hr("═")

def section(title):
    print()
    hr()
    print(f"  {title}")
    hr()

# ─────────────────────────────────────────────────────────────────────────────
header("DTC WIP_ITS_USE VIEW COLUMN CHECKER  —  Phase 6 readiness")
print(f"  Workspace : {DTC_WORKSPACE}")
print(f"  Document  : {DTC_DOCUMENT}")
print(f"  Env       : {DTC_ENV}")
print(f"  Proxy     : {PROXY}")

# ── Step 1: connect ────────────────────────────────────────────────────────────
section("Step 1 — Connect to DTC")
try:
    c = DTCConnector(api_key=DTC_API_KEY, environment=DTC_ENV,
                     workspace_name=DTC_WORKSPACE)
    print("  ✓ DTCConnector created")
except Exception as e:
    print(f"  ✗ Failed to create connector: {e}")
    sys.exit(1)

# ── Step 2: try the view definition endpoint ───────────────────────────────────
section(f"Step 2 — GET /v1/views/{DOC_VIEW_ID}  (document-level WIP_ITS_USE)")
print("  This endpoint should return the authoritative column list.")
print("  If it returns 403, the sync API key lacks read permission on view definitions.")
print()

VIEW_DEF_COLS = []
try:
    resp = requests.get(
        f"https://dtc-api.lfuat.net/api/v1/views/{DOC_VIEW_ID}",
        headers={"x-api-key": DTC_API_KEY, "Content-Type": "application/json"},
        proxies={"https": PROXY, "http": PROXY},
        timeout=15,
    )
    if resp.status_code == 200:
        dyn = resp.json().get("dynamicFields", [])
        VIEW_DEF_COLS = sorted(f.get("fieldName", "?") for f in dyn)
        print(f"  ✓  HTTP {resp.status_code} — {len(VIEW_DEF_COLS)} dynamicFields returned")
    else:
        print(f"  ✗  HTTP {resp.status_code} — {resp.text[:200]}")
        if resp.status_code == 403:
            print()
            print("  ⚠  403 Forbidden: the sync API key does NOT have permission to read")
            print("     view definitions via GET /v1/views/{viewId}.")
            print("     ACTION FOR DTC ADMIN: either grant this permission to the sync API key,")
            print("     or this script will fall back to a data-scan (Step 3).")
except Exception as e:
    print(f"  ✗  Exception: {e}")

# ── Step 3: data scan across all active KTB requests ──────────────────────────
section("Step 3 — Data scan across all active KTB WIP requests")
print("  Fetching all active KTB requests and scanning sheet data for populated columns.")
print("  Note: columns that exist in the view but are blank in ALL rows won't appear here.")
print()

t0 = time.perf_counter()
try:
    reqs = c.search_requests(DTC_WORKSPACE, DTC_DOCUMENT,
                             filters={"requestIsActive": "Y"})
    print(f"  Found {len(reqs)} active in-scope requests")
except Exception as e:
    print(f"  ✗  Failed to list requests: {e}")
    sys.exit(1)

DATA_SCAN_COLS: dict = {}   # col_name -> count of requests with ≥1 non-null value
scanned = 0
errors  = 0

for req in reqs:
    req_id = req.get("requestId") or req.get("id", "")
    ref    = req.get("requestReference", "?")
    try:
        scope = c.get_request_scope(req_id)
        sid, vid = scope["sheet_id"], scope["wip_view_id"]
        sheet = c.get_sheet(sid, vid)
        rows  = sheet.get("sheetData", [])
        for row in rows:
            for k in row:
                if k in ("rowId", "rowIndex"):
                    continue
                DATA_SCAN_COLS[k] = DATA_SCAN_COLS.get(k, 0) + 1
        scanned += 1
        if scanned % 10 == 0:
            print(f"    ... {scanned}/{len(reqs)} requests scanned", flush=True)
    except Exception as e:
        errors += 1

elapsed = time.perf_counter() - t0
print(f"\n  Scanned {scanned} requests in {elapsed:.1f}s  ({errors} errors)")
print(f"  Unique columns with ≥1 populated cell: {len(DATA_SCAN_COLS)}")

# ── Step 4: Phase 6 column status report ──────────────────────────────────────
section("Step 4 — Phase 6 column readiness report")

def col_status(col_name, required=False):
    """Return (symbol, detail) for a column name.

    required=True: columns that MUST be added for Phase 6. When the view
    definition is unavailable and no data is found, these are flagged as
    missing (not just unknown).
    """
    if VIEW_DEF_COLS:
        in_def  = col_name in VIEW_DEF_COLS
        in_data = col_name in DATA_SCAN_COLS
        if in_def and in_data:
            return "✓", f"in view definition + has data ({DATA_SCAN_COLS[col_name]} rows)"
        if in_def and not in_data:
            return "○", "in view definition, all cells blank"
        if not in_def and in_data:
            return "?", f"NOT in view definition but has data ({DATA_SCAN_COLS[col_name]} rows) — unexpected"
        return "✗", "NOT in view definition"
    else:
        # no view def available — use data scan only
        if col_name in DATA_SCAN_COLS:
            return "✓", f"has data in {DATA_SCAN_COLS[col_name]} row(s) across scanned requests"
        if required:
            return "✗", "NOT FOUND in any of the scanned requests — column likely does not exist yet"
        return "○", "no data found in any request (blank or not yet in use)"


MISSING = []

print()
print(f"  {'Column':<35} {'Phase1/Phase2':<8} {'Req':<12} {'Status'}")
print(f"  {'──────':<35} {'─────────':<8} {'───':<12} {'──────'}")

print(f"\n  ── BeProduct → DTC  (Phase 1, written by sync) ──")
for col, desc, req in PHASE1_COLUMNS:
    is_required = (req == "ADD REQUIRED")
    sym, detail = col_status(col, required=is_required)
    flag = "  ← ADD" if is_required else ""
    print(f"  {sym} {col:<33} Phase 1   {req:<12} {detail}{flag}")
    if is_required and sym not in ("✓", "○"):
        MISSING.append((col, desc))

print(f"\n  ── DTC → BeProduct  (Phase 2, read by sync) ──")
for col, desc, req in PHASE2_COLUMNS:
    sym, detail = col_status(col, required=False)
    print(f"  {sym} {col:<33} Phase 2   {req:<12} {detail}")

# ── Step 5: summary + action items ────────────────────────────────────────────
section("Step 5 — Summary & action items for DTC admin")

print()
if VIEW_DEF_COLS:
    print(f"  View definition columns: {len(VIEW_DEF_COLS)}")
else:
    print("  ⚠  View definition NOT accessible (403). Relying on data-scan only.")
    print(f"     Data-scan unique columns (populated): {len(DATA_SCAN_COLS)}")

print()
if MISSING:
    print(f"  ❌  {len(MISSING)} column(s) need to be ADDED to the WIP_ITS_USE view:")
    print()
    for col, desc in MISSING:
        print(f"       Column name  : {col!r}")
        print(f"       Purpose      : {desc}")
        print()
    print("  Once added, no code changes are needed — the sync pipeline already")
    print("  handles these columns and will start writing to them on the next run.")
else:
    print("  ✅  All required Phase 6 columns are present in the view.")

print()
print("  Additional action items:")
print()
print("  1. 'BP Style#' — MOST CRITICAL. After creation:")
print("       a. Migrate existing 'LF Style#' values → 'BP Style#' for all rows.")
print("       b. Notify the sync team so the match-key switch can be activated.")
print()
print("  2. 'Gender' — Add to view. Sync will start populating on next run.")
print()
print("  3. 'Supplier' — Add to view. Sync writes 'Supplier' as default when blank;")
print("       if a row already has a value in this column, it is never overwritten.")

if not VIEW_DEF_COLS:
    print()
    print("  4. View definition access — GET /v1/views/{viewId} is not accessible.")
    print(f"       Correct KTB WIP_ITS_USE view ID: {DOC_VIEW_ID!r}")
    print("       Ensure proxy http://100.64.0.7:8888 is set when running this script.")

hr("═")
print("  Check complete.")
hr("═")
