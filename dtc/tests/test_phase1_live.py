#!/usr/bin/env python3
"""
LIVE, REVERSIBLE end-to-end Phase 1 test against the sacrificial in-scope
request 'KTB FW26 Wrangler' (UAT). Exercises the real connector + core:

  1. pull current DTC rows (WIP_ITS_USE)
  2. synthesize BeProduct rows: one that UPDATES an existing real row
     (Product Status), and one that INSERTS a brand-new sentinel style
  3. compute_upsert() -> plan
  4. push via connector.patch_rows(to_sheet_data(plan))
  5. verify the update + insert landed
  6. revert the update to its original value and neutralize the inserted row

Requires network access to DTC UAT. Skips cleanly if unreachable.
"""
import sys, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))
from connectors.dtc import DTCConnector
from sync import phase1

API_KEY = "49A127E0942071B4BD440DD00386C6B3"
REQUEST_ID = "6a26581854e92e7acd8fa71b"   # KTB FW26 Wrangler (sacrificial)
SENTINEL_LF = "ZZ_PHASE1_LIVE_TEST"

failures = []
def check(c, m):
    print(("  ✅ " if c else "  ❌ ") + m)
    if not c: failures.append(m)

c = DTCConnector(api_key=API_KEY, environment="uat", workspace_name="KTB")
scope = c.get_request_scope(REQUEST_ID)
sid, vid = scope["sheet_id"], scope["wip_view_id"]
print(f"Request: {scope['request_reference']} | season={scope['season_code']} brand={scope['brand']}")

allowed = set(c.get_view_column_names(sid, vid))
rows = c.get_sheet(sid, vid)["sheetData"]
print(f"Current rows: {len(rows)} | view cols: {len(allowed)}")

# pick a real, non-blank existing row whose (LF Style#, Color/Wash) match-key is
# UNIQUE in this sheet (the sacrificial request has duplicate-key test rows, which
# would otherwise resolve the update to a different physical row - correct per the
# key, but not what this single-row assertion expects).
from collections import Counter
key_counts = Counter(
    (phase1.norm(r.get("LF Style#")), phase1.norm(r.get("Color / Wash"))) for r in rows
)
target = next(
    r for r in rows
    if phase1.norm(r.get("LF Style#")) and phase1.norm(r.get("Color / Wash"))
    and key_counts[(phase1.norm(r.get("LF Style#")), phase1.norm(r.get("Color / Wash")))] == 1
)
orig_status = target.get("Product Status")
new_status = "Proto" if phase1.norm(orig_status) != "Proto" else "Production"
print(f"Update target rowId={target['rowId']} LF={target['LF Style#']!r} "
      f"Product Status {orig_status!r} -> {new_status!r}")

bp_rows = [
    {  # UPDATE existing row
        "lf_style_number": target["LF Style#"], "color": target["Color / Wash"],
        "brands": scope["brand"], "season_code": scope["season_code"],
        "product_status": new_status,
    },
    {  # INSERT new sentinel row
        "lf_style_number": SENTINEL_LF, "color": "TestColor",
        "brands": scope["brand"], "season_code": scope["season_code"],
        "product_status": "Proto", "description": "phase1 live test",
    },
]

plan = phase1.compute_upsert(scope, rows, bp_rows, allowed_cols=allowed)
print("Plan summary:", plan.summary())
check(plan.summary()["updates"] == 1, "1 UPDATE planned")
check(plan.summary()["inserts"] == 1, "1 INSERT planned")
expected_idx = phase1.max_row_index(rows) + 1
check(plan.inserts[0].row_index == expected_idx, f"INSERT rowIndex == max+1 ({expected_idx})")

upd_sd = phase1.update_sheet_data(plan)
ins_sd = phase1.insert_sheet_data(plan)
print(f"Pushing {len(upd_sd)} update(s) + {len(ins_sd)} insert(s) as separate batches ...")
resp_u = c.patch_rows(sid, vid, upd_sd)
resp_i = c.patch_rows(sid, vid, ins_sd)
print("  update resp:", resp_u, "| insert resp:", resp_i)
check(resp_u.get("status_code") == 204 and resp_i.get("status_code") == 204,
      "both PATCH batches returned 204")

time.sleep(2)
after = c.get_sheet(sid, vid)["sheetData"]
upd_row = next((r for r in after if r.get("rowId") == target["rowId"]), None)
ins_row = next((r for r in after if phase1.norm(r.get("LF Style#")) == SENTINEL_LF), None)
check(upd_row is not None and phase1.norm(upd_row.get("Product Status")) == phase1.norm(new_status),
      "UPDATE applied on live DTC row")
check(ins_row is not None, "INSERT created a new live DTC row")
if ins_row:
    check(phase1.norm(ins_row.get("Style Description")) == "phase1 live test", "insert carried mapped field")

# ---- revert / cleanup ----
print("Reverting ...")
revert = [{"rowId": target["rowId"], "Product Status": orig_status if orig_status is not None else ""}]
if ins_row:
    revert.append({"rowId": ins_row["rowId"], "LF Style#": "", "Color / Wash": "",
                   "Product Status": "", "Style Description": "", "Brand": ""})
c.patch_rows(sid, vid, revert)
time.sleep(2)
final = c.get_sheet(sid, vid)["sheetData"]
rb = next((r for r in final if r.get("rowId") == target["rowId"]), None)
check(rb is not None and phase1.norm(rb.get("Product Status")) == phase1.norm(orig_status),
      "update reverted to original Product Status")
check(not any(phase1.norm(r.get("LF Style#")) == SENTINEL_LF for r in final),
      "inserted sentinel neutralized")

print("\n" + "=" * 70)
if failures:
    print(f"❌ {len(failures)} live failure(s):");  [print("  -", f) for f in failures]; sys.exit(1)
print("✅ LIVE PHASE 1 END-TO-END TEST PASSED (changes reverted)")
