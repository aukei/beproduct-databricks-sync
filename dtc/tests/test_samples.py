#!/usr/bin/env python3
"""
Unit tests for the Phase 7 sample-app formatter (dtc/python/sync/samples.py).

Pure-Python, no Spark, no network. Run:
    python3 dtc/tests/test_samples.py

Phase 7: BeProduct sample-app submit history -> DTC status columns (all 6 apps).
Each app's DTC field = complete list of submits, one triple per submit from the
submit's FIRST size: [submit_name, submitStatus, submitStatusDate].

DTC column mapping (all 6 confirmed in 198-field WIP_ITS_USE view, 2026-07-07):
    Proto Sample    -> "Proto Sample - Sample Status"
    PreLine Sample  -> "Pre-line Sample - Status"       (lowercase 'l', dash)
    SMS Sample      -> "SMS - Sample Status"
    Fit Sample      -> "1st Fit Sample Approval Status"
    PP Sample       -> "2nd Fit Sample Approval Status"
    TOP Sample      -> "TOP Sample Approval Status"
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from sync import samples
from sync.samples import format_sample_field, SAMPLE_SUBMIT_FIELDS
from sync import phase1

_failures = []


def check(cond, msg):
    print(f"  {'✅' if cond else '❌'} {msg}")
    if not cond:
        _failures.append(msg)


# Raw record shape as stored by p1p7_beproduct_style_sync.extract_sample_submits
def rec(submit_id, name, size, status, date):
    return {
        "submit_id": submit_id, "submit_name": name,
        "size_id": f"sz-{size}", "size": size, "is_sample_size": False,
        "submit_status": status, "submit_status_date": date,
        "due_date": None, "received_date": None, "fit_date": None,
    }


print("\n[1] empty / blank inputs -> ''")
check(format_sample_field(None) == "", "None -> ''")
check(format_sample_field("") == "", "'' -> ''")
check(format_sample_field("[]") == "", "'[]' -> ''")
check(format_sample_field([]) == "", "empty list -> ''")
check(format_sample_field("not json") == "", "malformed JSON -> ''")
check(format_sample_field("{}") == "", "non-list JSON -> ''")

print("\n[2] single submit, single size (real HOODED-K263 Proto shape)")
raw = json.dumps([rec("s1", "1ST Submit", "S", "Requested", "2026-05-14T16:18:10.194Z")])
out = format_sample_field(raw)
check(out == '[["1ST Submit","Requested","2026-05-14T16:18:10.194Z"]]',
      f"one submit -> compact JSON triple  (got {out})")
check(json.loads(out) == [["1ST Submit", "Requested", "2026-05-14T16:18:10.194Z"]],
      "output parses back to the expected triple list")

print("\n[3] value with spaces (Boy Short Sleeve Tee PP: 'Approved with Corrections')")
raw = json.dumps([rec("s1", "1ST Submit", "M", "Approved with Corrections",
                      "2026-05-11T11:39:48.528Z")])
out = format_sample_field(raw)
check(out == '[["1ST Submit","Approved with Corrections","2026-05-11T11:39:48.528Z"]]',
      "status with spaces preserved")
check(phase1.norm(out) == out, "phase1.norm() leaves compact JSON unchanged (stable diff)")

print("\n[4] multiple submits -> complete ordered list")
raw = json.dumps([
    rec("s1", "1ST Submit", "S", "Requested", "2026-05-14T00:00:00Z"),
    rec("s2", "2ND Submit", "S", "Approved",  "2026-06-20T00:00:00Z"),
])
out = format_sample_field(raw)
check(json.loads(out) == [
    ["1ST Submit", "Requested", "2026-05-14T00:00:00Z"],
    ["2ND Submit", "Approved",  "2026-06-20T00:00:00Z"],
], "two submits, order preserved")

print("\n[5] multiple sizes per submit -> uses FIRST size only")
raw = json.dumps([
    rec("s1", "1ST Submit", "S", "Approved", "2026-05-01T00:00:00Z"),  # first size ← kept
    rec("s1", "1ST Submit", "M", "Rejected", "2026-05-02T00:00:00Z"),  # 2nd size  ← ignored
    rec("s1", "1ST Submit", "L", "Pending",  "2026-05-03T00:00:00Z"),  # 3rd size  ← ignored
    rec("s2", "2ND Submit", "S", "Approved", "2026-06-01T00:00:00Z"),
])
out = format_sample_field(raw)
check(json.loads(out) == [
    ["1ST Submit", "Approved", "2026-05-01T00:00:00Z"],
    ["2ND Submit", "Approved", "2026-06-01T00:00:00Z"],
], "one triple per submit, taken from first size")

print("\n[6] accepts an already-parsed list (not just JSON string)")
recs = [rec("s1", "1ST Submit", "S", "Requested", "2026-05-14T00:00:00Z")]
check(format_sample_field(recs) == '[["1ST Submit","Requested","2026-05-14T00:00:00Z"]]',
      "list input handled same as JSON string")

print("\n[7] null status / date preserved as JSON null")
raw = json.dumps([rec("s1", "1ST Submit", "S", None, None)])
out = format_sample_field(raw)
check(json.loads(out) == [["1ST Submit", None, None]], "null status/date -> JSON null")

print("\n[8] records without submit_id fall back to submit_name grouping")
raw = json.dumps([
    {"submit_name": "1ST Submit", "size": "S", "submit_status": "Approved",
     "submit_status_date": "2026-05-01T00:00:00Z"},
    {"submit_name": "1ST Submit", "size": "M", "submit_status": "Rejected",
     "submit_status_date": "2026-05-02T00:00:00Z"},
])
out = format_sample_field(raw)
check(json.loads(out) == [["1ST Submit", "Approved", "2026-05-01T00:00:00Z"]],
      "no submit_id: grouped by name, first size kept")

print("\n[9] SAMPLE_SUBMIT_FIELDS has exactly 6 entries (all apps)")
check(len(SAMPLE_SUBMIT_FIELDS) == 6, "6 entries in SAMPLE_SUBMIT_FIELDS")
expected_raw_cols = {
    "proto_sample_json", "preline_sample_json", "sms_sample_json",
    "fit_sample_json", "pp_sample_json", "top_sample_json",
}
check(set(SAMPLE_SUBMIT_FIELDS.keys()) == expected_raw_cols,
      "raw column keys match all 6 sample prefixes")

print("\n[10] SAMPLE_SUBMIT_FIELDS -> correct DTC column names (all 6)")
EXPECTED_DTC = {
    "proto_sample_json":   "Proto Sample - Sample Status",
    "preline_sample_json": "Pre-line Sample - Status",
    "sms_sample_json":     "SMS - Sample Status",
    "fit_sample_json":     "1st Fit Sample Approval Status",
    "pp_sample_json":      "2nd Fit Sample Approval Status",
    "top_sample_json":     "TOP Sample Approval Status",
}
for raw_col, expected_dtc in EXPECTED_DTC.items():
    actual = SAMPLE_SUBMIT_FIELDS[raw_col]["dtc"]
    check(actual == expected_dtc,
          f"{raw_col} -> DTC={actual!r}  (expected {expected_dtc!r})")

print("\n[11] all 6 staging columns present in phase1.FIELD_MAPPING")
for raw_col, spec in SAMPLE_SUBMIT_FIELDS.items():
    staging = spec["staging"]
    dtc     = spec["dtc"]
    check(phase1.FIELD_MAPPING.get(staging) == dtc,
          f"phase1.FIELD_MAPPING[{staging!r}] == {dtc!r}")

print("\n[12] staging column names are correct")
EXPECTED_STAGING = {
    "proto_sample_json":   "proto_sample_status",
    "preline_sample_json": "preline_sample_status",
    "sms_sample_json":     "sms_sample_status",
    "fit_sample_json":     "fit_sample_status",
    "pp_sample_json":      "pp_sample_status",
    "top_sample_json":     "top_sample_status",
}
for raw_col, expected_staging in EXPECTED_STAGING.items():
    actual = SAMPLE_SUBMIT_FIELDS[raw_col]["staging"]
    check(actual == expected_staging,
          f"{raw_col} staging={actual!r}  (expected {expected_staging!r})")

print("\n[13] 'Pre-line Sample - Status' uses lowercase 'l' and dash (DTC exact name)")
check(SAMPLE_SUBMIT_FIELDS["preline_sample_json"]["dtc"] == "Pre-line Sample - Status",
      "Pre-line uses lowercase 'l' and dash — matches DTC view exactly")

print("\n" + "=" * 70)
if _failures:
    print(f"❌ {len(_failures)} FAILURE(S):")
    for f in _failures:
        print("   -", f)
    sys.exit(1)
print("✅ ALL PHASE 7 SAMPLE FORMATTER TESTS PASSED")
