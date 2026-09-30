#!/usr/bin/env python3
"""
Unit tests for the Phase 7 sample-app formatter (dtc/python/sync/samples.py).

Pure-Python, no Spark, no network. Run:
    python3 dtc/tests/test_samples.py

Phase 7: BeProduct sample-app submit history -> DTC status columns (all 6 apps).
Each app's DTC field = complete list of submits, one LINE per submit from the
submit's FIRST size: "submit_name","submitStatus","submitStatusDate" - multiple
submits on their own newline-separated line (changed 2026-08-28). This is a
plain quoted/comma-separated string, NOT a JSON array - no [ ] brackets at all.

DTC column mapping (all 6 confirmed in 204-field WIP_ITS_USE view, 2026-08-28,
after a DTC WIP doc restructure changed Fit/PP from the 2026-07-07 mapping):
    Proto Sample    -> "Proto Sample - Sample Status"
    PreLine Sample  -> "Pre-line Sample - Status"       (lowercase 'l', dash)
    SMS Sample      -> "SMS - Sample Status"
    Fit Sample      -> "2nd Fit Sample Approval Status"           (was "1st Fit ...")
    PP Sample       -> "PP Sample Submission Approval Status"     (was "2nd Fit ...";
                        confirmed correct by the project team 2026-08-28)
    TOP Sample      -> "TOP Sample Approval Status"
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from sync import samples
from sync.samples import (format_sample_field, format_submit_status, SAMPLE_SUBMIT_FIELDS,
                          FIT_SUBMIT_FIELDS, FIT_MAX_SUBMITS)
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

print("\n[2] single submit, single size -> one quoted comma-separated line, no brackets")
raw = json.dumps([rec("s1", "1ST Submit", "S", "Requested", "2026-05-14T16:18:10.194Z")])
out = format_sample_field(raw)
check(out == '"1ST Submit","Requested","2026-05-14T16:18:10.194Z"',
      f"one submit -> plain quoted line  (got {out})")
check("[" not in out and "]" not in out, "no square brackets at all")
check("\n" not in out, "single submit has no newline")

print("\n[3] value with spaces (Boy Short Sleeve Tee PP: 'Approved with Corrections')")
raw = json.dumps([rec("s1", "1ST Submit", "M", "Approved with Corrections",
                      "2026-05-11T11:39:48.528Z")])
out = format_sample_field(raw)
check(out == '"1ST Submit","Approved with Corrections","2026-05-11T11:39:48.528Z"',
      "status with spaces preserved")
check(phase1.norm(out) == out, "phase1.norm() leaves the quoted line unchanged (stable diff)")

print("\n[4] multiple submits -> one line PER submit, newline-separated (no brackets)")
raw = json.dumps([
    rec("s1", "1ST Submit", "S", "Requested", "2026-05-14T00:00:00Z"),
    rec("s2", "2ND Submit", "S", "Approved",  "2026-06-20T00:00:00Z"),
])
out = format_sample_field(raw)
check(out == '"1ST Submit","Requested","2026-05-14T00:00:00Z"\n'
             '"2ND Submit","Approved","2026-06-20T00:00:00Z"',
      f"two submits -> two newline-separated lines  (got {out!r})")
check("[" not in out and "]" not in out, "no square brackets anywhere (2 submits)")
check(out.count("\n") == 1, "exactly one newline between the two submit lines")

print("\n[4b] phase1.norm() preserves the newline between submit lines (critical: "
      "build_target_payload pushes norm(value), so a collapsed newline would "
      "silently flatten this back into one line before reaching DTC)")
raw2 = json.dumps([
    rec("s1", "1ST Submit", "S", "Requested", "2026-05-14T00:00:00Z"),
    rec("s2", "2ND Submit", "S", "Approved",  "2026-06-20T00:00:00Z"),
])
out2 = format_sample_field(raw2)
check(phase1.norm(out2) == out2, "norm() is a no-op on the already-clean multi-line output")
check("\n" in phase1.norm(out2), "the newline itself survives norm() (not collapsed to a space)")

print("\n[5] multiple sizes per submit -> uses FIRST size only")
raw = json.dumps([
    rec("s1", "1ST Submit", "S", "Approved", "2026-05-01T00:00:00Z"),  # first size ← kept
    rec("s1", "1ST Submit", "M", "Rejected", "2026-05-02T00:00:00Z"),  # 2nd size  ← ignored
    rec("s1", "1ST Submit", "L", "Pending",  "2026-05-03T00:00:00Z"),  # 3rd size  ← ignored
    rec("s2", "2ND Submit", "S", "Approved", "2026-06-01T00:00:00Z"),
])
out = format_sample_field(raw)
check(out == '"1ST Submit","Approved","2026-05-01T00:00:00Z"\n'
             '"2ND Submit","Approved","2026-06-01T00:00:00Z"',
      "one line per submit, taken from first size")

print("\n[6] accepts an already-parsed list (not just JSON string)")
recs = [rec("s1", "1ST Submit", "S", "Requested", "2026-05-14T00:00:00Z")]
check(format_sample_field(recs) == '"1ST Submit","Requested","2026-05-14T00:00:00Z"',
      "list input handled same as JSON string")

print("\n[7] null status / date -> empty quotes, never the literal 'None'")
raw = json.dumps([rec("s1", "1ST Submit", "S", None, None)])
out = format_sample_field(raw)
check(out == '"1ST Submit","",""', f"null status/date -> empty quotes  (got {out})")
check("None" not in out, "never renders the Python literal 'None'")

print("\n[8] records without submit_id fall back to submit_name grouping")
raw = json.dumps([
    {"submit_name": "1ST Submit", "size": "S", "submit_status": "Approved",
     "submit_status_date": "2026-05-01T00:00:00Z"},
    {"submit_name": "1ST Submit", "size": "M", "submit_status": "Rejected",
     "submit_status_date": "2026-05-02T00:00:00Z"},
])
out = format_sample_field(raw)
check(out == '"1ST Submit","Approved","2026-05-01T00:00:00Z"',
      "no submit_id: grouped by name, first size kept")

print("\n[8b] embedded double-quote in a value is escaped by doubling (CSV-style)")
raw = json.dumps([rec("s1", 'Submit "A"', "S", "Approved", "2026-05-01T00:00:00Z")])
out = format_sample_field(raw)
check(out == '"Submit ""A""","Approved","2026-05-01T00:00:00Z"',
      f"embedded quote doubled, not backslash-escaped  (got {out})")

# CHANGED 2026-09-30: Fit left SAMPLE_SUBMIT_FIELDS for FIT_SUBMIT_FIELDS (one
# DTC column per submit). [9]-[12] were "all 6 apps"; they now cover the 5 others.
print("\n[9] SAMPLE_SUBMIT_FIELDS has the 5 non-Fit apps")
expected_raw_cols = {
    "proto_sample_json", "preline_sample_json", "sms_sample_json",
    "pp_sample_json", "top_sample_json",
}
check(set(SAMPLE_SUBMIT_FIELDS.keys()) == expected_raw_cols,
      "raw column keys = the 5 non-Fit sample prefixes (Fit moved to FIT_SUBMIT_FIELDS)")

print("\n[10] SAMPLE_SUBMIT_FIELDS -> correct DTC column names")
EXPECTED_DTC = {
    "proto_sample_json":   "Proto Sample - Sample Status",
    "preline_sample_json": "Pre-line Sample - Status",
    "sms_sample_json":     "SMS - Sample Status",
    "pp_sample_json":      "PP Sample Submission Approval Status",
    "top_sample_json":     "TOP Sample Approval Status",
}
for raw_col, expected_dtc in EXPECTED_DTC.items():
    actual = SAMPLE_SUBMIT_FIELDS[raw_col]["dtc"]
    check(actual == expected_dtc,
          f"{raw_col} -> DTC={actual!r}  (expected {expected_dtc!r})")

print("\n[11] every sample staging column (incl. both Fit columns) is in phase1.FIELD_MAPPING")
for raw_col, spec in SAMPLE_SUBMIT_FIELDS.items():
    check(phase1.FIELD_MAPPING.get(spec["staging"]) == spec["dtc"],
          f"phase1.FIELD_MAPPING[{spec['staging']!r}] == {spec['dtc']!r}")
for staging, spec in FIT_SUBMIT_FIELDS.items():
    check(phase1.FIELD_MAPPING.get(staging) == spec["dtc"],
          f"phase1.FIELD_MAPPING[{staging!r}] == {spec['dtc']!r}")
check("fit_sample_status" not in phase1.FIELD_MAPPING,
      "old single Fit column (full history) is gone from FIELD_MAPPING")

print("\n[12] staging column names are correct")
EXPECTED_STAGING = {
    "proto_sample_json":   "proto_sample_status",
    "preline_sample_json": "preline_sample_status",
    "sms_sample_json":     "sms_sample_status",
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

print("\n[14] Fit and PP never collide")
fit_cols = {spec["dtc"] for spec in FIT_SUBMIT_FIELDS.values()}
check(SAMPLE_SUBMIT_FIELDS["pp_sample_json"]["dtc"] not in fit_cols,
      "PP's column is not one of the Fit columns")

print("\n[15] FIT_SUBMIT_FIELDS -- submit 1 -> '1st Fit', submit 2 -> '2nd Fit' (owner spec 2026-09-30)")
check(FIT_MAX_SUBMITS == 2, "at most 2 Fit submits")
check(FIT_SUBMIT_FIELDS["fit_1st_sample_status"] ==
      {"raw": "fit_sample_json", "submit": 1, "dtc": "1st Fit Sample Approval Status"},
      "submit 1 -> 1st Fit Sample Approval Status")
check(FIT_SUBMIT_FIELDS["fit_2nd_sample_status"] ==
      {"raw": "fit_sample_json", "submit": 2, "dtc": "2nd Fit Sample Approval Status"},
      "submit 2 -> 2nd Fit Sample Approval Status")
check(len(FIT_SUBMIT_FIELDS) == FIT_MAX_SUBMITS, "one column per allowed submit, no more")
check(fit_cols <= phase1.CLEAR_WHEN_BLANK_COLS, "both Fit columns follow BeProduct to blank")

print("\n[16] format_submit_status() -- status + timestamp only, one submit")
# Live shape: KTB-00029's Fit history (2 submits, each with several sizes).
fit_raw = json.dumps([
    {"submit_id": "s1", "submit_name": "1ST Submit", "size": "M",
     "submit_status": "Requested", "submit_status_date": "2026-09-23T11:16:14.37Z"},
    {"submit_id": "s1", "submit_name": "1ST Submit", "size": "L",
     "submit_status": "IGNORED-2nd-size", "submit_status_date": "x"},
    {"submit_id": "s2", "submit_name": "2ND Submit", "size": "M",
     "submit_status": "Approved", "submit_status_date": "2026-09-23T16:02:25.877Z"},
    {"submit_id": "s3", "submit_name": "3RD Submit", "size": "M",
     "submit_status": "Approved", "submit_status_date": "2026-10-01T00:00:00Z"},
])
check(format_submit_status(fit_raw, 1) == '"Requested","2026-09-23T11:16:14.37Z"',
      "submit 1 -> first size's status + date, NO submit name")
check(format_submit_status(fit_raw, 2) == '"Approved","2026-09-23T16:02:25.877Z"',
      "submit 2 -> its own status + date")
check(format_submit_status(fit_raw, 3) != "" and 3 > FIT_MAX_SUBMITS,
      "a 3rd submit exists in the data, but no column maps it (ignored by config, not by the formatter)")
one_submit = json.dumps([json.loads(fit_raw)[0]])
check(format_submit_status(one_submit, 2) == "", "missing 2nd submit -> '' (phase1 clears the cell)")
check(format_submit_status("[]", 1) == "" and format_submit_status(None, 1) == "", "no history -> ''")
check(format_submit_status(fit_raw, 0) == "", "submit 0 is invalid -> ''")
with_none = json.dumps([{"submit_id": "s1", "submit_name": "1ST", "submit_status": "Requested",
                         "submit_status_date": None}])
check(format_submit_status(with_none, 1) == '"Requested",""', "missing date -> empty quotes")

print("\n[17] phase1: Fit columns update when the status text changes, and CLEAR when gone")
base = {"BP Style#": "S1", "Color / Wash": "Red"}
dtc_now = dict(base, **{"1st Fit Sample Approval Status": '"Requested","t1"',
                        "2nd Fit Sample Approval Status": '"Requested","t2"'})
bp_row = {"bp_style_number": "S1", "color": "Red",
          "fit_1st_sample_status": '"Approved","t3"', "fit_2nd_sample_status": ""}
changed = phase1.diff_updatable_fields(dtc_now, bp_row)
check(changed.get("1st Fit Sample Approval Status") == '"Approved","t3"',
      "status text changed in BeProduct -> DTC updated")
check("2nd Fit Sample Approval Status" in changed and changed["2nd Fit Sample Approval Status"] is None,
      "2nd submit removed in BeProduct -> DTC cell CLEARED (null)")
dtc_blank = dict(base)
changed = phase1.diff_updatable_fields(dtc_blank, {"bp_style_number": "S1", "color": "Red",
                                                   "fit_1st_sample_status": "", "fit_2nd_sample_status": ""})
check(not any(c in changed for c in fit_cols), "blank in both -> nothing written (zero-diff)")
changed = phase1.diff_updatable_fields(
    dict(base, **{"Proto Sample - Sample Status": '"1ST","Approved","t"'}),
    {"bp_style_number": "S1", "color": "Red", "proto_sample_status": ""})
check("Proto Sample - Sample Status" not in changed,
      "other sample columns keep the old rule: a blank never clears")
changed = phase1.diff_updatable_fields(dtc_now, bp_row, allowed_cols={"1st Fit Sample Approval Status"})
check("2nd Fit Sample Approval Status" not in changed, "a clear respects allowed_cols")

print("\n" + "=" * 70)
if _failures:
    print(f"❌ {len(_failures)} FAILURE(S):")
    for f in _failures:
        print("   -", f)
    sys.exit(1)
print("✅ ALL PHASE 7 SAMPLE FORMATTER TESTS PASSED")
