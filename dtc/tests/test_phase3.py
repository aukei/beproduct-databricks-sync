#!/usr/bin/env python3
"""
Unit tests for the Phase 3 BeProduct -> DTC image-upload core
(dtc/python/sync/phase3.py).

Pure-Python, no Spark, no network. Run:
    python3 dtc/tests/test_phase3.py
or with pytest:
    pytest dtc/tests/test_phase3.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from sync.phase3 import (
    is_image_populated, is_valid_image_url, compute_image_uploads,
    classify_image_type,
)

_failures = []


def check(cond, msg):
    if cond:
        print(f"  ✅ {msg}")
    else:
        print(f"  ❌ {msg}")
        _failures.append(msg)


# ---------------------------------------------------------------------------
print("\n[1] is_image_populated()")
check(is_image_populated({"Style Image": "https://cdn/x.jpg"}) is True, "url cell -> populated")
check(is_image_populated({"Style Image": ""}) is False, "empty string -> not populated")
check(is_image_populated({"Style Image": "  "}) is False, "whitespace -> not populated")
check(is_image_populated({"Style Image": "N/A"}) is False, "null sentinel -> not populated")
check(is_image_populated({}) is False, "absent column -> not populated")

print("\n[2] is_valid_image_url()")
check(is_valid_image_url("https://cdn.beproduct/img.jpg") is True, "https accepted")
check(is_valid_image_url("http://cdn/img.png") is True, "http accepted")
check(is_valid_image_url("ftp://cdn/img.jpg") is False, "non-http rejected")
check(is_valid_image_url("") is False, "empty rejected")
check(is_valid_image_url(None) is False, "None rejected")
check(is_valid_image_url("N/A") is False, "null sentinel rejected")

print("\n[3] compute_image_uploads() - blank DTC + valid BP url -> upload")
dtc_rows = [
    {"LF Style#": "S1", "Color / Wash": "Blue", "rowId": "r1", "rowIndex": 1},  # blank image
]
bp_rows = [
    {"lf_style_number": "S1", "color": "Blue", "front_image_url": "https://cdn/s1.jpg"},
]
plan = compute_image_uploads(dtc_rows, bp_rows)
check(len(plan.uploads) == 1 and len(plan.skips) == 0, "one upload, no skip")
op = plan.uploads[0]
check(op.row_index == 1 and op.image_url == "https://cdn/s1.jpg" and op.row_id == "r1",
      "upload carries rowIndex, url, rowId")

print("\n[4] already-populated image -> skipped silently")
dtc_rows = [
    {"LF Style#": "S1", "Color / Wash": "Blue", "rowId": "r1", "rowIndex": 1,
     "Style Image": "https://cdn/existing.jpg"},
]
plan = compute_image_uploads(dtc_rows, bp_rows)
check(plan.uploads == [] and plan.skips == [], "no upload and no skip record (idempotent)")

print("\n[5] blank DTC but BP has no usable url -> recorded skip")
dtc_rows = [{"LF Style#": "S1", "Color / Wash": "Blue", "rowId": "r1", "rowIndex": 1}]
bp_rows = [{"lf_style_number": "S1", "color": "Blue", "front_image_url": "N/A"}]
plan = compute_image_uploads(dtc_rows, bp_rows)
check(len(plan.uploads) == 0 and len(plan.skips) == 1, "no upload, one skip")
check(plan.skips[0].reason == "no_source_image", "skip reason no_source_image")

print("\n[6] DTC row with no matching BeProduct row -> left alone (no upload/skip)")
dtc_rows = [{"LF Style#": "GHOST", "Color / Wash": "Red", "rowId": "r9", "rowIndex": 9}]
bp_rows = [{"lf_style_number": "S1", "color": "Blue", "front_image_url": "https://cdn/s1.jpg"}]
plan = compute_image_uploads(dtc_rows, bp_rows)
check(plan.uploads == [] and plan.skips == [], "unmatched DTC row ignored")

print("\n[7] blank DTC, valid url, but missing rowIndex -> recorded skip")
dtc_rows = [{"LF Style#": "S1", "Color / Wash": "Blue", "rowId": "r1"}]  # no rowIndex
bp_rows = [{"lf_style_number": "S1", "color": "Blue", "front_image_url": "https://cdn/s1.jpg"}]
plan = compute_image_uploads(dtc_rows, bp_rows)
check(len(plan.uploads) == 0 and len(plan.skips) == 1
      and plan.skips[0].reason == "missing_row_index", "missing rowIndex -> skip")

print("\n[8] rowIndex == 0 is valid (not treated as missing)")
dtc_rows = [{"LF Style#": "S1", "Color / Wash": "Blue", "rowId": "r1", "rowIndex": 0}]
bp_rows = [{"lf_style_number": "S1", "color": "Blue", "front_image_url": "https://cdn/s1.jpg"}]
plan = compute_image_uploads(dtc_rows, bp_rows)
check(len(plan.uploads) == 1 and plan.uploads[0].row_index == 0, "rowIndex 0 uploads")

print("\n[9] fully-blank DTC rows (no key) are ignored")
dtc_rows = [{"rowId": "r1", "rowIndex": 1}, {"LF Style#": "", "Color / Wash": "", "rowIndex": 2}]
bp_rows = [{"lf_style_number": "S1", "color": "Blue", "front_image_url": "https://cdn/s1.jpg"}]
plan = compute_image_uploads(dtc_rows, bp_rows)
check(plan.uploads == [] and plan.skips == [], "keyless DTC rows skipped")

print("\n[10] match-key whitespace normalisation")
dtc_rows = [{"LF Style#": " S1 ", "Color / Wash": "Blue ", "rowId": "r1", "rowIndex": 1}]
bp_rows = [{"lf_style_number": "S1", "color": "Blue", "front_image_url": "https://cdn/s1.jpg"}]
plan = compute_image_uploads(dtc_rows, bp_rows)
check(len(plan.uploads) == 1, "whitespace-different keys still match")

print("\n[11] classify_image_type() - native jpg/png upload as-is")
e = classify_image_type("image/jpeg", "https://cdn/x.jpg")
check(e.action == "upload" and e.content_type == "image/jpeg", "jpeg -> upload")
e = classify_image_type("image/png; charset=binary", "https://cdn/x.png")
check(e.action == "upload" and e.content_type == "image/png", "png (with params) -> upload")

print("\n[12] classify_image_type() - webp/gif/bmp/tiff convert to png")
for ct in ["image/webp", "image/gif", "image/bmp", "image/tiff"]:
    e = classify_image_type(ct, None)
    check(e.action == "convert" and e.content_type == "image/png", f"{ct} -> convert png")

print("\n[13] classify_image_type() - svg/unknown skipped")
check(classify_image_type("image/svg+xml", "https://cdn/x.svg").action == "skip", "svg -> skip")
check(classify_image_type("application/pdf", "https://cdn/x.pdf").action == "skip", "pdf -> skip")

print("\n[14] classify_image_type() - falls back to URL extension when CT generic")
e = classify_image_type("application/octet-stream", "https://cdn/pic.webp?sig=abc")
check(e.action == "convert" and e.content_type == "image/png", "octet-stream + .webp -> convert")
e = classify_image_type(None, "https://cdn/pic.JPG?x=1")
check(e.action == "upload" and e.content_type == "image/jpeg", "no CT + .JPG -> upload jpeg")
e = classify_image_type("", "https://cdn/logo.svg")
check(e.action == "skip", "no CT + .svg -> skip")

# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
if _failures:
    print(f"❌ {len(_failures)} test(s) failed:")
    for f in _failures:
        print(f"   - {f}")
    sys.exit(1)
else:
    print("✅ All Phase 3 tests passed")
