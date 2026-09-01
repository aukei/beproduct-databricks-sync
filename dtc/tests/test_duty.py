#!/usr/bin/env python3
"""
Unit tests for the Phase 9b NT Orbit Duty Tools core (dtc/python/sync/duty.py).

Pure-Python, no Spark, no network. Run:
    python3 dtc/tests/test_duty.py
or with pytest:
    pytest dtc/tests/test_duty.py
"""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from sync import duty
from sync.duty import (
    build_product_description, build_calc_request, cache_key,
    markets_needing_lookup, row_needs_any_lookup, extract_duty_fields,
    merge_lookup_into_row, build_wip_patch_fields, DutyLookupResult,
    is_cache_entry_stale, build_cache_row, cache_row_to_result,
    DUTY_CACHE_KEY_COLS, DEFAULT_CACHE_TTL_DAYS,
)

_failures = []


def check(cond, msg):
    if cond:
        print(f"  ✅ {msg}")
    else:
        print(f"  ❌ {msg}")
        _failures.append(msg)


# ---------------------------------------------------------------------------
print("\n[1] build_product_description()")
row = {
    "style_description": "Cotton t-shirt with printed design",
    "fabric_content": "100% Cotton",
    "gender": "Mens",
    "class_name": "Tops",
    "sub_class": "T-Shirts",
}
check(
    build_product_description(row)
    == "Cotton t-shirt with printed design 100% Cotton Mens Tops T-Shirts",
    "concatenates description + content + gender + class + sub_class",
)
check(build_product_description({}) == "", "all-blank row -> empty string")
check(
    build_product_description({"style_description": "X", "gender": None, "class_name": ""})
    == "X",
    "blanks are skipped, no double spaces / stray tokens",
)

print("\n[2] build_calc_request() / cache_key()")
row2 = {**row, "production_country": "BD"}
req = build_calc_request(row2, "US")
check(req == {
    "product_description": build_product_description(row2),
    "origin_country_code": "BD",
    "import_country_code": "US",
    "export_country_code": "BD",
    "de_minimis": False,
    "mode_of_transport": "freight",
}, "builds the exact NT Orbit request shape from the spec example")
check(
    cache_key(row2, "US") == cache_key(row2, "US"),
    "cache_key is stable for identical rows",
)
check(
    cache_key(row2, "US") != cache_key(row2, "CA"),
    "cache_key differs per market",
)

print("\n[3] markets_needing_lookup() / row_needs_any_lookup()")
blank_row = {**row2, "hts_code": None, "duty_rate_us": None, "duty_rate_ca": None, "duty_rate_mx": None}
check(set(markets_needing_lookup(blank_row)) == {"US", "CA", "MX"},
      "all 3 markets needed when everything is blank")
partially_filled = {**blank_row, "duty_rate_us": 0.165}
check(set(markets_needing_lookup(partially_filled)) == {"CA", "MX"},
      "already-filled market is skipped")
fully_filled = {**blank_row, "hts_code": "6109100012",
                "duty_rate_us": 0.165, "duty_rate_ca": 0.1, "duty_rate_mx": 0.05}
check(markets_needing_lookup(fully_filled) == [], "no lookups needed when everything is filled")
no_country = {**blank_row, "production_country": None}
check(markets_needing_lookup(no_country) == [],
      "no production_country -> no lookups possible (API requires it)")
check(row_needs_any_lookup(blank_row) is True, "row_needs_any_lookup True when gaps exist")
check(row_needs_any_lookup(fully_filled) is False, "row_needs_any_lookup False when nothing missing")

print("\n[4] extract_duty_fields() — spec example response")
example_response = {
    "success": True,
    "data": {
        "total_duty": 266.25,
        "duty_rate": 0.26625,
        "hs_code": "6109100012",
        "classification_name": "T-Shirts - cotton (knitted)",
        "detailed_lines": [
            {"name": "General Duty", "code": "6109100012", "amount": 165,
             "rate": 0.165, "type": "duty"},
            {"name": "10% Section 122 tariff on foreign-origin items", "code": "",
             "amount": 100, "rate": 0.1, "type": "duty"},
            {"name": "0.125% Harbor Maintenance Fee (HMF)", "code": "",
             "amount": 1.25, "rate": 0.00125, "type": "fee"},
        ],
    },
}
result = extract_duty_fields(example_response)
check(result.hts_code == "6109100012", "hs_code -> hts_code")
check(result.duty_rate == 0.165, "duty_rate = General Duty line's own rate (not the 0.26625 total)")
check(abs(result.tariff_rate - 0.1) < 1e-9,
      "tariff_rate = sum of non-General-Duty 'duty' lines (fee line excluded)")

print("\n[4b] extract_duty_fields() — multiple tariff lines summed, no tariff line")
multi_tariff_resp = {
    "success": True,
    "data": {"hs_code": "X", "detailed_lines": [
        {"name": "General Duty", "rate": 0.1, "type": "duty"},
        {"name": "Section 301 tariff", "rate": 0.075, "type": "duty"},
        {"name": "Section 122 tariff", "rate": 0.1, "type": "duty"},
    ]},
}
r2 = extract_duty_fields(multi_tariff_resp)
check(abs(r2.tariff_rate - 0.175) < 1e-9, "multiple tariff lines are summed")

no_tariff_resp = {"success": True, "data": {"hs_code": "X",
                  "detailed_lines": [{"name": "General Duty", "rate": 0.1, "type": "duty"}]}}
r3 = extract_duty_fields(no_tariff_resp)
check(r3.tariff_rate is None, "no non-General-Duty duty lines -> tariff_rate stays None")

try:
    extract_duty_fields({"success": False})
    check(False, "success=False should raise")
except ValueError:
    check(True, "success=False raises ValueError")

try:
    extract_duty_fields({"success": True, "data": {}})
    check(False, "empty data should raise")
except ValueError:
    check(True, "empty data raises ValueError")

print("\n[5] merge_lookup_into_row()")
target_row = {"hts_code": None, "duty_rate_us": None, "duty_rate_ca": None,
              "duty_rate_mx": None, "tariff_rate": None}
updates_us = merge_lookup_into_row(target_row, "US", result)
check(updates_us == {"hts_code": "6109100012", "duty_rate_us": 0.165, "tariff_rate": 0.1},
      "US lookup fills hts_code + duty_rate_us + tariff_rate")

updates_ca = merge_lookup_into_row(target_row, "CA", DutyLookupResult(
    hts_code="6109100012", duty_rate=0.18, tariff_rate=None))
check(updates_ca == {"hts_code": "6109100012", "duty_rate_ca": 0.18},
      "CA lookup never sets tariff_rate")

already_has_hts = {**target_row, "hts_code": "EXISTING"}
updates_no_overwrite = merge_lookup_into_row(already_has_hts, "US", result)
check(updates_no_overwrite.get("hts_code") is None,
      "existing non-blank hts_code is never overwritten (write-once)")
check(updates_no_overwrite.get("duty_rate_us") == 0.165,
      "but a still-blank sibling column IS filled")

print("\n[6] build_wip_patch_fields()")
plan = build_wip_patch_fields("Main", {
    "hts_code": "6109100012", "duty_rate_us": 0.165, "tariff_rate": 0.1,
})
check(plan.fields == {
    "Main Factory HTS Code": "6109100012",
    "Main Factory Duty Rate (US)": 0.165,
}, "maps hts_code + duty_rate_us to the exact live WIP 'Main' slot column names")
check(len(plan.skipped) == 1 and "Tariff Rate" in plan.skipped[0],
      "tariff_rate is reported as skipped (no live WIP column yet), not silently dropped")

plan_slot1 = build_wip_patch_fields("1", {"duty_rate_ca": 0.2, "duty_rate_mx": 0.05})
check(plan_slot1.fields == {
    "Factory 1 - Duty Rate (CA)": 0.2,
    "Factory 1 - Duty Rate (MX)": 0.05,
}, "maps CA/MX duty rates to the exact 'Factory 1' slot column names")

try:
    build_wip_patch_fields("5", {"hts_code": "X"})
    check(False, "unknown factory_slot should raise")
except ValueError:
    check(True, "unknown factory_slot raises ValueError")

print("\n[7] Persistent cross-run cache — is_cache_entry_stale() / build_cache_row() / cache_row_to_result()")
now = datetime(2026, 9, 1, tzinfo=timezone.utc)

check(DUTY_CACHE_KEY_COLS == ("product_description", "origin_country_code", "import_country_code"),
      "cache key columns match cache_key()'s tuple order")

check(is_cache_entry_stale(None, now) is True, "missing looked_up_at is always treated as stale")
fresh = now - timedelta(days=1)
check(is_cache_entry_stale(fresh, now, ttl_days=DEFAULT_CACHE_TTL_DAYS) is False,
      "1-day-old entry is NOT stale under the default TTL")
old = now - timedelta(days=DEFAULT_CACHE_TTL_DAYS + 1)
check(is_cache_entry_stale(old, now, ttl_days=DEFAULT_CACHE_TTL_DAYS) is True,
      "entry older than the TTL IS stale")
boundary = now - timedelta(days=30)
check(is_cache_entry_stale(boundary, now, ttl_days=30) is False,
      "exactly at the TTL boundary is NOT yet stale (> comparison, not >=)")
check(is_cache_entry_stale(boundary, now, ttl_days=10) is True,
      "a shorter custom ttl_days is respected")

print("\n[7b] is_cache_entry_stale() — naive vs. aware datetime mix (live bug, 2026-09-01)")
# Spark's TIMESTAMP columns come back as NAIVE datetimes via .asDict(), while
# the notebook's `now = datetime.now(timezone.utc)` is AWARE. This combo
# raised "TypeError: can't subtract offset-naive and offset-aware datetimes"
# in production before the fix (_as_naive_utc normalization).
aware_now = datetime(2026, 9, 1, tzinfo=timezone.utc)
naive_now = datetime(2026, 9, 1)
naive_fresh = datetime(2026, 8, 31)   # naive, 1 day before aware_now
naive_old = datetime(2026, 1, 1)      # naive, well past the default TTL
aware_fresh = datetime(2026, 8, 31, tzinfo=timezone.utc)

try:
    r1 = is_cache_entry_stale(naive_fresh, aware_now)  # naive looked_up_at, aware now
    r2 = is_cache_entry_stale(aware_fresh, naive_now)  # aware looked_up_at, naive now
    r3 = is_cache_entry_stale(naive_old, aware_now)
    check(r1 is False, "naive looked_up_at + aware now: 1-day-old is not stale, no crash")
    check(r2 is False, "aware looked_up_at + naive now: 1-day-old is not stale, no crash")
    check(r3 is True, "naive looked_up_at + aware now: old entry correctly detected as stale")
except TypeError as e:
    check(False, f"naive/aware datetime mix raised TypeError (the live bug): {e}")

key_us = cache_key(row2, "US")
cache_row = build_cache_row(key_us, result, looked_up_at=now)
check(cache_row == {
    "product_description": build_product_description(row2),
    "origin_country_code": "BD",
    "import_country_code": "US",
    "hts_code": "6109100012",
    "duty_rate": 0.165,
    "tariff_rate": 0.1,
    "classification_name": "T-Shirts - cotton (knitted)",
    "looked_up_at": now,
}, "build_cache_row() produces the exact persistent-table row shape")

reconstructed = cache_row_to_result(cache_row)
check(reconstructed.hts_code == result.hts_code
      and reconstructed.duty_rate == result.duty_rate
      and reconstructed.tariff_rate == result.tariff_rate
      and reconstructed.classification_name == result.classification_name,
      "cache_row_to_result() round-trips a persistent-cache row back into a usable DutyLookupResult")

# A cache hit reconstructed this way must behave identically to a fresh
# lookup when merged onto a row - this is the whole point of the persistent
# cache (skip the ~30s API call, not skip the fill).
merged_from_cache = merge_lookup_into_row(target_row, "US", reconstructed)
check(merged_from_cache == updates_us,
      "merging a cache-reconstructed result produces the same fields as a live lookup")

# ---------------------------------------------------------------------------
print(f"\n{'='*60}")
if _failures:
    print(f"❌ {len(_failures)} check(s) failed:")
    for f in _failures:
        print(f"   - {f}")
    sys.exit(1)
else:
    print("✅ All checks passed")
