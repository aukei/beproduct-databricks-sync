#!/usr/bin/env python3
"""
Unit tests for style/WIP-row lifecycle gating (dtc/python/sync/lifecycle.py).

Pure-Python, no Spark, no network. Run:
    python3 dtc/tests/test_lifecycle.py

Owner spec, 2026-09-08. See lifecycle.py's module docstring for the full
spec this resolves (the EXCLUDED_STATUSES / ktb_styles completeness bug
found during a full-pipeline conflict scan).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from sync.lifecycle import (
    EXCLUDED_STATUSES, DROPPED_VALUE, WIP_FIELD_ACTIVE_DROPPED,
    is_wip_row_dropped, should_include_in_staging,
)

_failures = []


def check(cond, msg):
    print(f"  {'✅' if cond else '❌'} {msg}")
    if not cond:
        _failures.append(msg)


print("=" * 60)
print("Lifecycle gating tests")
print("=" * 60)

# ---------------------------------------------------------------------------
print("\n[1] EXCLUDED_STATUSES")
check(EXCLUDED_STATUSES == frozenset({"Finalized", "Drop"}),
      "exact terminal-status set (Title Case, confirmed live via folder_schema)")

# ---------------------------------------------------------------------------
print("\n[2] is_wip_row_dropped()")
check(is_wip_row_dropped("Dropped") is True, "exact case match")
check(is_wip_row_dropped("dropped") is True, "lowercase")
check(is_wip_row_dropped("DROPPED") is True, "uppercase")
check(is_wip_row_dropped(" Dropped ") is True, "surrounding whitespace stripped")
check(is_wip_row_dropped("Active") is False, "a real, non-dropped value")
check(is_wip_row_dropped(None) is False, "None (missing/blank column) -> NOT dropped (safe default)")
check(is_wip_row_dropped("") is False, "empty string -> NOT dropped")
check(is_wip_row_dropped("Dropped Something Else") is False,
      "must be an EXACT match, not a substring")

# ---------------------------------------------------------------------------
print("\n[3] should_include_in_staging() — normal active style")
check(should_include_in_staging("Proto", None) is True,
      "active status, no WIP row yet -> include (first push)")
check(should_include_in_staging("Proto", "Proto") is True,
      "active status, WIP already matches -> still include (normal ongoing sync)")
check(should_include_in_staging("SMS", "Proto") is True,
      "active status, WIP shows a DIFFERENT (older) active status -> include (normal update)")

print("\n[4] should_include_in_staging() — terminal status, first time")
check(should_include_in_staging("Finalized", None) is True,
      "just went Finalized, WIP row doesn't exist yet -> include (creates it with the terminal status)")
check(should_include_in_staging("Finalized", "Proto") is True,
      "just went Finalized, WIP still shows the OLD active status -> one last push")
check(should_include_in_staging("Drop", "SMS") is True,
      "just went Drop, WIP still shows an old active status -> one last push")

print("\n[5] should_include_in_staging() — terminal status, already caught up")
check(should_include_in_staging("Finalized", "Finalized") is False,
      "WIP's Product Status already matches Finalized -> leave WIP alone from now on")
check(should_include_in_staging("Drop", "Drop") is False,
      "WIP's Product Status already matches Drop -> leave WIP alone from now on")

print("\n[6] should_include_in_staging() — reactivation")
check(should_include_in_staging("Proto", "Finalized") is True,
      "style REACTIVATED (BeProduct moved back to an active status after being "
      "Finalized) -> resumes syncing like any other active style, no special-casing")
check(should_include_in_staging("SMS", "Drop") is True,
      "reactivation from Drop back to an active status -> resumes syncing")

print("\n[7] should_include_in_staging() — custom excluded_statuses override")
check(should_include_in_staging("Cancelled", "Cancelled", excluded_statuses=frozenset({"Cancelled"})) is False,
      "a caller-supplied terminal-status set is honored instead of the module default")

print(f"\n{'='*60}")
if _failures:
    print(f"❌ {len(_failures)} check(s) FAILED:")
    for f in _failures:
        print(f"   - {f}")
    sys.exit(1)
else:
    print("✅ All checks passed")
