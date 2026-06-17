#!/usr/bin/env python3
"""
Unit tests for the pure (Spark-free) registry helpers (sync/registry.py):
  - build_registry_row
  - discover_request_ids

No Spark, no network. Run:
    python3 dtc/tests/test_registry.py
"""
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from sync import registry

_failures = []
NOW = datetime(2026, 1, 1)


def check(cond, msg):
    print(f"  {'✅' if cond else '❌'} {msg}")
    if not cond:
        _failures.append(msg)


def make_scope(**kw):
    base = {
        "request_id": "r1", "request_reference": "KTB FW26 Wrangler",
        "sheet_id": "s1", "wip_view_id": "v1", "view_name": "WIP_ITS_USE",
        "request_is_active": "Y", "season_code": "FW26", "brand": "Wrangler",
        "parse_ok": True,
    }
    base.update(kw)
    return base


print("\n[1] in-scope request -> in_scope=True, mapped fields")
row = registry.build_registry_row(
    make_scope(), environment="uat", workspace="KTB", document="KTB WIP",
    customer="KTB", now=NOW)
check(row["in_scope"] is True, "in_scope True for matching customer")
check(row["msgs"] == "registered", "msg 'registered'")
check(row["brands"] == "Wrangler", "brand mapped to brands")
check(row["view_id"] == "v1", "wip_view_id mapped to view_id")
check(row["request_id"] == "r1" and row["season_code"] == "FW26", "ids/season carried")
check(row["row_count"] is None and row["last_pushed"] is None, "sync-state left null")

print("\n[2] wrong customer -> out of scope")
row = registry.build_registry_row(
    make_scope(request_reference="KON FW26 Wrangler", request_id="r2"),
    environment="uat", workspace="KTB", document="KTB WIP", customer="KTB", now=NOW)
check(row["in_scope"] is False, "in_scope False for KON")
check("OUT_OF_SCOPE" in row["msgs"], "msg flags out-of-scope")

print("\n[3] WIP_ITS_USE view missing -> warning msg")
row = registry.build_registry_row(
    make_scope(view_name="SOME_OTHER_VIEW"),
    environment="uat", workspace="KTB", document="KTB WIP", customer="KTB", now=NOW)
check("WIP_ITS_USE view not found" in row["msgs"], "msg warns missing WIP view")

print("\n[4] unparseable reference -> out of scope")
row = registry.build_registry_row(
    make_scope(request_reference="garbage", parse_ok=False),
    environment="uat", workspace="KTB", document="KTB WIP", customer="KTB", now=NOW)
check(row["in_scope"] is False, "in_scope False when parse_ok False")

print("\n[5] error row")
row = registry.build_registry_row(
    None, environment="uat", workspace="KTB", document="KTB WIP", customer="KTB",
    now=NOW, request_id="rX", error="boom")
check(row["request_id"] == "rX", "error row keeps request_id")
check(row["in_scope"] is False and row["msgs"].startswith("read_error:"), "error row flagged")

print("\n[6] discover_request_ids: dedup + skip empty, preserve order")
class FakeConn:
    def __init__(self, items):
        self.items = items
        self.called_with = None

    def search_requests(self, ws, document_name=None):
        self.called_with = (ws, document_name)
        return self.items

conn = FakeConn([{"requestId": "a"}, {"requestId": "a"}, {"requestId": "b"}, {}, {"requestId": None}])
ids = registry.discover_request_ids(conn, "KTB", "KTB WIP")
check(ids == ["a", "b"], f"deduped/ordered ids == ['a','b'] (got {ids})")
check(conn.called_with == ("KTB", "KTB WIP"), "search_requests called with workspace+document")

print("\n[6b] discover_requests keeps full dicts (for reference pre-filter)")
conn2 = FakeConn([
    {"requestId": "a", "requestReference": "KTB FW26 Wrangler"},
    {"requestId": "b", "requestReference": "KON FW26 Wrangler"},
    {"requestId": "a", "requestReference": "KTB FW26 Wrangler"},  # dup
])
items = registry.discover_requests(conn2, "KTB", "KTB WIP")
check([i["requestId"] for i in items] == ["a", "b"], "discover_requests deduped by requestId")
in_scope = [i for i in items if i.get("requestReference", "").startswith("KTB ")]
check(len(in_scope) == 1 and items[0]["requestReference"] == "KTB FW26 Wrangler",
      "references available for pre-filter (1 KTB in-scope, 1 KON out)")

print("\n[7] REGISTRY_COLS matches expected 19-col schema order")
check(len(registry.REGISTRY_COLS) == 19, "19 registry columns")
check(registry.REGISTRY_COLS[0] == "environment" and registry.REGISTRY_COLS[4] == "request_id",
      "key column positions stable")

print("\n" + "=" * 70)
if _failures:
    print(f"❌ {len(_failures)} REGISTRY TEST(S) FAILED")
    sys.exit(1)
print("✅ ALL REGISTRY PURE-FUNCTION TESTS PASSED")
