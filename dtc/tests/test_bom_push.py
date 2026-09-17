#!/usr/bin/env python3
"""
Unit tests for sync/bom_push.py -- DTC -> BeProduct material-master push.

    python3 dtc/tests/test_bom_push.py

The property that matters most here is the CONFLICT refusal. Materials are
shared across styles, so "last row wins" would silently corrupt a master record
that many styles depend on.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from sync import bom, bom_push, phase1  # noqa: E402

STYLE_COL, COLOR_COL = phase1.MATCH_KEY_COLS
COL = bom_push.DTC_CUSTOMER_CODE_COL
FG, MA = bom.WIP_FIELD_FABRIC_GROUP, bom.WIP_FIELD_MILL_FABRIC_ARTICLE

_failures = []


def check(cond, msg):
    if cond:
        print(f"  ✅ {msg}")
    else:
        print(f"  ❌ {msg}")
        _failures.append(msg)


def dtc(style, colour, group, article, value):
    return {STYLE_COL: style, COLOR_COL: colour,
            FG: group, MA: article, COL: value}


def seg(group, article, material_id, lf=None, ad_hoc=False):
    return {"fabric_group": group, "mill_fabric_article": article,
            "placement": None, "content": None,
            "material_id": material_id, "lf_material_id": lf,
            "is_ad_hoc": ad_hoc}


SEGS = {"KTB-1": [seg("Main Fabric", "WV-0003", "mat-A", "LF-A"),
                  seg("Fabric", "WV-0061", "mat-B", "LF-B")]}

# ---------------------------------------------------------------------------
print("\n[1] Matching DTC row -> BOM segment -> material")
p = bom_push.plan_customer_code_push(
    [dtc("KTB-1", "Blue", "Main Fabric", "WV-0003", "CUST-123")], SEGS)
check(len(p.writes) == 1 and p.writes[0].material_id == "mat-A",
      "resolves via (Fabric Group, Mill Fabric Article #) to the linked material")
check(p.writes[0].value == "CUST-123", "carries the DTC value")
check(p.writes[0].lf_material_id == "LF-A",
      "carries LF MATERIAL ID for traceability")
check(p.writes[0].sources == [("KTB-1", "Blue")],
      "records which (style, colour) asked for it")
check(not p.is_empty(), "is_empty() False when there is work")

# ---------------------------------------------------------------------------
print("\n[2] A blank DTC value is never pushed (and can never CLEAR a code)")
for blank in (None, "", "   ", "n/a"):
    pb = bom_push.plan_customer_code_push(
        [dtc("KTB-1", "Blue", "Main Fabric", "WV-0003", blank)], SEGS)
    check(pb.is_empty() and not pb.writes, f"blank {blank!r} -> no write")

# ---------------------------------------------------------------------------
print("\n[3] CONFLICT: a shared material wanted two different codes")
# The same material reached from two styles, disagreeing. Refusing is the whole
# point -- letting row order decide would corrupt a shared master record.
segs2 = {"KTB-1": [seg("Main Fabric", "WV-0003", "mat-A", "LF-A")],
         "KTB-2": [seg("Main Fabric", "WV-0003", "mat-A", "LF-A")]}
pc = bom_push.plan_customer_code_push([
    dtc("KTB-1", "Blue", "Main Fabric", "WV-0003", "CODE-X"),
    dtc("KTB-2", "Red", "Main Fabric", "WV-0003", "CODE-Y"),
], segs2)
check(pc.is_empty() and not pc.writes, "conflicting material is NOT written")
check(len(pc.conflicts) == 1, "the conflict is reported")
check(set(pc.conflicts[0]["values"]) == {"CODE-X", "CODE-Y"},
      "both candidate values are named")
check(pc.conflicts[0]["material_id"] == "mat-A", "and the material")

# Same value from two rows is NOT a conflict -- they agree.
pa = bom_push.plan_customer_code_push([
    dtc("KTB-1", "Blue", "Main Fabric", "WV-0003", "SAME"),
    dtc("KTB-2", "Red", "Main Fabric", "WV-0003", "SAME"),
], segs2)
check(len(pa.writes) == 1 and not pa.conflicts,
      "two rows agreeing is one write, not a conflict")
check(len(pa.writes[0].sources) == 2, "both sources recorded on the single write")

# ---------------------------------------------------------------------------
print("\n[4] Lean writes: skip when the material already holds the value")
pn = bom_push.plan_customer_code_push(
    [dtc("KTB-1", "Blue", "Main Fabric", "WV-0003", "CUST-123")], SEGS,
    current_by_material={"mat-A": "CUST-123"})
check(pn.is_empty() and pn.noops == ["mat-A"], "already-correct material -> noop")
pd = bom_push.plan_customer_code_push(
    [dtc("KTB-1", "Blue", "Main Fabric", "WV-0003", "CUST-999")], SEGS,
    current_by_material={"mat-A": "CUST-123"})
check(len(pd.writes) == 1 and pd.writes[0].current_value == "CUST-123",
      "a genuine change is written and records the previous value")

# ---------------------------------------------------------------------------
print("\n[5] Unmatched and ad-hoc rows are reported, never guessed")
pu = bom_push.plan_customer_code_push(
    [dtc("KTB-1", "Blue", "Fabric", "NOT-IN-BOM", "CUST-1")], SEGS)
check(pu.is_empty() and len(pu.unmatched) == 1,
      "a (group, article) with no BOM segment is reported, not written")
pu2 = bom_push.plan_customer_code_push(
    [dtc("NO-SUCH-STYLE", "Blue", "Main Fabric", "WV-0003", "CUST-1")], SEGS)
check(pu2.is_empty() and len(pu2.unmatched) == 1, "unknown style -> unmatched")

segs_adhoc = {"KTB-1": [seg("Main Fabric", "WV-0003", None, None, ad_hoc=True)]}
pah = bom_push.plan_customer_code_push(
    [dtc("KTB-1", "Blue", "Main Fabric", "WV-0003", "CUST-1")], segs_adhoc)
check(pah.is_empty() and len(pah.ad_hoc_skipped) == 1,
      "an ad-hoc row has no linked material -> skipped and reported")

# ---------------------------------------------------------------------------
print("\n[6] The match key agrees with the enrichment direction")
# Both directions must mean the same thing by "the same fabric assignment",
# otherwise enrichment and push could disagree about which material a DTC row
# belongs to.
check(bom.segment_key(SEGS["KTB-1"][0]) == ("Main Fabric", "WV-0003"),
      "bom.segment_key is the shared (Fabric Group, Mill Fabric Article #) pair")
pk = bom_push.plan_customer_code_push(
    [dtc("KTB-1", "Blue", "  Main Fabric  ", " WV-0003 ", "CUST-1")], SEGS)
check(len(pk.writes) == 1, "matching is whitespace-insensitive, like norm()")

# ---------------------------------------------------------------------------
print("\n[7] Summary shape")
s = pc.summary()
check(set(s) == {"writes", "conflicts", "unmatched", "noops",
                 "ad_hoc_skipped", "empty"}, f"summary keys: {sorted(s)}")
check(s["empty"] is True and s["conflicts"] == 1, "summary reflects the plan")

print("\n" + "=" * 60)
if _failures:
    print(f"❌ {len(_failures)} FAILURE(S):")
    for f in _failures:
        print(f"   - {f}")
    sys.exit(1)
print("✅ All bom_push checks passed")
