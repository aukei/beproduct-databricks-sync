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
                 "ad_hoc_skipped", "unresolved", "empty"},
      f"summary keys: {sorted(s)}")
check(s["empty"] is True and s["conflicts"] == 1, "summary reflects the plan")

# ---------------------------------------------------------------------------
# 2026-09-22 walkback: the Lakebase BOM source carries no materialId, only
# `lf_material_id` (**MaterialCode). The caller resolves those to GUIDs and
# hands the map in. Everything above this line exercises the PageBomVariation
# path and must keep passing UNCHANGED -- that is the signal that adding this
# route did not disturb the existing contract.
print("\n[8] Lakebase segments -- resolving material_id from the LF material code")


def lakebase_seg(group, article, code):
    """A segment as `bom.extract_enrichment_fields()` now emits it: no GUID."""
    return {"fabric_group": group, "mill_fabric_article": article,
            "placement": None, "content": None,
            "lf_material_id": code, "material_id": None, "is_ad_hoc": False}


LB = {"KTB-1": [lakebase_seg("Main Fabric", "WV-0003", "LF-BD26-000002--SH")]}
ROW = [dtc("KTB-1", "Blue", "Main Fabric", "WV-0003", "CUST-123")]

p = bom_push.plan_customer_code_push(
    ROW, LB, material_id_by_code={"LF-BD26-000002--SH": "mat-A"})
check(len(p.writes) == 1 and p.writes[0].material_id == "mat-A",
      "a resolved LF code yields the same write the GUID path would")
check(p.writes[0].lf_material_id == "LF-BD26-000002--SH",
      "the LF code is carried through for logging")

# Without a resolver the behaviour must be EXACTLY what it was before the
# walkback: a segment with no material_id is reported as ad-hoc, never written.
p = bom_push.plan_customer_code_push(ROW, LB)
check(p.is_empty() and len(p.ad_hoc_skipped) == 1 and not p.unresolved,
      "material_id_by_code omitted -> pre-walkback behaviour, byte for byte")

# Every way a code can fail to resolve is a REFUSAL, reported in `unresolved`.
p = bom_push.plan_customer_code_push(ROW, LB, material_id_by_code={})
check(p.is_empty() and len(p.unresolved) == 1
      and "no single material" in p.unresolved[0]["reason"],
      "a code absent from the map -> unresolved, never written")

p = bom_push.plan_customer_code_push(
    ROW, LB, material_id_by_code={"LF-BD26-000002--SH": None})
check(p.is_empty() and len(p.unresolved) == 1,
      "a code the caller REFUSED (mapped to None, e.g. 2 matches) -> unresolved")

p = bom_push.plan_customer_code_push(
    ROW, {"KTB-1": [lakebase_seg("Main Fabric", "WV-0003", None)]},
    material_id_by_code={"LF-BD26-000002--SH": "mat-A"})
check(p.is_empty() and len(p.unresolved) == 1
      and "**MaterialCode" in p.unresolved[0]["reason"],
      "a blank **MaterialCode -> unresolved, with a reason naming the column")

# An ad-hoc row is checked BEFORE the code lookup, so it keeps its own bucket
# rather than being misreported as a lookup failure.
adhoc = {"KTB-1": [{**lakebase_seg("Main Fabric", "WV-0003", None),
                    "is_ad_hoc": True}]}
p = bom_push.plan_customer_code_push(ROW, adhoc, material_id_by_code={})
check(len(p.ad_hoc_skipped) == 1 and not p.unresolved,
      "ad-hoc beats the code lookup -- it is not a resolution failure")

# A segment that HAS a GUID ignores the map entirely (both sources in one run).
p = bom_push.plan_customer_code_push(
    ROW, SEGS, material_id_by_code={"LF-A": "WRONG"})
check(len(p.writes) == 1 and p.writes[0].material_id == "mat-A",
      "an existing material_id always wins over the code map")

print("\n[9] required_material_codes() -- fetch only what the work needs")
check(bom_push.required_material_codes(ROW, LB) == ["LF-BD26-000002--SH"],
      "returns the code a row with a value actually resolves to")
check(bom_push.required_material_codes(
    [dtc("KTB-1", "Blue", "Main Fabric", "WV-0003", "")], LB) == [],
      "a BLANK DTC value needs no lookup -- today's live state, zero API calls")
check(bom_push.required_material_codes(ROW, SEGS) == [],
      "segments that already carry a GUID need no lookup")
check(bom_push.required_material_codes(ROW, adhoc) == [],
      "ad-hoc segments need no lookup")
check(bom_push.required_material_codes(
    ROW + [dtc("KTB-1", "Red", "Main Fabric", "WV-0003", "CUST-999")], LB)
    == ["LF-BD26-000002--SH"],
      "two rows sharing one material produce ONE code, not two")

print("\n" + "=" * 60)
if _failures:
    print(f"❌ {len(_failures)} FAILURE(S):")
    for f in _failures:
        print(f"   - {f}")
    sys.exit(1)
print("✅ All bom_push checks passed")
