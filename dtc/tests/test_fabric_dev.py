#!/usr/bin/env python3
"""
Unit tests for sync/fabric_dev.py -- BeProduct style BOM -> KTB FABRIC "- DEV".

    python3 dtc/tests/test_fabric_dev.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from sync import fabric_dev as fd  # noqa: E402

_failures = []


def check(cond, msg):
    print(f"  {'✅' if cond else '❌'} {msg}")
    if not cond:
        _failures.append(msg)


def cw(i, number, name, hidden=False):
    return {"id": i, "colorNumber": number, "colorName": name, "hideColorway": hidden}


def bom_row(group, article, material="M"):
    return {"materialNumber": material, "fields": [
        {"id": "group", "name": "Group", "value": group},
        {"id": "vendor_material_reference_no", "name": "MILL FABRIC CODE/SUPPLIER ITEM CODE",
         "value": article}]}


def variation(rows, sync=True, selected=(), name="v"):
    return {"metadata": {"variationName": name, "syncColorways": sync,
                         "selectedVariationColorways": list(selected)}, "rows": rows}


def dtc(i, article, color=None, **extra):
    return {"rowId": f"r{i}", "rowIndex": i, fd.COL_ARTICLE: article, fd.COL_COLOR: color, **extra}


COLORWAYS = [cw("g", "17-1444 TCX", "Ginger"), cw("k", "CKAU", "CKAU"),
             cw("h", "19-4003TCX", "Hidden One", hidden=True)]

print("[1] colorway label")
check(fd.colorway_label("17-1444 TCX", "Ginger") == "17-1444 TCX-GINGER", "number-NAME upper")
check(fd.colorway_label(None, "Ginger") == "GINGER", "blank number -> name upper")
check(fd.colorway_label("  CKAU ", " CKAU") == "CKAU-CKAU", "number == name -> both, trimmed")
check(fd.colorway_label("X", None) == "X", "blank name -> number")
check(fd.dev_request_name("KTB FW28 Collaborations") == "KTB FW28 Collaborations - DEV", "DEV name")

print("[2] needs: groups, hidden colorways, variation affinity")
needs = fd.style_fabric_needs(COLORWAYS, [
    variation([bom_row("Main Fabric", "ART-1"), bom_row("Trim", "T-1"), bom_row("Fabric", "ART-2"),
               bom_row("Packaging", "P-1")]),
    variation([bom_row("Fabric", "ART-3")], sync=False, selected=["k", "missing"]),
])
check(set(needs.needs) == {"ART-1", "ART-2", "ART-3"}, "only Main Fabric + Fabric rows")
check([l for l, _ in needs.needs["ART-1"]] == ["17-1444 TCX-GINGER", "CKAU-CKAU", "19-4003TCX-HIDDEN ONE"],
      "synced variation -> every colorway, hidden included, style order")
check([l for l, _ in needs.needs["ART-3"]] == ["CKAU-CKAU"], "unsynced variation -> its selected colorways only")
n2 = fd.style_fabric_needs(COLORWAYS, [variation([bom_row("Main Fabric", " "), bom_row("Fabric", "ART-1")]),
                                       variation([bom_row("Fabric", "ART-1")])])
check(len(n2.no_article) == 1 and n2.no_article[0]["group"] == "Main Fabric", "blank article reported")
check(len(n2.needs["ART-1"]) == 3, "same article in two variations -> colours not duplicated")

GINGER = ("17-1444 TCX-GINGER", ("Ginger", "Ginger 17-1444 TCX"))
CKAU = ("CKAU-CKAU", ("CKAU", "CKAU CKAU"))

print("[3] first colour claims the blank line, the rest copy it")
p = fd.plan_request({"ART-1": [GINGER, CKAU]},
                    [dtc(1, "ART-1", None, ITS_Key="K1", sysSource="ML", **{"Mill Name": "MILL", "Fabric Content": "100% Cotton"})])
check(p.updates == [{"rowId": "r1", fd.COL_COLOR: "17-1444 TCX-GINGER"}], "blank line updated in place")
check(len(p.inserts) == 1 and p.inserts[0][fd.COL_COLOR] == "CKAU-CKAU", "second colour inserted")
ins = p.inserts[0]
check(ins["Mill Name"] == "MILL" and ins[fd.COL_ARTICLE] == "ART-1", "insert copies the line")
check(not {"rowId", "rowIndex", "ITS_Key", "sysSource"} & set(ins), "insert drops locators, ITS_Key, sysSource")

print("[4] zero-diff-zero-write on a rerun")
p = fd.plan_request({"ART-1": [GINGER, CKAU]},
                    [dtc(1, "ART-1", "17-1444 TCX-GINGER"), dtc(2, "ART-1", "ckau-ckau")])
check(not p.has_writes and p.noops == 2, "all labels present (case-insensitive) -> no write")

print("[5] bare hand-typed name renamed in place; unknown colours untouched")
p = fd.plan_request({"ART-1": [GINGER, CKAU]},
                    [dtc(1, "ART-1", "Something Else"), dtc(2, "ART-1", "ginger")])
check(p.updates == [{"rowId": "r2", fd.COL_COLOR: "17-1444 TCX-GINGER"}], "bare name -> renamed")
check(len(p.inserts) == 1 and p.inserts[0][fd.COL_COLOR] == "CKAU-CKAU",
      "no blank line left -> copy (first line as base)")
check(p.inserts[0].get("rowId") is None, "copied from r1 without its rowId")

print("[5b] WIP notation '<name> <number>' renamed in place")
p = fd.plan_request({"ART-1": [GINGER]}, [dtc(1, "ART-1", None), dtc(2, "ART-1", "Ginger 17-1444 TCX")])
check(p.updates == [{"rowId": "r2", fd.COL_COLOR: "17-1444 TCX-GINGER"}] and not p.inserts,
      "alias wins over the blank line, no insert")
check(needs.needs["ART-1"][0] == GINGER, "needs carry both older notations")

print("[6] missing article reported, never created")
p = fd.plan_request({"ART-9": [GINGER]}, [dtc(1, "ART-1")])
check(p.missing_articles == ["ART-9"] and not p.has_writes, "gap, no write")

print("[7] non-copyable columns from the view definition")
cols = fd.non_copyable_cols([{"fieldName": "SBU", "type": "lookup"},
                             {"fieldName": "F", "type": "string", "formula": "x"},
                             {"fieldName": "Mill Name", "type": "string"}])
check({"SBU", "F", "ITS_Key", "rowId"} <= cols and "Mill Name" not in cols, "lookup + formula + ITS_Key excluded")

if _failures:
    print(f"\n{len(_failures)} FAILED")
    sys.exit(1)
print("\nall passed")
