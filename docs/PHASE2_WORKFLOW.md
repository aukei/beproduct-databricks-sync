# Phase 2: Bi-Directional BeProduct <-> DTC Sync

**Status**: Implemented ✅ (cores unit-tested + live-verified on UAT 2026-06-17)

Phase 2 adds the reverse leg (DTC → BeProduct) on top of Phase 1 (BeProduct → DTC),
giving a clean **one-way-per-field** bidirectional sync with no loops.

> Forward flow: `PHASE1_WORKFLOW.md`. Image sync: `PHASE3_WORKFLOW.md`.
> Data model for every table named here: `docs/ARCHITECTURE.md`.

---

## Field ownership (direction partition)

Each field syncs in exactly ONE direction. A field is never pushed both ways.

| Field (DTC column)        | Direction          | BeProduct location (fieldId)                  | Notes |
|---------------------------|--------------------|-----------------------------------------------|-------|
| Product Status            | BeProduct → DTC    | header `style_status`                         | |
| Style Description         | BeProduct → DTC    | header `header_name`                          | |
| Class / Sub Class         | BeProduct → DTC    | header `product_category` / `product_sub_category` | |
| Division                  | BeProduct → DTC    | header `division_hk`                          | |
| Brand                     | BeProduct → DTC    | header `brand_hk`                             | Phase 6: single-value field |
| Garment Finish            | BeProduct → DTC    | header `garment_finish`                       | |
| Tech Pack Stage           | BeProduct → DTC    | header `techpack_stage`                       | |
| Fabric Group / Placement  | BeProduct → DTC    | header `core_main_material`                   | |
| Gender                    | BeProduct → DTC    | header `gender`                               | Phase 6 |
| Legacy Code               | BeProduct → DTC    | header `customer_style_number`                | Phase 6: direction changed from DTC→BP |
| LF Style#                 | BeProduct → DTC    | header `lf_style_number`                      | Phase 6 optional |
| Supplier                  | BeProduct → DTC    | *(constant "Supplier")*                       | Phase 6 default-fill only |
| BP Style# **(key)**       | BeProduct → DTC    | header `header_number`                        | Phase 6 match key |
| Color / Wash **(key)**    | key (not pushed)   | colorway `colorName`                          | |
| **Main Vendor (Sampling)**| **DTC → BeProduct**| header `parent_vendor`                        | |
| **Main Factory (Sampling)**|**DTC → BeProduct**| header `factory`                              | |
| **Lot#**                  | **DTC → BeProduct**| **colorway `drawing_number_walmart`**         | |
| **Main Factory Customer ID**| **DTC → BeProduct**| header `customer_factory_code`               | Wired up 2026-09-03 (was unsupported) |
| Style Image               | BeProduct → DTC (image only) | `front_image_url` (Phase 3, binary) | See `PHASE3_WORKFLOW.md` |
| *Sample status columns (×6)* | BeProduct → DTC | sample apps `proto`/`preline`/`sms`/`fit`/`pp`/`top` | Phase 7; JSON list per app |

**Removed (Phase 6):** "Legacy Code" was DTC→BP; now BP→DTC. "Customer Style#" DTC column not created.
The DTC-owned columns are deliberately absent from Phase 1 `FIELD_MAPPING`
(`dtc/python/sync/phase1.py`) and handled by Phase 2 (`dtc/python/sync/phase2.py`).

---

## Daily flow (high level)

```
1. BeProduct → Databricks       p1p7_beproduct_style_sync.py      (styles + colorway detail)
2. Transform / denormalize      p1p7_beproduct_to_dtc_transform.py (1 row per style×color,
                                                               carries beproduct_style_id
                                                               + colorway_id)
3. DTC → Databricks             dtc/notebooks/p1_pull_masters_to_delta.py  (dtc_wip_<cust>)
4. Resolve requests             beproduct/p1_dtc_request_manager.py
5. Push BeProduct → DTC         beproduct/p1p7_beproduct_to_dtc_push.py   (Phase 1 + orphan marks)
6. Push DTC → BeProduct         dtc/notebooks/p2_push_dtc_to_beproduct.py  (Phase 2)
```

Steps 5 and 6 touch **disjoint field sets**, so order between them is safe and there
are no field-level conflicts.

---

## Colorway identity (why the transform carries `colorway_id`)

`Lot#` is a **colorway-level** field. The BeProduct SDK addresses it by colorway
**id**:

```python
api.style.attributes_update(
    header_id=<beproduct_style_id>,
    fields={"customer_style_number": ..., "parent_vendor": ..., "factory": ...},
    colorways=[{"id": <colorway_id>, "fields": {"drawing_number_walmart": <lot>}}],
)
```

The denormalized staging row only knew the colorway *name*, so Phase 2 would not be
able to target the colorway. We therefore:

- `p1p7_beproduct_style_sync.py` now also emits `colorways_json`
  (`[{colorway_id, color_name, color_number}, ...]`).
- `p1p7_beproduct_to_dtc_transform.py` explodes that detail, so every staging row carries
  `beproduct_style_id` **and** `colorway_id`.

This is a lightweight group-by on pushback (one `attributes_update` per style with a
`colorways[]` list) rather than a full reverse-reconstruction of the raw style JSON —
evaluated and chosen for simplicity and correctness (see point 4 of the design).

---

## Phase 2 pushback (`p2_push_dtc_to_beproduct.py`)

1. Build an identity map from staging: `(request, LF Style#, Color) → (style_id, colorway_id)`.
2. Read DTC rows from `dtc_wip_<customer>`, extract the 5 DTC-owned values from
   `data_json` (exact column names), keep rows with at least one value.
3. Join each DTC row to a BeProduct identity. Unmatched rows (style moved / not in
   BeProduct) are logged and skipped.
4. Read **current** BeProduct values live (`attributes_get`, cached per style) so the
   NOOP diff is accurate — the staging `lot_code` is the legacy header value, not the
   colorway Lot#.
5. `phase2.build_beproduct_updates()` → changed-only per-style payloads;
   `phase2.to_sdk_calls()` → `attributes_update(**call)`.
6. Blank DTC values do **not** clear BeProduct unless `push_blanks=true`.
7. `UNSUPPORTED_FIELDS` is currently empty (`Main Factory Customer ID` was the last
   entry, wired up to `customer_factory_code` 2026-09-03) — kept as an empty tuple
   so future unsupported columns have an obvious place to land.
8. Everything is logged to `lft.beproduct.dtc_to_beproduct_sync_log`. `dry_run=true`
   computes + logs without writing.

---

## Moved-key handling (Phase 1, requirement: point 1)

When a BeProduct **key field** (LF Style#, brand or season) changes, the row's DTC
request changes (e.g. brand Wrangler → Lee moves it to a different request). The new
request receives an INSERT (normal upsert). The stale row left behind in the OLD
request is flagged by setting its DTC **`Product Status` = `(removed)`** — an
intentionally invalid BeProduct status that signals the DTC user. It is **not**
deleted, and only rows whose key now lives under a *different* request are marked
(user-entered rows whose key isn't seen anywhere in BeProduct are left untouched).

Implemented as `phase1.compute_orphan_marks()` and wired into
`p1p7_beproduct_to_dtc_push.py` (runs for every resolved request, independent of the delta
filter, so the moved-out request is always reconciled).

---

## Tests

| Test | Scope |
|------|-------|
| `dtc/tests/test_phase1.py`        | Phase 1 upsert core + field partition + orphan marks (unit) |
| `dtc/tests/test_phase2.py`        | Phase 2 reverse mapping / payload / diff / skip (unit) |
| `dtc/tests/test_phase1_live.py`   | Live reversible BeProduct→DTC insert/update + DELETE cleanup |
| Phase 2 live check                | `attributes_update` header + colorway write/clear (validated 2026-06-17) |

---

## Status code / API references

- DTC sheet upsert: `PATCH /v1/sheets/{sheetId}/views/{viewId}` (204)
- DTC row delete:   `DELETE /v1/sheets/{sheetId}/views/{viewId}/rows` body `{"rowIndexes":[...]}` (204)
- BeProduct write:  `api.style.attributes_update(header_id, fields=, colorways=)`
