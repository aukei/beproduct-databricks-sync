# Sync contract (v2) — field ownership, keys, and the PATCH allow-list

**Branch `v2`.** One place for *which field moves which way, what identifies a
row, and what is allowed into a DTC write*. Consolidates the field tables
previously duplicated across `PHASE1/2/7/10_WORKFLOW.md` and AGENTS.md's
"Current direction partition" / Ground rule #6.

Stage ordering and the gates a row must pass — see [PIPELINE.md](PIPELINE.md).

---

## Source of truth

**`docs/beproduct_style_interested_fields.txt`** is the SSOT: DTC column ⇄
BeProduct field, fieldId, JSONPath, sync direction. **Update it first**, then the
code constants below, then the tests. Companion SSOTs:
`costing_interested_fields.txt` (costing/duty columns),
`beproduct_directory_xts_interested_fields.txt` (Stage 00),
`beproduct_material_interested_fields.txt`.

### Where a mapping lives in code — edit all that apply, together

| Direction | Constant | File |
|---|---|---|
| BeProduct → DTC | `FIELD_MAPPING` | `dtc/python/sync/phase1.py` |
| BeProduct → DTC (image only) | `STYLE_IMAGE_COL` + `compute_image_uploads` | `dtc/python/sync/phase3.py` |
| DTC → BeProduct (header) | `REVERSE_HEADER_FIELDS` | `dtc/python/sync/phase2.py` |
| DTC → BeProduct (colorway) | `REVERSE_COLORWAY_FIELDS` | `dtc/python/sync/phase2.py` |
| DTC → BeProduct (no target yet) | `UNSUPPORTED_FIELDS` | `dtc/python/sync/phase2.py` |
| BeProduct extraction (raw → master) | `COMPULSORY_FIELDS` / `INTERESTED_FIELDS` | `beproduct/p1p7_beproduct_style_sync.py` |
| Sample-app status (title → column prefix) | `SAMPLE_APPS` | `beproduct/p1p7_beproduct_style_sync.py` + `beproduct/00_init_style_app_registry.py` |
| Sample submits → DTC | `SAMPLE_SUBMIT_FIELDS` + `format_sample_field` | `dtc/python/sync/samples.py` |
| BOM → DTC material fields | `to_wip_fields()` | `dtc/python/sync/bom.py` |
| Costing/duty → DTC | `WIP_HTS_COL` / `WIP_DUTY_COL` / `WIP_TARIFF_COL` | `dtc/python/sync/duty.py` |
| **Plan composition (v2)** | `compute_request_plan()` | `dtc/python/sync/wip_plan.py` *(NEW)* |
| Staging denormalization | `FIELD_MAPPING` + staging `select` | `beproduct/v2_build_wip_staging.py` *(NEW)* |

Then update the tests: `test_phase1.py`, `test_phase2.py`, `test_phase3.py`,
`test_samples.py`, `test_bom.py`, `test_duty.py`, and `test_wip_plan.py` *(NEW)*.

---

## Invariants

1. **One field, one direction.** Never add a field to both `phase1.FIELD_MAPPING`
   and `phase2.REVERSE_*`. There are no loops anywhere in this pipeline.
2. **Match BeProduct fields by `fieldId`, never by display name.** Names are
   inconsistently cased and carry trailing spaces.
3. **Every WIP PATCH body must be lean** — only fields from the allow-list below,
   a subset per call is fine, nothing outside it ever. This is explicit
   DTC-developer guidance, and in v2 it is also what keeps a 2-hourly cadence
   tolerable. Each payload builder is the sole source of its own keys, so the
   property holds by construction; verify it still does after any change.
4. **Batch every change for one row into one PATCH object.** Never two calls for
   one row when they could be merged. In v2 this extends across contributions:
   style, material and duty changes for the same `rowId` merge into a single
   object before the call.
5. **A run that changes nothing writes nothing.** See PIPELINE.md design rule 2.

---

## Direction partition

### BeProduct → DTC (style fields)

| DTC column | BeProduct source (fieldId) | Notes |
|---|---|---|
| **BP Style#** *(key)* | header `header_number` | Match key |
| **Color / Wash** *(key)* | colorway `colorName` | In-request key; `DUMMY_COLOR` ("NO BP COLORWAY") when the style has no colorways |
| **Brand** *(routing)* | header `brand_hk` | Constant per request; `brand_hk`, not `brands_multi` |
| Product Status | header `style_status` | |
| Style Description | header `header_name` | |
| Class / Sub Class | header `product_category` / `product_sub_category` | |
| Division | header `division_hk` | |
| Garment Finish | header `garment_finish` | |
| Tech Pack Stage | header `techpack_stage` | |
| Gender | header `gender` | |
| LF Style# | header `lf_style_number` | Optional |
| Legacy Code | header `customer_style_number` | Optional; was DTC→BP before Phase 6 |
| Supplier | *(constant `"Supplier"`)* | Default-fill; written only when the DTC cell is blank |

**Filter:** styles whose `Product Status` has gone terminal (`Finalized` / `Drop`)
get one last push so the terminal status itself reaches DTC, then are excluded
until reactivated.

### BeProduct → DTC (sample submit history)

All 6 applications. Each value is one quoted, comma-separated line **per submit** —
`"submit_name","submitStatus","submitStatusDate"` — **not** a JSON array (no `[`
or `]` at all), with multiple submits stacked on newline-separated lines.
`phase1.norm()` preserves embedded newlines so the structure survives to the push.

| BeProduct app | DTC column |
|---|---|
| `proto_sample` | `Proto Sample - Sample Status` |
| `preline_sample` | `Pre-line Sample - Status` — lowercase `l`, dash |
| `sms_sample` | `SMS - Sample Status` |
| `fit_sample` | `2nd Fit Sample Approval Status` |
| `pp_sample` | `PP Sample Submission Approval Status` |
| `top_sample` | `TOP Sample Approval Status` |

All 6 confirmed present in the 204-field view. Fit and PP destinations changed on
2026-08-28 after a DTC WIP restructure (were `1st Fit …` and `2nd Fit …`); the PP
destination originally requested, `PP Sample Approval Status`, does not exist live
— `PP Sample Submission Approval Status` was the only plausible match and has since
been confirmed correct by the project team.

### BOM → DTC (material fields)

Sourced from `customer_teckpack_style_log.custom_fields`, the `**`-prefixed
columns of the `Type == "BOM"` table.

| DTC column | Source column | Module field |
|---|---|---|
| Fabric Group | `**MaterialCategory` | `fabric_group` |
| Placement | `**Placement` | `placement` |
| Mill Fabric Article # | `**SupplierRefNo` | `mill_fabric_article` |
| Content | `**MaterialContent` | `content` |

**Ownership boundary.** The style contribution sets `DUMMY_FABRIC_GROUP` /
`DUMMY_FABRIC_ARTICLE` ("NO TPM BOM") and a blank Placement on **INSERT only**;
the BOM contribution is the sole ongoing owner of real values. This is enforced by
`DEFAULT_FILL_COLS` being write-once (PIPELINE.md, Stage 40 style gate 3), not by
ordering.

> `Content`'s mapping has changed three times. It is currently
> `**MaterialContent` and is a real, live PATCH key. It was briefly written from
> `bom_unified.material_name`, which pushed material-*code*-shaped garbage into
> live DTC, then removed entirely, then reinstated on 2026-09-09 from the
> dedicated column. Do not re-derive it from a description or name field.

### Costing/duty → DTC

Per vendor slot (`Main` / `1` / `2` / `3`):

| Slot | HTS column | Duty columns |
|---|---|---|
| Main | `Main Factory HTS Code` | `Main Factory Duty Rate (US/CA/MX)` |
| 1 / 2 / 3 | `<slot> Factory HTS Code` | `<slot> Factory Duty Rate (US/CA/MX)` |

`<slot> Factory Tariff rate` is defined in `duty.WIP_TARIFF_COL` but **not live**
in the WIP view — `duty.WIP_TARIFF_COLS_LIVE = False` (confirmed 2026-07-17).
Computed tariff stays in `costing_chart` only, and the skip is logged rather than
silently dropped. Flip the flag when DTC adds the columns; no other change needed.

### DTC → BeProduct

| DTC column | BeProduct target | Level |
|---|---|---|
| Main Vendor (Sampling) | `parent_vendor` | header |
| Main Factory (Sampling) | `factory` | header |
| Main Factory Customer ID | `customer_factory_code` | header |
| Factory Production Country for Main Factory | `country_of_origin` ("COO") | header |
| Lot# | `drawing_number_walmart` | colorway |

COO is the only field requiring a value transform — DTC's 2-char country code →
BeProduct's country name, via `phase2.resolve_coo_country_name()` +
`beproduct_master_coo`. An unresolved code returns `None` and is treated exactly
like a blank DTC value.

Removed: `Legacy Code` DTC→BP (now BP→DTC). `Customer Style#` was never created as
a DTC column.

### BeProduct → DTC, image only

`front_image_url` → DTC `Style Image`. Binary, so it never rides a `sheetData`
PATCH — uploaded through the multipart `/images` endpoint by the separate images
job, only when the DTC cell is blank and BeProduct has a valid URL. One-directional:
never read back, never in `phase2.REVERSE_*`.

---

## Keys

| Key | Definition | Used for |
|---|---|---|
| In-request row key | `(BP Style#, Color / Wash)` | Matching a staging row to DTC rows. Matches **multiple** physical rows — one per material segment |
| Request routing key | `(Customer, BP Style#, SeasonCode, Brand)` | Which request a row belongs to; `brand_hk` |
| Material discriminator | `Mill Fabric Article #` | Distinguishing a style × color's several physical rows |
| BOM segment key | `(Fabric Group, Mill Fabric Article #)` | Matching a BOM segment to an existing row. Placement/Content excluded by design |
| Duty target key | `(bp_style_number, color_wash, Mill Fabric Article #)` | Joining a `costing_chart` row to its WIP row |
| `duty.COSTING_KEY` | `[customer, season_code, brand, bp_style_no, lf_style_no, color_name, lineplan_ref, material_no, supplier_type, supplier, factory]` | `costing_chart` identity and MERGE |
| NT Orbit cache key | `(product_description, origin_country, import_country)` | Cross-run duty cache; no style/color/vendor identity at all |
| Directory key | `(name, partner_type)` | Stage 00 upsert. Not `id`, not `name` alone |

`product_description` concatenates `duty.PRODUCT_DESCRIPTION_COLS`:
`style_description`, `color_name`, `fabric_content`, `gender`, `class_name`,
`sub_class`. `color_name` was added 2026-09-07 — different colors of one style now
always get their own lookup.

> **`COSTING_KEY` comparisons must be null-safe.** `lf_style_no` can legitimately
> be `NULL`, and `NULL = NULL` is `NULL` in Spark SQL — a row silently never
> matches itself, with no error and no log line. Use `<=>` in SQL and
> `.eqNullSafe()` in PySpark; the `.join(other, on=[list])` shorthand compiles to
> the unsafe form. Python dict lookups keyed on a tuple are unaffected
> (`None == None` is `True` there).

---

## The WIP `sheetData` PATCH allow-list

**Canonical and complete.** Nothing outside this set may appear in any WIP
`sheetData` PATCH body, from any contribution.

**Style + material fields** (`phase1.FIELD_MAPPING` / `bom.to_wip_fields()`):

```
Product Status          Style Description       Class
Sub Class               Division                Brand
Color / Wash            Garment Finish          Tech Pack Stage
BP Style#               LF Style#               Legacy Code
Gender                  Supplier                Fabric Group
Placement               Mill Fabric Article #   Content
Proto Sample - Sample Status                    Pre-line Sample - Status
SMS - Sample Status                             2nd Fit Sample Approval Status
PP Sample Submission Approval Status            TOP Sample Approval Status
```

**Costing/duty fields** (`duty.WIP_HTS_COL` / `WIP_DUTY_COL` / `WIP_TARIFF_COL`):
the per-slot HTS and Duty Rate columns tabulated above.

**`Style Image` is explicitly excluded** from every `sheetData` PATCH. Image cells
can only be set through the multipart `/images` endpoint; DTC rejects any
`sheetData` write to that column.

In v1 these were three separately-audited call sites. In v2 they are one, and the
allow-list is the union above — which makes the audit easier, and makes a leak
from any one contribution a whole-push problem. `wip_plan.compute_request_plan()`
should assert its output keys against this set.

---

## Scope

| Concept | Value |
|---|---|
| Customer | `KTB` (job param) |
| Workspace | `${customer}` = `KTB` |
| Document | `"${customer} WIP"` = `KTB WIP` |
| View | `WIP_ITS_USE` (job param) — the admin view, exposing all fields |
| In-scope request name | `"${customer} ${DTC seasoncode} ${brands}"`, e.g. `KTB FW26 Wrangler` |

One brand per request, agreeing with the request name (project guarantee).
Requests not matching this convention, and any request whose name contains a
`(BACKUP` marker in any position or variant, are out of scope — see PIPELINE.md,
Stage 10.

There is a sacrificial in-scope request for reversible live write tests:
**`KTB FW26 Wrangler`**, UAT request `6a26581854e92e7acd8fa71b`.
