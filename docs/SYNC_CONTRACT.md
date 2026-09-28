# Sync contract (v2) — field ownership, keys, and the PATCH allow-list

**Branch `v2`.** One place for *which field moves which way, what identifies a
row, and what is allowed into a DTC write*. Consolidates the field tables
previously duplicated across `PHASE1/2/7/10_WORKFLOW.md` and AGENTS.md's
"Current direction partition" / Ground rule #6.

Stage ordering and the gates a row must pass — see [PIPELINE.md](PIPELINE.md).
Symptom-first runbooks — see [TROUBLESHOOTING.md](TROUBLESHOOTING.md).

---

## Source of truth

**`docs/beproduct_style_interested_fields.txt`** is the SSOT: DTC column ⇄
BeProduct field, fieldId, JSONPath, sync direction. **Update it first**, then the
code constants below, then the tests. Companion SSOTs:
`costing_interested_fields.txt` (costing_chart columns),
`beproduct_directory_xts_interested_fields.txt` (Stage 00),
`beproduct_material_interested_fields.txt` (Stage 55 material-master target and
the "LF Material ID" / "LF Fabric ID" naming trap; its Phase 8 section is
retired). The `.txt` files hold fieldIds / JSONPaths / raw column names; this
file owns direction, keys and the allow-list.

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
| **Plan composition (v2)** | `compute_request_plan()` | `dtc/python/sync/wip_plan.py` |
| **Canonical allow-list (derived)** | `allowed_patch_columns()` | `dtc/python/sync/wip_plan.py` |
| Duty value columns | `DUTY_VALUE_FIELDS` | `dtc/python/sync/duty.py` |
| Staging denormalization | `FIELD_MAPPING` + staging `select` | `beproduct/p1p7_beproduct_to_dtc_transform.py` |

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
   DTC-developer guidance, and in v2 it is also what keeps a 15-minute cadence
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

All 6 confirmed present in the view (204 fields then; 205 since 2026-09-22). Fit and PP destinations changed on
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
| Content | `**MaterialContent` | `content` — **write-once** (fills a blank cell only) |
| LF Fabric ID | `**MaterialCode` | `lf_material_id` — normal upsert, a blank never written |

Write rules shared by all five: a blank source value is **never** written over a
real one; a row holding some other real (Fabric Group, Mill Fabric Article #)
pair is left alone; nothing is ever reverted. Full per-row decision tree:
PIPELINE.md → Stage 40, "Gates — material fields".

**`Content` is write-once** (`material_fill_if_blank_columns="Content"`). DTC's
own trigger rewrites it in another notation, so an owning writer would diff on
every row every run. The value only has to be non-blank for costing (Stage 30
gate 3). A hand-typed Content in DTC is therefore kept.

> **`LF Fabric ID` added 2026-09-22 (owner spec).** `LF_Material_ID` is the
> material key *across* systems — BeProduct, the techpack extraction and DTC all
> identify a material by it — so DTC now carries it explicitly rather than only
> carrying the mill's own article code. Live-verified in `WIP_ITS_USE` the same
> day: `{"fieldName": "LF Fabric ID", "type": "string", formula: false}`.
> **The WIP column is "LF *Fabric* ID"**; `LF Material ID` does not exist in that
> view, though it is the label on the BeProduct material master and in the
> retired Phase 8a fabric pull.
>
> It is **written, never matched on** — the BOM segment key stays
> `(Fabric Group, Mill Fabric Article #)`. Making it a key would leave every
> pre-existing row (blank in this column) looking unmatched and trigger mass
> re-enrichment.
>
> A **blank is never written** — stricter than the Placement/Content guard, which
> only holds a blank back when the row already has a real value. There is no
> useful blank material key. Rows enriched before the column existed are
> backfilled from the matched branch.
>
> It is a **normal upsert, deliberately NOT write-once** (revised 2026-09-22 the
> same day it was added). The tempting analogy to `Content` is false: `Content`
> is write-once because *DTC's own trigger* rewrites it in a different notation,
> so the two systems would fight every run. **Nothing competes for `LF Fabric
> ID`** — this pipeline is its only writer. Meanwhile the BOM source is
> externally prepared and can only land *after* a style reaches DTC, so it
> arrives late and progressively: a first extraction may carry a blank
> `**MaterialCode` and a later one fill or correct it. Write-once would freeze
> whichever value landed first and a source correction could never reach DTC.
>
> It gets **no `"NO TPM BOM"` filler** at INSERT either. `Fabric Group`'s
> sentinel already signals "awaiting BOM extraction" for the whole row, and a
> second one here would actively harm: `material_fill_if_blank_cols` does not
> know the sentinel is a placeholder (only `is_unenriched()` does), so a filler
> would read as "already has a value".
>
> `MATERIAL_OWNED_COLS` in `sync/wip_plan.py` must list it. Ground rule #6 strips
> any column outside the canonical allow-list in `_finalize()` **and** logs a
> violation, so an omission looks correct right up to the final pass.

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

| Slot | HTS | Duty rate (one column per market) | Tariff |
|---|---|---|---|
| Main | `Main Factory HTS Code` | `Main Factory Duty Rate (US)` / `(CA)` / `(MX)` | `Main Factory Tariff` |
| 1 | `Factory 1 - HTS code` | `Factory 1 - Duty Rate (US)` / `(CA)` / `(MX)` | `Factory 1 - Tariff` |
| 2 | `Factory 2 - HTS code` | `Factory 2 - Duty Rate (US)` / `(CA)` / `(MX)` | `Factory 2 - Tariff` |
| 3 | `Factory 3 - HTS code` | `Factory 3 - Duty Rate (US)` / `(CA)` / `(MX)` | `Factory 3 - Tariff` |

Transcribe these **exactly** (executable definition: `duty.WIP_HTS_COL` /
`WIP_DUTY_COL` / `WIP_TARIFF_COL`). They are not symmetric — `HTS Code` vs
`HTS code`, no `rate` suffix on tariff, ` - ` for numbered slots — and a name
absent from the view is dropped by the allow-list, so the value silently never
lands (`wip_push` reports it under `columns_not_seen_in_view`).

Source of each value: `hts_code` is the **US** answer (CA/MX return different
codes and may only fill a blank); `duty_rate_xx` is that market's "General
Duty" line; `tariff_rate` comes only from the US call. Only the **Main Fabric**
row of a style × colour carries duty — "Fabric" segment rows never do.
Values are fill-blank-only unless `force_refresh_duty` is run
(PIPELINE.md → duty_compute).

Tariff columns: All four were verified live against the view
definition on 2026-09-17 and are written unconditionally, overwriting whatever
DTC holds; the former `WIP_TARIFF_COLS_LIVE` switch is gone. DTC types these
`string` where the duty rates are `number` — immaterial, because
`values_equal()` compares normalised strings.

### DTC → BeProduct

| DTC column | BeProduct target | Level |
|---|---|---|
| Main Vendor (Sampling) | `parent_vendor` | header |
| Main Factory (Sampling) | `factory` | header |
| Main Factory Customer ID | `customer_factory_code` | header |
| Factory Production Country for Main Factory | `country_of_origin` ("COO") | header |
| Lot# | `drawing_number_walmart` | colorway |
| Fabric Customer # or SAP # | material master `customer_material_code` | material (Stage 55 — **disabled**) |

All read from the start-of-run `dtc_wip_ktb` snapshot; a blank DTC value never
clears BeProduct (`push_blanks=false`). Header fields are style-level, so every
DTC row of a style must agree (or be blank) — see PIPELINE.md → Stage 50.

`Main Factory Customer ID` and `Factory Production Country for Main Factory` are
DTC **lookup** fields on the factory. DTC stores a lookup only if the column is
on the view the user **saved through** ("Full"); otherwise the stored value is
NULL although the UI shows one, and nothing reaches BeProduct.

COO is the only field requiring a value transform — DTC's 2-char country code →
BeProduct's country name, via `phase2.resolve_coo_country_name()` +
`beproduct_master_coo`. An unresolved code returns `None` and is treated exactly
like a blank DTC value.

Removed: `Legacy Code` DTC→BP (now BP→DTC). `Customer Style#` was never created as
a DTC column.

### BeProduct → DTC, image only

`front_image_url` → DTC `Style Image`. Binary, so it never rides a `sheetData`
PATCH — uploaded through the multipart `/images` endpoint by Stage 45
(`phase3_images`, addressing the row by `rowid`), only when the DTC cell is
blank and a source exists (a sibling row's image, else BeProduct's URL). One-directional:
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
LF Fabric ID
Proto Sample - Sample Status                    Pre-line Sample - Status
SMS - Sample Status                             2nd Fit Sample Approval Status
PP Sample Submission Approval Status            TOP Sample Approval Status
```

**Costing/duty fields** (`duty.WIP_HTS_COL` / `WIP_DUTY_COL` / `WIP_TARIFF_COL`):
the per-slot HTS, Duty Rate and Tariff columns tabulated above (20 columns).

**`Style Image` is explicitly excluded** from every `sheetData` PATCH. Image cells
can only be set through the multipart `/images` endpoint; DTC rejects any
`sheetData` write to that column.

In v1 these were three separately-audited call sites. In v2 they are one, and the
allow-list is the union above — which makes the audit easier, and makes a leak
from any one contribution a whole-push problem.

**This table is documentation; the executable definition is
`wip_plan.allowed_patch_columns()`**, which derives the set from the payload
builders themselves (`phase1.FIELD_MAPPING`, the material column constants, and
`duty.WIP_HTS_COL` / `WIP_DUTY_COL` / `WIP_TARIFF_COL`) rather than restating
it. Add a field to a mapping and it is allowed automatically — the two cannot
drift. Every plan is checked against it, and a field outside it is dropped and
recorded in `RequestPlan.violations` rather than reaching a PATCH body.

All three duty families — HTS, duty rate and tariff — are included
unconditionally (the tariff gate was removed 2026-09-17).

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
Out of scope: any request not matching this convention, any whose name contains
the word **cancel / backup / archive / delete** (any case, any suffix — e.g.
`(BACKUP 2)`, `Cancelled`, `DELETED`), and any DTC-generated `…-SUPPLIER …`
request — see PIPELINE.md, Stage 10. Renaming a live request to include one of
those words silently stops its rows from syncing. LinePlan requests are **not**
filtered at all.

There is a sacrificial in-scope request for reversible live write tests:
**`KTB FW26 Wrangler`**, UAT request `6ab113b708ef2276cf34c0d2` (re-created
2026-09-23; grep for the id before relying on it).
