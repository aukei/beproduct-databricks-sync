# Architecture — BeProduct ⇄ DTC Sync on Databricks

Bi-directional synchronization between **BeProduct** (style PLM) and **DTC**
("Data Collab" sheets), staged through **Databricks / Delta** under Unity Catalog
schema `lft.beproduct`.

This document is the single reference for **components**, **data flow**, and the
**data model on Azure Databricks (ADB)**. It merges what used to live in
`BEPRODUCT_TO_DTC_GUIDE.md`, `dtc/README.md`, and `dtc/DATA_MODEL.md`.

- Phase 0 (DTC XTS Master → BeProduct Directory): `PHASE0_WORKFLOW.md`
- Forward field sync (BeProduct → DTC): `PHASE1_WORKFLOW.md`
- Reverse field sync (DTC → BeProduct): `PHASE2_WORKFLOW.md`
- Image sync (BeProduct → DTC): `PHASE3_WORKFLOW.md`
- Component API/SDK + per-side tables: `DTC_GUIDE.md`, `BEPRODUCT_GUIDE.md`
- Style field-mapping SSOT: `beproduct_style_interested_fields.txt`
- Material field-mapping SSOT: `beproduct_material_interested_fields.txt`
- Costing chart field-mapping SSOT: `costing_interested_fields.txt`
- Directory/XTS field-mapping SSOT: `beproduct_directory_xts_interested_fields.txt`
- Phase 5 (Master Data): `PHASE5_WORKFLOW.md`
- Phase 7 (Sample history): `PHASE7_WORKFLOW.md`
- Pipeline diagram (Mermaid source, render locally): `DIAGRAM.md`
- Verified API behaviour & invariants: `../AGENTS.md`

---

## 1. Systems

| System | What it is | Access |
|--------|------------|--------|
| **BeProduct** | Style PLM. Parent/child JSON model: `STYLE` header ↔ `Colorways`, `Size`, `BOM`. One environment, data partitioned by **Folder** (e.g. `KTB`). | OAuth 2.0 + Python SDK (`beproduct`) |
| **DTC** | Excel-like data-entry tool. `Workspace → Document → Request → Sheet → View`. Project data lives denormalized in one flat wide sheet per request. | REST API, `x-api-key`; envs **UAT** / **PROD** |
| **Databricks** | Staging + compute. All tables under `lft.beproduct`. Notebooks orchestrate; pure logic is unit-tested Python modules. | `databricks` CLI / SDK |

**Timezones:** BeProduct timestamps are UTC. DTC returns UTC but expects input in
the user-profile timezone (treated as **+08:00 HKT** here).

---

## 2. Repository layout

```
beproduct/                         # BeProduct-side notebooks (also host the cross-platform push)
├── 00_init_style_app_registry.py  # Cache folder application IDs → beproduct_style_app_registry
├── p1p7_beproduct_style_sync.py        # BeProduct API → lft.beproduct.ktb_styles (+ sample-app status)
├── p5utl_beproduct_master_data_sync.py  # Admin: pull/push-back MasterData (dropdowns) + Directory
├── p1p7_beproduct_to_dtc_transform.py  # ktb_styles → beproduct_to_dtc_staging (denormalize)
├── p1_dtc_request_manager.py         # Resolve / CREATE / SHARE DTC requests → dtc_request_mapping
├── p1p7_beproduct_to_dtc_push.py       # Phase 1: BeProduct → DTC upsert + orphan marks
├── p3_beproduct_to_dtc_images.py     # Phase 3: front image → DTC "Style Image"
├── p1utl_dtc_share_requests.py          # Idempotent request-sharing backfill
└── orchestrate_sync.py            # ⚠️ RETIRED — single-notebook fallback only

dtc/
├── notebooks/
│   ├── 00_init_request_registry.py  # Standalone WIP registry build/refresh
│   ├── 00_init_season_mapping.py    # Seed dtc_seasoncode_mapping
│   ├── p1_pull_masters_to_delta.py     # Pull KTB WIP sheets → dtc_wip_ktb + registry (Steps 3 + 7)
│   ├── p8a_pull_fabric_to_delta.py      # ⚠️ RETIRED 2026-09-01 (superseded by MaterialLib) — manual fallback only
│   ├── p9a_pull_lineplan_to_delta.py    # Phase 9a: pull KTB LinePlan → dtc_lineplan_ktb
│   ├── p9a_build_costing_chart.py       # Phase 9a: WIP × LinePlan → costing_chart (transpose 4 slots)
│   ├── p9b_fill_duty_rates.py           # Phase 9b: NT Orbit Duty Tools HTS/Duty/Tariff fill (persistent cache)
│   └── p2_push_dtc_to_beproduct.py  # Phase 2: DTC → BeProduct pushback
├── python/                          # Importable modules (deployed as Workspace files)
│   ├── client/rest_client.py        # Generic REST client (retry, multipart)
│   ├── connectors/dtc.py            # DTC API connector
│   └── sync/
│       ├── phase1.py                # BeProduct → DTC upsert core (pure; DEFAULT_FILL_COLS)
│       ├── phase2.py                # DTC → BeProduct pushback core (pure)
│       ├── phase3.py                # Image upload planning + type classification (pure)
│       ├── samples.py               # Phase 7: sample-app submit formatter (pure)
│       └── registry.py              # Shared registry refresh (discover→enrich→merge)
└── tests/                           # Unit + live tests for the pure cores

standalone/beproduct_style_push.py   # Standalone Delta → BeProduct push-back (not in daily pipeline)
scripts/
├── upload_notebooks.py              # Deploy notebooks + modules to the workspace
└── deploy_job.py                    # Create / reset the multi-task job (BeProduct_DTC_sync_dag)
docs/                                # This documentation set
```

**Notebook vs module split (invariant):** notebooks can't run locally (Spark /
`dbutils`). Deterministic logic lives in `dtc/python/sync/*.py` (pure Python,
unit-tested); notebooks are thin Spark/IO wrappers around it. All HTTP lives in
`connectors/dtc.py` + `client/rest_client.py`.

---

## 3. Components & data flow

The whole pipeline runs as the **multi-task Databricks job** `BeProduct_DTC_sync_dag`
(job 294837488757511, defined in `scripts/deploy_job.py`). Steps 1–2 and Step 3 run
in **parallel** (they are independent); the rest follow in dependency order. Each
step is a first-class task with its own logs and per-task timing in the Jobs UI.

```
Task               notebook                      inputs → outputs              parallel group
─────────────────  ────────────────────────────  ────────────────────────────  ──────────────
wait_cluster       wait_cluster                  (root / cold-start sentinel)  root
bp_style_sync      p1p7_beproduct_style_sync          BP API → ktb_styles           after wait ┐
transform          p1p7_beproduct_to_dtc_transform    ktb_styles → staging          after 1    │ WIP chain
pull_master_dtc    p1_pull_masters_to_delta         DTC API → dtc_wip + registry  after wait ┘ parallel
request_manager    p1_dtc_request_manager           staging+registry → mapping    after 2+3
gate_phase1        (condition: run_phase1)        after request_manager
phase1_push        p1p7_beproduct_to_dtc_push         staging → DTC upsert+orphans  after gate1
gate_phase2        (condition: run_phase2)        after transform+pull_master
phase2_push        p2_push_dtc_to_beproduct      dtc_wip → BP pushback         after gate2
gate_phase3        (condition: run_phase3)        after request_manager
repull_dtc         p1_pull_masters_to_delta         targeted re-pull (inserts)    after gate3+phase1 ALL_DONE
phase3_images      p3_beproduct_to_dtc_images       staging+wip → DTC image       after repull

gate_phase9a       (condition: run_phase9a)       after wait_cluster            ┐ parallel,
pull_lineplan_dtc  p9a_pull_lineplan_to_delta         DTC LinePlan → lineplan_ktb  │ independent
p9a_build_costing_chart p9a_build_costing_chart           wip+lineplan → costing_chart ┘ after pull_lineplan+pull_master
```

Condition tasks (`gate_phase*`) evaluate `run_phase*` job parameters; their `true`
edge gates the respective push/pull tasks. `dry_run` (steps 4/5/6/8) computes +
logs without writing. Phase 9a runs fully in parallel with the WIP chain.

Phase 8a/8b (DTC FABRIC → Delta → BeProduct Material Master) are RETIRED
(2026-09-01), confirmed by the project team to be replaced by a separate
"MaterialLib" application, and have been removed from the DAG entirely.
`p8a_pull_fabric_to_delta.py` remains in the repo as a historical/manual-
fallback artifact only; its output tables `dtc_fabric_<customer>` /
`dtc_fabric_registry` were DROPPED from Delta (2026-09-01, owner-confirmed).

```
   BeProduct (PLM)               Databricks (lft.beproduct)              DTC (sheets)
   ┌────────────┐  style sync  ┌──────────────┐ transform ┌────────────────────────┐
   │ STYLE +    │ ────────────▶│ ktb_styles   │ ─────────▶│ beproduct_to_dtc_      │
   │ Colorways  │  (excl.      │ (1/style)    │           │ staging (1/style×color)│
   │ + 6 apps   │  Finalized)  └──────────────┘           └──────────┬─────────────┘
   └────────────┘                                                      │ Phase 1 + 7
        ▲                                                              │ Phase 3 image
        │ Phase 2                                                       ▼
        │ (Vendor, Factory, Lot#)   ┌──────────────┐ resolve ┌────────────────────────┐
   ┌────┴───────┐  pull (WIP)        │ dtc_wip_ktb  │◀────────│ DTC WIP_ITS_USE rows   │
   │ attributes │◀───────────────── │ + registry   │ mapping │ (KTB WIP document)    │
   │ _update    │                   └──────────────┘         └────────────────────────┘

   DTC LinePlan ─────────────────▶  dtc_lineplan_ktb  (Phase 9a)
                                    dtc_wip_ktb       (Phase 9a join)
                                          │ join + transpose
                                          ▼
                                    costing_chart      (Style × Color × Vendor/Factory)
                                          │
                                          ▼
                              NT Orbit Duty Tools API  (Phase 9b, persistent cache)
                                          │
                                          ▼
                          hts_code / duty_rate_* / tariff_rate filled in-place
```

   (DTC FABRIC → dtc_fabric_ktb, Phase 8a, is RETIRED 2026-09-01 — superseded by
   a separate "MaterialLib" application; no longer part of this data flow.
   dtc_fabric_ktb / dtc_fabric_registry were DROPPED from Delta 2026-09-01.)

### Field-ownership partition (one field, one direction)

| Direction | Fields |
|-----------|--------|
| **BeProduct → DTC** (Phase 1) | Product Status, Style Description, Class, Sub Class, Division, Brand, Garment Finish, Tech Pack Stage, Fabric Group, Placement, Gender; BP Style# (new match key), LF Style# (optional), Legacy Code (optional); Supplier (default-fill "Supplier" when blank) |
| **BeProduct → DTC** (Phase 7) | Proto/PreLine/SMS/Fit/PP/TOP sample submit history (JSON list per app) |
| **BeProduct → DTC, image only** (Phase 3) | Style Image (`front_image_url`); binary multipart upload, blank cells only |
| **DTC → BeProduct** (Phase 2) | Main Vendor (Sampling), Main Factory (Sampling) [header]; Lot# [colorway]; Main Factory Customer ID → no target, skipped |
| **Keys** (match, not overwritten) | `(BP Style#, Color / Wash)` in-request; `[Customer, BP Style#, SeasonCode, Brand]` composite/routing |
| **Filter** | Styles with Product Status = "Finalized" are excluded from all DTC sync |

Removed directions (Phase 6): "Legacy Code" was DTC→BP (now BP→DTC only). "Customer Style#" DTC column not created.
A field is never synced in both directions (no loops). SSOT: `beproduct_style_interested_fields.txt`.

### Denormalization (transform)

One BeProduct style explodes to **one row per colorway**. The current phase
hardcodes **one BOM/fabric line** per (style × color):
`Fabric Group = "MAIN MATERIAL CONTENT"`, `Placement = main_material_content`.
(A future `style × bom` table will allow (style × color × bom) rows.) Each staging
row carries `beproduct_style_id` and `colorway_id` so Phase 2 can write the
colorway-level Lot# back by id.

### Season mapping (forward-only)

DTC identifies a season as `(Customer, SeasonCode)`; BeProduct as
`(Customer, Season, Year)`. `SeasonCode = DTCCODE + last 2 digits of the year`,
e.g. `SPRING + 2028 → SS28`. Only the **prefix** (`SS`/`FW`) is looked up from
`dtc_seasoncode_mapping`; the year is algorithmic. Applied in the transform; Phase 2
never reverse-maps it (season is a fixed per-request key).

### Moved-key orphans

If a BeProduct key field (`BP Style#`, brand, season) changes, the style's request
changes: the new request gets an INSERT, and the stale row left in the old request
is flagged `Product Status = "(removed)"` (an invalid value signalling the DTC
user). Not deleted. Core: `phase1.compute_orphan_marks`.

---

## 4. DTC organization & identity

```
Workspace ("KTB")
  └─ Document ("KTB WIP")              # defines the JSON schema
      └─ Requests ("KTB <SeasonCode> <Brand>", e.g. "KTB FW26 Wrangler")
          └─ Sheet (1:1 with request)  # holds the data
              └─ Views                 # column projections; sync reads WIP_ITS_USE only
                  └─ Rows (rowId, rowIndex, columns…)
```

- **In-scope request name:** `<customer> <seasonCode> <brand>` where `seasonCode`
  is 2 letters + 2 digits (`FW26`) and the customer token matches. Other naming
  conventions (e.g. developer `KON …`) are ignored. One brand per request,
  agreeing with the name (project guarantee).
- **View:** sync always reads **`WIP_ITS_USE`** (complete, unfiltered projection).
  Requests whose registered view is anything else are skipped + logged.
- **Row keys:** `rowId` (UUID) → UPDATE via PATCH; `rowIndex` (int) → INSERT /
  DELETE. A single PATCH cannot mix the two. In-request match key is
  `(LF Style#, Color / Wash)` (season & brand are fixed per request).

---

## 5. Data model on ADB (`lft.beproduct`)

### BeProduct source tables

| Table | Grain | Key columns / notes |
|-------|-------|---------------------|
| `ktb_styles` | 1 row / style | `id`, `bp_style_number` (header_number; was `lf_style_number`), `lf_style_number` (new separate field), `brand` (brand_hk), `brands` (brands_multi), `gender`, `season`, `year`, `product_status` (excl. Finalized at sync time), `description`, `product_category`, `product_sub_category`, `division`, `garment_finish`, `techpack_stage`, `customer_style_number`, `lot_code`, `parent_vendor`, `factory`; `colorways_json`; `front_image_url`; **6 sample-app columns** `{proto,preline,sms,fit,pp,top}_sample_json` (JSON arrays of submit×size records; transform formats into DTC status strings via `sync.samples`); `data_json`; timestamps |
| `beproduct_style_app_registry` | 1 row / (folder × app) | Cache of folder-constant application IDs (`00_init_style_app_registry`). `folder_name`, `app_id`, `app_title`, `app_type`, `is_sample`, `column_prefix`, `registered_at`. Sync reads `is_sample=true` to know which apps to `app_get`. |
| `beproduct_master_*` | 1 row / valid choice | 11 tables (brands, teams, seasons, years, product_status, product_category, product_sub_category, division, techpack_stage, parent_vendor, factory); columns `field_id`, `value`, `code`, `active`, `data_json`, `synced_at`. `garment_finish` omitted — free-text field, no choices. Used to validate dropdown/multiselect values before push-back. Written (and optionally pushed back to BeProduct) by `p5utl_beproduct_master_data_sync`. |
| `beproduct_directory` | 1 row / company | Directory of vendors, factories, and partners. Columns: `id` (BeProduct UUID, null for new records), `directory_id` (human-readable code), `name`, `partner_type`, `address`, `country`, `state`, `zip`, `city`, `phone`, `fax`, `website`, `notes`, `active`, `data_json`, `synced_at`. `id = NULL` rows are Added; `id = <uuid>` rows are Updated on next push. Matched by **`name`** (Phase 0 upsert), not `directory_id`. |
| `beproduct_directory_contacts` | 1 row / contact | Contacts within a directory company. Columns: `directory_id` (parent company UUID), `contact_id` (null = new), `email`, `first_name`, `last_name`, `title`, `mobile_phone`, `work_phone`, `role`, `active`, `data_json`, `synced_at`. |

Details + BeProduct API/SDK usage: `BEPRODUCT_GUIDE.md`.

### Integration tables

| Table | Grain | Purpose / key columns |
|-------|-------|-----------------------|
| `beproduct_to_dtc_staging` | 1 row / (style × color) | Denormalized push source. `dtc_request_name`, `bp_style_number` (match key), `lf_style_number`, `color`, `colorway_id`, `brand` (brand_hk), `season_code`, all Phase 1 fields, Phase 7 sample status columns (`{proto,preline,sms,fit,pp,top}_sample_status`), `supplier` (constant "Supplier"), `front_image_url`, `beproduct_style_id`, `colorway_id`, `sync_status` |
| `dtc_request_mapping` | 1 row / resolved request | `environment`, `dtc_request_name`, `request_id`, `sheet_id`, `view_id`, `season_code`, `brands`, `resolved_at`. Overwritten each run; consumed by the push. |
| `dtc_seasoncode_mapping` | 1 row / (customer, season) | `CUSTOMER`, `BPSEASON`, `DTCCODE` (prefix only). Forward-only. |
| `beproduct_to_dtc_sync_log` | 1 row / operation | BeProduct→DTC audit. `stage` ∈ {`resolve`,`create`,`share`,`push`,`images`}, `operation`, `status`, `reason`, `detail`, `payload`, match key, `run_id`, `log_time`. |
| `dtc_to_beproduct_sync_log` | 1 row / operation | Phase 2 audit (DTC→BeProduct). |

### DTC tables

| Table | Grain | Purpose / key columns |
|-------|-------|-----------------------|
| `dtc_request_registry` | 1 row / request | WIP request control table. `environment`, `request_id`, `view_id`, `customer`, `season_code`, `brands`, `sheet_id`, `request_reference`, `document_name`, `in_scope`, `request_is_active`, `row_count`, `last_extracted`, `last_pushed`, `msgs`. Upserted (`mode=merge`); absent-from-scan in-scope rows are **marked** inactive, not deleted. |
| `dtc_wip_<customer>` | 1 row / DTC sheet row | Pulled `WIP_ITS_USE` data (e.g. `dtc_wip_ktb`). Fixed columns: `bp_style_number` (Phase 6 match key), `lf_style_number`, `color_wash`, `row_id`, `row_index`, `extracted_at`, `data_json` (full row JSON). |
| `dtc_fabric_<customer>` | *(DROPPED 2026-09-01)* | **RETIRED (Phase 8a, superseded by MaterialLib) and DROPPED from Delta**, owner-confirmed. Historical shape: `lf_material_id`, `its_key`, `mill_fabric_code`, `mill_name`, `material_class`, `fabric_type`, `fabric_content`, `kb_fabric_code`, `adoption`, `season_code`, `brand`, `sheet_type` (PROD/DEV/MILL), `mill_code`, `data_json`. Filter: Adoption=Y only. |
| `dtc_fabric_registry` | *(DROPPED 2026-09-01)* | **RETIRED (Phase 8a) and DROPPED from Delta**, owner-confirmed. Historical registry, same shape as `dtc_request_registry`. |
| `dtc_lineplan_<customer>` | 1 row / LinePlan row | Phase 9a. `lineplan_ref`, `projected_volume`, `target_ldp`, `target_fob`, `internal_sourced` (raw LinePlan "INTERNAL/ SOURCED" — captured here but NOT joined into `costing_chart`'s `supplier_type`, see below), `gender`, `category`, `product_line`, `region`, `season_launched`, `data_json`. |
| `dtc_lineplan_registry` | 1 row / LinePlan request | Phase 9a registry. |
| `dtc_xts_master_ktb` | 1 row / kept XTS sheet row | Phase 0. `partner_type` (SUPPLIER/FACTORY/MILL), `name`, `directory_id`, `country`, always-NULL optional cols (no address/phone/etc. exist in XTS Master), `request_id`, `request_reference`, `view_name`, `data_json`. Brand-config rows (`Type="Brand"`/`"Fabric Brand"`) already filtered out at pull time. |
| `dtc_xts_master_registry` | 1 row / XTS Master request | Phase 0 registry: `partner_type`, `request_id`, `request_reference`, `sheet_id`, `view_id`, `view_name`, `row_count`, `last_extracted`, `msgs`. |
| `costing_chart` | 1 row / (style × color × vendor slot) | Phase 9a output. Key: `[customer, bp_style_no, color_name, lineplan_ref, supplier_type, supplier, factory]`. `supplier_type` = `"Main"\|"1"\|"2"\|"3"` GENERATED from which WIP vendor/factory column-pair the row came from (per original spec "Supplier Type - Generated from Master Chart data"; corrected 2026-09-01 — this is NOT LinePlan's "INTERNAL/ SOURCED", which does not flow into this table at all). `hts_code`/`duty_rate_*`/`tariff_rate` filled by Phase 9b (NT Orbit). Full overwrite each Phase 9a run — Phase 9b re-fills from `nt_orbit_duty_cache` after each rebuild. **Has real downstream readers — must stay stable; use `costing_chart_kei` for testing instead of writing here directly.** |
| `costing_chart_kei` | same shape as `costing_chart` | Owner's dedicated Phase 9b DAG-testing table (`costing_chart_table` job param override). Intentionally kept alongside the real table — never used by the default/production job run. |
| `nt_orbit_duty_cache` | 1 row / (product_description, origin_country, import_country) | Phase 9b PERSISTENT cross-run cache (never wiped by Phase 9a) — avoids re-paying the ~30s/call NT Orbit cost every daily run. Stale after `cache_ttl_days` (default 180). |
| `nt_orbit_oauth_state` | 1 row (latest) | Phase 9b — persisted rotated Entra `refresh_token` (`dbutils.secrets` is read-only, so this table is the actual live credential store after the first seed). |

- **DTC operation keys:** `row_id` → UPDATE; `row_index` → INSERT/DELETE.
- **In-request match key (WIP):** `(BP Style#, Color / Wash)` (Phase 6; was `(LF Style#, Color / Wash)`).
- **Cross-request identity:** `(customer, season_code, brand, bp_style_number, color_wash)`.
- Per-request metadata lives in the row columns + the registry (no `TBLPROPERTIES`).

---

## 6. Security

- All credentials in the Databricks secret scope **`beproduct`**:
  BeProduct OAuth (`client_id`, `client_secret`, `refresh_token`, `company_domain`)
  and DTC keys (`dtc_api_key_uat`, `dtc_api_key_prod`).
- No credentials in code/config. Environment-specific DTC keys (UAT/PROD).
- Local deploy uses `.env` (`DATABRICKS_HOST`, `DATABRICKS_PAT`).

---

## 7. Where things are verified

`../AGENTS.md` is the durable log of **live-validated** API behaviour and project
invariants (DTC write/create/share contracts, BeProduct schema quirks, the
field-direction partition). Update it (and the SSOT field file) before changing any
field mapping.
