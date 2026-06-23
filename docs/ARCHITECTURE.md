# Architecture — BeProduct ⇄ DTC Sync on Databricks

Bi-directional synchronization between **BeProduct** (style PLM) and **DTC**
("Data Collab" sheets), staged through **Databricks / Delta** under Unity Catalog
schema `lft.beproduct`.

This document is the single reference for **components**, **data flow**, and the
**data model on Azure Databricks (ADB)**. It merges what used to live in
`BEPRODUCT_TO_DTC_GUIDE.md`, `dtc/README.md`, and `dtc/DATA_MODEL.md`.

- Forward field sync (BeProduct → DTC): `PHASE1_WORKFLOW.md`
- Reverse field sync (DTC → BeProduct): `PHASE2_WORKFLOW.md`
- Image sync (BeProduct → DTC): `PHASE3_WORKFLOW.md`
- Component API/SDK + per-side tables: `DTC_GUIDE.md`, `BEPRODUCT_GUIDE.md`
- Field mapping SSOT: `beproduct_style_interested_fields.txt`
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
├── beproduct_style_sync.py        # BeProduct API → lft.beproduct.ktb_styles (+ sample-app status)
├── beproduct_master_data_sync.py  # Admin: pull/push-back MasterData (dropdowns) + Directory
├── beproduct_to_dtc_transform.py  # ktb_styles → beproduct_to_dtc_staging (denormalize)
├── dtc_request_manager.py         # Resolve / CREATE / SHARE DTC requests → dtc_request_mapping
├── beproduct_to_dtc_push.py       # Phase 1: BeProduct → DTC upsert + orphan marks
├── beproduct_to_dtc_images.py     # Phase 3: front image → DTC "Style Image"
├── dtc_share_requests.py          # Idempotent request-sharing backfill
└── orchestrate_sync.py            # ⚠️ RETIRED — single-notebook fallback only

dtc/
├── notebooks/
│   ├── 00_init_request_registry.py  # Standalone registry build/refresh
│   ├── 00_init_season_mapping.py    # Seed dtc_seasoncode_mapping
│   ├── pull_requests_to_delta.py    # DTC API → dtc_wip_<customer> (+ registry refresh)
│   └── 05_push_dtc_to_beproduct.py  # Phase 2: DTC → BeProduct pushback
├── python/                          # Importable modules (deployed as Workspace files)
│   ├── client/rest_client.py        # Generic REST client (retry, multipart)
│   ├── connectors/dtc.py            # DTC API connector
│   └── sync/
│       ├── phase1.py                # BeProduct → DTC upsert core (pure)
│       ├── phase2.py                # DTC → BeProduct pushback core (pure)
│       ├── phase3.py                # Image upload planning + type classification (pure)
│       └── registry.py             # Shared registry refresh (discover→enrich→merge)
├── tests/                           # Unit + live tests for the pure cores
├── DTC-api-2026-05-08.json          # DTC API Postman collection
└── DTC-api-2026-05.pdf              # DTC API description/examples

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
Step 1  bp_style_sync    BeProduct API ─▶ ktb_styles (+ 6 sample-app stat) ┐ parallel
Step 3  pull_dtc         DTC API       ─▶ dtc_wip_<customer> + registry   ┘
Step 2  transform        ktb_styles    ─▶ beproduct_to_dtc_staging  (after Step 1)
Step 4  request_manager  staging+registry ─▶ dtc_request_mapping    (after 2+3)
                          (creates + shares missing in-scope requests)
Step 5  phase1_push      Phase 1: staging ─▶ DTC sheets (upsert + orphan marks)
Step 6  phase2_push      Phase 2: dtc_wip ─▶ BeProduct (pushback)   (after 2+3)
Step 7  repull_dtc       refresh dtc_wip after Phase 1 inserts       (after 5)
Step 8  phase3_images    Phase 3: front image ─▶ DTC "Style Image"
```

Dependencies: 1→2 (chain); 3 independent of 1/2; 4 needs 2+3; 5 needs 4; 6 needs
2+3 (disjoint fields from 5); 7 after 5 (`run_if=ALL_DONE` so a skipped Phase 1
still lets Phase 3 proceed); 8 after 7. Condition tasks `gate_phase1/2/3` evaluate
`run_phase1/2/3` job parameters and gate the respective push tasks. `dry_run`
(applied to steps 4/5/6/8) computes + logs without writing.

```
   BeProduct (PLM)                  Databricks  (lft.beproduct)                 DTC (sheets)
   ┌────────────┐   style sync   ┌───────────────┐  transform  ┌──────────────────────┐
   │ STYLE +    │ ─────────────▶ │ ktb_styles     │ ──────────▶ │ beproduct_to_dtc_     │
   │ Colorways  │                │ (1 row/style)  │             │ staging (1 row/style× │
   └────────────┘                └───────────────┘             │ color)                │
        ▲                                                       └─────────┬────────────┘
        │ Phase 2 pushback                                                 │ Phase 1 upsert
        │ (Legacy Code, Lot#, vendors)                                     ▼  Phase 3 image
   ┌────┴───────┐   pull         ┌───────────────┐  resolve    ┌──────────────────────┐
   │ attributes │ ◀───────────── │ dtc_wip_<cust> │ ◀────────── │ DTC WIP_ITS_USE rows  │
   │ _update    │                │ + registry     │   mapping   │ (per in-scope request)│
   └────────────┘                └───────────────┘             └──────────────────────┘
```

### Field-ownership partition (one field, one direction)

| Direction | Fields |
|-----------|--------|
| **BeProduct → DTC** (Phase 1) | Product Status, Style Description, Class, Sub Class, Division, Brand, Garment Finish, Tech Pack Stage, Fabric Group, Placement |
| **BeProduct → DTC, image only** (Phase 3) | Style Image (`front_image_url`) |
| **DTC → BeProduct** (Phase 2) | Legacy Code, Main Vendor (Sampling), Main Factory (Sampling) [header]; Lot# [colorway]; Main Factory Customer ID → no target, skipped |
| **Keys** (match, not overwritten) | LF Style#, Color / Wash |

A field is never synced in both directions (no loops). SSOT for the exact
column ⇄ fieldId ⇄ direction mapping: `beproduct_style_interested_fields.txt`.

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

If a BeProduct key field (LF Style#, brand, season) changes, the style's request
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
| `ktb_styles` | 1 row / style | `id`, `lf_style_number`, `brands`, `season`, `year`, `product_status`, `description`, `product_category`, `product_sub_category`, `division`, `garment_finish`, `techpack_stage`, `customer_style_number`, `lot_code`, `parent_vendor`, `factory`; arrays `colorways_array`/`colorways_count`; `colorways_json` (`[{colorway_id,color_name,color_number}]`); `front_image_url`; **sample-app submits** `{proto,preline,sms,fit,pp,top}_sample_json` (6 JSON arrays of submit×size records, `'[]'` when no data; transform flattens); `data_json` (full record); change tracking `modified_at`/`last_modified`, `synced_at`/`extracted`, `created_at` |
| `beproduct_style_app_registry` | 1 row / (folder × app) | Cache of folder-constant application IDs (`00_init_style_app_registry`). `folder_name`, `app_id`, `app_title`, `app_type`, `is_sample`, `column_prefix`, `registered_at`. Sync reads `is_sample=true` to know which apps to `app_get`. |
| `beproduct_master_*` | 1 row / valid choice | 11 tables (brands, teams, seasons, years, product_status, product_category, product_sub_category, division, techpack_stage, parent_vendor, factory); columns `field_id`, `value`, `code`, `active`, `data_json`, `synced_at`. `garment_finish` omitted — free-text field, no choices. Used to validate dropdown/multiselect values before push-back. Written (and optionally pushed back to BeProduct) by `beproduct_master_data_sync`. |
| `beproduct_directory` | 1 row / company | Directory of vendors, factories, and partners. Columns: `id` (BeProduct UUID, null for new records), `directory_id` (human-readable code), `name`, `partner_type`, `address`, `country`, `state`, `zip`, `city`, `phone`, `fax`, `website`, `notes`, `active`, `data_json`, `synced_at`. `id = NULL` rows are Added; `id = <uuid>` rows are Updated on next push. |
| `beproduct_directory_contacts` | 1 row / contact | Contacts within a directory company. Columns: `directory_id` (parent company UUID), `contact_id` (null = new), `email`, `first_name`, `last_name`, `title`, `mobile_phone`, `work_phone`, `role`, `active`, `data_json`, `synced_at`. |

Details + BeProduct API/SDK usage: `BEPRODUCT_GUIDE.md`.

### Integration tables

| Table | Grain | Purpose / key columns |
|-------|-------|-----------------------|
| `beproduct_to_dtc_staging` | 1 row / (style × color) | Denormalized push source. `dtc_request_name`, `lf_style_number`, `color`, `colorway_id`, `brands`, `season_code`, mapped fields, `front_image_url`, `beproduct_style_id`, `beproduct_modified_at`, `sync_status` (`pending`/`pushed`/`error`), `pushed_at` |
| `dtc_request_mapping` | 1 row / resolved request | `environment`, `dtc_request_name`, `request_id`, `sheet_id`, `view_id`, `season_code`, `brands`, `resolved_at`. Overwritten each run; consumed by the push. |
| `dtc_seasoncode_mapping` | 1 row / (customer, season) | `CUSTOMER`, `BPSEASON`, `DTCCODE` (prefix only). Forward-only. |
| `beproduct_to_dtc_sync_log` | 1 row / operation | BeProduct→DTC audit. `stage` ∈ {`resolve`,`create`,`share`,`push`,`images`}, `operation`, `status`, `reason`, `detail`, `payload`, match key, `run_id`, `log_time`. |
| `dtc_to_beproduct_sync_log` | 1 row / operation | Phase 2 audit (DTC→BeProduct). |

### DTC tables

| Table | Grain | Purpose / key columns |
|-------|-------|-----------------------|
| `dtc_request_registry` | 1 row / request | Control table driving discovery. `environment`, `request_id`, `view_id`, `customer`, `season_code`, `brands`, `sheet_id`, `request_reference`, `document_name`, `in_scope`, `request_is_active`, `row_count`, `last_extracted`, `last_pushed`, `msgs`. Upserted (`mode=merge`) so sync state survives; absent-from-scan in-scope rows are **marked** inactive, not deleted. |
| `dtc_wip_<customer>` | 1 row / DTC sheet row | Pulled `WIP_ITS_USE` data (e.g. `dtc_wip_ktb`). Built from an **explicit schema** so all-NULL columns don't trip `CANNOT_DETERMINE_TYPE`. |

**`dtc_wip_<customer>` fixed columns:** `customer`, `workspace_name`,
`document_name`, `request_id`, `request_reference`, `season_code`, `brands`,
`row_id` (STRING), `row_index` (LONG), `lf_style_number`, `color_wash`,
`extracted_at` (TIMESTAMP), `data_json` (full row JSON). **Dynamic columns:** every
DTC view column is also flattened to `col_<normalized_name>` (STRING); empty view
columns may be absent for a given request — the union is aligned by name. Full
fidelity always remains in `data_json`.

- **DTC operation keys:** `row_id` → UPDATE; `row_index` → INSERT/DELETE.
- **In-request match key:** `(lf_style_number, color_wash)`.
- **Cross-request identity:** `(customer, season_code, brands, lf_style_number, color_wash)`.
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
