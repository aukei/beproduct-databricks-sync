# Architecture — BeProduct ⇄ DTC Sync on Databricks

Bi-directional synchronization between **BeProduct** (style PLM) and **DTC**
("Data Collab" sheets), staged through **Databricks / Delta** under Unity Catalog
schema `lft.beproduct`.

This document is the single reference for **systems**, **repository layout**, and
the **data model on Azure Databricks (ADB)**.

**Branch `v2`.** Stage ordering, the task graph and gating conditions are NOT
here — they live in [PIPELINE.md](PIPELINE.md).

| Looking for | Read |
|---|---|
| What runs, in what order, and what gates a row | [PIPELINE.md](PIPELINE.md) |
| Which field goes which way, keys, PATCH allow-list | [SYNC_CONTRACT.md](SYNC_CONTRACT.md) |
| Why v2 exists, what changed, rollout and rollback | [MIGRATION_V1_V2.md](MIGRATION_V1_V2.md) |
| Per-side API/SDK surface and tables | [DTC_GUIDE.md](DTC_GUIDE.md), [BEPRODUCT_GUIDE.md](BEPRODUCT_GUIDE.md) |
| Verified API behaviour, invariants, decisions log | [../AGENTS.md](../AGENTS.md) |
| The v1 phase-numbered documents | [v1/](v1/) — archived, still the historical record |

Field-mapping SSOTs: `beproduct_style_interested_fields.txt` (style),
`costing_interested_fields.txt` (costing/duty),
`beproduct_directory_xts_interested_fields.txt` (Directory/XTS),
`beproduct_material_interested_fields.txt` (material).

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

`v2` marks notebooks/modules introduced by the v2 revamp; everything else is
carried over unchanged. See [MIGRATION_V1_V2.md](MIGRATION_V1_V2.md) for which
v1 artifacts are retired vs. still deployed.

```
beproduct/                            # BeProduct-side notebooks (also host the cross-platform push)
├── 00_init_style_app_registry.py     # Cache folder application IDs → beproduct_style_app_registry
├── p1p7_beproduct_style_sync.py      # BeProduct API → ktb_styles (+ sample-app status)
├── p5utl_beproduct_master_data_sync.py  # Admin: pull/push-back MasterData (dropdowns) + Directory
├── p1p7_beproduct_to_dtc_transform.py  # v2 Stage 20: ktb_styles × BOM → staging (style×color×material)
├── p1_dtc_request_manager.py         # v2 Stage 25: resolve / CREATE / SHARE requests → dtc_request_mapping
├── p3_beproduct_to_dtc_images.py     # v2 Stage 45: front image → DTC "Style Image" (folded into the main DAG 2026-09-15)
├── p0_xts_master_to_directory_upsert.py  # v2 Stage 00: XTS Master → BeProduct Directory upsert
├── p1utl_dtc_share_requests.py       # Idempotent request-sharing backfill
├── p1p7_beproduct_to_dtc_push.py     # v1 Phase 1 push — superseded by v2_wip_push
├── wait_cluster.py                   # v1 cold-start sentinel — unused on serverless
└── orchestrate_sync.py               # RETIRED — single-notebook fallback only

dtc/
├── notebooks/
│   ├── 00_init_request_registry.py   # Standalone WIP registry build/refresh
│   ├── 00_init_season_mapping.py     # Seed dtc_seasoncode_mapping
│   ├── p0_pull_xts_master_to_delta.py   # v2 Stage 00: DTC XTS Master → Delta
│   ├── p1_pull_masters_to_delta.py   # v2 Stage 10: KTB WIP sheets → dtc_wip_ktb + registry
│   ├── p9a_pull_lineplan_to_delta.py # v2 Stage 10: KTB LinePlan → dtc_lineplan_ktb
│   ├── p9a_build_costing_chart.py    # v2 Stage 30 (wip_effective_mode=intent): staging × WIP × LinePlan → costing_chart
│   ├── v2_pull_bom_segments.py       # v2 Stage 20b: BeProduct PageBomVariation → bom_segments
│   ├── v2_wip_push.py                # v2 Stage 40: THE single DTC write window
│   ├── v2_push_customer_code.py      # v2 Stage 55: DTC customer code → BeProduct material master
│   ├── p2_push_dtc_to_beproduct.py   # v2 Stage 50: DTC → BeProduct pushback
│   ├── p9b1_compute_duty_rates.py    # duty_compute job: NT Orbit → costing_chart only
│   ├── p10_pull_bom_and_enrich.py    # v1 Phase 10 — folded into v2_pull_bom_segments + v2_wip_push
│   ├── p9b2_push_duty_to_wip.py      # v1 Phase 9b push — folded into v2_wip_push
│   ├── p9b_fill_duty_rates.py        # SUPERSEDED 2026-09-03 — manual-fallback artifact only
│   └── p8a_pull_fabric_to_delta.py   # RETIRED 2026-09-01 (MaterialLib) — manual fallback only
├── python/                           # Importable modules (deployed as Workspace files)
│   ├── client/rest_client.py         # Generic REST client (retry, multipart)
│   ├── client/entra_auth.py          # Entra ID delegated OAuth2 (NT Orbit)
│   ├── connectors/dtc.py             # DTC API connector
│   ├── connectors/nt_orbit.py        # NT Orbit Duty Tools connector
│   └── sync/
│       ├── wip_plan.py               # v2: composes phase1 + bom + duty into ONE request plan
│       ├── phase1.py                 # BeProduct → DTC upsert core (pure; DEFAULT_FILL_COLS)
│       ├── phase2.py                 # DTC → BeProduct pushback core (pure)
│       ├── phase3.py                 # Image upload planning + type classification (pure)
│       ├── bom.py                    # BOM parsing + enrichment decision tree (pure)
│       ├── duty.py                   # COSTING_KEY, cache staleness, WIP duty columns (pure)
│       ├── lifecycle.py              # Terminal-status staging eligibility (pure)
│       ├── samples.py                # Sample-app submit formatter (pure)
│       ├── xts_master.py             # Stage 00 Directory extraction (pure)
│       └── registry.py               # Shared registry refresh (discover→enrich→merge)
└── tests/                            # Unit + live tests for the pure cores

standalone/beproduct_style_push.py    # Standalone Delta → BeProduct push-back (not in the pipeline)
scripts/
├── upload_notebooks.py               # Deploy notebooks + modules (--root selects v1 / v2 workspace root)
├── deploy_job.py                     # Create / reset jobs (--job main|duty_compute|images|v2)
└── nt_orbit_oauth_setup.py           # One-time Entra delegated-OAuth seeding
docs/                                 # This documentation set (docs/v1/ = archived v1 phase docs)
```

**Notebook vs module split (invariant):** notebooks can't run locally (Spark /
`dbutils`). Deterministic logic lives in `dtc/python/sync/*.py` (pure Python,
unit-tested); notebooks are thin Spark/IO wrappers around it. All HTTP lives in
`connectors/dtc.py` + `client/rest_client.py`.

---

## 3. Components & data flow

The pipeline runs as **independent Databricks jobs**, all defined in
`scripts/deploy_job.py`:

| Job | Contents | Compute |
|---|---|---|
| `BeProduct_DTC_sync_v2` (367710575109755) | The main DAG — Stages 00–55, **including `phase3_images`** (folded in 2026-09-15) | serverless |
| `BeProduct_DTC_sync_duty_compute` (1026599988408090) | NT Orbit lookups → `costing_chart` only; zero DTC contact | serverless (moved off classic 2026-09-15) |
| `BeProduct_DTC_sync_images` (847087837807970) | Style Image upload — **superseded and PAUSED**; its task now runs inside the v2 DAG | classic |
| `BeProduct_DTC_sync_dag` (294837488757511) | **v1 main job** — PAUSED after the v2 cutover; kept for rollback | classic + pool |

**Two live jobs, not four.** The instance pool has been scaled to 0 VMs — only
the two paused rollback jobs still reference it.

> **The task graph, every stage and every gate live in [PIPELINE.md](PIPELINE.md).**
> This section covers only the architectural shape; that document is
> authoritative on ordering and conditions, and is not duplicated here.

The shape worth knowing at this level:

- **One DTC write window per request per run.** Every WIP write in the main job
  happens in the `wip_push` stage and nowhere else, in ≤2 back-to-back PATCH
  calls. This is forced by DTC's request-level optimistic locking — any write
  moves the request's server-side `last_read` and silently invalidates every
  browser session that loaded earlier. See [MIGRATION_V1_V2.md](MIGRATION_V1_V2.md).
- **Planning is separate from writing.** Stages 20–30 compute the complete
  intended state into Delta (`beproduct_to_dtc_staging`, `costing_chart`)
  touching no DTC at all; Stage 40 diffs that intent against one live read per
  request and writes once.
- **Reads are cheap, writes are not.** Only writes move `last_read`, so the
  pipeline reads DTC freely (`pull_master_dtc`, `pull_lineplan_dtc`, plus
  `wip_push`'s own live read) and writes as rarely as possible.
- **Two remaining write streams.** `wip_push` (JSON `sheetData` PATCH by `rowId`
  / `rowIndex`) and the images job (binary multipart `/images` by `rowIndex`).
  They cannot be merged — DTC rejects any `sheetData` write to `Style Image` —
  but they touch disjoint columns and the images job re-reads the live sheet
  immediately before writing, so they are safe concurrently.

```
   BeProduct (PLM)                Databricks (lft.beproduct)               DTC (sheets)
   ┌────────────┐  style sync   ┌──────────────┐              ┌────────────────────────┐
   │ STYLE +    │ ─────────────▶│ ktb_styles   │              │ DTC WIP_ITS_USE rows   │
   │ Colorways  │               │ (1 / style)  │              │ (KTB WIP document)     │
   │ + 6 apps   │               └──────┬───────┘              └───────────┬────────────┘
   └────────────┘                      │                                  │ pull
        ▲                              │ transform  ◀── BOM ──┐           ▼
        │ Stage 50                     ▼            (Lakebase) │   ┌──────────────┐
        │ (Vendor, Factory,     ┌────────────────────────┐     │   │ dtc_wip_ktb  │
        │  Customer ID, COO,    │ beproduct_to_dtc_      │     │   │ + registry   │
        │  Lot#)                │ staging                │     │   └──────┬───────┘
   ┌────┴───────┐               │ (1 / style×color×      │     │          │
   │ attributes │◀──────────────│  material)             │     │          │
   │ _update    │               └───────────┬────────────┘     │          │
   └────────────┘                           │                  │          │
                                            │   ┌──────────────┴──────────┘
   DTC LinePlan ──▶ dtc_lineplan_ktb ───────┼──▶│ build_costing (staging × WIP × LinePlan)
                                            │   └──────────────┬──────────┘
                                            │                  ▼
                                            │            costing_chart
                                            │        (style × color × material
                                            │         × vendor/factory slot)
                                            │                  │
                                            │                  │  ◀── nt_orbit_duty_cache
                                            │                  │      (read-only fill)
                                            ▼                  ▼
                                    ┌───────────────────────────────────┐
                                    │  wip_push — ONE write window      │
                                    │  style + material + duty fields   │
                                    │  ≤2 PATCH calls per request       │
                                    └───────────────┬───────────────────┘
                                                    ▼
                                          DTC WIP (live sheet)

   Separate job:  costing_chart ──▶ NT Orbit Duty Tools API ──▶ costing_chart
                                    (+ nt_orbit_duty_cache, persistent, never wiped)
   Separate job:  staging + live DTC ──▶ DTC "Style Image" (multipart /images)
```

Phase 8a/8b (DTC FABRIC → Delta → BeProduct Material Master) are RETIRED
(2026-09-01), confirmed by the project team to be replaced by a separate
"MaterialLib" application. `p8a_pull_fabric_to_delta.py` remains as a
historical/manual-fallback artifact only; its tables `dtc_fabric_<customer>` /
`dtc_fabric_registry` were DROPPED from Delta.

### Field-ownership partition (one field, one direction)

| Direction | Fields |
|-----------|--------|
| **BeProduct → DTC** (Phase 1) | Product Status, Style Description, Class, Sub Class, Division, Brand, Garment Finish, Tech Pack Stage, Fabric Group, Placement, Gender; BP Style# (new match key), LF Style# (optional), Legacy Code (optional); Supplier (default-fill "Supplier" when blank) |
| **BeProduct → DTC** (Phase 7) | Proto/PreLine/SMS/Fit/PP/TOP sample submit history (JSON list per app) |
| **BeProduct → DTC, image only** (Phase 3) | Style Image (`front_image_url`); binary multipart upload, blank cells only |
| **DTC → BeProduct** (Phase 2) | Main Vendor (Sampling), Main Factory (Sampling), Main Factory Customer ID (`customer_factory_code`, wired up 2026-09-03) [header]; Lot# [colorway] |
| **Keys** (match, not overwritten) | `(BP Style#, Color / Wash)` in-request; `[Customer, BP Style#, SeasonCode, Brand]` composite/routing |
| **Filter** | Styles with Product Status = "Finalized" are excluded from all DTC sync |

Removed directions (Phase 6): "Legacy Code" was DTC→BP (now BP→DTC only). "Customer Style#" DTC column not created.
A field is never synced in both directions (no loops). SSOT: `beproduct_style_interested_fields.txt`.

### Denormalization (transform)

"WIP = style x color x material" (2026-09-11): one BeProduct style explodes
to one row per colorway — or exactly ONE row with `Color / Wash =
DUMMY_COLOR` ("NO BP COLORWAY") if the style has zero colorways, so a
colorless style still always reaches DTC instead of being dropped from
staging. Each row is staged with `Fabric Group`/`Mill Fabric Article #` set
to the `DUMMY_FABRIC_GROUP`/`DUMMY_FABRIC_ARTICLE` sentinels ("NO TPM BOM")
and a blank `Placement` — Phase 1 has no BOM data of its own; Phase 10
(`dtc/notebooks/p10_pull_bom_and_enrich.py`) is the sole owner of real
material data and fans a style×color out into one physical DTC row per
material segment, upgrading these dummy values in place. Each staging
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
| `ktb_styles` | 1 row / style | `id`, `bp_style_number` (header_number; was `lf_style_number`), `lf_style_number` (new separate field), `brand` (brand_hk) — `brands` (brands_multi) REMOVED 2026-09-03, `brand` is now the only brand field —, `gender`, `season`, `year`, `product_status` (excl. Finalized at sync time), `description`, `product_category`, `product_sub_category`, `division`, `garment_finish`, `techpack_stage`, `customer_style_number`, `lot_code`, `parent_vendor`, `factory`; `colorways_json`; `front_image_url`; **6 sample-app columns** `{proto,preline,sms,fit,pp,top}_sample_json` (JSON arrays of submit×size records; transform formats into DTC status strings via `sync.samples`); `data_json`; timestamps |
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
| `costing_chart` | 1 row / (style × color × vendor slot) | Phase 9a output. Key: `[customer, bp_style_no, color_name, lineplan_ref, supplier_type, supplier, factory]`. `supplier_type` = `"Main"\|"1"\|"2"\|"3"` GENERATED from which WIP vendor/factory column-pair the row came from (per original spec "Supplier Type - Generated from Master Chart data"; corrected 2026-09-01 — this is NOT LinePlan's "INTERNAL/ SOURCED", which does not flow into this table at all). `hts_code`/`duty_rate_*`/`tariff_rate` filled by `duty_compute` (NT Orbit). Full overwrite each Stage 30 run; the values survive because Stage 30 re-reads all five from the **live DTC WIP columns** and then refills any blank from `nt_orbit_duty_cache`. `tariff_rate` joined that WIP fallback on 2026-09-17 when its DTC columns went live, which is what made the former Step 4b carry-forward redundant. **All five are fill-blank-only**, so an already-populated but outdated value is never corrected — that is what `force_refresh_duty` exists for (PIPELINE.md, companion jobs). **Has real downstream readers (the `duty_compute` job MERGEs it; `wip_push` reads it).** Routine runs write it directly; to build a comparison copy without replacing it, override `costing_chart_table_name`. Recovery for a bad build is Delta time travel (`RESTORE TABLE … VERSION AS OF <n>`), since the table is fully overwritten every run regardless. |
| `nt_orbit_duty_cache` | 1 row / (product_description, origin_country, import_country) | `duty_compute` PERSISTENT cross-run cache (never wiped by Stage 30) — avoids re-paying the ~30s/call NT Orbit cost every run. Nominally stale after `cache_ttl_days` (default 180), but **that TTL is unreachable for a populated row**: a market is only queried when its cell is blank, so a filled row's entry is never examined (live-confirmed 2026-09-17). `force_refresh_duty=true` is the only thing that re-queries it. **The key is the rendered `product_description`**, so anything that changes that text (the Phase 10 BOM rewrite changed `fabric_content`, and `sub_class` backfill changed another part) silently re-keys the whole cache. NT Orbit is also **not fully deterministic** — an identical request has returned a different HTS ~2h apart — so "same input → same output" is not a reliable premise. |
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
