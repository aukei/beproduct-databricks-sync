# Quick Start

Setup, how to use, and which notebook to run for the BeProduct ⇄ DTC sync.

> Concepts & data model: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
> Per-phase detail: `docs/PHASE1_WORKFLOW.md`, `PHASE2_WORKFLOW.md`, `PHASE3_WORKFLOW.md`.

---

## Prerequisites

- Databricks workspace with Unity Catalog (schema `lft.beproduct`).
- BeProduct API credentials (OAuth client-credentials).
- DTC API key (UAT and/or PROD).
- Local: Python + `pip install databricks-sdk` for deployment.

---

## 1. Setup

### 1a. Databricks secrets (scope `beproduct`)

```bash
databricks secrets create-scope beproduct

# BeProduct OAuth
databricks secrets put-secret beproduct client_id
databricks secrets put-secret beproduct client_secret
databricks secrets put-secret beproduct refresh_token
databricks secrets put-secret beproduct company_domain

# DTC API keys
databricks secrets put-secret beproduct dtc_api_key_uat
databricks secrets put-secret beproduct dtc_api_key_prod
```

### 1b. Deploy notebooks + modules

```bash
pip install databricks-sdk
cp .env.example .env
# Edit .env:
#   DATABRICKS_HOST=https://adb-XXXXXXXX.azuredatabricks.net
#   DATABRICKS_PAT=dapi...

python scripts/upload_notebooks.py            # notebooks (beproduct/, dtc/notebooks/) + modules (dtc/python/)
python scripts/upload_notebooks.py --dry-run  # preview only
```

Notebooks deploy under `/Workspace/Repos/beproduct-sync/…`; Python modules deploy
to `/Workspace/Repos/beproduct-sync/DTC/python` (imported by the notebooks).

### 1c. One-time: seed the season-code mapping

```
Notebook: /Workspace/Repos/beproduct-sync/DTC/notebooks/00_init_season_mapping
```
Then insert prefixes (year is algorithmic — last 2 digits of the BeProduct year):
```sql
INSERT INTO lft.beproduct.dtc_seasoncode_mapping (CUSTOMER, BPSEASON, DTCCODE) VALUES
  ('KTB','SPRING','SS'), ('KTB','FALL','FW');
```

---

## 2. How to use — run the full sync (recommended)

The entire pipeline is one schedulable job:

```
Notebook: /Workspace/Repos/beproduct-sync/beproduct/orchestrate_sync
```

| Parameter | Default | Notes |
|-----------|---------|-------|
| `dtc_environment` | `uat` | `uat` or `prod` |
| `dry_run` | `false` | `true` = compute + log, **no writes** (preview); set `false` to apply |
| `run_phase1` / `run_phase2` / `run_phase3` | `true` | toggle each direction |
| `customer` / `dtc_workspace` | `KTB` | scope |
| `dtc_document` | `KTB WIP` | document for created requests |
| `refresh_mode` | `FULL` | BeProduct pull mode (`FULL`/`INCREMENTAL`) |

It runs, in order: BeProduct style sync → transform → DTC pull → resolve/create/
share requests → **Phase 1** push → **Phase 2** pushback → dtc_wip refresh →
**Phase 3** image upload. Each step is dependency-guarded and reported in a final
summary; a failed step fails the job.

**First time:** run with `dry_run=true` to preview the plan, then `dry_run=false`.

Review results:
```sql
SELECT * FROM lft.beproduct.beproduct_to_dtc_sync_log WHERE stage IN ('resolve','create','share','push','images') ORDER BY log_time DESC LIMIT 100;
SELECT * FROM lft.beproduct.dtc_to_beproduct_sync_log ORDER BY log_time DESC LIMIT 100;
```

---

## 3. How to use — run notebooks individually

Run these in order if you prefer step-by-step control (params shown are the key ones).

**WIP sync chain (Phases 1 / 2 / 3 / 7):**

| # | DAG task | Notebook | Purpose | Output |
|---|----------|----------|---------|--------|
| 1 | `bp_style_sync` | `beproduct/p1p7_beproduct_style_sync` | Pull styles, excl. Finalized, enrich 6 sample apps | `ktb_styles` |
| 2 | `transform` | `beproduct/p1p7_beproduct_to_dtc_transform` | Denormalize style × color; format Phase 7 sample statuses | `beproduct_to_dtc_staging` |
| 3 | `pull_master_dtc` | `dtc/notebooks/p1_pull_masters_to_delta` | Pull KTB WIP `WIP_ITS_USE` rows + refresh registry | `dtc_wip_ktb` |
| 4 | `request_manager` | `beproduct/p1_dtc_request_manager` | Resolve / create / share requests | `dtc_request_mapping` |
| 5 | `phase1_push` | `beproduct/p1p7_beproduct_to_dtc_push` | **Phase 1+7** upsert (`dry_run`, `delta_only`) | DTC WIP sheets |
| 6 | `phase2_push` | `dtc/notebooks/p2_push_dtc_to_beproduct` | **Phase 2** pushback (`push_blanks=false`) | BeProduct |
| 7 | `repull_dtc` | `dtc/notebooks/p1_pull_masters_to_delta` | Targeted re-pull after Phase 1 inserts | `dtc_wip_ktb` |
| 8 | `phase3_images` | `beproduct/p3_beproduct_to_dtc_images` | **Phase 3** image upload (`dry_run`, `max_uploads`) | DTC "Style Image" |

> ⚠️ **Phase 8a/8b (FABRIC → Delta → BeProduct Material Master) are RETIRED
> (2026-09-01)** — confirmed by the project team to be replaced by a separate
> "MaterialLib" application, and removed from the DAG entirely (`gate_phase8a`
> / `pull_fabric_dtc` no longer exist as job tasks). `dtc/notebooks/
> p8a_pull_fabric_to_delta.py` remains in the repo as a historical/manual-
> fallback artifact only; the `dtc_fabric_ktb` / `dtc_fabric_registry` tables
> were DROPPED from Delta the same day (owner-confirmed, zero downstream readers).

**LinePlan + Costing chain (Phase 9a — parallel, independent):**

| DAG task | Notebook | Purpose | Output |
|----------|----------|---------|--------|
| `pull_lineplan_dtc` | `dtc/notebooks/p9a_pull_lineplan_to_delta` | Pull KTB LinePlan (Full view) | `dtc_lineplan_ktb` |
| `p9a_build_costing_chart` | `dtc/notebooks/p9a_build_costing_chart` | Join WIP × LinePlan; transpose 4 vendor/factory slots | `costing_chart` |

**Duty/Tariff chain (Phase 9b — after `build_costing_chart`):**

| DAG task | Notebook | Purpose | Output |
|----------|----------|---------|--------|
| `fill_duty_rates` | `dtc/notebooks/p9b_fill_duty_rates` | NT Orbit Duty Tools HTS/Duty/Tariff fill, persistent cross-run cache (`gate_phase9b`, `run_phase9b=true` live) | `costing_chart`, `nt_orbit_duty_cache` (+ optional DTC WIP push) |

**Other notebooks (on-demand, not in DAG):**

- `dtc/notebooks/00_init_request_registry` — standalone WIP registry build/refresh (first run or targeted `request_ids`).
- `beproduct/p5utl_beproduct_master_data_sync` — **admin-only.** Pull and/or push-back BeProduct MasterData + Directory.
  Modes: `PULL_ONLY`, `PUSH_MASTER_DATA`, `PUSH_DIRECTORY`, `PUSH_ONLY`. Use `dry_run=true` first.
  Writes `beproduct_master_*`, `beproduct_directory`, `beproduct_directory_contacts`.
- `beproduct/p1utl_dtc_share_requests` — idempotently (re-)share existing requests.
- `scripts/check_dtc_view.py` — DTC WIP_ITS_USE column readiness check (Phase 6 pending columns).

---

## 4. Common queries

```sql
-- BeProduct styles freshness
SELECT MAX(last_modified) latest, MAX(extracted) last_sync, COUNT(*) FROM lft.beproduct.ktb_styles;

-- Staging push status
SELECT sync_status, COUNT(*) FROM lft.beproduct.beproduct_to_dtc_staging GROUP BY sync_status;

-- Pulled DTC WIP rows
SELECT request_reference, COUNT(*) FROM lft.beproduct.dtc_wip_ktb GROUP BY request_reference;

-- Costing chart summary (costing_chart has real downstream readers — for
-- Phase 9b testing use lft.beproduct.costing_chart_kei instead, never write
-- test data into costing_chart directly)
SELECT factory_slot, COUNT(*) rows, COUNT(hts_code) with_hts FROM lft.beproduct.costing_chart GROUP BY factory_slot;

-- Duty/tariff persistent cache size (Phase 9b — avoids re-paying the ~30s/call
-- NT Orbit cost every daily run; costing_chart itself is fully overwritten
-- by Phase 9a each run, this cache table is not)
SELECT COUNT(*) FROM lft.beproduct.nt_orbit_duty_cache;

-- (Phase 8a/8b RETIRED 2026-09-01 — dtc_fabric_ktb / dtc_fabric_registry were
-- DROPPED from Delta the same day; superseded by "MaterialLib".)

-- Sync log (WIP push)
SELECT stage, operation, status, COUNT(*) FROM lft.beproduct.beproduct_to_dtc_sync_log
WHERE log_time > current_timestamp() - INTERVAL 1 HOUR GROUP BY 1,2,3;
```

---

## 5. Troubleshooting

| Issue | Fix |
|-------|-----|
| `ModuleNotFoundError: connectors.dtc` | `python scripts/upload_notebooks.py --modules-only` |
| `401 Unauthorized` (DTC) | `dtc_api_key_<env>` secret missing/expired |
| `401 / unauthorized_client` (BeProduct) | refresh token expired → update `refresh_token` secret |
| `NO_IN_SCOPE_REQUESTS` | no in-scope active request for that customer/env — check naming + `request_is_active` |
| `400` creating a request | handled — uses the validated `POST /v1/sheets` shape (see `docs/DTC_GUIDE.md`) |
| Image upload `400` | webp source → auto-transcoded to PNG (Phase 3) |
| Pushed field silently blanked | MultiSelect must be sent as array + a valid Master Data value (`docs/BEPRODUCT_GUIDE.md`) |

More: `docs/DTC_GUIDE.md`, `docs/BEPRODUCT_GUIDE.md`, `AGENTS.md`.
