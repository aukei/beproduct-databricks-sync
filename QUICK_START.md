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

| # | Notebook | Purpose | Output |
|---|----------|---------|--------|
| 1 | `beproduct/beproduct_style_sync` | Pull styles (`folder_name=KTB`, `refresh_mode=FULL`) | `ktb_styles` |
| 2 | `beproduct/beproduct_to_dtc_transform` | Denormalize style × color | `beproduct_to_dtc_staging` |
| 3 | `dtc/notebooks/pull_requests_to_delta` | Pull DTC `WIP_ITS_USE` rows (+ refresh registry) | `dtc_wip_<customer>` |
| 4 | `beproduct/dtc_request_manager` | Resolve / **create** / **share** requests (`dry_run=false` to create) | `dtc_request_mapping` |
| 5 | `beproduct/beproduct_to_dtc_push` | **Phase 1** upsert (`dry_run`, `delta_only`) | DTC sheets |
| 6 | `dtc/notebooks/05_push_dtc_to_beproduct` | **Phase 2** pushback (`push_blanks=false`) | BeProduct |
| 7 | `dtc/notebooks/pull_requests_to_delta` | Refresh dtc_wip after Phase 1 inserts | `dtc_wip_<customer>` |
| 8 | `beproduct/beproduct_to_dtc_images` | **Phase 3** image upload (`dry_run`, `max_uploads`) | DTC "Style Image" |

Other notebooks:
- `dtc/notebooks/00_init_request_registry` — standalone registry build/refresh
  (first build or targeted `request_ids`).
- `beproduct/beproduct_master_data_sync` — **admin-only, not in DAG.** Pull and/or
  push-back BeProduct MasterData (dropdown choices) and Directory (vendors/factories/
  contacts). Modes: `PULL_ONLY` (default), `PUSH_MASTER_DATA`, `PUSH_DIRECTORY`,
  `PUSH_ALL`. Use `dry_run=true` to preview push changes before committing.
  Writes `beproduct_master_*` (11 tables), `beproduct_directory`,
  `beproduct_directory_contacts`. Typical workflow: run PULL_ONLY, edit rows in
  Databricks SQL, run PUSH_* with `dry_run=true`, then `dry_run=false`.
- `beproduct/dtc_share_requests` — idempotently (re-)share existing requests
  (all views → `aiagentwip@lifung.com`; Full Version → `Fabric Group`).
- `standalone/beproduct_style_push` — standalone Delta → BeProduct push-back
  (see `standalone/README.md`; not auto-deployed by `upload_notebooks.py`).

---

## 4. Common queries

```sql
-- BeProduct styles freshness
SELECT MAX(last_modified) latest, MAX(extracted) last_sync, COUNT(*) FROM lft.beproduct.ktb_styles;

-- Staging push status
SELECT sync_status, COUNT(*) FROM lft.beproduct.beproduct_to_dtc_staging GROUP BY sync_status;

-- Resolved DTC requests this run
SELECT dtc_request_name, request_id, sheet_id FROM lft.beproduct.dtc_request_mapping WHERE environment='uat';

-- Pulled DTC rows
SELECT request_reference, COUNT(*) FROM lft.beproduct.dtc_wip_ktb GROUP BY request_reference;
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
