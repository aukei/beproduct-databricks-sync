# Quick Start

Setup, how to use, and which notebook to run for the BeProduct ⇄ DTC sync.

> Something missing or wrong: [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md).
> What runs and in what order: [docs/PIPELINE.md](docs/PIPELINE.md).
> Field directions and keys: [docs/SYNC_CONTRACT.md](docs/SYNC_CONTRACT.md).
> Concepts & data model: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

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

# NT Orbit (duty_compute) -- Entra delegated OAuth; refresh token seeded by
# scripts/nt_orbit_oauth_setup.py (see docs/TROUBLESHOOTING.md 4.3)
databricks secrets put-secret beproduct nt_orbit_tenant_id
databricks secrets put-secret beproduct nt_orbit_client_id
databricks secrets put-secret beproduct nt_orbit_client_secret
databricks secrets put-secret beproduct nt_orbit_refresh_token
```

### 1b. Deploy notebooks + modules

```bash
pip install databricks-sdk
cp .env.example .env
# Edit .env:
#   DATABRICKS_HOST=https://adb-XXXXXXXX.azuredatabricks.net
#   DATABRICKS_PAT=dapi...

R=/Workspace/Repos/beproduct-sync-v2
python scripts/upload_notebooks.py --root $R --dry-run   # preview only
python scripts/upload_notebooks.py --root $R             # notebooks + modules
python scripts/upload_notebooks.py --root $R --modules-only
```

v2 notebooks deploy under `/Workspace/Repos/beproduct-sync-v2/…` and import
modules from `/Workspace/Repos/beproduct-sync-v2/DTC/python` (the job parameter
`module_path`). The script's default root is the **v1** root
(`/Workspace/Repos/beproduct-sync`) — always pass `--root` for v2.

### 1c. One-time: seed the season-code mapping

```
Notebook: /Workspace/Repos/beproduct-sync-v2/DTC/notebooks/00_init_season_mapping
```
Then insert prefixes (year is algorithmic — last 2 digits of the BeProduct year).
A BeProduct season with no row here makes `transform` fail:
```sql
INSERT INTO lft.beproduct.dtc_seasoncode_mapping (CUSTOMER, BPSEASON, DTCCODE) VALUES
  ('KTB','SPRING','SS'), ('KTB','FALL','FW');
```

---

## 2. How to use — the scheduled jobs

Production is two jobs (full detail: [docs/PIPELINE.md](docs/PIPELINE.md)):

| Job | ID | Schedule (HKT) |
|---|---|---|
| `BeProduct_DTC_sync_v2` | 367710575109755 | every 2 h at :05 on odd hours |
| `BeProduct_DTC_sync_duty_compute` | 1026599988408090 | 10:00 and 15:00 |

Key job parameters (defaults in `scripts/deploy_job.py` → `JOB_PARAMS`):

| Parameter | Default | Notes |
|-----------|---------|-------|
| `dtc_environment` | `uat` | `uat` or `prod` |
| `dry_run` | `false` | `true` = compute + log, **no writes** |
| `folder_name` | `TEST KTB` | BeProduct folder; switch to `KTB` at go-live |
| `run_phase0`, `run_bom`, `run_costing`, `run_wip_push`, `run_duty_push`, `run_phase3`, `run_phase2` | `true` | per-stage switches, read inside each notebook (a disabled stage still shows SUCCESS) |
| `run_customer_code_push` | `false` | Stage 55, disabled until its resolver is proven |
| `force_refresh_duty` | `false` | re-query NT Orbit and overwrite; see TROUBLESHOOTING runbook 5 |
| `refresh_mode` | `FULL` | keep FULL: sample-app edits don't bump `style.modifiedAt` |

**Ad-hoc runs** (from a machine with `.env`):

```bash
# Whole job, one-off override, collecting every task's exit JSON
python scripts/run_v2_job.py --job-id 367710575109755 dry_run=true
# One notebook as a one-off serverless run (dry_run defaults to the notebook's own default: true)
python scripts/run_v2_task.py v2_wip_push dry_run=true
```

Serverless runs return **no stdout** through the API — only each notebook's exit
value, which these scripts print. In the Databricks UI, a task's cell output is
still visible.

**Deploying job changes:**

```bash
python scripts/deploy_job.py --job v2 --dry-run                           # preview the graph
python scripts/deploy_job.py --job v2 --reset-existing 367710575109755
python scripts/deploy_job.py --job duty_compute --reset-existing 1026599988408090
```

**Rollback** (see [docs/MIGRATION_V1_V2.md](docs/MIGRATION_V1_V2.md)): pause
`BeProduct_DTC_sync_v2`, unpause `BeProduct_DTC_sync_dag` and
`BeProduct_DTC_sync_images`.

---

## 3. On-demand notebooks (not in any job)

- `dtc/notebooks/00_init_season_mapping` — seed `dtc_seasoncode_mapping`.
- `dtc/notebooks/00_init_request_registry` — standalone WIP registry build/refresh.
- `beproduct/00_init_style_app_registry` — re-cache folder sample-app IDs (after BeProduct app setup changes).
- `beproduct/p5utl_beproduct_master_data_sync` — **admin-only.** Modes
  `PULL_ONLY` (refresh `beproduct_master_*`, incl. `beproduct_master_coo` used by
  Stage 50), `PUSH_MASTER_DATA`, `PUSH_DIRECTORY`, `PUSH_ONLY`. Use `dry_run=true` first.
- `beproduct/p1utl_dtc_share_requests` — idempotently (re-)share existing requests.
- `dtc/notebooks/v2_inspect_requests` — read-only: every DTC request and whether it is in scope.
- `dtc/notebooks/v2_set_dtc_cell` — set one DTC cell safely (matched + read back).
- `scripts/check_dtc_view.py` — DTC `WIP_ITS_USE` column check.

---

## 4. Common queries

```sql
-- BeProduct styles freshness
SELECT MAX(last_modified) latest, MAX(extracted) last_sync, COUNT(*) FROM lft.beproduct.ktb_styles;

-- Staging push status
SELECT sync_status, COUNT(*) FROM lft.beproduct.beproduct_to_dtc_staging GROUP BY sync_status;

-- Pulled DTC WIP rows
SELECT request_reference, COUNT(*) FROM lft.beproduct.dtc_wip_ktb GROUP BY request_reference;

-- Costing chart summary (fully rebuilt every main run; recover with RESTORE ... VERSION AS OF)
SELECT supplier_type, COUNT(*) rows, COUNT(hts_code) with_hts, COUNT(tariff_rate) with_tariff
FROM lft.beproduct.costing_chart GROUP BY supplier_type;

-- NT Orbit cache (never wiped; watch looked_up_at, not the row count)
SELECT COUNT(*), MAX(looked_up_at) FROM lft.beproduct.nt_orbit_duty_cache;

-- Sync logs, last 2 hours
SELECT stage, operation, status, reason, COUNT(*) FROM lft.beproduct.beproduct_to_dtc_sync_log
WHERE log_time > current_timestamp() - INTERVAL 2 HOURS GROUP BY ALL ORDER BY 5 DESC;
SELECT operation, status, reason, COUNT(*) FROM lft.beproduct.dtc_to_beproduct_sync_log
WHERE log_time > current_timestamp() - INTERVAL 2 HOURS GROUP BY ALL ORDER BY 4 DESC;
```

---

## 5. Troubleshooting

See [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) — symptom runbooks
(missing record, field not pushed, no costing line, missing / outdated duty) and
a table of job-level errors.
