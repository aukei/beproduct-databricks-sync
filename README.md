# BeProduct ⇄ DTC Databricks Sync

Bi-directional synchronization between **BeProduct** (style PLM) and **DTC**
("Data Collab" sheets), staged through **Databricks / Delta** under Unity Catalog
schema `lft.beproduct`. Each field syncs **one way only** (no loops).

> **v2 is live (since 2026-09-15).** One serverless job writes each DTC request
> at most once per run, every 2 hours. v1 is paused and kept only for rollback.
> Design record: [docs/MIGRATION_V1_V2.md](docs/MIGRATION_V1_V2.md).

## Something is missing or wrong?

Open **[docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)**. It has runbooks for:

- a missing DTC WIP record
- a field not pushed to BeProduct
- no costing chart line
- a missing duty rate
- an outdated tariff
- job failures

Every runbook needs at most two other documents:
[docs/PIPELINE.md](docs/PIPELINE.md) and
[docs/SYNC_CONTRACT.md](docs/SYNC_CONTRACT.md).

## What it does

| Stage | Direction | Description |
|---|---|---|
| **00** | DTC → BeProduct | DTC "XTS Master" (Supplier/Factory) → `beproduct_directory` → BeProduct Directory |
| **10** | pulls | BeProduct styles + sample apps → `ktb_styles`; DTC WIP → `dtc_wip_ktb`; DTC LinePlan → `dtc_lineplan_ktb` |
| **20 / 20b** | → Delta | Style × colour staging; techpack BOM from Lakebase → `bom_segments` |
| **25** | → DTC | Resolve / create / share the DTC requests staging needs |
| **30** | Delta → Delta | Staging + BOM + WIP + LinePlan → `costing_chart`, duty filled from the NT Orbit cache |
| **40** | → DTC | **The single DTC write window**: style fields, sample history, BOM material fields and duty/tariff |
| **45** | → DTC | Style Image upload (multipart endpoint; runs right after 40) |
| **50** | DTC → BeProduct | Vendor, factory, customer factory ID, COO, Lot# |
| **55** | DTC → BeProduct | Customer material code → material master (**disabled**) |

| Job | ID | Schedule (HKT) | Status |
|---|---|---|---|
| `BeProduct_DTC_sync_v2` | 367710575109755 | every 2 h at :05, odd hours | **live**, serverless |
| `BeProduct_DTC_sync_duty_compute` | 1026599988408090 | 10:00, 15:00 | **live**, serverless. NT Orbit → cache + `costing_chart`, no DTC contact |
| `BeProduct_DTC_sync_dag` | 294837488757511 | — | v1, **paused** (rollback only) |
| `BeProduct_DTC_sync_images` | 847087837807970 | — | v1, **paused** (superseded by Stage 45) |

`folder_name` is still **`TEST KTB`** until go-live.

---

## Documentation

**Operate and troubleshoot**

| Document | Description |
|----------|-------------|
| [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Symptom → checks → cause → fix |
| [docs/PIPELINE.md](docs/PIPELINE.md) | What runs, in what order, and every gate a row must pass |
| [docs/SYNC_CONTRACT.md](docs/SYNC_CONTRACT.md) | Which field goes which way, exact DTC column names, keys, the PATCH allow-list |
| [QUICK_START.md](QUICK_START.md) | Setup, deploy, run, common queries |

**Reference**

| Document | Description |
|----------|-------------|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Systems, repository layout, ADB data model |
| [docs/DTC_GUIDE.md](docs/DTC_GUIDE.md) | DTC API + DTC tables on ADB |
| [docs/BEPRODUCT_GUIDE.md](docs/BEPRODUCT_GUIDE.md) | BeProduct SDK/API + BeProduct tables on ADB |
| [docs/MIGRATION_V1_V2.md](docs/MIGRATION_V1_V2.md) | Why v2 exists, rollout, rollback, remaining go-live items |
| [docs/PERFORMANCE.md](docs/PERFORMANCE.md) | Performance history (v1-era measurements) |
| [AGENTS.md](AGENTS.md) | Append-only log of verified API behaviour and decisions (for maintainers and coding agents) |
| [docs/v1/](docs/v1/) | Archived v1 phase-numbered documents |

**Field-level SSOTs** (fieldIds, JSONPaths, raw column names)

| Document | Description |
|----------|-------------|
| [docs/beproduct_style_interested_fields.txt](docs/beproduct_style_interested_fields.txt) | **Style**: DTC column ⇄ BeProduct fieldId |
| [docs/costing_interested_fields.txt](docs/costing_interested_fields.txt) | **Costing chart** columns |
| [docs/beproduct_directory_xts_interested_fields.txt](docs/beproduct_directory_xts_interested_fields.txt) | **Directory / XTS**: Stage 00 |
| [docs/beproduct_material_interested_fields.txt](docs/beproduct_material_interested_fields.txt) | **Material master**: Stage 55 target (its Phase 8 part is retired) |

---

## Repository structure

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) § 2.

**Notebook vs module split (invariant):** notebooks can't run locally (Spark /
`dbutils`). Deterministic logic lives in `dtc/python/sync/*.py` (pure Python,
unit-tested); notebooks are thin Spark/IO wrappers around it.

---

## Quick commands

```bash
# Unit tests (pure Python, no Spark or network)
for t in dtc/tests/test_{phase1,phase2,phase3,samples,bom,bom_push,duty,wip_plan,lifecycle,registry,xts_master,dtc_connector,entra_auth}.py; do python3 "$t" || break; done

# Run the job with no DTC / BeProduct writes (Delta is still rebuilt), collecting every task's exit JSON
python scripts/run_v2_job.py --job-id 367710575109755 dry_run=true
python scripts/run_v2_task.py v2_wip_push dry_run=true            # one notebook, one-off

# Deploy notebooks + modules to the v2 workspace root
python scripts/upload_notebooks.py --root /Workspace/Repos/beproduct-sync-v2

# Preview / update the jobs
python scripts/deploy_job.py --job v2 --dry-run
python scripts/deploy_job.py --job v2 --reset-existing 367710575109755
python scripts/deploy_job.py --job duty_compute --reset-existing 1026599988408090
```
