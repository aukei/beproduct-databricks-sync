# BeProduct ⇄ DTC Databricks Sync

Bi-directional synchronization between **BeProduct** (style PLM) and **DTC**
("Data Collab" sheets), staged through **Databricks / Delta** under Unity Catalog
schema `lft.beproduct`. Each field syncs **one way only** (no loops).

> **Branch `v2`.** The pipeline is being consolidated: the three separate DTC
> write passes merge into one, eliminating two full re-pulls and reducing DTC
> lock contention from 3 write windows per request per run to 1. The v1 job keeps
> running untouched until cutover. Start with
> [docs/MIGRATION_V1_V2.md](docs/MIGRATION_V1_V2.md).

## What it does

| Stage | Direction | Description |
|---|---|---|
| **00** | DTC → Delta → BeProduct | Pull DTC "XTS Master" (Supplier/Factory) → upsert `beproduct_directory` → push to BeProduct |
| **10** | pulls | BeProduct styles + sample apps → `ktb_styles`; DTC WIP → `dtc_wip_ktb`; DTC LinePlan → `dtc_lineplan_ktb` |
| **20** | Lakebase + Delta → Delta | Denormalize to **style × color × material**, joining externally-processed techpack BOM data |
| **25** | Delta → DTC | Resolve / create / share the DTC requests the staging rows need |
| **30** | Delta → Delta | staging × WIP × LinePlan → `costing_chart`, with duty fields filled from the persistent NT Orbit cache |
| **40** | Delta → DTC | **The single DTC write window** — style fields, sample history, BOM material fields and duty rates in ≤2 PATCH calls per request |
| **50** | DTC → BeProduct | Push DTC-owned fields back into the BeProduct style |

Companion jobs, deliberately outside the main DAG:

| Job | ID | Contents |
|---|---|---|
| `BeProduct_DTC_sync_v2` | *(create with `--job v2`)* | The main DAG above. Serverless |
| `BeProduct_DTC_sync_duty_compute` | 1026599988408090 | NT Orbit duty lookups → `costing_chart` only; zero DTC contact |
| `BeProduct_DTC_sync_images` | 847087837807970 | Front image → DTC "Style Image" (multipart endpoint; cannot ride the PATCH) |
| `BeProduct_DTC_sync_dag` | 294837488757511 | **v1 main job** — still live until v2 cutover |

Each field syncs **one way only** (no loops). Retired: Phase 8a/8b (DTC FABRIC →
BeProduct Material Master), superseded by a separate "MaterialLib" application.

---

## Documentation

**Start here**

| Document | Description |
|----------|-------------|
| [docs/MIGRATION_V1_V2.md](docs/MIGRATION_V1_V2.md) | Why v2 exists, what changed, rollout and rollback |
| [docs/PIPELINE.md](docs/PIPELINE.md) | What runs, in what order, and every gate a row must pass |
| [docs/SYNC_CONTRACT.md](docs/SYNC_CONTRACT.md) | Which field goes which way, match keys, the WIP PATCH allow-list |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Systems, repository layout, ADB data model |

**Reference**

| Document | Description |
|----------|-------------|
| [QUICK_START.md](QUICK_START.md) | Setup, how to use, which notebook to run |
| [docs/BEPRODUCT_GUIDE.md](docs/BEPRODUCT_GUIDE.md) | BeProduct SDK/API + BeProduct tables on ADB |
| [docs/DTC_GUIDE.md](docs/DTC_GUIDE.md) | DTC API + DTC tables on ADB |
| [docs/PERFORMANCE.md](docs/PERFORMANCE.md) | Performance history and optimisations (v1-era measurements) |
| [AGENTS.md](AGENTS.md) | Durable log of verified API behaviour, field directions, decisions |
| [docs/v1/](docs/v1/) | Archived v1 phase-numbered documents — still the historical record |

**Field-mapping SSOTs**

| Document | Description |
|----------|-------------|
| [docs/beproduct_style_interested_fields.txt](docs/beproduct_style_interested_fields.txt) | **Style** — DTC column ⇄ BeProduct fieldId ⇄ direction |
| [docs/costing_interested_fields.txt](docs/costing_interested_fields.txt) | **Costing chart** — WIP × LinePlan → `costing_chart` |
| [docs/beproduct_directory_xts_interested_fields.txt](docs/beproduct_directory_xts_interested_fields.txt) | **Directory/XTS** — Stage 00 |
| [docs/beproduct_material_interested_fields.txt](docs/beproduct_material_interested_fields.txt) | SUPERSEDED (Phase 8a/8b retired) — replaced by "MaterialLib" |

---

## Repository structure

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) § 2 — the annotated tree lives
there so it only has to be kept accurate in one place.

**Notebook vs module split (invariant):** notebooks can't run locally (Spark /
`dbutils`). Deterministic logic lives in `dtc/python/sync/*.py` (pure Python,
unit-tested); notebooks are thin Spark/IO wrappers around it.

---

## Quick commands

```bash
# Unit tests (pure Python, no Spark or network)
python3 dtc/tests/test_phase1.py
python3 dtc/tests/test_phase2.py
python3 dtc/tests/test_phase3.py
python3 dtc/tests/test_samples.py
python3 dtc/tests/test_bom.py
python3 dtc/tests/test_duty.py

# DTC view readiness check
python scripts/check_dtc_view.py

# Deploy notebooks + modules
python scripts/upload_notebooks.py                                       # v1 workspace root
python scripts/upload_notebooks.py --root /Workspace/Repos/beproduct-sync-v2   # v2

# Preview / deploy jobs
python scripts/deploy_job.py --job v2 --dry-run                          # preview the v2 DAG
python scripts/deploy_job.py --job v2 --no-schedule                      # create BeProduct_DTC_sync_v2
python scripts/deploy_job.py --job main --reset-existing 294837488757511  # update the live v1 job
```
