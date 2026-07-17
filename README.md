# BeProduct ⇄ DTC Databricks Sync

Bi-directional synchronization between **BeProduct** (style PLM) and **DTC**
("Data Collab" sheets), staged through **Databricks / Delta** under Unity Catalog
schema `lft.beproduct`. Each field syncs **one way only** (no loops).

| Phase | Direction | Description |
|-------|-----------|-------------|
| **1** | BeProduct → DTC | Push style fields into the matching WIP request (upsert); create + share missing in-scope requests |
| **2** | DTC → BeProduct | Push DTC-owned fields back into the BeProduct style |
| **3** | BeProduct → DTC | Upload front image into the DTC "Style Image" cell (binary, separate endpoint) |
| **7** | BeProduct → DTC | Push sample-app submit history (all 6 apps: Proto/PreLine/SMS/Fit/PP/TOP) into DTC status columns |
| **8a** | DTC → Delta | Pull KTB FABRIC sheets (Adoption=Y rows) into `dtc_fabric_ktb` for Phase 8b |
| **8b** | Delta → BeProduct | *(planned)* Upsert adopted fabric rows into BeProduct Material Master |
| **9a** | DTC → Delta → Delta | Pull KTB LinePlan; join with WIP; build `costing_chart` (Style × Color × Vendor/Factory) |
| **9b** | API → Delta → DTC | *(planned)* NT Orbit Duty Tools API fill for HTS/Duty/Tariff fields; push changes back to WIP |

The whole pipeline runs as a **multi-task Databricks job** (`BeProduct_DTC_sync_dag`,
job 294837488757511), defined in `scripts/deploy_job.py`.

---

## Documentation

| Document | Description |
|----------|-------------|
| [QUICK_START.md](QUICK_START.md) | Setup, how to use, which notebook to run |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Components, full pipeline DAG, and ADB data model |
| [docs/PHASE1_WORKFLOW.md](docs/PHASE1_WORKFLOW.md) | BeProduct → DTC style field upsert (Phases 1 + 7 ride same push) |
| [docs/PHASE2_WORKFLOW.md](docs/PHASE2_WORKFLOW.md) | DTC → BeProduct pushback |
| [docs/PHASE3_WORKFLOW.md](docs/PHASE3_WORKFLOW.md) | BeProduct image → DTC "Style Image" |
| [docs/PHASE5_WORKFLOW.md](docs/PHASE5_WORKFLOW.md) | BeProduct Master Data & Directory sync (admin utility, not in DAG) |
| [docs/PHASE7_WORKFLOW.md](docs/PHASE7_WORKFLOW.md) | Sample-app submit history → DTC status columns |
| [docs/BEPRODUCT_GUIDE.md](docs/BEPRODUCT_GUIDE.md) | BeProduct SDK/API + BeProduct tables on ADB |
| [docs/DTC_GUIDE.md](docs/DTC_GUIDE.md) | DTC API + DTC tables on ADB |
| [docs/DIAGRAM.md](docs/DIAGRAM.md) | Pipeline data-flow Mermaid diagram (render locally — PNG/SVG not committed) |
| [docs/beproduct_style_interested_fields.txt](docs/beproduct_style_interested_fields.txt) | **Style field-mapping SSOT** (DTC column ⇄ BeProduct fieldId ⇄ direction) |
| [docs/beproduct_material_interested_fields.txt](docs/beproduct_material_interested_fields.txt) | **Material field-mapping SSOT** (DTC FABRIC ⇄ BeProduct Material Master) |
| [docs/costing_interested_fields.txt](docs/costing_interested_fields.txt) | **Costing chart field-mapping SSOT** (WIP × LinePlan → costing_chart) |
| [docs/PERFORMANCE.md](docs/PERFORMANCE.md) | Pipeline performance history and optimisations |
| [AGENTS.md](AGENTS.md) | Durable log of verified API behaviour, field directions & project invariants |

---

## Repository structure

```
beproduct/                              # BeProduct-side notebooks + cross-platform push
├── 00_init_style_app_registry.py       # Cache folder app IDs → beproduct_style_app_registry (on-demand)
├── beproduct_style_sync.py             # BeProduct API → ktb_styles (+ 6 sample-app arrays; Finalized filtered)
├── beproduct_master_data_sync.py       # Admin: pull/push MasterData (dropdowns) + Directory
├── beproduct_to_dtc_transform.py       # ktb_styles → beproduct_to_dtc_staging (denormalize; sample status UDFs)
├── dtc_request_manager.py              # Resolve / CREATE / SHARE DTC requests → dtc_request_mapping
├── beproduct_to_dtc_push.py            # Phase 1: BeProduct → DTC upsert + orphan marks
├── beproduct_to_dtc_images.py          # Phase 3: front image → DTC "Style Image"
├── dtc_share_requests.py               # Idempotent request-sharing backfill
└── orchestrate_sync.py                 # ⚠️ RETIRED — single-notebook fallback only

dtc/
├── notebooks/
│   ├── 00_init_request_registry.py     # Standalone WIP registry build/refresh
│   ├── 00_init_season_mapping.py       # Seed dtc_seasoncode_mapping
│   ├── pull_masters_to_delta.py        # Pull KTB WIP sheets → dtc_wip_ktb + registry (Step 3 / Step 7)
│   ├── pull_fabric_to_delta.py         # Phase 8a: pull KTB FABRIC sheets → dtc_fabric_ktb
│   ├── pull_lineplan_to_delta.py       # Phase 9a: pull KTB LinePlan → dtc_lineplan_ktb
│   ├── build_costing_chart.py          # Phase 9a: WIP × LinePlan join → costing_chart
│   └── 05_push_dtc_to_beproduct.py     # Phase 2: DTC → BeProduct pushback
├── python/                             # Importable modules (deployed as Workspace files)
│   ├── client/rest_client.py           # Generic REST client (retry, multipart)
│   ├── connectors/dtc.py               # DTC API connector
│   └── sync/
│       ├── phase1.py                   # BeProduct → DTC upsert core (pure-Python, unit-tested)
│       ├── phase2.py                   # DTC → BeProduct pushback core (pure-Python)
│       ├── phase3.py                   # Image upload planning + type classification (pure-Python)
│       ├── samples.py                  # Phase 7: sample-app submit formatter (pure-Python)
│       └── registry.py                 # Shared registry refresh (discover→enrich→merge)
└── tests/
    ├── test_phase1.py                  # Phase 1 core unit tests
    ├── test_phase2.py                  # Phase 2 core unit tests
    ├── test_phase3.py                  # Phase 3 image-upload unit tests
    └── test_samples.py                 # Phase 7 sample formatter unit tests

scripts/
├── upload_notebooks.py                 # Deploy notebooks + modules to the Databricks workspace
├── deploy_job.py                       # Create / reset BeProduct_DTC_sync_dag (21 job parameters)
└── check_dtc_view.py                   # DTC WIP_ITS_USE column readiness check (Phase 6 pending cols)

docs/                                   # This documentation set
standalone/beproduct_style_push.py      # Standalone Delta → BeProduct push-back (not in daily pipeline)
```

**Notebook vs module split (invariant):** notebooks can't run locally (Spark / `dbutils`).
Deterministic logic lives in `dtc/python/sync/*.py` (pure Python, unit-tested); notebooks are thin Spark/IO wrappers.

---

## Quick commands

```bash
# Unit tests
python3 dtc/tests/test_phase1.py
python3 dtc/tests/test_phase2.py
python3 dtc/tests/test_phase3.py
python3 dtc/tests/test_samples.py

# DTC view readiness check
python scripts/check_dtc_view.py

# Deploy
python scripts/upload_notebooks.py --dry-run
python scripts/upload_notebooks.py
python scripts/deploy_job.py --dry-run
python scripts/deploy_job.py --reset-existing 294837488757511
```
