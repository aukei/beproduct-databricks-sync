# BeProduct ⇄ DTC Databricks Sync

Bi-directional synchronization between **BeProduct** (style PLM) and **DTC**
("Data Collab" sheets), staged through **Databricks / Delta** under Unity Catalog
schema `lft.beproduct`. Each field syncs **one way only** (no loops).

- **Phase 1 — BeProduct → DTC:** push BeProduct-owned style fields into the
  matching DTC request (upsert), creating + sharing missing in-scope requests.
- **Phase 2 — DTC → BeProduct:** push DTC-owned fields back into the BeProduct style.
- **Phase 3 — BeProduct → DTC (image):** upload the front image into the DTC
  "Style Image" cell (binary, separate endpoint).

The whole pipeline runs as **one schedulable Databricks job**
(`beproduct/orchestrate_sync.py`).

---

## Documentation

| Document | Description |
|----------|-------------|
| [QUICK_START.md](QUICK_START.md) | Setup, how to use, which notebook to run |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Components, data flow, and the full ADB data model |
| [docs/PHASE1_WORKFLOW.md](docs/PHASE1_WORKFLOW.md) | BeProduct → DTC field upsert |
| [docs/PHASE2_WORKFLOW.md](docs/PHASE2_WORKFLOW.md) | DTC → BeProduct pushback |
| [docs/PHASE3_WORKFLOW.md](docs/PHASE3_WORKFLOW.md) | BeProduct image → DTC "Style Image" |
| [docs/BEPRODUCT_GUIDE.md](docs/BEPRODUCT_GUIDE.md) | BeProduct SDK/API used + BeProduct tables on ADB |
| [docs/DTC_GUIDE.md](docs/DTC_GUIDE.md) | DTC API used + DTC tables on ADB |
| [docs/beproduct_style_interested_fields.txt](docs/beproduct_style_interested_fields.txt) | **Field-mapping SSOT** (DTC column ⇄ BeProduct fieldId ⇄ direction) |
| [AGENTS.md](AGENTS.md) | Durable log of verified API behaviour & project invariants |
| [implement_prompts.txt](implement_prompts.txt) | Original requirements / spec braindump |

---

## Repository structure

```
beproduct/                         # BeProduct-side notebooks (+ cross-platform push)
├── beproduct_style_sync.py        # BeProduct API → lft.beproduct.ktb_styles
├── beproduct_master_data_sync.py  # Reference/master data → beproduct_master_*
├── beproduct_to_dtc_transform.py  # ktb_styles → beproduct_to_dtc_staging (denormalize)
├── dtc_request_manager.py         # Resolve / CREATE / SHARE DTC requests → dtc_request_mapping
├── beproduct_to_dtc_push.py       # Phase 1: BeProduct → DTC upsert + orphan marks
├── beproduct_to_dtc_images.py     # Phase 3: front image → DTC "Style Image"
├── dtc_share_requests.py          # Idempotent request-sharing backfill
└── orchestrate_sync.py            # One job: Phases 1+2+3 in order (schedulable)

dtc/
├── notebooks/
│   ├── 00_init_request_registry.py  # Standalone registry build/refresh
│   ├── 00_init_season_mapping.py    # Seed dtc_seasoncode_mapping
│   ├── pull_requests_to_delta.py    # DTC API → dtc_wip_<customer> (+ registry refresh)
│   └── 05_push_dtc_to_beproduct.py  # Phase 2: DTC → BeProduct pushback
├── python/                          # Importable modules (deployed as Workspace files)
│   ├── client/rest_client.py        # Generic REST client (retry, multipart)
│   ├── connectors/dtc.py            # DTC API connector
│   └── sync/{phase1,phase2,phase3,registry}.py   # Pure, unit-tested cores
├── tests/                           # Unit + live tests
└── DTC-api-2026-05-08.json / .pdf   # DTC API spec (Postman + description)

standalone/                          # Standalone utilities (not in the daily pipeline)
└── beproduct_style_push.py          # Generic Delta → BeProduct push-back
scripts/upload_notebooks.py          # Deploy notebooks + modules to the workspace
docs/                                # Documentation (see table above)
```

**Notebook vs module split:** notebooks can't run locally (Spark/`dbutils`).
Deterministic logic lives in `dtc/python/sync/*.py` (pure Python, unit-tested);
notebooks are thin Spark/IO wrappers. All HTTP lives in `connectors/dtc.py`.

---

## Quick start

```bash
# 1. Configure Databricks secrets (scope: beproduct)
databricks secrets create-scope beproduct
#   BeProduct OAuth: client_id, client_secret, refresh_token, company_domain
#   DTC keys:        dtc_api_key_uat, dtc_api_key_prod

# 2. Deploy notebooks + modules
pip install databricks-sdk
cp .env.example .env          # set DATABRICKS_HOST + DATABRICKS_PAT
python scripts/upload_notebooks.py

# 3. Run the full sync (Databricks job) — notebook:
#    /Workspace/Repos/beproduct-sync/beproduct/orchestrate_sync
#    params: dtc_environment=uat, dry_run=true (preview) → dry_run=false (apply)
```

See **[QUICK_START.md](QUICK_START.md)** for step-by-step instructions, individual
notebooks, and parameters.

---

## Data model (overview)

All tables live in `lft.beproduct`. Full schema in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

```
BeProduct source        Integration                     DTC
ktb_styles              beproduct_to_dtc_staging         dtc_request_registry   (control table)
beproduct_master_*      dtc_request_mapping              dtc_wip_<customer>     (pulled sheet rows)
                        dtc_seasoncode_mapping
                        beproduct_to_dtc_sync_log
                        dtc_to_beproduct_sync_log
```

---

## Development

```bash
# Deploy
python scripts/upload_notebooks.py --dry-run        # preview
python scripts/upload_notebooks.py                  # notebooks + modules
python scripts/upload_notebooks.py --modules-only   # just dtc/python modules

# Tests (pure cores; no Spark/network)
python3 dtc/tests/test_phase1.py
python3 dtc/tests/test_phase2.py
python3 dtc/tests/test_phase3.py
python3 dtc/tests/test_phase1_live.py   # live reversible DTC write (needs UAT)
```

---

## Security

All credentials live in the Databricks secret scope `beproduct` (BeProduct OAuth +
DTC `dtc_api_key_<env>`). No credentials in code/config. Local deploy uses `.env`
(`DATABRICKS_HOST`, `DATABRICKS_PAT`).
