# BeProduct Databricks Sync

Enterprise data synchronization platform for syncing BeProduct and DTC data to Databricks Delta Lake with universal change tracking and cross-platform integration.

## 🎯 Overview

Unified data platform with three core capabilities:

1. **BeProduct Sync** - STYLE master data and reference data (Materials, Colors, Blocks)
2. **DTC Sync** - Pull DTC WIP Request worksheets with change tracking
3. **Cross-Platform** - Automated BeProduct → DTC data flow with denormalization

### Key Features

✨ **Single Source of Truth** - One table per entity, serves all use cases  
📊 **Universal Change Tracking** - `last_modified` + `extracted` on all tables  
🔄 **Bi-Directional Sync** - BeProduct ↔ Delta Lake ↔ DTC  
🎨 **Colorways & BOM** - Extended STYLE data with arrays and materials  
🚀 **Auto-Denormalization** - Style × Color × BOM → Flat DTC rows  
⚡ **PATCH API** - Efficient change detection and push to DTC

---

## 📁 Repository Structure

```
beproduct-databricks-sync/
├── beproduct/                        # BeProduct sync notebooks
│   ├── beproduct_style_sync.py      # STYLE sync (enhanced)
│   ├── beproduct_style_push.py      # Push changes back
│   ├── beproduct_master_data_sync.py # Reference data
│   │
│   └── BeProduct → DTC Integration:
│       ├── beproduct_to_dtc_transform.py    # Denormalize
│       ├── dtc_request_manager.py           # Auto-create requests
│       └── beproduct_to_dtc_push.py         # Push via PATCH API
│
├── dtc/                              # DTC sync notebooks
│   ├── notebooks/
│   │   ├── pull_dtc_to_delta.py     # Pull DTC data
│   │   └── 00_init_season_mapping.py # Season mapping setup
│   ├── python/
│   │   └── connectors/
│   │       └── dtc.py               # DTC API connector
│   ├── CHANGE_TRACKING_DESIGN.md    # Change tracking design
│   └── PHASE2_WORKFLOW.md           # Future workflow
│
├── scripts/
│   ├── upload_notebooks.py          # Deploy notebooks to workspace
│   └── upload_to_databricks.py      # Upload data (SQLite → Delta)
│
├── docs/                             # Comprehensive documentation
│   ├── BEPRODUCT_TO_DTC_GUIDE.md    # Cross-platform integration
│   ├── BEPRODUCT_GUIDE.md           # BeProduct sync guide
│   ├── DTC_GUIDE.md                 # DTC sync guide
│   ├── ARCHITECTURE.md              # Architecture details
│   └── requirements.md              # Original requirements
│
├── README.md                         # This file
├── QUICK_START.md                   # Get started in 5 minutes
└── .env.example                     # Environment template
```

---

## 🚀 Quick Start

### 1. Configure Databricks Secrets (2 min)

```bash
# Create secret scope
databricks secrets create-scope --scope beproduct

# BeProduct OAuth
databricks secrets put --scope beproduct --key client_id
databricks secrets put --scope beproduct --key client_secret
databricks secrets put --scope beproduct --key refresh_token
databricks secrets put --scope beproduct --key company_domain

# DTC API keys
databricks secrets put --scope beproduct --key dtc_api_key_uat
databricks secrets put --scope beproduct --key dtc_api_key_prod
```

### 2. Deploy Notebooks (2 min)

```bash
# Install SDK
pip install databricks-sdk

# Configure .env file (one-time setup)
cp .env.example .env
# Edit .env and add your credentials:
#   DATABRICKS_HOST=https://adb-XXXXXXXX.azuredatabricks.net
#   DATABRICKS_PAT=dapi...

# Upload all notebooks (automatically reads .env)
python scripts/upload_notebooks.py
```

### 3. Run First Sync (1 min)

**BeProduct STYLE Sync:**
- Notebook: `/Workspace/Repos/beproduct-sync/beproduct/beproduct_style_sync`
- Parameters: `folder_name=KTB`, `refresh_mode=FULL`

**DTC Sync:**
- Notebook: `/Workspace/Repos/beproduct-sync/DTC/notebooks/pull_dtc_to_delta`
- Parameters: `dtc_request_id=<id>`, `dtc_environment=uat`

See **[QUICK_START.md](QUICK_START.md)** for detailed instructions.

---

## 📊 Architecture: Single Source of Truth

### Unified Data Model

```
lft.beproduct Schema (Single Source of Truth)
├─ BeProduct Tables
│  ├─ ktb_styles              [last_modified, extracted]
│  │  • Standard STYLE fields
│  │  • colorways_array (ARRAY<STRING>)
│  │  • bom_material_1, bom_material_2
│  │  • front_image_url
│  │
│  ├─ materials               [last_modified, extracted]
│  ├─ colors                  [last_modified, extracted]
│  └─ blocks                  [last_modified, extracted]
│
├─ DTC Tables
│  ├─ dtc_requests            [last_modified, extracted]
│  ├─ dtc_sheets              [last_modified, extracted]
│  └─ dtc_master_chart_uat    [last_modified, extracted]
│
└─ Integration Tables
   ├─ beproduct_to_dtc_staging
   ├─ dtc_request_mapping
   ├─ push_log
   └─ dtc_seasoncode_mapping     [CUSTOMER, BPSEASON, DTCCODE]
```

### Universal Change Tracking

Every source table includes:
- **`last_modified`** - From source system (for delta detection)
- **`extracted`** - When we pulled it (for audit trail)

### Data Flows

```
ORIGINAL WORKFLOWS (Keep Running):
──────────────────────────────────────────────────
BeProduct → Delta Lake     # Internal reporting, analytics
Delta Lake → BeProduct     # Bi-directional updates
DTC → Delta Lake          # Monitor DTC data

NEW WORKFLOW (BeProduct → DTC):
──────────────────────────────────────────────────
BeProduct → Delta Lake (with colorways/BOM)
    ↓
Transform (denormalize: Style × Color × BOM)
    ↓
Auto-create DTC requests/sheets
    ↓
Push to DTC via PATCH API
```

---

## 📚 Documentation

| Document | Description |
|----------|-------------|
| **[QUICK_START.md](QUICK_START.md)** | Get started in 5 minutes |
| **[docs/BEPRODUCT_GUIDE.md](docs/BEPRODUCT_GUIDE.md)** | BeProduct sync (STYLE, master data, push) |
| **[docs/DTC_GUIDE.md](docs/DTC_GUIDE.md)** | DTC sync (pull, change tracking) |
| **[docs/BEPRODUCT_TO_DTC_GUIDE.md](docs/BEPRODUCT_TO_DTC_GUIDE.md)** | Cross-platform integration (complete workflow) |
| **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** | Architecture details and implementation |
| **[docs/requirements.md](docs/requirements.md)** | Original requirements specification |

---

## 🎯 Common Use Cases

### Use Case 1: Daily STYLE Data Sync

**Objective:** Keep Delta Lake up-to-date with BeProduct STYLE changes

**Notebook:** `beproduct/beproduct_style_sync.py`  
**Schedule:** Daily at 11:00 UTC  
**Output:** `lft.beproduct.ktb_styles`

**Features:**
- Incremental sync (only changed records)
- Colorways array extraction
- BOM materials (2 lines per style)
- Front image URL
- Change tracking timestamps

---

### Use Case 2: Populate DTC WIP Requests

**Objective:** Automatically push BeProduct data to DTC requests

**Notebooks:**
1. `beproduct/beproduct_style_sync.py` (11:00 UTC)
2. `beproduct/beproduct_to_dtc_transform.py` (12:00 UTC)
3. `beproduct/dtc_request_manager.py` (12:30 UTC)
4. `beproduct/beproduct_to_dtc_push.py` (13:00 UTC)

**Output:** Denormalized rows in DTC WIP Requests (via PATCH API)

See **[docs/BEPRODUCT_TO_DTC_GUIDE.md](docs/BEPRODUCT_TO_DTC_GUIDE.md)** for complete workflow.

---

### Use Case 3: Monitor DTC Data

**Objective:** Pull DTC worksheet data for analysis

**Notebook:** `dtc/notebooks/pull_dtc_to_delta.py`  
**Schedule:** Daily at 02:00 UTC  
**Output:** `lft.beproduct.dtc_master_chart_uat`

**Features:**
- Change detection (INSERT/UPDATE/DELETE)
- Row-level tracking
- Historical snapshots

---

## 🛠️ Development

### Upload Notebooks

```bash
# Preview uploads (dry run)
python scripts/upload_notebooks.py --dry-run

# Upload all notebooks
python scripts/upload_notebooks.py

# Upload specific directory
python scripts/upload_notebooks.py --dir beproduct
```

### Run Tests

```bash
# DTC connector tests
pytest dtc/tests/

# Install test dependencies
pip install -r requirements-dev.txt
```

### Local Development

```bash
# Clone repository
git clone https://github.com/your-org/beproduct-databricks-sync.git
cd beproduct-databricks-sync

# Configure environment
cp .env.example .env
# Edit .env with your credentials

# Install dependencies
pip install -r requirements.txt
```

---

## 🔐 Security

- ✅ All credentials stored in Databricks secrets
- ✅ OAuth 2.0 for BeProduct API
- ✅ API keys for DTC (stored in secrets)
- ✅ No credentials in code or config files
- ✅ Environment-specific secrets (UAT/Production)

---

## 📊 Performance

### Typical Sync Times

| Operation | Records | Time |
|-----------|---------|------|
| BeProduct FULL sync | 50 styles | 30-60s |
| BeProduct INCREMENTAL | No changes | 10-15s |
| DTC pull | 247 rows × 114 cols | <1s |
| Transform (denormalize) | 50 styles → 200 rows | 5-10s |
| Push to DTC (PATCH) | 200 rows | 20-30s |

### Scaling Recommendations

- ✅ Use larger Databricks clusters for >1000 records
- ✅ Adjust batch sizes in push operations (default: 100)
- ✅ Enable auto-scaling for variable workloads
- ✅ Use INCREMENTAL mode for daily syncs

---

## 🤝 Contributing

1. Create feature branch: `git checkout -b feature/your-feature`
2. Make changes and test locally
3. Upload notebooks: `python scripts/upload_notebooks.py --dir your-dir`
4. Test in Databricks workspace
5. Commit and push: `git push origin feature/your-feature`
6. Create pull request

---

## 📝 License

[Your License Here]

---

## 🆘 Support

- **Issues:** Open GitHub issue with logs and error messages
- **Documentation:** See `docs/` folder for detailed guides
- **Quick Help:** Check `QUICK_START.md` for common tasks

---

## 🎯 Roadmap

### ✅ Phase 1: Core Integration (Complete)
- BeProduct STYLE sync with colorways/BOM
- DTC sync with change tracking
- Cross-platform BeProduct → DTC flow
- Universal change tracking (last_modified, extracted)

### 🚧 Phase 2: Enhancements (Planned)
- DTC → BeProduct reverse sync
- Image sync workflow
- Advanced change detection (UPDATE/DELETE)
- Multi-region support

### 💡 Phase 3: Advanced Features (Future)
- Real-time sync with change data capture
- Conflict resolution for bi-directional sync
- Data quality monitoring and alerts
- Performance optimization for large datasets

---

**Quick Links:**
- [Get Started](QUICK_START.md)
- [BeProduct Guide](docs/BEPRODUCT_GUIDE.md)
- [DTC Guide](docs/DTC_GUIDE.md)
- [Architecture](docs/ARCHITECTURE.md)
