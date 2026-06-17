# DTC integration

DTC ("Data Collab") sync: pull in-scope DTC WIP requests to Databricks Delta and
push BeProduct data to / from them.

## Layout

```
dtc/
├── notebooks/
│   ├── 00_init_request_registry.py   # Build/refresh the request registry (control table)
│   ├── 00_init_season_mapping.py     # Seed dtc_seasoncode_mapping [CUSTOMER, BPSEASON, DTCCODE]
│   ├── pull_requests_to_delta.py     # Pull WIP_ITS_USE view → lft.beproduct.dtc_wip_<customer>
│   └── 05_push_dtc_to_beproduct.py   # Phase 2 pushback (DTC → BeProduct)
├── python/
│   ├── connectors/dtc.py             # DTC REST connector
│   └── sync/
│       ├── phase1.py                 # BeProduct → DTC core (pure-Python, unit-tested)
│       └── phase2.py                 # DTC → BeProduct core (pure-Python, unit-tested)
├── tests/                            # phase1/phase2 + connector tests
├── DATA_MODEL.md                     # Tables, keys, dtc_wip schema, season mapping
├── PHASE1_WORKFLOW.md                # BeProduct → DTC
└── PHASE2_WORKFLOW.md                # DTC → BeProduct
```

## Where to start

- **Operational guide:** `../docs/DTC_GUIDE.md`
- **Data model / table schemas:** `DATA_MODEL.md`
- **Forward flow (BeProduct → DTC):** `PHASE1_WORKFLOW.md`
- **Reverse flow (DTC → BeProduct):** `PHASE2_WORKFLOW.md`
- **Invariants & verified API behaviour:** `../AGENTS.md`

## Tests

```bash
python3 tests/test_phase1.py        # BeProduct → DTC core
python3 tests/test_phase2.py        # DTC → BeProduct core
python3 tests/test_phase1_live.py   # live reversible DTC write (needs UAT)
```
