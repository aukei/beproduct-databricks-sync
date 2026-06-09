# Phase 2: Bi-Directional Sync Workflow

**Status**: Implementation Complete ✅  
**Goal**: Enable ADB → DTC push by tracking row-level changes

---

## Overview

Phase 2 enables you to:
1. **Pull** data from DTC (Phase 1 ✅)
2. **Edit** the table in Databricks (users make changes)
3. **Detect** what changed (snapshots + diffs)
4. **Push** changes back to DTC
5. **Verify** changes synced correctly

---

## Workflow

### Step 1: Pull Data from DTC (Existing)

Run the standard pull notebook:

```bash
databricks runs submit \
  --notebook-task notebook_path=/Workspace/Repos/beproduct-sync/DTC/notebooks/pull_dtc_to_delta \
  --base-parameters '{
    "dtc_workspace_name": "Kontoor",
    "dtc_request_id": "69f076f0b7247a661226be9a",
    "dtc_environment": "uat",
    "target_catalog": "lft",
    "target_schema": "beproduct",
    "target_table": "dtc_master_chart_uat",
    "write_mode": "overwrite"
  }' \
  --existing-cluster-id <CLUSTER_ID>
```

**Output**: Data in `lft.beproduct.dtc_master_chart_uat` (247 rows × 114 columns)

---

### Step 2: Create Infrastructure Tables (One-time Setup)

Initialize the metadata and change log tables:

```bash
databricks runs submit \
  --notebook-task notebook_path=/Workspace/Repos/beproduct-sync/DTC/notebooks/01_create_sync_tables \
  --base-parameters '{
    "target_catalog": "lft",
    "target_schema": "beproduct",
    "target_table": "dtc_master_chart_uat"
  }' \
  --existing-cluster-id <CLUSTER_ID>
```

**Creates**:
- `lft.beproduct.dtc_sync_metadata_uat` — Snapshot baselines
- `lft.beproduct.dtc_master_chart_changes_uat` — Change audit trail

*Note: Run this once per environment (uat/prod). For prod, use `dtc_master_chart_prod`*

---

### Step 3: Create Initial Snapshot

After the first pull, establish a baseline:

```bash
databricks runs submit \
  --notebook-task notebook_path=/Workspace/Repos/beproduct-sync/DTC/notebooks/02_create_snapshot \
  --base-parameters '{
    "dtc_request_id": "69f076f0b7247a661226be9a",
    "dtc_environment": "uat",
    "target_catalog": "lft",
    "target_schema": "beproduct",
    "target_table": "dtc_master_chart_uat"
  }' \
  --existing-cluster-id <CLUSTER_ID>
```

**What it does**:
1. Reads all 247 rows from `dtc_master_chart_uat`
2. Calculates SHA256 hash of all row data
3. Stores hash in `dtc_sync_metadata_uat`

**Output in metadata table**:
```
request_id: 69f076f0b7247a661226be9a
environment: uat
sync_direction: pull
snapshot_hash: abc123def456...
row_count: 247
sync_timestamp: 2024-01-15 10:30:00
```

---

### Step 4: User Edits Data (In Databricks)

Users modify the table:

```sql
-- UPDATE a cell
UPDATE lft.beproduct.dtc_master_chart_uat
SET FOB_Price_USD = 2.99
WHERE row_id = 'a1b2c3d4-e5f6-4789-ab12-cd34ef567890';

-- INSERT new rows
INSERT INTO lft.beproduct.dtc_master_chart_uat
VALUES (...new_row_data...);

-- DELETE rows
DELETE FROM lft.beproduct.dtc_master_chart_uat
WHERE row_id = 'x9y0z1a2-b3c4-5d6e-f7g8-hij9klmnopqr';
```

---

### Step 5: Detect Changes

Run change detection to find all modifications since the snapshot:

```bash
databricks runs submit \
  --notebook-task notebook_path=/Workspace/Repos/beproduct-sync/DTC/notebooks/03_detect_changes \
  --base-parameters '{
    "dtc_request_id": "69f076f0b7247a661226be9a",
    "dtc_environment": "uat",
    "target_catalog": "lft",
    "target_schema": "beproduct",
    "target_table": "dtc_master_chart_uat"
  }' \
  --existing-cluster-id <CLUSTER_ID>
```

**What it does**:
1. Fetches current table state (247 rows)
2. Compares with last snapshot baseline
3. Detects:
   - **INSERT**: Rows not in snapshot
   - **UPDATE**: Rows with different values
   - **DELETE**: Rows in snapshot but removed
4. Stores changes in `dtc_master_chart_changes_uat`

**Output in changes table**:
```
change_id: uuid-1
row_id: a1b2c3d4
operation: UPDATE
columns_changed: {
  "FOB_Price_USD": {
    "old_value": "3.07",
    "new_value": "2.99"
  }
}
status: pending
detected_timestamp: 2024-01-15 14:00:00
```

---

### Step 6: Review & Push Changes

View pending changes:

```sql
SELECT
  change_id,
  row_id,
  operation,
  columns_changed,
  detected_timestamp
FROM lft.beproduct.dtc_master_chart_changes_uat
WHERE status = 'pending'
ORDER BY detected_timestamp ASC;
```

Push changes to DTC:

```bash
databricks runs submit \
  --notebook-task notebook_path=/Workspace/Repos/beproduct-sync/DTC/notebooks/04_push_changes \
  --base-parameters '{
    "dtc_workspace_name": "Kontoor",
    "dtc_request_id": "69f076f0b7247a661226be9a",
    "dtc_environment": "uat",
    "target_catalog": "lft",
    "target_schema": "beproduct",
    "target_table": "dtc_master_chart_uat"
  }' \
  --existing-cluster-id <CLUSTER_ID>
```

**What it does**:
1. Gets pending changes
2. For each change:
   - **INSERT**: `POST /v1/sheets/{sheetId}/rows`
   - **UPDATE**: `PATCH /v1/sheets/{sheetId}/rows/{rowId}`
   - **DELETE**: `DELETE /v1/sheets/{sheetId}/rows/{rowId}`
3. Updates change log with status:
   - `pushed` ✅ Success
   - `rejected` ❌ Error
   - `conflict` ⚠️ Conflict (not yet implemented)
4. **Creates new snapshot** if push successful

---

## Tables Reference

### 1. Data Table (Edited by Users)
**Table**: `lft.beproduct.dtc_master_chart_uat`

```sql
SELECT * FROM lft.beproduct.dtc_master_chart_uat
WHERE request_id = '69f076f0b7247a661226be9a'
LIMIT 1;

-- Columns:
-- - row_id (PK): Unique identifier from DTC
-- - [114 data columns]: Product info, prices, etc.
-- - request_id, document_name, request_status (metadata)
-- - sync_timestamp, fetched_at (sync tracking)
```

### 2. Metadata Table (Snapshots)
**Table**: `lft.beproduct.dtc_sync_metadata_uat`

```sql
SELECT * FROM lft.beproduct.dtc_sync_metadata_uat
WHERE request_id = '69f076f0b7247a661226be9a'
ORDER BY sync_timestamp DESC;

-- Columns:
-- - request_id (PK)
-- - sync_timestamp (PK)
-- - sync_direction: 'pull' or 'push'
-- - snapshot_hash: SHA256 of all row data
-- - row_count: Number of rows at snapshot time
-- - details: Additional metadata (JSON)
```

### 3. Change Log Table (Audit Trail)
**Table**: `lft.beproduct.dtc_master_chart_changes_uat`

```sql
SELECT * FROM lft.beproduct.dtc_master_chart_changes_uat
WHERE request_id = '69f076f0b7247a661226be9a'
AND status IN ('pending', 'pushed', 'rejected');

-- Columns:
-- - change_id (PK): UUID
-- - row_id: Which row changed
-- - operation: 'INSERT', 'UPDATE', or 'DELETE'
-- - columns_changed: Map of column → {old_value, new_value}
-- - status: 'pending', 'pushed', 'conflict', 'rejected'
-- - detected_timestamp: When change was detected
-- - push_timestamp: When pushed to DTC
-- - conflict_reason: If status='conflict'
```

---

## Environment-Aware Naming

Each environment has separate tables:

| Environment | Data Table | Metadata Table | Changes Table |
|------------|-----------|---------------|---------------|
| **UAT** | `dtc_master_chart_uat` | `dtc_sync_metadata_uat` | `dtc_master_chart_changes_uat` |
| **PROD** | `dtc_master_chart_prod` | `dtc_sync_metadata_prod` | `dtc_master_chart_changes_prod` |

This prevents accidental mixing of uat/prod data and changes.

---

## Change Status Lifecycle

```
pending
├─ → pushed (after successful push to DTC)
├─ → rejected (if push fails)
└─ → conflict (if detected during review - manual decision needed)

rejected (failed push)
└─ → pending (retry after fixing)

conflict (simultaneous edits in DTC & ADB)
├─ → pending (resolve conflict, retry)
└─ → rejected (manual decision to skip)
```

---

## Error Handling

### Issue: "No snapshot found for this request"

**Cause**: Change detection ran before creating initial snapshot

**Fix**: Run `02_create_snapshot` after pull:
```bash
databricks runs submit \
  --notebook-task notebook_path=/Workspace/Repos/beproduct-sync/DTC/notebooks/02_create_snapshot \
  --base-parameters '{...}'
```

### Issue: Push fails with "401 Unauthorized"

**Cause**: API key expired or invalid

**Fix**: Update secret:
```bash
databricks secrets put-secret beproduct dtc_api_key_uat \
  --string-value "NEW_KEY"
```

### Issue: Some changes marked as "rejected"

**Cause**: DTC API returned error (e.g., row locked, invalid data)

**Fix**: Review error in change log:
```sql
SELECT change_id, operation, row_id, conflict_reason
FROM lft.beproduct.dtc_master_chart_changes_uat
WHERE status = 'rejected'
AND request_id = '69f076f0b7247a661226be9a';
```

Fix the issue and retry push.

---

## Best Practices

### Daily Workflow

1. **Morning**: Run pull notebook (daily scheduled at 2am UTC)
2. **Throughout day**: Users edit data in Databricks
3. **Evening**: Run detect changes, review, and push
4. **Next morning**: New pull resets the baseline

### Scheduled Jobs (Recommended)

**Pull Job** (Daily 2am UTC):
```json
{
  "name": "DTC-Pull-UAT-Daily",
  "notebook_task": {
    "notebook_path": "/Workspace/Repos/beproduct-sync/DTC/notebooks/pull_dtc_to_delta",
    "base_parameters": {
      "dtc_workspace_name": "Kontoor",
      "dtc_request_id": "69f076f0b7247a661226be9a",
      "dtc_environment": "uat",
      "target_table": "dtc_master_chart_uat",
      "write_mode": "overwrite"
    }
  },
  "schedule": {
    "quartz_cron_expression": "0 2 * * * UTC",
    "timezone_id": "UTC"
  },
  "new_cluster": {...}
}
```

**Push Job** (On-demand or daily evening):
```json
{
  "name": "DTC-Push-UAT-Evening",
  "notebook_task": {
    "notebook_path": "/Workspace/Repos/beproduct-sync/DTC/notebooks/04_push_changes",
    "base_parameters": {
      "dtc_workspace_name": "Kontoor",
      "dtc_request_id": "69f076f0b7247a661226be9a",
      "dtc_environment": "uat",
      "target_table": "dtc_master_chart_uat"
    }
  },
  "schedule": {
    "quartz_cron_expression": "0 18 * * * UTC",
    "timezone_id": "UTC"
  },
  "new_cluster": {...}
}
```

### Data Quality Checks

Before pushing, verify data integrity:

```sql
-- Check for NULLs in critical columns
SELECT COUNT(*) as null_rows
FROM lft.beproduct.dtc_master_chart_uat
WHERE request_id IS NULL
  OR row_id IS NULL;

-- Verify no duplicate row_ids
SELECT row_id, COUNT(*) as cnt
FROM lft.beproduct.dtc_master_chart_uat
WHERE request_id = '69f076f0b7247a661226be9a'
GROUP BY row_id
HAVING cnt > 1;

-- Check pending changes before push
SELECT COUNT(*) as pending
FROM lft.beproduct.dtc_master_chart_changes_uat
WHERE status = 'pending';
```

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│                    DTC (Source)                         │
│            Request: 69f076f0b7247a661226be9a            │
│                  Sheet: 247 rows × 114 cols             │
└────────────────────────┬────────────────────────────────┘
                         │
                         │ Pull (Phase 1)
                         ↓
┌─────────────────────────────────────────────────────────┐
│          Databricks: dtc_master_chart_uat               │
│                  247 rows × 114 cols                    │
│  Users edit: UPDATE, INSERT, DELETE                     │
└────────────────────────┬────────────────────────────────┘
                         │
                         │ Create Snapshot
                         ↓
         ┌───────────────────────────────┐
         │ Snapshot A (SHA256 hash)      │
         │ dtc_sync_metadata_uat         │
         └───────────────────────────────┘
                         │
      ┌──────────────────┴──────────────────┐
      │ (Users edit data)                   │
      │ Rows change in dtc_master_chart_uat │
      │                                     │
      └─────────────────┬────────────────────┘
                        │
                        │ Detect Changes
                        ↓
         ┌───────────────────────────────┐
         │ Changes logged:               │
         │ - INSERT (new rows)           │
         │ - UPDATE (modified values)    │
         │ - DELETE (removed rows)       │
         │ dtc_master_chart_changes_uat  │
         └───────────────────┬───────────┘
                             │
                             │ Push Changes
                             ↓
         ┌───────────────────────────────────────┐
         │ DTC API Calls:                        │
         │ - POST /sheets/{sheetId}/rows         │
         │ - PATCH /sheets/{sheetId}/rows/{id}   │
         │ - DELETE /sheets/{sheetId}/rows/{id}  │
         └───────────────────┬───────────────────┘
                             │
                             ↓
┌─────────────────────────────────────────────────────────┐
│                    DTC (Updated)                        │
│            Request: 69f076f0b7247a661226be9a            │
│            Changes from Databricks applied              │
└─────────────────────────────────────────────────────────┘
```

---

## Next Steps (Phase 3)

- [ ] Conflict resolution automation (LWW strategy)
- [ ] Change approval workflow
- [ ] Audit reports and change history
- [ ] Bulk operations optimization
- [ ] Delta Lake versioning (time travel for snapshots)

