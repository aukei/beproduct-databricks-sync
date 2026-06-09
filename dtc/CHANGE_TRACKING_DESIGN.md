# Change Tracking Design for DTC Bi-directional Sync

**Status**: Design Document (Phase 2)  
**Goal**: Enable ADB → DTC push by tracking row-level changes

---

## Problem

Current state:
- ✅ **Pull (DTC → ADB)**: Overwrite table daily, simple
- ❌ **Push (ADB → DTC)**: Need to know which rows changed

Without change tracking:
- Can't do incremental push (would push entire 247 rows every time)
- Can't detect conflicts (same row modified in both DTC and ADB)
- Can't distinguish INSERT/UPDATE/DELETE operations

---

## Solution: Snapshot + Change Log Pattern

### Architecture

```
DTC Request (sheetId)
    ↓
[PULL] Create Snapshot A
    ↓
Delta Table: dtc_master_chart_uat
    ↓ (user edits rows)
    ↓
[COMPARE] Snapshot A vs Current State → Changes
    ↓
Change Log Table: dtc_master_chart_changes
    ├── operation: INSERT | UPDATE | DELETE
    ├── row_id: (primary key)
    ├── changes: {field: [old_value, new_value]}
    └── timestamp
    ↓
[PUSH] Apply changes back to DTC via PATCH
    ↓
DTC Request updated
    ↓
[PULL AGAIN] Create Snapshot B (new baseline)
```

---

## Implementation Steps

### Step 1: Metadata Table for Snapshots

Track sync timestamps and snapshot hashes:

```sql
CREATE TABLE IF NOT EXISTS lft.beproduct.dtc_sync_metadata (
  request_id STRING,
  environment STRING,
  sync_direction STRING,          -- 'pull' or 'push'
  sync_timestamp TIMESTAMP,
  snapshot_hash STRING,           -- Hash of all rows at sync time
  row_count INT,
  document_name STRING,
  details MAP<STRING, STRING>,
  PRIMARY KEY (request_id, sync_timestamp)
);
```

**Purpose**: Record baseline after each pull so we can detect changes.

### Step 2: Change Log Table

Track all modifications between syncs:

```sql
CREATE TABLE IF NOT EXISTS lft.beproduct.dtc_master_chart_changes (
  change_id STRING,               -- UUID
  request_id STRING,
  row_id STRING,                  -- DTC rowId (primary key)
  operation STRING,               -- 'INSERT', 'UPDATE', 'DELETE'
  detected_timestamp TIMESTAMP,    -- When change was detected
  columns_changed MAP<STRING, STRUCT<old_value: STRING, new_value: STRING>>,
  change_source STRING,           -- 'databricks' or 'dtc'
  status STRING,                  -- 'pending', 'pushed', 'conflict', 'rejected'
  push_timestamp TIMESTAMP,
  conflict_reason STRING,         -- If status='conflict'
  PRIMARY KEY (change_id)
);
```

**Example row**:
```json
{
  "change_id": "uuid-123",
  "request_id": "69f076f0b7247a661226be9a",
  "row_id": "e25849e3-f160-4617-b123-9d7c810599cf",
  "operation": "UPDATE",
  "columns_changed": {
    "Product_Status": {
      "old_value": "Production",
      "new_value": "Cancelled"
    },
    "FOB_Price_USD_yd_in_CW": {
      "old_value": "3.07",
      "new_value": "2.99"
    }
  },
  "status": "pending"
}
```

### Step 3: Snapshot Detection (on each pull)

After pulling from DTC, calculate baseline:

```python
def create_snapshot(df: pd.DataFrame, request_id: str, metadata: Dict) -> str:
    """
    Create a snapshot hash after pulling from DTC.
    
    This becomes the baseline for detecting future changes.
    """
    import hashlib
    
    # Sort by row_id to ensure consistent hashing
    df_sorted = df.sort_values('row_id')
    
    # Create a hash of all data (excluding metadata columns)
    data_cols = [c for c in df.columns if not c.startswith('sync_') 
                 and not c.startswith('document_') 
                 and c not in ['fetched_at', 'updated_at']]
    
    content = df_sorted[data_cols].to_csv(index=False)
    snapshot_hash = hashlib.sha256(content.encode()).hexdigest()
    
    # Store in metadata table
    metadata_record = {
        'request_id': request_id,
        'sync_direction': 'pull',
        'snapshot_hash': snapshot_hash,
        'row_count': len(df),
        'sync_timestamp': datetime.now(timezone.utc),
    }
    
    return snapshot_hash, metadata_record
```

### Step 4: Change Detection (before push)

Compare current state with last snapshot:

```python
def detect_changes(
    current_df: pd.DataFrame,
    previous_snapshot_hash: str,
    change_log_table: str
) -> pd.DataFrame:
    """
    Detect what changed since last sync.
    
    Returns DataFrame with columns:
    - row_id, operation, columns_changed
    """
    # Get previous snapshot from metadata
    previous_df = spark.sql(f"""
        SELECT * FROM {change_log_table}
        WHERE request_id = '{request_id}'
        AND status IN ('pushed', 'pending')
    """).toPandas()
    
    # Compare row by row
    changes = []
    
    for _, current_row in current_df.iterrows():
        row_id = current_row['row_id']
        prev_row = previous_df[previous_df['row_id'] == row_id]
        
        if prev_row.empty:
            # New row (INSERT)
            changes.append({
                'row_id': row_id,
                'operation': 'INSERT',
                'columns_changed': {col: current_row[col] for col in current_df.columns}
            })
        else:
            # Check for updates
            prev_row = prev_row.iloc[0]
            changed_cols = {}
            for col in current_df.columns:
                if current_row[col] != prev_row[col]:
                    changed_cols[col] = {
                        'old_value': str(prev_row[col]),
                        'new_value': str(current_row[col])
                    }
            
            if changed_cols:
                changes.append({
                    'row_id': row_id,
                    'operation': 'UPDATE',
                    'columns_changed': changed_cols
                })
    
    # Deleted rows
    current_row_ids = set(current_df['row_id'].unique())
    previous_row_ids = set(previous_df['row_id'].unique())
    
    for deleted_row_id in previous_row_ids - current_row_ids:
        changes.append({
            'row_id': deleted_row_id,
            'operation': 'DELETE'
        })
    
    return pd.DataFrame(changes)
```

### Step 5: Push Changes to DTC

Map Databricks changes to DTC PATCH operations:

```python
def push_changes_to_dtc(
    connector: DTCConnector,
    request_id: str,
    changes: pd.DataFrame
) -> Dict:
    """
    Push detected changes back to DTC.
    
    For each change:
    - INSERT: Use DTC API to create new row
    - UPDATE: Use PATCH endpoint to update row
    - DELETE: Use DELETE endpoint
    """
    results = {
        'inserted': 0,
        'updated': 0,
        'deleted': 0,
        'errors': []
    }
    
    for _, change in changes.iterrows():
        try:
            row_id = change['row_id']
            operation = change['operation']
            
            if operation == 'INSERT':
                # POST /v1/sheets/{sheetId}/rows
                connector.create_row(
                    sheet_id=sheet_id,
                    row_data=change['columns_changed']
                )
                results['inserted'] += 1
                
            elif operation == 'UPDATE':
                # PATCH /v1/sheets/{sheetId}/rows/{rowId}
                connector.update_row(
                    sheet_id=sheet_id,
                    row_id=row_id,
                    updates=change['columns_changed']
                )
                results['updated'] += 1
                
            elif operation == 'DELETE':
                # DELETE /v1/sheets/{sheetId}/rows/{rowId}
                connector.delete_row(
                    sheet_id=sheet_id,
                    row_id=row_id
                )
                results['deleted'] += 1
        
        except Exception as e:
            results['errors'].append({
                'row_id': row_id,
                'operation': operation,
                'error': str(e)
            })
    
    return results
```

---

## Conflict Resolution

When the same row is modified in both DTC and ADB between syncs:

### Strategy: Last-Write-Wins (LWW)

**Assumption**: Timestamp indicates which system won.

```
DTC_updated_at > ADB_sync_timestamp → Use DTC version
ADB_changed > DTC_updated_at → Use ADB version
```

### Log for Manual Review

If conflict detected:
1. **Mark in change log**: `status = 'conflict'`
2. **Store both versions**: Keep old_value (from DTC), new_value (from ADB)
3. **Alert user**: Email or Slack notification
4. **Manual resolution**: User decides which version to keep via UI/API

```sql
INSERT INTO dtc_master_chart_changes (
  row_id, operation, status, conflict_reason, columns_changed
)
VALUES (
  'row-123',
  'UPDATE',
  'conflict',
  'Row modified in both DTC (2026-05-29 10:00 UTC) and ADB (2026-05-29 14:30 UTC)',
  {...}
);
```

---

## Implementation Roadmap

### Phase 2a: Change Tracking Infrastructure
- [ ] Create metadata table (`dtc_sync_metadata`)
- [ ] Create change log table (`dtc_master_chart_changes`)
- [ ] Implement snapshot calculation
- [ ] Implement change detection algorithm

### Phase 2b: Push Logic
- [ ] Add PATCH, POST, DELETE methods to DTCConnector
- [ ] Implement push notebook (`push_changes_to_dtc.py`)
- [ ] Implement conflict detection
- [ ] Add retry + error handling

### Phase 2c: Monitoring
- [ ] Dashboard: Track push success rate
- [ ] Alerts: Notify on conflicts or failures
- [ ] Audit log: Maintain history of all pushes

### Phase 3: Advanced Features
- [ ] Real-time sync (webhook-based, not daily batches)
- [ ] Field-level ACLs (some fields read-only, others writable)
- [ ] Custom transformation rules (e.g., calculated fields)

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Snapshot Hash** | Detect *any* change, not just row count |
| **Row ID as PK** | DTC's unique identifier for each row |
| **Change Log Table** | Audit trail for compliance & debugging |
| **Last-Write-Wins** | Simple default; can be overridden for specific fields |
| **Pending Status** | Queue changes before push for validation |
| **Separate Metadata Table** | Cleaner schema, easy to query sync history |

---

## SQL Examples

### Query pending changes
```sql
SELECT 
  row_id, operation, columns_changed, detected_timestamp
FROM lft.beproduct.dtc_master_chart_changes
WHERE status = 'pending'
AND request_id = '69f076f0b7247a661226be9a'
ORDER BY detected_timestamp DESC;
```

### Check last sync
```sql
SELECT 
  request_id, sync_timestamp, row_count, snapshot_hash
FROM lft.beproduct.dtc_sync_metadata
WHERE sync_direction = 'pull'
ORDER BY sync_timestamp DESC
LIMIT 1;
```

### Monitor push status
```sql
SELECT 
  operation, status, COUNT(*) as count
FROM lft.beproduct.dtc_master_chart_changes
WHERE request_id = '69f076f0b7247a661226be9a'
GROUP BY operation, status;
```

---

## Testing Strategy

1. **Unit Tests**: Test change detection on known datasets
2. **Integration Tests**: Pull → Edit → Detect → Verify
3. **Conflict Tests**: Simultaneous edits in both systems
4. **Performance Tests**: 1000+ rows, detect changes in <5 seconds

---

## Future: Event-Driven Sync

Once Phase 2 is stable, upgrade to real-time:

- **Databricks**: Capture table change events (via DBX Event Log)
- **DTC**: Webhook for row changes → Databricks
- Result: Changes synced in seconds, not daily batches

---

**Next Steps**: Implement Phase 2a (metadata tables) first, then move to detection/push logic.
