# Databricks BeProduct ETL - Quick Reference

## Common Operations

### Setup (One-Time)

```bash
# Create secret scope
databricks secrets create-scope --scope beproduct

# Add secrets
databricks secrets put --scope beproduct --key client_id --string-value "..."
databricks secrets put --scope beproduct --key client_secret --string-value "..."
databricks secrets put --scope beproduct --key refresh_token --string-value "..."
databricks secrets put --scope beproduct --key company_domain --string-value "..."

# Upload notebooks
databricks workspace import-dir notebooks/inbound /Workspace/beproduct/notebooks/inbound
databricks workspace import-dir notebooks/outbound /Workspace/beproduct/notebooks/outbound
```

### Monitor Sync Status

```sql
-- Check last sync for all masters
SELECT 
    'styles' as master, MAX(synced_at) as last_sync, COUNT(*) as total_records
FROM main.beproduct.bp_styles
UNION ALL SELECT 'materials', MAX(synced_at), COUNT(*) FROM main.beproduct.bp_materials
UNION ALL SELECT 'colors', MAX(synced_at), COUNT(*) FROM main.beproduct.bp_colors
UNION ALL SELECT 'images', MAX(synced_at), COUNT(*) FROM main.beproduct.bp_images
UNION ALL SELECT 'blocks', MAX(synced_at), COUNT(*) FROM main.beproduct.bp_blocks
UNION ALL SELECT 'directory', MAX(synced_at), COUNT(*) FROM main.beproduct.bp_directory
UNION ALL SELECT 'users', MAX(synced_at), COUNT(*) FROM main.beproduct.bp_users
ORDER BY last_sync DESC;
```

### Manually Trigger Sync

```bash
# List job IDs
databricks jobs list

# Run specific job
databricks jobs run-now --job-id <job_id>

# Check run status
databricks runs get --run-id <run_id>

# Get run output
databricks runs get-output --run-id <run_id>
```

### Test Dry-Run Push

```bash
# Trigger push job with dry_run=true (default)
databricks jobs run-now --job-id <push_job_id>

# Check output
databricks runs get-output --run-id <run_id>
```

### Enable Live Push (After Testing)

```bash
# Update job to use dry_run=false
databricks jobs update \
  --job-id <push_job_id> \
  --json '{"base_parameters": {"dry_run": "false", ...}}'

# OR use Databricks UI: Jobs > Edit > Parameters
```

### Query Data

```sql
-- Sample styles
SELECT 
    id, header_number, header_name, 
    colorway_count, size_range_count,
    synced_at, modified_at
FROM main.beproduct.bp_styles
LIMIT 10;

-- Search by name
SELECT * FROM main.beproduct.bp_styles 
WHERE header_name LIKE '%denim%'
LIMIT 20;

-- Count by folder
SELECT folder_name, COUNT(*) as count 
FROM main.beproduct.bp_styles 
GROUP BY folder_name 
ORDER BY count DESC;

-- Find recently modified
SELECT * FROM main.beproduct.bp_styles 
WHERE modified_at > CURRENT_TIMESTAMP() - INTERVAL 7 DAY 
ORDER BY modified_at DESC 
LIMIT 50;
```

### Export Data

```sql
-- Export to CSV
SELECT * FROM main.beproduct.bp_styles
TO CSV 's3://my-bucket/exports/styles.csv';

-- OR export via Databricks UI: click "Export" on result

-- OR extract JSON for API re-import
SELECT data_json FROM main.beproduct.bp_styles 
WHERE id = '<specific-uuid>'
```

### Audit Log Queries

```sql
-- All push operations in last 24 hours
SELECT * FROM main.beproduct.bp_audit_log
WHERE timestamp > CURRENT_TIMESTAMP() - INTERVAL 1 DAY
ORDER BY timestamp DESC;

-- Summary by action
SELECT action, COUNT(*) as count, MAX(timestamp) as latest
FROM main.beproduct.bp_audit_log
GROUP BY action
ORDER BY latest DESC;

-- Check for errors
SELECT * FROM main.beproduct.bp_audit_log
WHERE action = 'ERROR'
ORDER BY timestamp DESC
LIMIT 20;

-- Trace specific record
SELECT * FROM main.beproduct.bp_audit_log
WHERE record_id = '<uuid>'
ORDER BY timestamp DESC;
```

### Troubleshooting

```bash
# Check job definition
databricks jobs get --job-id <job_id>

# Get full run log
databricks runs get-output --run-id <run_id> | grep -A 20 "Error\|Exception\|ERROR"

# List recent failures
databricks jobs list-runs --job-id <job_id> --limit 10 | grep -i failed

# Check cluster logs
databricks clusters get --cluster-id <cluster_id>
```

### Rebuild Tables

```sql
-- Clear and rebuild (full sync)
-- WARNING: This will delete all existing data!

DROP TABLE IF EXISTS main.beproduct.bp_styles;

-- Then run inbound job with incremental_mode=false
-- OR execute in notebook:
-- client.fetch_styles()  # with no incremental_filter
```

## Common Issues & Solutions

### Rate Limit (429 error)

```
❌ Problem: "Too many requests" errors
✅ Solution: 
   1. Check job logs for 429 responses
   2. Increase timeout: timeout_seconds=7200
   3. Reduce batch_size: batch_size=500
   4. Contact BeProduct for rate limit increase
```

### Secret Not Found

```
❌ Problem: "scope 'beproduct' not found"
✅ Solution:
   # Create scope
   databricks secrets create-scope --scope beproduct
   
   # List to verify
   databricks secrets list-scopes
```

### Table Not Found (First Run)

```
❌ Problem: "Table 'bp_styles' does not exist"
✅ Solution:
   # This is normal on first run. The notebook will create it.
   # Just run the inbound job once.
   databricks jobs run-now --job-id <inbound_job_id>
```

### Incremental Filter Broken

```
❌ Problem: Incremental sync returns 0 rows
✅ Solution:
   # Check if synced_at is being set
   SELECT MAX(synced_at) FROM main.beproduct.bp_styles;
   
   # If NULL, do a full sync first (incremental_mode=false)
```

## Performance Tips

```
For <5,000 records:
  batch_size: 1000 (default) ✓
  workers: 1 ✓
  node_type: i3.xlarge ✓
  timeout: 3600s ✓

For 5,000-50,000 records:
  batch_size: 2000
  workers: 2
  node_type: i3.xlarge
  timeout: 5400s

For >50,000 records:
  batch_size: 5000
  workers: 4
  node_type: i3.2xlarge
  timeout: 7200s
```

## Disaster Recovery

### Backup Data

```sql
-- Export table to Delta clone (zero-copy backup)
CREATE TABLE main.beproduct.bp_styles_backup_2026_05_05 
CLONE main.beproduct.bp_styles;

-- OR export to cloud storage
SELECT * FROM main.beproduct.bp_styles
TO CSV 's3://backup-bucket/styles_backup.csv';
```

### Restore from Backup

```sql
-- Restore from Delta clone
ALTER TABLE main.beproduct.bp_styles SET TBLPROPERTIES 
  ('delta.tupleId' = (SELECT MAX('delta.tupleId') FROM main.beproduct.bp_styles_backup_2026_05_05));

-- OR manually re-import and re-sync
-- Run full inbound sync (incremental_mode=false)
```

### Recover from Bad Push

```sql
-- 1. Check audit log to see what was pushed
SELECT * FROM main.beproduct.bp_audit_log
WHERE record_id = '<affected_record>' 
AND timestamp > CURRENT_TIMESTAMP() - INTERVAL 1 DAY;

-- 2. Check data_json in Delta to see original
SELECT data_json FROM main.beproduct.bp_styles 
WHERE id = '<affected_record>';

-- 3. Manually fix in BeProduct UI or:
-- 4. Re-sync from BeProducct (run inbound job)
-- 5. Verify in Delta table
SELECT * FROM main.beproduct.bp_styles 
WHERE id = '<affected_record>';
```

## Useful SQL Patterns

```sql
-- Find dirty/modified records (for marking to push)
SELECT * FROM main.beproduct.bp_styles 
WHERE modified_at > synced_at
ORDER BY modified_at DESC;

-- Cross-reference tables
SELECT 
    s.header_name as style_name,
    m.header_name as material_name
FROM main.beproduct.bp_styles s
CROSS JOIN main.beproduct.bp_materials m
WHERE s.id = '<specific_style_uuid>'
LIMIT 100;

-- Find orphaned references
SELECT id, header_name 
FROM main.beproduct.bp_styles 
WHERE id NOT IN (SELECT last_beproduct_id FROM main.beproduct.bp_audit_log WHERE action != 'ERROR');

-- Aggregate statistics
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT folder_id) as unique_folders,
    MIN(created_at) as earliest,
    MAX(modified_at) as latest,
    SUM(active) as active_count
FROM main.beproduct.bp_styles;
```

## Environment Variables (For Local Testing)

If testing locally (outside Databricks), set in `.env`:

```bash
BEPRODUCT_CLIENT_ID=your_client_id
BEPRODUCT_CLIENT_SECRET=your_client_secret
BEPRODUCT_REFRESH_TOKEN=your_refresh_token
BEPRODUCT_COMPANY_DOMAIN=your_domain
DATABRICKS_HOST=https://adb-xxxxxxxx.azuredatabricks.net
DATABRICKS_TOKEN=dapi...
```

## Cheat Sheet Aliases

```bash
# Add to ~/.bashrc or ~/.zshrc for quick access

alias dbtb='databricks workspace'
alias dbjobs='databricks jobs'
alias dbrun='databricks runs'
alias dbsync='databricks jobs run-now --job-id'
alias dblog='databricks runs get-output --run-id'
```

Usage:
```bash
dbjobs list
dbsync <job_id>
dblog <run_id> | tail -50
```

---

**Quick Reference v1.0 | Last Updated: 2026-05-05**
