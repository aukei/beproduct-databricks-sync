# Push-Back Quick Start

## 5-Minute Setup

### 1. Upload Notebook (1 min)

Upload `databricks/beproduct_style_push.py` to your workspace.

### 2. Create Job (2 min)

**Workflows → Jobs → Create job**

- **Name:** `BeProduct STYLE Push - KTB`
- **Notebook:** `/Repos/.../beproduct_style_push`
- **Parameters:**
  ```
  folder_name = KTB
  source_table_name = ktb_styles
  catalog = main
  schema = beproduct
  dry_run = false
  ```

### 3. Test (2 min)

```
Run now with dry_run = true
→ Review logs to see what would be pushed
→ Fix any data issues
→ Run again with dry_run = false
```

---

## How It Works

**Changed records** = `modified_at > synced_at`

```
Databricks (edit)          Push Job              BeProduct
┌─────────────────┐      ┌──────────────┐     ┌──────────────┐
│ ktb_styles      │      │ Detect       │     │ BeProduct    │
│ ────────────    │      │ changes      │     │ ─────────    │
│ modified_at:X   │─────→│ Build update │────→│ Update       │
│ synced_at:Y     │      │ payload      │     │ fields       │
│ Y < X ✓         │      │ Push via SDK │     │              │
└─────────────────┘      │ Update       │     └──────────────┘
                         │ synced_at    │
                         └──────────────┘
```

---

## Common Tasks

### Test Before Pushing

```sql
-- See what would be pushed
SELECT
    lf_style_number,
    description,
    season,
    modified_at,
    synced_at
FROM main.beproduct.ktb_styles
WHERE modified_at > synced_at
ORDER BY modified_at DESC
LIMIT 10;

-- Then run job with dry_run = true
```

### Push for Real

```sql
-- Run job with dry_run = false
-- Job will:
--   1. Push changes to BeProduct
--   2. Update synced_at to current time
--   3. Log results
```

### Reset Record (Clear Changes)

```sql
-- If you want to undo local changes without pushing:
UPDATE main.beproduct.ktb_styles
SET modified_at = synced_at
WHERE id = 'xxx-xxx-xxx';
```

### Bulk Update Before Push

```sql
-- Example: Change season for multiple styles
UPDATE main.beproduct.ktb_styles
SET season = 'Spring S2',
    modified_at = CURRENT_TIMESTAMP()
WHERE season = 'Spring' AND year = '2027';

-- Then run push job (with dry_run = true first!)
```

---

## Parameters

| Parameter | Default | Notes |
|-----------|---------|-------|
| `folder_name` | KTB | BeProduct folder |
| `source_table_name` | ktb_styles | Delta table |
| `catalog` | main | Databricks catalog |
| `schema` | beproduct | Databricks schema |
| `dry_run` | false | Set to `true` to preview |

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "Records pushed but synced_at not updated" | Table is read-only; manually run `UPDATE` SQL |
| "Field not found" | Field name changed in BeProduct; update mapping |
| Some records fail | Check logs; fix data; retry |
| Nothing to push | Check if `modified_at > synced_at` for any records |

---

## Best Practices

✅ Always dry run first (`dry_run = true`)
✅ Schedule the job to run hourly/daily
✅ Monitor the logs for failures
✅ Keep modified_at fresh when editing
✅ Test bulk updates before pushing

---

For detailed docs, see `PUSH_SETUP.md`
