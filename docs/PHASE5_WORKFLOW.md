# Phase 5: BeProduct Master Data & Directory Sync

**Status:** Implemented ✅ — admin-only, manually triggered, **not in the daily DAG**.

Notebook: `beproduct/p5utl_beproduct_master_data_sync.py`

Phase 5 is a utility for synchronising BeProduct's dropdown-choice lists (Master
Data) and partner registry (Directory) with the `lft.beproduct` Delta tables, and
for pushing admin-edited values back to BeProduct. Because the Directory list API
throughput is ~2 records/second (~30 min for 3,800 records), the pull and push
operations are separated into distinct modes.

> Data model details: `docs/ARCHITECTURE.md`.
> Verified API behaviour + invariants: `../AGENTS.md`.

---

## Scope

| Domain | What it covers |
|--------|---------------|
| **MasterData** | 11 dropdown / multiselect choice lists: brands, teams, seasons, years, product_status, product_category, product_sub_category, division, techpack_stage, parent_vendor, factory. (`garment_finish` excluded — free-text field, no Choices array.) |
| **Directory** | All vendors, factories, and mills in the BeProduct partner registry (~3,800 records). Contacts per company (opt-in — see `fetch_contacts` widget). |

---

## Modes

| Mode | What it does | Speed |
|------|-------------|-------|
| `PULL_ONLY` *(default)* | Pull MasterData + Directory from BeProduct → Delta. Full refresh — overwrites existing tables. | ~30 min (Directory at 2 rec/s) |
| `PUSH_MASTER_DATA` | Push dropdown choices from Delta → BeProduct (PATCH semantics: absent choices are untouched). No pull. | Fast |
| `PUSH_DIRECTORY` | Push Directory changes only (change-detected; only pending rows). No pull. | Fast |
| `PUSH_ONLY` | Push both MasterData and Directory. No pull. | Fast |

Typical admin workflow:
1. Run `PULL_ONLY` → inspect Delta tables → edit rows in Databricks SQL.
2. Run `PUSH_MASTER_DATA` or `PUSH_DIRECTORY` with `dry_run=true` → review plan.
3. Re-run with `dry_run=false` → commits to BeProduct.

---

## Delta tables written

| Table | Grain | Key columns |
|-------|-------|-------------|
| `beproduct_master_brands` | 1 row / choice | `field_id`, `value`, `code`, `active`, `data_json`, `synced_at` |
| `beproduct_master_teams` | 1 row / choice | ← same schema |
| `beproduct_master_seasons` | 1 row / choice | ← |
| `beproduct_master_years` | 1 row / choice | ← |
| `beproduct_master_product_status` | 1 row / choice | ← |
| `beproduct_master_product_category` | 1 row / choice | ← |
| `beproduct_master_product_sub_category` | 1 row / choice | ← |
| `beproduct_master_division` | 1 row / choice | ← |
| `beproduct_master_techpack_stage` | 1 row / choice | ← |
| `beproduct_master_parent_vendor` | 1 row / choice | ← |
| `beproduct_master_factory` | 1 row / choice | ← |
| `beproduct_directory` | 1 row / company | `id` (BP UUID; NULL = new), `directory_id`, `name`, `partner_type`, `address`, `country`, `state`, `zip`, `city`, `phone`, `fax`, `website`, `notes`, `active`, `data_json`, `extracted_at`, `bp_modified_at`, `modified_at` |
| `beproduct_directory_contacts` | 1 row / contact | `directory_id` (parent UUID), `contact_id` (NULL = new), `email`, `first_name`, `last_name`, `title`, `mobile_phone`, `work_phone`, `role`, `active`, `data_json`, `synced_at` |

---

## API surface used

### MasterData

```
Pull:  GET  /api/{company}/MasterData/{fieldId}
       → .properties.Choices[]  { value, code, active }

Push:  POST /api/{company}/MasterData/{fieldId}/Update
       body: { "choices": { "items": [{ value, code, active }, …] } }
       Semantics: PATCH (absent choices untouched); send full list for effective overwrite.
       To deactivate: set active=false. No delete endpoint.
```

### Directory

```
Pull:  SDK  api.directory.directory_list()          paginated, ~2 rec/s
       SDK  api.directory.directory_contact_list(header_id=<uuid>)   (opt-in)

Push (Add new):
       SDK  api.directory.directory_add(fields={…})          → returns { id }
       Atomic contacts: embed contacts[] in Add payload (or add separately).

Push (Update existing — SDK has no Update method):
       raw  POST /api/{company}/Directory/Update/{id}
       NOTE: partnerType CANNOT be changed after creation.
       NOTE: email/firstName/lastName cannot be updated for fully-registered users.

Push (Contact Add):
       SDK  api.directory.directory_contact_add(header_id=<uuid>, fields={…})

Push (Contact Update):
       raw  POST /api/{company}/Directory/{dirId}/Contact/{contactId}/Update
```

---

## Directory change-detection (PUSH_DIRECTORY)

`beproduct_directory` carries three timestamp columns:

| Column | Meaning |
|--------|---------|
| `extracted_at` | When this row was last pulled from BeProduct (set by PULL_ONLY). NULL = never pulled. |
| `bp_modified_at` | BeProduct's own `modifiedAt` for the record. |
| `modified_at` | When this Delta row last changed — by pull (= `extracted_at`) or external upsert. **Set to `current_timestamp()` by external upserts to flag a pending change.** |

Push filter: `id IS NULL` **OR** `extracted_at IS NULL` **OR** `modified_at > extracted_at`

After a successful push: `extracted_at ← modified_at ← now()` (row is "in sync").

### External-upsert MERGE template

The notebook (Cell 5b) contains a ready-to-use MERGE SQL template for
admin-supplied data:

```sql
MERGE INTO lft.beproduct.beproduct_directory AS tgt
USING (<your_source>) AS src
ON tgt.directory_id = src.directory_id
WHEN MATCHED AND (<fields changed>)
  THEN UPDATE SET …, tgt.modified_at = current_timestamp()   -- flags for push
WHEN NOT MATCHED BY TARGET
  THEN INSERT (id=NULL, …, modified_at=current_timestamp())  -- NULL id → Add on push
```

---

## Widgets

| Widget | Default | Notes |
|--------|---------|-------|
| `mode` | `PULL_ONLY` | `PULL_ONLY` \| `PUSH_ONLY` \| `PUSH_MASTER_DATA` \| `PUSH_DIRECTORY` |
| `dry_run` | `true` | Preview push without writing to BeProduct |
| `fetch_contacts` | `false` | Pull contacts during PULL_ONLY (~3,800 extra API calls). Skip unless needed. |
| `catalog` | `lft` | Unity Catalog |
| `schema_name` | `beproduct` | Schema (`schema_name` to avoid shadowing PySpark StructType) |

---

## Known constraints

- **partnerType** cannot be changed after a Directory company is created.
- **Contact email / firstName / lastName** cannot be updated for fully-registered BeProduct users.
- `beproduct_directory_contacts` is only created during `PULL_ONLY` with `fetch_contacts=true`.
  The push cell guards against the table not existing with `tableExists`.
- **No Directory delete** endpoint — use `active=false` to deactivate.
- Directory API throughput: ~2 records/sec (`directory_list`). Pull takes ~30 min for 3,800 records.
  Push uses change-detection so only pending rows are sent (fast for small changesets).
