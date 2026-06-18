# Standalone utilities

Notebooks here are **not part of the daily BeProduct ⇄ DTC pipeline**
(`beproduct/orchestrate_sync.py`) and are **not auto-deployed** by
`scripts/upload_notebooks.py` (which only scans `beproduct/` and `dtc/notebooks/`).
Deploy them manually when needed (see below).

---

## `beproduct_style_push.py` — generic Delta → BeProduct push-back

A standalone, **bi-directional** helper that pushes locally edited rows from a
Delta styles table back into BeProduct. It is independent of the DTC flow — the
DTC-driven pushback of DTC-owned fields is **Phase 2**
(`dtc/notebooks/05_push_dtc_to_beproduct.py`, see `docs/PHASE2_WORKFLOW.md`).

**What it does**
- Detects locally edited rows by comparing timestamps on the styles table:
  `modified_at > synced_at` ⇒ the row was changed in Databricks and should be pushed.
- Pushes the extracted style fields (compulsory + interested) via
  `api.style.attributes_update(...)`, shaping each value to the BeProduct field
  **type** (MultiSelect → one-element array, DropDown/Text → string). See
  `docs/BEPRODUCT_GUIDE.md` for the type-aware rules and the round-trip recipe.

**Parameters**

| Parameter | Default | Notes |
|-----------|---------|-------|
| `folder_name` | `KTB` | BeProduct folder |
| `source_table_name` | `ktb_styles` | source Delta table |
| `catalog` | `main` | ⚠️ the daily pipeline uses `lft`; set this to match your styles table |
| `schema` | `beproduct` | |
| `dry_run` | `false` | `true` = preview the payloads without pushing |

**Why standalone:** it pushes *all* extracted fields based on local edits, which is
broader than the one-field-per-direction DTC contract. Keep it separate so it can't
accidentally fight the field-ownership partition that Phases 1/2 enforce.

**Deploy manually** (it lives outside the auto-scanned dirs):

```python
# from a Python shell with databricks-sdk + .env configured
import base64, os
from pathlib import Path
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

load_dotenv()
w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_PAT"])
content = base64.b64encode(Path("standalone/beproduct_style_push.py").read_bytes()).decode()
w.workspace.import_(
    path="/Workspace/Repos/beproduct-sync/standalone/beproduct_style_push",
    format=ImportFormat.SOURCE, language=Language.PYTHON, content=content, overwrite=True,
)
```

Or add `("standalone", "/Workspace/Repos/beproduct-sync/standalone")` to
`NOTEBOOK_DIRS` in `scripts/upload_notebooks.py` if you want it deployed with the rest.
