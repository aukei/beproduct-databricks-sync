# Databricks Integration Skill

Guide for connecting to Databricks and performing operations with notebooks, tables, and jobs.

## When to Use This Skill

Use this skill when you need to:
- Connect to Databricks workspace and authenticate
- Read, write, or execute Databricks notebooks
- Query, create, or modify Delta Lake tables
- Create, configure, or manage Databricks jobs
- Upload files or code to Databricks workspace
- Work with Databricks CLI or SDK
- Manage Databricks secrets and configurations

## Prerequisites

Before working with Databricks, ensure:
1. Databricks workspace URL is available
2. Authentication method is configured (PAT token, OAuth, or Azure AD)
3. Python environment has required packages installed
4. Databricks CLI is configured (for CLI operations)

## Authentication Methods

### 1. Personal Access Token (PAT)

**Environment Setup (using .env - Recommended):**
```bash
# Create .env file
cp .env.example .env
# Add credentials to .env:
#   DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
#   DATABRICKS_PAT=dapi1234567890abcdef
```

**Alternative - Export directly:**
```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi1234567890abcdef"
```

**Python SDK:**
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(
    host="https://your-workspace.cloud.databricks.com",
    token="dapi1234567890abcdef"
)
```

**Databricks CLI:**
```bash
databricks configure --token
# Enter host URL and token when prompted
```

### 2. Azure Active Directory (for Azure Databricks)

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

config = Config(
    host="https://adb-1234567890123456.7.azuredatabricks.net",
    azure_workspace_resource_id="/subscriptions/{sub-id}/resourceGroups/{rg-name}/providers/Microsoft.Databricks/workspaces/{ws-name}"
)

w = WorkspaceClient(config=config)
```

### 3. OAuth M2M (Machine-to-Machine)

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(
    host="https://your-workspace.cloud.databricks.com",
    client_id="your-client-id",
    client_secret="your-client-secret"
)
```

## Databricks Notebooks

### Reading Notebooks

**Using Databricks CLI:**
```bash
# Export notebook as source file
databricks workspace export /Workspace/path/to/notebook.py notebook.py --format SOURCE

# Export as HTML
databricks workspace export /Workspace/path/to/notebook.py notebook.html --format HTML

# Export as Jupyter
databricks workspace export /Workspace/path/to/notebook.ipynb notebook.ipynb --format JUPYTER
```

**Using Python SDK:**
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Export notebook content
content = w.workspace.export(
    path="/Workspace/path/to/notebook",
    format="SOURCE"  # or "HTML", "JUPYTER", "DBC"
)

# Save to local file
with open("notebook.py", "wb") as f:
    f.write(content.content)
```

### Writing/Uploading Notebooks

**Using Databricks CLI:**
```bash
# Upload a notebook
databricks workspace import /Workspace/path/to/notebook.py \
    --file notebook.py \
    --language PYTHON \
    --format SOURCE \
    --overwrite

# Upload directory of notebooks
databricks workspace import_dir ./local_notebooks /Workspace/notebooks --overwrite
```

**Using Python SDK:**
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

w = WorkspaceClient()

# Upload notebook
with open("notebook.py", "rb") as f:
    content = f.read()

w.workspace.import_(
    path="/Workspace/path/to/notebook",
    format=ImportFormat.SOURCE,
    language="PYTHON",
    content=content,
    overwrite=True
)
```

**Using upload helper script (project-specific):**
```python
# The project deploy script is scripts/upload_notebooks.py — it auto-discovers
# notebooks (beproduct/, dtc/notebooks/) and modules (dtc/python/) and uploads them.
#   python scripts/upload_notebooks.py            # all
#   python scripts/upload_notebooks.py --dry-run  # preview
#   python scripts/upload_notebooks.py --modules-only
# The snippet below shows the underlying WorkspaceClient pattern it uses.
import os
from databricks.sdk import WorkspaceClient

def upload_notebooks_to_databricks(
    local_dir: str,
    workspace_path: str,
    file_extensions: list = ['.py', '.sql', '.ipynb']
):
    """Upload notebooks from local directory to Databricks workspace."""
    w = WorkspaceClient()
    
    for root, dirs, files in os.walk(local_dir):
        for file in files:
            if any(file.endswith(ext) for ext in file_extensions):
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, local_dir)
                remote_path = os.path.join(workspace_path, relative_path)
                
                # Remove file extension for workspace path
                remote_path = os.path.splitext(remote_path)[0]
                
                with open(local_path, 'rb') as f:
                    content = f.read()
                
                print(f"Uploading {local_path} -> {remote_path}")
                w.workspace.import_(
                    path=remote_path,
                    format="SOURCE",
                    content=content,
                    overwrite=True
                )

# Usage
upload_notebooks_to_databricks(
    local_dir="./dtc/notebooks",
    workspace_path="/Workspace/Repos/beproduct-sync/DTC/notebooks"
)
```

### Executing Notebooks

**Run notebook and get result:**
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

# Create one-time run
run = w.jobs.submit(
    run_name="Ad-hoc notebook run",
    tasks=[
        jobs.SubmitTask(
            task_key="notebook_task",
            notebook_task=jobs.NotebookTask(
                notebook_path="/Workspace/path/to/notebook",
                base_parameters={"param1": "value1", "param2": "value2"}
            ),
            new_cluster=jobs.ClusterSpec(
                spark_version="13.3.x-scala2.12",
                node_type_id="Standard_DS3_v2",
                num_workers=2
            )
        )
    ]
)

# Wait for completion
run_result = w.jobs.wait_get_run_job_terminated_or_skipped(run.run_id)
print(f"Run status: {run_result.state.result_state}")
```

**Run on existing cluster:**
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Run notebook on existing cluster
run = w.jobs.submit(
    run_name="Quick notebook run",
    tasks=[
        {
            "task_key": "main",
            "notebook_task": {
                "notebook_path": "/Workspace/path/to/notebook",
                "base_parameters": {"environment": "prod"}
            },
            "existing_cluster_id": "1234-567890-abc123"
        }
    ]
)
```

## Delta Lake Tables

### Querying Tables

**Using SQL in notebooks:**
```python
# In Databricks notebook
df = spark.sql("""
    SELECT * FROM lft.beproduct.ktb_styles
    WHERE last_modified >= current_date() - INTERVAL 7 DAYS
    LIMIT 100
""")

df.display()
```

**Using Python API:**
```python
# Read Delta table into DataFrame
df = spark.table("lft.beproduct.ktb_styles")

# With Unity Catalog
df = spark.read.table("lft.beproduct.ktb_styles")

# Filter and display
df.filter(df.season_code == "SS26").display()
```

**Using Databricks SQL API:**
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Execute SQL statement
statement = w.statement_execution.execute_statement(
    warehouse_id="abc123def456",
    catalog="lft",
    schema="beproduct",
    statement="SELECT * FROM ktb_styles LIMIT 10"
)

# Get results
print(statement.result.data_array)
```

### Creating Tables

**Create Delta table from DataFrame:**
```python
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Define schema
schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

# Create DataFrame
data = [
    ("1", "Item A", datetime.now()),
    ("2", "Item B", datetime.now())
]
df = spark.createDataFrame(data, schema)

# Write to Delta table
df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("lft.beproduct.my_table")
```

**Create table with SQL:**
```python
spark.sql("""
    CREATE TABLE IF NOT EXISTS lft.beproduct.my_table (
        id STRING NOT NULL,
        name STRING,
        created_at TIMESTAMP
    )
    USING DELTA
    LOCATION 's3://bucket/path/to/table'
""")
```

### Writing to Tables

**Append mode:**
```python
df.write.format("delta") \
    .mode("append") \
    .saveAsTable("lft.beproduct.ktb_styles")
```

**Overwrite mode:**
```python
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("lft.beproduct.ktb_styles")
```

**Merge (Upsert) operation:**
```python
from delta.tables import DeltaTable

# Load existing Delta table
delta_table = DeltaTable.forName(spark, "lft.beproduct.ktb_styles")

# Merge updates
delta_table.alias("target").merge(
    df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

**Using SQL MERGE:**
```python
spark.sql("""
    MERGE INTO lft.beproduct.ktb_styles AS target
    USING updates AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

### Table Operations

**Optimize table:**
```python
# Optimize with Z-ordering
spark.sql("""
    OPTIMIZE lft.beproduct.ktb_styles
    ZORDER BY (season_code, style_code)
""")
```

**Vacuum old files:**
```python
# Remove files older than 7 days
spark.sql("""
    VACUUM lft.beproduct.ktb_styles RETAIN 168 HOURS
""")
```

**Describe table:**
```python
# Get table schema
spark.sql("DESCRIBE EXTENDED lft.beproduct.ktb_styles").display()

# Get table history
spark.sql("DESCRIBE HISTORY lft.beproduct.ktb_styles").display()
```

## Databricks Jobs

### Creating Jobs

**Using Python SDK:**
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

# Create scheduled job
job = w.jobs.create(
    name="BeProduct STYLE Sync - Daily",
    tasks=[
        jobs.Task(
            task_key="sync_styles",
            notebook_task=jobs.NotebookTask(
                notebook_path="/Workspace/Repos/beproduct-sync/beproduct/beproduct_style_sync",
                base_parameters={
                    "folder_name": "KTB",
                    "refresh_mode": "INCREMENTAL"
                }
            ),
            new_cluster=jobs.ClusterSpec(
                spark_version="13.3.x-scala2.12",
                node_type_id="Standard_DS3_v2",
                num_workers=2
            )
        )
    ],
    schedule=jobs.CronSchedule(
        quartz_cron_expression="0 0 11 * * ?",  # Daily at 11:00 UTC
        timezone_id="UTC"
    ),
    email_notifications=jobs.JobEmailNotifications(
        on_failure=["team@company.com"],
        on_success=["team@company.com"]
    )
)

print(f"Created job with ID: {job.job_id}")
```

**Multi-task job with dependencies:**
```python
job = w.jobs.create(
    name="DTC Sync Pipeline",
    tasks=[
        jobs.Task(
            task_key="init_mapping",
            notebook_task=jobs.NotebookTask(
                notebook_path="/Workspace/Repos/beproduct-sync/DTC/notebooks/00_init_season_mapping"
            ),
            new_cluster=jobs.ClusterSpec(
                spark_version="13.3.x-scala2.12",
                node_type_id="Standard_DS3_v2",
                num_workers=1
            )
        ),
        jobs.Task(
            task_key="pull_dtc",
            depends_on=[jobs.TaskDependency(task_key="init_mapping")],
            notebook_task=jobs.NotebookTask(
                notebook_path="/Workspace/Repos/beproduct-sync/DTC/notebooks/pull_requests_to_delta",
                base_parameters={
                    "customer": "KTB",
                    "dtc_environment": "uat"
                }
            ),
            new_cluster=jobs.ClusterSpec(
                spark_version="13.3.x-scala2.12",
                node_type_id="Standard_DS3_v2",
                num_workers=2
            )
        )
    ]
)
```

### Running Jobs

**Trigger job run:**
```python
# Run existing job
run = w.jobs.run_now(
    job_id=123,
    notebook_params={
        "environment": "prod",
        "batch_size": "1000"
    }
)

print(f"Started run: {run.run_id}")

# Wait for completion
result = w.jobs.wait_get_run_job_terminated_or_skipped(run.run_id)
print(f"Run completed with state: {result.state.result_state}")
```

**Monitor job runs:**
```python
# Get recent runs for a job
runs = w.jobs.list_runs(job_id=123, limit=10)

for run in runs:
    print(f"Run {run.run_id}: {run.state.life_cycle_state}")
    
# Get specific run details
run_details = w.jobs.get_run(run_id=456)
print(f"Status: {run_details.state.result_state}")
print(f"Duration: {run_details.execution_duration}ms")
```

### Managing Jobs

**List all jobs:**
```python
# List jobs
jobs_list = w.jobs.list(limit=100)

for job in jobs_list:
    print(f"{job.job_id}: {job.settings.name}")
```

**Update job configuration:**
```python
# Update job schedule
w.jobs.update(
    job_id=123,
    new_settings=jobs.JobSettings(
        schedule=jobs.CronSchedule(
            quartz_cron_expression="0 0 2 * * ?",  # Changed to 2:00 AM
            timezone_id="UTC"
        )
    )
)
```

**Delete job:**
```python
w.jobs.delete(job_id=123)
```

## Secrets Management

### Working with Databricks Secrets

**Using Databricks CLI:**
```bash
# Create secret scope
databricks secrets create-scope --scope beproduct

# Put secrets
databricks secrets put --scope beproduct --key dtc_api_key_uat
databricks secrets put --scope beproduct --key client_id
databricks secrets put --scope beproduct --key client_secret

# List scopes
databricks secrets list-scopes

# List secrets in scope
databricks secrets list --scope beproduct

# Delete secret
databricks secrets delete --scope beproduct --key old_key
```

**Using secrets in notebooks:**
```python
# Access secrets in Databricks notebook
api_key = dbutils.secrets.get(scope="beproduct", key="dtc_api_key_uat")
client_id = dbutils.secrets.get(scope="beproduct", key="client_id")
client_secret = dbutils.secrets.get(scope="beproduct", key="client_secret")

# Use in connector
from dtc.python.connectors.dtc import DTCConnector

dtc = DTCConnector(
    api_key=api_key,
    environment="uat"
)
```

## Common Patterns

### Pattern 1: Read from API, Write to Delta Lake

```python
# Example: DTC to Delta Lake sync
from dtc.python.connectors.dtc import DTCConnector
from datetime import datetime

# Initialize connector
api_key = dbutils.secrets.get(scope="beproduct", key="dtc_api_key_uat")
dtc = DTCConnector(api_key=api_key, environment="uat")

# Fetch data
request = dtc.get_request("REQ_12345")
df = dtc.to_dataframe(sheet_id=request["sheetId"], view_id="view_123")

# Add metadata
df = df.withColumn("extracted_time", lit(datetime.now()))
df = df.withColumn("sync_date", current_date())

# Write to Delta Lake
df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("lft.beproduct.dtc_wip_ktb")

print(f"✅ Synced {df.count()} rows to Delta Lake")
```

### Pattern 2: Change Tracking with Merge

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit

# Load target table
target_table = DeltaTable.forName(spark, "lft.beproduct.ktb_styles")

# Add metadata to source
source_df = source_df \
    .withColumn("last_modified", current_timestamp()) \
    .withColumn("sync_batch_id", lit(batch_id))

# Perform merge with change tracking
target_table.alias("target").merge(
    source_df.alias("source"),
    "target.style_id = source.style_id"
).whenMatchedUpdate(
    condition="target.last_modified < source.last_modified",
    set={
        "name": "source.name",
        "season_code": "source.season_code",
        "last_modified": "source.last_modified",
        "modified_fields": "array_union(target.modified_fields, array('name', 'season_code'))"
    }
).whenNotMatchedInsertAll().execute()
```

### Pattern 3: Multi-Environment Configuration

```python
# Environment-specific configuration
def get_config(environment: str):
    """Get environment-specific configuration."""
    configs = {
        "dev": {
            "catalog": "dev",
            "schema": "beproduct",
            "dtc_env": "uat",
            "cluster_size": "small"
        },
        "prod": {
            "catalog": "lft",
            "schema": "beproduct",
            "dtc_env": "prod",
            "cluster_size": "medium"
        }
    }
    return configs.get(environment, configs["dev"])

# Usage in notebook
environment = dbutils.widgets.get("environment") or "dev"
config = get_config(environment)

# Use configuration
table_name = f"{config['catalog']}.{config['schema']}.ktb_styles"
df = spark.table(table_name)
```

## Reading Job Run Logs

### Overview

Databricks job run logs are exported as HTML with an embedded base64-encoded notebook model containing per-command outputs and timing. The CLI `databricks jobs get-run-output` doesn't support multi-task jobs, so we use the REST API directly.

### Step-by-Step: Extract and Parse Run Logs

**1. Get the task run_id from the parent job run:**
```bash
source .env
export DATABRICKS_HOST DATABRICKS_TOKEN="$DATABRICKS_PAT"

# List recent runs of a job
databricks api get "/api/2.1/jobs/runs/list?job_id=22324120218492&limit=5" \
  | jq '.runs[] | {run_id, start_time, state: .state.result_state}'

# Get task details for a specific run (finds the task run_id)
databricks api get "/api/2.1/jobs/runs/get?run_id=57360453476718" \
  | jq '.tasks[] | {task_key, run_id, result: .state.result_state, duration_ms: .execution_duration}'
```

**2. Export the run logs (returns HTML with embedded notebook model):**
```bash
databricks api get "/api/2.1/jobs/runs/export?run_id=274593806072640&views_to_export=ALL" \
  | jq -r '.views[0].content' > /tmp/run_export.html
```

**3. Extract and decode the notebook model:**
```bash
# Extract base64 from HTML
grep -oP "__DATABRICKS_NOTEBOOK_MODEL = '\K[^']+" /tmp/run_export.html > /tmp/b64.txt

# Decode: base64 → URL-decode → JSON
python3 << 'EOF'
import base64, json
from urllib.parse import unquote

with open('/tmp/b64.txt', 'r') as f:
    b64_data = f.read().strip()

decoded = base64.b64decode(b64_data).decode('utf-8')
url_decoded = unquote(decoded)
obj = json.loads(url_decoded)

# obj['commands'] is a list of notebook cells with outputs
print(f"Total commands: {len(obj.get('commands', []))}")
EOF
```

**4. Extract command outputs with timing:**
```python
import base64, json
from urllib.parse import unquote

with open('/tmp/b64.txt', 'r') as f:
    b64_data = f.read().strip()

decoded = base64.b64decode(b64_data).decode('utf-8')
url_decoded = unquote(decoded)
obj = json.loads(url_decoded)

for i, cmd in enumerate(obj.get('commands', [])):
    command = cmd.get('command', '')
    results = cmd.get('results', {})
    start_time = cmd.get('startTime')
    finish_time = cmd.get('finishTime')
    
    # Command identification (first line)
    cmd_lines = command.split('\n')
    cmd_preview = cmd_lines[0][:120] if cmd_lines else command[:120]
    
    print(f"\n{'='*80}")
    print(f"Command {i+1}: {cmd_preview}")
    if start_time and finish_time:
        duration_ms = finish_time - start_time
        print(f"Duration: {duration_ms}ms ({duration_ms/1000:.1f}s)")
    
    # Extract output from results.data (list of {type: 'ansi', data: '...'})
    if isinstance(results, dict) and 'data' in results:
        data_list = results['data']
        if isinstance(data_list, list):
            for item in data_list:
                if isinstance(item, dict) and item.get('type') == 'ansi':
                    output = item.get('data', '')
                    print(f"Output ({len(output.split(chr(10)))} lines):")
                    for line in output.split('\n')[:60]:
                        print(f"  {line[:200]}")
                    if len(output.split('\n')) > 60:
                        print(f"  ... (truncated)")
```

### Key Fields in the Notebook Model

| Field | Description |
|-------|-------------|
| `commands[].command` | The notebook cell code |
| `commands[].startTime` | Unix timestamp (ms) when cell started |
| `commands[].finishTime` | Unix timestamp (ms) when cell finished |
| `commands[].results.data[].type` | Output type: `ansi` (text), `table`, `image`, etc. |
| `commands[].results.data[].data` | The actual output content |
| `commands[].state` | Cell state: `Finished`, `Error`, etc. |
| `commands[].error` | Error message if cell failed |

### Performance Analysis Pattern

To identify bottlenecks, extract timing per step and correlate with Spark job overhead:

```python
# Build timing table
for i, cmd in enumerate(obj.get('commands', [])):
    start = cmd.get('startTime')
    finish = cmd.get('finishTime')
    if start and finish:
        duration_s = (finish - start) / 1000
        # Identify step from command content
        command = cmd.get('command', '')
        step_match = command.split('\n')[0] if command else f"Command {i+1}"
        print(f"Step {i+1}: {duration_s:6.1f}s  {step_match[:80]}")
```

**Common Spark overhead patterns:**
- Each `spark.sql("UPDATE delta_table ...")` on a Delta table launches a separate Spark job (~5-15s overhead)
- `CREATE TABLE IF NOT EXISTS`, `SELECT COUNT(*)`, `information_schema` queries each launch Spark jobs
- Per-request UPDATE loops (e.g., 66 iterations) = 66 Spark jobs = ~400-600s overhead
- Fix: Replace per-request UPDATE loops with a single batch `MERGE INTO` using a temp view

### Per-step logs: prefer the multi-task job

The production pipeline is now the **multi-task job** `BeProduct_DTC_sync_dag`
(job 294837488757511, defined in `scripts/deploy_job.py`). Each step is a
first-class task, so per-step timing/logs come straight from
`GET /api/2.1/jobs/runs/get?run_id=<JOB_RUN>` → `.tasks[]` (each has its own
`run_id`, `execution_duration`, state). Export any single step's command-level
model with `runs/export?run_id=<tasks[].run_id>` — **no `WORKFLOW_RUN` hunting**.
The technique below is only needed for the RETIRED single-notebook
`orchestrate_sync.py` (or any other `dbutils.notebook.run` caller).

### Child notebooks via `dbutils.notebook.run` (legacy orchestrate_sync) — HOW TO GET THEIR LOGS

`beproduct/orchestrate_sync.py` launches every step with
`dbutils.notebook.run(notebook_path, timeout, params)` (`orchestrate_sync.py:186`).
Each child is a real run with its own `run_id` and its own full command-level
notebook model — but it is hidden in non-obvious ways:

- The parent run export (`runs/export`) contains **only the orchestrator's own**
  cells. Each step shows just the wrapper print (`✅ STEP N done in Xs`), NOT the
  child's per-command output, and **no child `run_id` is embedded** in it.
- `GET /api/2.1/jobs/runs/get?run_id=<task>` returns only `parent_run_id` (a pointer
  UP to the job run); there is **no downward child pointer**.
- A plain `runs/list?job_id=…` does **not** list the children.

**The trick: children are `run_type=WORKFLOW_RUN` runs.** List them by time window
(NOT by job_id — they have no `job_id`), match to the parent by start-time/duration,
then export each child exactly like any other run. `limit` max is **25**.

```bash
source .env; export DATABRICKS_HOST DATABRICKS_TOKEN="$DATABRICKS_PAT"

# 1. Parent job run -> task run_id + its start/end window (ms since epoch)
databricks api get "/api/2.1/jobs/runs/get?run_id=<JOB_RUN_ID>" \
  | jq '{task: .tasks[0].run_id, start: .start_time, end: .tasks[0].end_time, setup_ms: .tasks[0].setup_duration}'

# 2. List the dbutils.notebook.run children in that window (run_type=WORKFLOW_RUN).
#    parent_run_id is NOT returned here, so map children to steps by start_time order
#    and execution_duration (they line up 1:1 with the orchestrator step timings).
databricks api get "/api/2.1/jobs/runs/list?run_type=WORKFLOW_RUN&start_time_from=<START_MS>&start_time_to=<END_MS>&limit=25" \
  | jq -r '.runs[]? | "\(.run_id)\tstart=\(.start_time)\tdur=\(.execution_duration/1000)s\t\(.run_name)"' \
  | sort -t= -k2 -n

# 3. Export a specific child and decode its notebook model (same recipe as above).
databricks api get "/api/2.1/jobs/runs/export?run_id=<CHILD_RUN_ID>&views_to_export=ALL" \
  | jq -r '.views[0].content' > /tmp/child.html
grep -oP "__DATABRICKS_NOTEBOOK_MODEL = '\K[^']+" /tmp/child.html > /tmp/child.b64
# then base64 -> unquote -> json, and read commands[].startTime/finishTime per cell.
```

This gives **exact per-cell timing inside each step** (e.g. for run 57360453476718
it revealed Step 3's cost was a 277 s union+write of 422 rows and a 179 s 66×
control-table `UPDATE` loop — NOT the DTC API, which was ~24 s). See
`docs/PERFORMANCE.md` "Validation run 3".

Caveats:
- The CLI/REST is the only programmatic route; there is no `--child` flag — you must
  use the `run_type=WORKFLOW_RUN` + time-window query above.
- A child cell's stdout is occasionally not captured in the export (only its
  timing). If you need the printed values too, enable cluster `cluster_log_conf`
  (driver-log delivery; currently `null` on job 22324120218492) or add
  `time.perf_counter()` prints. The job cluster's `setup_duration` (here 441 s) is
  the cold-start and is reported on the parent task run.

### Quick Reference: Common API Endpoints

| Endpoint | Purpose |
|----------|---------|
| `/api/2.1/jobs/runs/list?job_id=X&limit=N` | List recent runs of a job (max `limit`=25). Does NOT include `dbutils.notebook.run` children. |
| `/api/2.1/jobs/runs/list?run_type=WORKFLOW_RUN&start_time_from=MS&start_time_to=MS&limit=25` | List `dbutils.notebook.run` child runs by time window (they have no `job_id`). |
| `/api/2.1/jobs/runs/get?run_id=X` | Get run details (tasks, state, duration). Single-task job: task `run_id` is `.tasks[0].run_id`; `setup_duration` = cold start; `parent_run_id` points up only. |
| `/api/2.1/jobs/runs/export?run_id=X&views_to_export=ALL` | Export run logs (HTML w/ embedded notebook model). Works on child run_ids too. |
| `/api/2.1/jobs/runs/get-output?run_id=X` | Returns ONLY the notebook's `dbutils.notebook.exit()` string (+ a `logs` field for JAR/Python-script tasks). **No per-cell timing, no stdout.** Verified: on the task run it gives just the orchestrator exit; on a child WORKFLOW_RUN it's empty `{}` (steps exit `None`). Use `runs/export` for timing, NOT this. |
| `/api/2.0/clusters/get?cluster_id=X` | Check `cluster_log_conf` (driver-log delivery) + node type / workers. |

## Troubleshooting

### Common Issues

**Authentication errors:**
```python
# Check if token is valid
try:
    w = WorkspaceClient()
    user = w.current_user.me()
    print(f"Authenticated as: {user.user_name}")
except Exception as e:
    print(f"Authentication failed: {e}")
    # Regenerate token or check configuration
```

**Table not found:**
```python
# Check if table exists
if spark.catalog.tableExists("lft.beproduct.ktb_styles"):
    df = spark.table("lft.beproduct.ktb_styles")
else:
    print("Table does not exist, creating...")
    # Create table logic
```

**Schema mismatch:**
```python
# Use mergeSchema option to handle schema evolution
df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("lft.beproduct.ktb_styles")
```

**Notebook execution timeout:**
```python
# Set timeout in job configuration
job = w.jobs.create(
    tasks=[
        jobs.Task(
            timeout_seconds=3600,  # 1 hour timeout
            # ... other config
        )
    ]
)
```

## Best Practices

1. **Always use secrets** - Never hardcode credentials
2. **Enable mergeSchema** - Handle schema evolution gracefully
3. **Add metadata columns** - Track `extracted_time`, `sync_date`, `batch_id`
4. **Use Unity Catalog** - Three-level namespace (catalog.schema.table)
5. **Optimize tables regularly** - Run OPTIMIZE with Z-ordering
6. **Monitor job runs** - Set up email notifications
7. **Use smaller clusters for dev** - Save costs in development
8. **Test with limits** - Use LIMIT in development queries
9. **Document parameters** - Use notebook widgets with descriptions
10. **Version control notebooks** - Keep notebooks in Git repos

## Reference

### Key Databricks SDK Modules
- `databricks.sdk.WorkspaceClient` - Main client
- `databricks.sdk.service.workspace` - Workspace operations
- `databricks.sdk.service.jobs` - Job management
- `databricks.sdk.service.sql` - SQL operations
- `delta.tables.DeltaTable` - Delta Lake operations

### Documentation Links
- Databricks SDK: https://databricks-sdk-py.readthedocs.io/
- Delta Lake: https://docs.delta.io/
- Unity Catalog: https://docs.databricks.com/unity-catalog/

### Project-Specific Patterns
- Deploy script: `scripts/upload_notebooks.py` (notebooks + `dtc/python` modules)
- Orchestrator job: `beproduct/orchestrate_sync.py` (runs Phases 1+2+3)
- DTC connector: `dtc/python/connectors/dtc.py`
- BeProduct notebooks: `beproduct/*.py`
- Documentation: `README.md`, `QUICK_START.md`, `docs/ARCHITECTURE.md`
