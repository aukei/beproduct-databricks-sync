# Kilo Skills for BeProduct Databricks Sync

This directory contains specialized Kilo skills for working with the BeProduct Databricks Sync platform.

## Available Skills

### 1. databricks-integration
**File:** `.kilo/skill/databricks-integration/SKILL.md`

Comprehensive guide for Databricks operations:
- **Connect** to Databricks workspace with various auth methods (PAT, Azure AD, OAuth)
- **Notebooks** - Read, write, upload, and execute Databricks notebooks
- **Tables** - Query, create, write to Delta Lake tables with Unity Catalog
- **Jobs** - Create, configure, run, and manage Databricks jobs
- **Secrets** - Manage Databricks secrets and credentials
- **Common Patterns** - API to Delta Lake sync, change tracking, multi-environment configs

**When to use:**
- Working with Databricks workspace, notebooks, or Delta tables
- Creating or managing Databricks jobs
- Executing SQL queries or PySpark operations
- Managing secrets and authentication

### 2. dtc-integration
**File:** `.kilo/skill/dtc-integration/SKILL.md`

Guide for DTC (Data Collaboration Tool) integration:
- **Connect** to DTC API (UAT or Production)
- **Read Sheets** - Fetch worksheets and views from DTC
- **Parse Data** - Extract business logic from request names
- **DataFrames** - Convert to Pandas/Spark DataFrames
- **Change Tracking** - Track modifications and push updates
- **Push Updates** - Send changes back to DTC

**When to use:**
- Pulling data from DTC API to Delta Lake
- Working with DTC requests, sheets, or views
- Implementing DTC change tracking
- Pushing updates back to DTC

### 3. beproduct-integration
**File:** `.kilo/skill/beproduct-integration/SKILL.md`

Guide for BeProduct API integration:
- **Connect** with OAuth 2.0 using BeProduct SDK
- **Read STYLE Data** - Fetch product master data from folders
- **Master Data** - Read reference data (brands, seasons, teams, colors, etc.)
- **Field Mapping** - Map BeProduct field IDs to table columns
- **Validation** - Validate dropdown values before pushing
- **Push Updates** - Update STYLE records with proper validation

**When to use:**
- Syncing BeProduct STYLE data to Delta Lake
- Reading or updating BeProduct master data
- Validating field values against BeProduct dropdowns
- Working with BeProduct SDK or REST API

## How to Use Skills

### Loading Skills in Kilo

Skills are automatically available in Kilo when placed in `.kilo/skill/` directory.

**Load a skill:**
```
Use the skill tool to load specific integration guidance
```

**Example prompts that trigger skills:**
- "How do I connect to Databricks?" → `databricks-integration`
- "Show me how to read DTC sheets" → `dtc-integration`
- "How do I fetch BeProduct styles?" → `beproduct-integration`

### Skill Structure

Each skill contains:
- **Overview** - What the skill covers
- **When to Use** - Trigger conditions
- **Prerequisites** - Required setup
- **Authentication** - Credential management
- **Core Operations** - Step-by-step guides
- **Complete Workflows** - End-to-end examples
- **Troubleshooting** - Common issues and solutions
- **Best Practices** - Recommended patterns
- **Reference** - API methods, endpoints, project files

## Quick Reference

### Databricks Operations
```python
# Connect
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# Read notebook
content = w.workspace.export("/path/to/notebook", format="SOURCE")

# Query table
df = spark.table("lft.beproduct.ktb_styles")

# Create job
job = w.jobs.create(name="My Job", tasks=[...])
```

### DTC Operations
```python
# Connect
from dtc.python.connectors.dtc import DTCConnector
dtc = DTCConnector(api_key=api_key, environment="uat")

# Read sheet
df = dtc.to_dataframe(sheet_id="...", view_id="...")

# Get request
request = dtc.get_request("REQ_12345")
```

### BeProduct Operations
```python
# Connect
from beproduct.sdk import BeProduct
api = BeProduct(
    client_id=client_id,
    client_secret=client_secret,
    refresh_token=refresh_token,
    company_domain=company_domain
)

# Get styles
styles = api.get_all_styles(folder_name="KTB")

# Get master data
access_token = api.oauth2_client.get_access_token()
# Fetch from /api/{company}/MasterData/{field_id}
```

## Integration Workflows

### Full Sync: DTC → Delta Lake
1. Load `dtc-integration` skill
2. Connect to DTC
3. Fetch request and views
4. Convert to Spark DataFrame
5. Apply business logic
6. Write to Delta Lake with change tracking

See: `dtc/notebooks/pull_dtc_to_delta.py`

### Full Sync: BeProduct → Delta Lake
1. Load `beproduct-integration` skill
2. Connect with BeProduct SDK
3. Fetch all styles from folder
4. Map fields to table columns
5. Convert to Spark DataFrame
6. Write to Delta Lake

See: `beproduct/beproduct_style_sync.py`

### Push Changes: Delta Lake → BeProduct
1. Load `beproduct-integration` skill
2. Query modified styles from Delta
3. Fetch master data for validation
4. Validate field values
5. Push updates to BeProduct
6. Log results

See: `beproduct/beproduct_style_push.py`

## Project Documentation

Additional documentation in the repository:
- `README.md` - Project overview and architecture
- `QUICK_START.md` - Setup and first run
- `QUICK_REFERENCE.md` - All jobs and parameters
- `dtc/README.md` - DTC sync platform details
- `dtc/DATA_MODEL.md` - DTC data model
- `dtc/CHANGE_TRACKING_DESIGN.md` - Change tracking design
- `MASTER_DATA_SETUP.md` - BeProduct master data setup
- `PUSH_SETUP.md` - BeProduct push configuration

## Support

For skill-related questions:
1. Load the relevant skill using the skill tool
2. Refer to the **Troubleshooting** section
3. Check **Best Practices** for recommended patterns
4. Review **Complete Workflows** for end-to-end examples

For project-specific issues:
- See project documentation in root directory
- Review existing notebooks for working examples
- Check `.env.example` for required configuration

## Skill Maintenance

To update skills:
1. Edit the `SKILL.md` file in the skill directory
2. Keep examples aligned with actual project code
3. Update field IDs and endpoints as BeProduct/DTC APIs evolve
4. Add new patterns discovered during implementation

## Version

**Created:** 2026-06-09  
**Skills:** 3 (databricks-integration, dtc-integration, beproduct-integration)  
**Total Lines:** 2,571 lines of documentation  
**Status:** Production Ready
