# Databricks notebook source
"""
Phase 1 Control Table: DTC Request Registry
===========================================

Creates and populates the Phase 1 control table (requirement 1a):

    lft.beproduct.dtc_request_registry

Discovery: by default (request_ids left blank) this notebook auto-discovers
EVERY request in the given workspace+document via DTCConnector.search_requests()
and registers them all, so the registry mirrors the document at sync time. Each
request is then enriched via by-id reads (get_request + get_views) and tagged
with in_scope (computed from its reference, see below). Pass request_ids
explicitly to restrict the run to specific requests (targeted re-registration).

Listing works because GET /v1/requests reads workspaceName + filters from the
JSON BODY (see DTCConnector.search_requests(); validated live). An earlier note
claimed the key could not list; that was a client bug from sending workspaceName
as a query param, not an API/permission limitation. Out-of-scope developer
requests (e.g. "KON ...") are still kept out of the pipeline because they are
recorded with in_scope = false and ignored by pull/push.

In scope == request reference parses as "<customer> <seasonCode> <brand>" AND its
customer token equals `customer` (e.g. "KTB FW26 Wrangler"). Developer test
requests such as "KON FW26 Wrangler" are out of scope and recorded with
in_scope = false (kept for audit, ignored by pull/push).

Control columns (per requirement 1a):
  request_id, view_id, customer, season_code, brands, last_extracted, msgs
plus operational fields (sheet_id, request_reference, row_count, last_pushed...).

Parameters:
  - request_ids: comma-separated DTC request IDs to register. OPTIONAL - leave
    blank to auto-discover all requests in dtc_workspace+dtc_document.
  - dtc_environment: uat | prod (default: uat)
  - customer: in-scope customer token (default: KTB)
  - dtc_workspace: DTC workspace name (default: KTB)
  - dtc_document: DTC document name (default: KTB WIP). Used as the documentName
    filter when auto-discovering.
  - catalog / schema: target location (default: lft / beproduct)
  - mode: "merge" (upsert by request_id, preserves last_extracted/last_pushed/
    row_count) or "replace" (full overwrite, wipes sync state). Default: merge.
    Use merge for routine/seasonal refreshes.
"""

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/beproduct-sync/DTC/python")

from connectors.dtc import DTCConnector
from sync import registry

dbutils.widgets.text("request_ids", "", "Request IDs (blank = auto-discover)")
dbutils.widgets.text("dtc_environment", "uat", "DTC Environment")
dbutils.widgets.text("customer", "KTB", "In-scope customer token")
dbutils.widgets.text("dtc_workspace", "KTB", "DTC Workspace Name")
dbutils.widgets.text("dtc_document", "KTB WIP", "DTC Document Name")
dbutils.widgets.text("catalog", "lft", "Catalog")
dbutils.widgets.text("schema", "beproduct", "Schema")
dbutils.widgets.text("mode", "merge", "merge | replace")

request_ids = [r.strip() for r in dbutils.widgets.get("request_ids").split(",") if r.strip()]
environment = dbutils.widgets.get("dtc_environment").strip().lower()
customer = dbutils.widgets.get("customer").strip()
workspace = dbutils.widgets.get("dtc_workspace").strip()
document = dbutils.widgets.get("dtc_document").strip()
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
mode = dbutils.widgets.get("mode").strip().lower()

registry_table = f"{catalog}.{schema}.dtc_request_registry"
auto_discover = len(request_ids) == 0

print("=" * 80)
print("DTC REQUEST REGISTRY (Phase 1 control table)")
print("=" * 80)
print(f"  Registry: {registry_table}")
print(f"  Environment: {environment} | Customer: {customer}")
print(f"  Workspace/Document: {workspace} / {document}")
print(f"  Discovery: {'AUTO (all requests in workspace+document)' if auto_discover else f'MANUAL ({len(request_ids)} request_ids)'}")
print(f"  Mode: {mode}")

# COMMAND ----------

# Discover (or use explicit ids), enrich by-id, and upsert — all via the shared
# registry helper so pull_requests_to_delta / dtc_request_manager reuse identical
# logic. request_ids blank → auto-discover the whole workspace+document.
secret_key = f"dtc_api_key_{environment}"
api_key = dbutils.secrets.get(scope="beproduct", key=secret_key)
connector = DTCConnector(api_key=api_key, environment=environment, workspace_name=workspace)

try:
    summary = registry.refresh(
        spark, connector,
        environment=environment, workspace=workspace, document=document,
        customer=customer, registry_table=registry_table,
        request_ids=(request_ids or None), mode=mode,
    )
finally:
    connector.close()

if summary["registered"] == 0:
    dbutils.notebook.exit("NO_REQUESTS_DISCOVERED")
print(f"✅ Registry {'REPLACED' if mode == 'replace' else 'MERGED'}: "
      f"registered={summary['registered']} in_scope={summary['in_scope']}")

# COMMAND ----------

print("\nIn-scope active requests in registry:")
spark.sql(f"""
  SELECT request_reference, season_code, brands, request_id, view_id,
         last_extracted, last_pushed, msgs
  FROM {registry_table}
  WHERE environment = '{environment}' AND in_scope = true
  ORDER BY request_reference
""").show(truncate=False)
print("✅ Registry init complete")
