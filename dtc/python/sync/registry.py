"""
Shared DTC request-registry helpers (Phase 1 control table).

`lft.beproduct.dtc_request_registry` lists the in-scope DTC requests and their sync
state. `refresh()` (re)discovers requests in a workspace+document (or a
caller-supplied id list), enriches each by-id, and **upserts** them
(`mode="merge"` preserves `last_extracted` / `last_pushed` / `row_count`).

Design notes:
- The Spark session and DTC connector are passed in by the notebook.
- The pure helpers (`build_registry_row`, `discover_request_ids`) have **no Spark
  dependency** so they can be unit-tested locally; PySpark is imported lazily inside
  the Spark-only functions (`ensure_table`, `merge_rows`, `refresh`).
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from . import phase1

WIP_VIEW_NAME = "WIP_ITS_USE"

# The requests-list item carries an active flag (DTC dev: "IsActive"). Inactive
# requests 400 on get-by-id, so discovery drops them. Casing in the list may differ
# from the by-id field (`requestIsActive`), so check several spellings.
ACTIVE_FLAG_KEYS = ("requestIsActive", "isActive", "IsActive", "active", "requestActive")


def is_active_item(item: Dict[str, Any]) -> bool:
    """True unless the list item carries an explicit falsey active flag.

    Inactive requests (IsActive=false) 400 on get-by-id, so discovery skips them.
    If none of the known flag keys is present, default to True so a differently
    named field doesn't silently drop every request.
    """
    for k in ACTIVE_FLAG_KEYS:
        v = item.get(k)
        if v is not None:
            return str(v).strip().lower() in ("y", "true", "1", "yes")
    return True

# Column order of the registry table (also the order fed to createDataFrame).
REGISTRY_COLS = [
    "environment", "workspace_name", "document_name", "customer",
    "request_id", "sheet_id", "view_id", "view_name", "request_reference",
    "season_code", "brands", "request_is_active", "in_scope", "row_count",
    "last_extracted", "last_pushed", "msgs", "registered_at", "updated_at",
]


# ---------------------------------------------------------------------------
# Pure helpers (no Spark) - unit-testable
# ---------------------------------------------------------------------------

def discover_requests(connector, workspace: str, document: str) -> List[Dict[str, Any]]:
    """Return the distinct raw request dicts (deduped by requestId) from
    search_requests. Each carries at least requestId + requestReference, which lets
    callers pre-filter to in-scope names BEFORE doing any by-id read."""
    discovered = connector.search_requests(workspace, document_name=document)
    out: List[Dict[str, Any]] = []
    seen = set()
    for r in discovered:
        rid = r.get("requestId")
        if rid and rid not in seen:
            seen.add(rid)
            out.append(r)
    return out


def discover_request_ids(connector, workspace: str, document: str) -> List[str]:
    """Distinct requestIds in a workspace+document (see discover_requests)."""
    return [r["requestId"] for r in discover_requests(connector, workspace, document)]


def build_registry_row(
    scope: Optional[Dict[str, Any]],
    *,
    environment: str,
    workspace: str,
    document: str,
    customer: str,
    now: datetime,
    request_id: Optional[str] = None,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build one registry row dict from a `connector.get_request_scope()` result.

    Pass `error=<str>` (and `request_id`) instead of `scope` to record a request
    that could not be read. `in_scope` is computed from the parsed reference and the
    target customer (see `phase1.is_in_scope`).
    """
    if error is not None:
        return {
            "environment": environment, "workspace_name": workspace,
            "document_name": document, "customer": customer, "request_id": request_id,
            "sheet_id": None, "view_id": None, "view_name": None,
            "request_reference": None, "season_code": None, "brands": None,
            "request_is_active": None, "in_scope": False, "row_count": None,
            "last_extracted": None, "last_pushed": None,
            "msgs": f"read_error: {str(error)[:200]}",
            "registered_at": now, "updated_at": now,
        }

    ref = scope.get("request_reference") or ""
    in_scope = bool(scope.get("parse_ok")) and phase1.is_in_scope(ref, customer)
    if scope.get("view_name") != WIP_VIEW_NAME:
        msg = f"WARNING: {WIP_VIEW_NAME} view not found (using {scope.get('view_name')})"
    elif not in_scope:
        msg = f"OUT_OF_SCOPE for customer {customer} (ref={ref!r})"
    else:
        msg = "registered"

    return {
        "environment": environment, "workspace_name": workspace,
        "document_name": document, "customer": customer,
        "request_id": scope.get("request_id") or request_id,
        "sheet_id": scope.get("sheet_id"), "view_id": scope.get("wip_view_id"),
        "view_name": scope.get("view_name"), "request_reference": ref,
        "season_code": scope.get("season_code"), "brands": scope.get("brand"),
        "request_is_active": scope.get("request_is_active"), "in_scope": in_scope,
        "row_count": None, "last_extracted": None, "last_pushed": None,
        "msgs": msg, "registered_at": now, "updated_at": now,
    }


# ---------------------------------------------------------------------------
# Spark-dependent (PySpark imported lazily)
# ---------------------------------------------------------------------------

def _registry_schema():
    from pyspark.sql.types import (
        StructType, StructField, StringType, BooleanType, LongType, TimestampType,
    )
    return StructType([
        StructField("environment", StringType()), StructField("workspace_name", StringType()),
        StructField("document_name", StringType()), StructField("customer", StringType()),
        StructField("request_id", StringType()), StructField("sheet_id", StringType()),
        StructField("view_id", StringType()), StructField("view_name", StringType()),
        StructField("request_reference", StringType()), StructField("season_code", StringType()),
        StructField("brands", StringType()), StructField("request_is_active", StringType()),
        StructField("in_scope", BooleanType()), StructField("row_count", LongType()),
        StructField("last_extracted", TimestampType()), StructField("last_pushed", TimestampType()),
        StructField("msgs", StringType()), StructField("registered_at", TimestampType()),
        StructField("updated_at", TimestampType()),
    ])


def ensure_table(spark, registry_table: str) -> None:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {registry_table} (
      environment        STRING,
      workspace_name     STRING,
      document_name      STRING,
      customer           STRING,
      request_id         STRING,
      sheet_id           STRING,
      view_id            STRING,
      view_name          STRING,
      request_reference  STRING,
      season_code        STRING,
      brands             STRING,
      request_is_active  STRING,
      in_scope           BOOLEAN,
      row_count          BIGINT,
      last_extracted     TIMESTAMP,
      last_pushed        TIMESTAMP,
      msgs               STRING,
      registered_at      TIMESTAMP,
      updated_at         TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES ('description'='Phase 1 control table: in-scope DTC requests + sync state')
    """)


def merge_rows(spark, registry_table: str, rows: List[Dict[str, Any]], *, mode: str = "merge") -> int:
    """Upsert (merge) or overwrite (replace) the registry from row dicts."""
    data = [tuple(r.get(c) for c in REGISTRY_COLS) for r in rows]
    df = spark.createDataFrame(data, _registry_schema())
    if mode == "replace":
        (df.write.format("delta").mode("overwrite")
           .option("overwriteSchema", "true").saveAsTable(registry_table))
    else:
        # MERGE upsert by (environment, request_id). Preserve sync-state columns
        # (last_extracted / last_pushed / row_count) on update.
        df.createOrReplaceTempView("incoming_registry")
        spark.sql(f"""
            MERGE INTO {registry_table} t
            USING incoming_registry s
            ON t.environment = s.environment AND t.request_id = s.request_id
            WHEN MATCHED THEN UPDATE SET
              t.workspace_name = s.workspace_name, t.document_name = s.document_name,
              t.customer = s.customer, t.sheet_id = s.sheet_id, t.view_id = s.view_id,
              t.view_name = s.view_name, t.request_reference = s.request_reference,
              t.season_code = s.season_code, t.brands = s.brands,
              t.request_is_active = s.request_is_active, t.in_scope = s.in_scope,
              t.msgs = s.msgs, t.updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT *
        """)
    return len(rows)


def refresh(
    spark,
    connector,
    *,
    environment: str,
    workspace: str,
    document: str,
    customer: str,
    registry_table: str,
    request_ids: Optional[List[str]] = None,
    mode: str = "merge",
    now: Optional[datetime] = None,
    verbose: bool = True,
) -> Dict[str, Any]:
    """
    (Re)discover + enrich + upsert the request registry.

    - `request_ids` blank/None → auto-discover requests in workspace+document, then
      drop **inactive** items (IsActive=false; they 400 on get-by-id) and pre-filter
      on the listed `requestReference` so only ACTIVE + IN-SCOPE names are read by-id
      (`get_request`/`get_views`) and registered. Inactive and out-of-scope/foreign
      requests are skipped entirely (no by-id call → no HTTP 400, no registry rows).
    - `request_ids` given → enrich exactly those ids (targeted; no pre-filter).
    - Returns a summary: {discovered, skipped_inactive, skipped_out_of_scope,
      registered, in_scope, rows}.
    """
    now = now or datetime.now(timezone.utc)
    ensure_table(spark, registry_table)

    # Resolve which request ids to enrich by-id.
    #  - explicit request_ids (targeted) → enrich all of them (no pre-filter).
    #  - auto-discover → pre-filter on the LIST's requestReference and only enrich
    #    IN-SCOPE names, so we never call get_request on foreign/out-of-scope
    #    requests (which return HTTP 400) and never register them.
    discovered_n = 0
    skipped_oos = 0
    skipped_inactive = 0
    if request_ids:
        ids_to_enrich = list(request_ids)
        if verbose:
            print(f"   registry.refresh: enriching {len(ids_to_enrich)} explicit request id(s)")
    else:
        items = discover_requests(connector, workspace, document)
        discovered_n = len(items)
        # Drop inactive requests first (they 400 on get-by-id), then out-of-scope.
        active_items = [it for it in items if is_active_item(it)]
        skipped_inactive = discovered_n - len(active_items)
        ids_to_enrich = [
            it.get("requestId") for it in active_items
            if phase1.is_in_scope(it.get("requestReference") or "", customer)
        ]
        skipped_oos = len(active_items) - len(ids_to_enrich)
        if verbose:
            print(f"   registry.refresh: discovered {discovered_n} in workspace={workspace!r}, "
                  f"document={document!r}; active {len(active_items)} "
                  f"(skipped {skipped_inactive} inactive); in-scope {len(ids_to_enrich)} "
                  f"(skipped {skipped_oos} out-of-scope)")

    rows: List[Dict[str, Any]] = []
    for rid in ids_to_enrich:
        try:
            scope = connector.get_request_scope(rid)
        except Exception as e:  # keep going; record the read failure
            rows.append(build_registry_row(
                None, environment=environment, workspace=workspace, document=document,
                customer=customer, now=now, request_id=rid, error=str(e)))
            continue
        rows.append(build_registry_row(
            scope, environment=environment, workspace=workspace, document=document,
            customer=customer, now=now, request_id=rid))

    summary = {
        "discovered": discovered_n if not request_ids else len(ids_to_enrich),
        "skipped_inactive": skipped_inactive,
        "skipped_out_of_scope": skipped_oos,
        "registered": len(rows),
        "in_scope": sum(1 for r in rows if r.get("in_scope")),
        "rows": rows,
    }
    if rows:
        merge_rows(spark, registry_table, rows, mode=mode)
    if verbose:
        print(f"   registry.refresh: registered={summary['registered']} "
              f"in_scope={summary['in_scope']} skipped_inactive={skipped_inactive} "
              f"skipped_oos={skipped_oos} (mode={mode})")
    return summary
