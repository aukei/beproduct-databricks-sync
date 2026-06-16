"""
DTC API Connector for pulling requests and sheet data.

Provides methods to:
- Fetch a specific request by ID
- Get available views for a request
- Fetch sheet data from a specific view
- Convert to Pandas DataFrame for Databricks
"""

import json
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
import pandas as pd

from client.rest_client import RestClient

logger = logging.getLogger(__name__)


class DTCConnector:
    """Connector for DTC (Data Collaboration Application) API."""

    def __init__(
        self,
        api_key: str,
        environment: str = "uat",
        workspace_name: str = "KTB",
    ):
        """
        Initialize DTC connector.

        Args:
            api_key: DTC API key
            environment: "uat" or "prod"
            workspace_name: Default workspace name
        """
        env_map = {
            "uat": "https://dtc-api.lfuat.net",
            "prod": "https://dtc-api.lfapps.net",
        }
        base_url = env_map.get(environment.lower(), "https://dtc-api.lfuat.net")

        self.client = RestClient(
            base_url=f"{base_url}/api",
            api_key=api_key,
            timeout=30,
        )
        self.workspace_name = workspace_name
        logger.info(
            f"DTCConnector initialized: workspace={workspace_name}, env={environment}"
        )

    def get_request(self, request_id: str) -> Dict[str, Any]:
        """
        Get a single request by ID.

        Args:
            request_id: DTC request ID

        Returns:
            Request details dict
        """
        logger.info(f"Fetching request: {request_id}")
        return self.client.get(f"/v1/requests/{request_id}")

    def get_views(self, request_id: str) -> List[Dict[str, str]]:
        """
        Get all available views for a request.

        Args:
            request_id: DTC request ID

        Returns:
            List of views with viewId and viewName
        """
        logger.info(f"Fetching views for request: {request_id}")
        response = self.client.get(f"/v1/requests/{request_id}/views")
        return response.get("data", [])

    def get_sheet(
        self, sheet_id: str, view_id: str, filters: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Get sheet data for a specific view.

        Args:
            sheet_id: DTC sheet ID
            view_id: DTC view ID
            filters: Optional filters dict

        Returns:
            Sheet data dict with sheetData array
        """
        logger.info(f"Fetching sheet: {sheet_id}, view: {view_id}")
        return self.client.get(f"/v1/sheets/{sheet_id}/views/{view_id}")

    @staticmethod
    def parse_request_name(request_reference: str) -> Dict[str, str]:
        """
        Parse request name to extract customer, seasonCode, and brand.
        
        Format: <customer> <seasonCode> <brand>
        Example: "KTB SS28 Wrangler Western" → 
                 {dtc_customer: "KTB", season_code: "SS28", brand: "Wrangler Western"}
        
        Args:
            request_reference: Request name from DTC (e.g., "KTB SS28 Wrangler Western")
            
        Returns:
            Dict with keys: dtc_customer, season_code, brand
            
        Raises:
            ValueError: If request name doesn't match expected pattern
        """
        parts = request_reference.strip().split()
        
        if len(parts) < 3:
            raise ValueError(
                f"Request name '{request_reference}' doesn't match pattern "
                "<customer> <seasonCode> <brand>"
            )
        
        dtc_customer = parts[0]
        season_code = parts[1]
        brand = " ".join(parts[2:])  # Brand can be multiple words
        
        return {
            "dtc_customer": dtc_customer,
            "season_code": season_code,
            "brand": brand
        }

    def get_document_metadata(self, request_id: str) -> Dict[str, Any]:
        """
        Get Document metadata for a request.
        
        Document is the schema definition. A Request is an instance of a Document.
        Views are column projections defined on a Document and auto-apply to all Requests.
        
        Args:
            request_id: DTC request ID
            
        Returns:
            Document metadata dict with schema info
        """
        req = self.get_request(request_id)
        request_reference = req.get("requestReference", "")
        
        # Parse request name to extract customer, seasonCode, brand
        parsed = {}
        try:
            parsed = self.parse_request_name(request_reference)
        except ValueError as e:
            logger.warning(f"Could not parse request name: {e}")
        
        return {
            "document_name": req.get("documentName"),
            "request_id": req.get("requestId"),
            "request_reference": request_reference,
            "request_description": req.get("requestDescription"),
            "workspace_name": req.get("workspaceName"),
            "sheet_id": req.get("sheetId"),
            "request_status": req.get("requestStatusName"),
            "request_is_active": req.get("requestIsActive"),
            "owner_name": req.get("ownerName"),
            "owner_email": req.get("ownerUserEmail", req.get("ownerEmail")),
            "created_at": req.get("createdDat"),
            "updated_at": req.get("updatedDat"),
            # Parsed from request name
            "dtc_customer": parsed.get("dtc_customer"),
            "season_code": parsed.get("season_code"),
            "brand": parsed.get("brand"),
        }

    def pull_request_to_dataframe(
        self, request_id: str, view_id: str
    ) -> tuple:
        """
        Pull a specific request's sheet data and convert to DataFrame.
        
        Returns both the data and document metadata for Delta table properties.

        Args:
            request_id: DTC request ID
            view_id: DTC view ID

        Returns:
            Tuple of (DataFrame, document_metadata_dict)
            - DataFrame: Row data with metadata columns
            - dict: Document metadata for Delta table properties
        """
        # Get request metadata
        req = self.get_request(request_id)
        sheet_id = req.get("sheetId")

        if not sheet_id:
            raise ValueError(f"Request {request_id} has no sheetId")

        # Get sheet data
        sheet = self.get_sheet(sheet_id, view_id)

        # Parse request name to extract customer, seasonCode, brand
        request_reference = req.get("requestReference", "")
        parsed = {}
        try:
            parsed = self.parse_request_name(request_reference)
        except ValueError as e:
            logger.warning(f"Could not parse request name: {e}")

        # Extract metadata (for row columns - only non-null, non-singleton values)
        # Note: workspace_name, owner_name, owner_email are metadata about the Request itself
        # and don't vary per row, so they're stored as table properties instead
        metadata = {
            "request_id": req.get("requestId"),
            "request_reference": request_reference,
            "request_description": req.get("requestDescription"),
            "document_name": req.get("documentName"),
            "request_status": req.get("requestStatusName"),
            "request_is_active": req.get("requestIsActive"),
            "updated_at": req.get("updatedDat"),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            # Parsed from request name
            "dtc_customer": parsed.get("dtc_customer"),
            "season_code": parsed.get("season_code"),
            "brand": parsed.get("brand"),
        }

        # Convert sheet data to DataFrame
        rows = sheet.get("sheetData", [])
        logger.info(f"Converting {len(rows)} rows to DataFrame")

        # Flatten: each row becomes one DataFrame row with metadata
        data = []
        for row in rows:
            flat_row = {**metadata, **row}
            # Ensure rowIndex and rowId are captured
            flat_row["row_index"] = row.get("rowIndex")
            flat_row["row_id"] = row.get("rowId")
            data.append(flat_row)

        df = pd.DataFrame(data)

        # Normalize column names (remove HTML tags, clean up)
        df.columns = [self._normalize_column_name(col) for col in df.columns]

        logger.info(f"Created DataFrame with {len(df)} rows, {len(df.columns)} columns")
        
        # Get document metadata separately for table properties
        doc_metadata = self.get_document_metadata(request_id)
        
        return df, doc_metadata

    @staticmethod
    def _normalize_column_name(name: str) -> str:
        """
        Normalize column names for Delta Lake compatibility.
        
        Delta Lake restricts column names: alphanumeric, underscores, backticks allowed.
        Removes HTML display markup and replaces invalid characters.
        
        This function:
        - Removes HTML tags (e.g., <BR/>, </>, etc.)
        - Replaces spaces, dashes, and special characters with underscores
        - Cleans up multiple consecutive underscores
        - Preserves alphanumeric characters and underscores

        Args:
            name: Original column name (may contain HTML, spaces, special chars)

        Returns:
            Delta-compatible column name
        """
        import re
        
        # Step 1: Remove HTML tags completely: <BR/>, </>, etc.
        normalized = re.sub(r'<[^>]+>', '', name)
        
        # Step 2: Replace spaces, dashes, and invalid characters with underscores
        # Keep only: alphanumeric, underscores, dots (for decimals)
        # Replace everything else with underscore
        normalized = re.sub(r'[^\w.]', '_', normalized)
        
        # Step 3: Clean up multiple consecutive underscores to single underscore
        normalized = re.sub(r'_+', '_', normalized)
        
        # Step 4: Remove leading/trailing underscores
        normalized = normalized.strip('_')
        
        # Step 5: Ensure name is not empty
        if not normalized:
            normalized = 'column'
        
        return normalized

    # ------------------------------------------------------------------
    # WRITE CONTRACT (validated live against DTC UAT on 2026-06-17)
    # ------------------------------------------------------------------
    # The sheet write API is a single endpoint:
    #
    #     PATCH /v1/sheets/{sheetId}/views/{viewId}
    #     body: {"sheetData": [ <rowObject>, ... ]}        -> HTTP 204 on success
    #
    # Each <rowObject> is a flat dict of {<DTC column display name>: value}, plus:
    #   - "rowId":   <uuid>  -> UPDATE that existing row
    #   - "rowIndex": <int>  -> INSERT a new row at that index
    # Provide exactly one of rowId / rowIndex per row object.
    #
    # IMPORTANT: every key in the row object MUST be a column that exists in the
    # view's mapping, otherwise the whole call fails with HTTP 400:
    #     "'<col>' is not found in the mapping."
    # Use get_view_column_names() to filter payloads before sending.
    #
    # There is NO row DELETE endpoint exposed to this API key (all variants 404),
    # and the older {"columnValues": ...} body shape is REJECTED ("sheetData is
    # required."). Phase 1 only performs INSERT/UPDATE, so delete is unsupported.
    # ------------------------------------------------------------------

    def create_row(
        self, sheet_id: str, view_id: str, row_values: Dict[str, Any], row_index: int
    ) -> Dict[str, Any]:
        """
        Insert a single new row via the validated PATCH/sheetData contract.

        Args:
            sheet_id: DTC sheet ID
            view_id: DTC view ID (e.g. the WIP_ITS_USE view)
            row_values: Dict of {DTC column name: value} (must be view columns)
            row_index: rowIndex to assign to the new row

        Returns:
            {"status_code": int} (endpoint returns 204 No Content)
        """
        return self.patch_rows(
            sheet_id, view_id, [{**row_values, "rowIndex": row_index}]
        )

    def update_row(
        self, sheet_id: str, view_id: str, row_id: str, row_values: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update a single existing row via the validated PATCH/sheetData contract.

        Args:
            sheet_id: DTC sheet ID
            view_id: DTC view ID
            row_id: DTC rowId (UUID) of the existing row
            row_values: Dict of {DTC column name: value} (must be view columns)

        Returns:
            {"status_code": int} (endpoint returns 204 No Content)
        """
        return self.patch_rows(
            sheet_id, view_id, [{**row_values, "rowId": row_id}]
        )

    def patch_rows(
        self, sheet_id: str, view_id: str, sheet_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Batch insert/update rows in one call (the native shape of the API).

        Args:
            sheet_id: DTC sheet ID
            view_id: DTC view ID
            sheet_data: List of row objects. Each must contain exactly one of
                        "rowId" (update) or "rowIndex" (insert), plus column values.

        Returns:
            {"status_code": int, "rows": int}

        Raises:
            ValueError: if a row object lacks both rowId and rowIndex.
        """
        has_row_id = has_row_index = False
        for i, row in enumerate(sheet_data):
            if "rowId" not in row and "rowIndex" not in row:
                raise ValueError(
                    f"sheet_data[{i}] must contain either 'rowId' (update) "
                    f"or 'rowIndex' (insert)"
                )
            has_row_id = has_row_id or ("rowId" in row)
            has_row_index = has_row_index or ("rowIndex" in row)
        # The API rejects a mix: "All rows must consistently use either rowId or
        # rowIndex as key, but not a mix of both." Send updates and inserts in
        # separate patch_rows() calls.
        if has_row_id and has_row_index:
            raise ValueError(
                "patch_rows() cannot mix rowId (update) and rowIndex (insert) "
                "in one call; send them as separate batches."
            )
        if not sheet_data:
            return {"status_code": 204, "rows": 0}
        logger.info(
            f"PATCH sheet {sheet_id} view {view_id}: {len(sheet_data)} row(s)"
        )
        resp = self.client.patch(
            f"/v1/sheets/{sheet_id}/views/{view_id}",
            data={"sheetData": sheet_data},
        )
        # 204 No Content -> RestClient returns {} ; normalise a small ack
        if not resp:
            resp = {"status_code": 204}
        resp.setdefault("rows", len(sheet_data))
        return resp

    def delete_row(self, sheet_id: str, row_id: str) -> Dict[str, Any]:
        """
        Not supported: the DTC API exposes no row-delete endpoint to this key
        (all known variants return 404), and Phase 1 performs no deletes.
        """
        raise NotImplementedError(
            "DTC API exposes no row-delete endpoint; deletes are out of scope "
            "for Phase 1 (upsert only)."
        )

    def get_view_column_names(self, sheet_id: str, view_id: str) -> List[str]:
        """
        Return the set of column display names available in a view's mapping.

        Derived from the keys present in the view's sheet data (excluding the
        rowId / rowIndex control keys). For an empty sheet this returns [] - in
        that case callers should fall back to a known column list for the
        Document, since the column set is a Document/view-level property.

        Args:
            sheet_id: DTC sheet ID
            view_id: DTC view ID

        Returns:
            Sorted list of column display names.
        """
        sheet = self.get_sheet(sheet_id, view_id)
        cols = set()
        for row in sheet.get("sheetData", []):
            cols.update(row.keys())
        cols.discard("rowId")
        cols.discard("rowIndex")
        return sorted(cols)
    
    def create_sheet(
        self,
        workspace_name: str,
        document_name: str,
        request_name: str,
        request_description: str = "",
        **kwargs
    ) -> Dict[str, str]:
        """
        Create new DTC Request and Sheet.
        
        POST /v1/sheets
        
        Args:
            workspace_name: DTC workspace name (e.g., "KTB", "KTB")
            document_name: Document name (e.g., "KTB WIP")
            request_name: Request name (e.g., "KTB SS26 Wrangler")
            request_description: Optional description
            **kwargs: Additional fields for the request
        
        Returns:
            {
                "requestId": "...",
                "sheetId": "...",
                ...
            }
        """
        logger.info(f"Creating sheet: workspace={workspace_name}, document={document_name}, name={request_name}")
        
        payload = {
            "workspaceName": workspace_name,
            "documentName": document_name,
            "requestName": request_name,
            "requestDescription": request_description,
            **kwargs
        }
        
        response = self.client.post("/v1/sheets", data=payload)
        logger.info(f"Created sheet: requestId={response.get('requestId')}, sheetId={response.get('sheetId')}")
        return response
    
    def search_requests(
        self,
        workspace_name: str,
        document_name: str = None
    ) -> List[Dict]:
        """
        Search for requests in a workspace via GET /v1/requests.

        NOTE (validated 2026-06-17): this collection endpoint is NOT usable with
        the current API key. It returns HTTP 400 "Invalid workspaceName." for
        every workspace value tried (KTB, Kontoor), and the related
        /v1/workspaces and /v1/documents/{id}/requests endpoints return 403.

        Phase 1 therefore does NOT rely on live listing for request discovery.
        In-scope requests are resolved from a maintained control/registry table
        (see dtc/notebooks/00_init_request_registry.py and get_request_scope()).
        This method is retained for environments/keys where listing is permitted.

        Args:
            workspace_name: DTC workspace name (the exact registered name)
            document_name: Optional document name to filter

        Returns:
            List of request dicts with requestId, requestReference, etc.
        """
        logger.info(f"Searching requests: workspace={workspace_name}, document={document_name}")

        params = {"workspaceName": workspace_name}
        if document_name:
            params["documentName"] = document_name

        response = self.client.get("/v1/requests", params=params)
        requests = response.get("data", []) if isinstance(response, dict) else (response or [])

        logger.info(f"Found {len(requests)} requests")
        return requests

    def get_request_scope(self, request_id: str) -> Dict[str, Any]:
        """
        Resolve a request into the control-table fields needed for Phase 1.

        Reads the request (and its Document-level views) and parses the request
        reference into (customer, season_code, brand). This is the per-request
        enrichment used to populate the registry/control table - it relies only
        on by-id reads, which ARE permitted with the current key.

        Args:
            request_id: DTC request ID

        Returns:
            Dict with: request_id, request_reference, document_name, sheet_id,
            wip_view_id, view_name, request_is_active, customer, season_code,
            brand, parse_ok, msg
        """
        req = self.get_request(request_id)
        ref = req.get("requestReference", "") or ""
        sheet_id = req.get("sheetId")

        wip_view_id, view_name = None, None
        try:
            views = self.get_views(request_id)
            wip = next((v for v in views if v.get("viewName") == "WIP_ITS_USE"), None)
            if wip:
                wip_view_id, view_name = wip.get("viewId"), "WIP_ITS_USE"
            elif views:
                wip_view_id, view_name = views[0].get("viewId"), views[0].get("viewName")
        except Exception as e:  # views are best-effort during enrichment
            logger.warning(f"Could not fetch views for {request_id}: {e}")

        customer = season_code = brand = None
        parse_ok, msg = False, ""
        try:
            parsed = self.parse_request_name(ref)
            customer = parsed["dtc_customer"]
            season_code = parsed["season_code"]
            brand = parsed["brand"]
            parse_ok = True
        except ValueError as e:
            msg = str(e)

        return {
            "request_id": req.get("requestId", request_id),
            "request_reference": ref,
            "document_name": req.get("documentName"),
            "sheet_id": sheet_id,
            "wip_view_id": wip_view_id,
            "view_name": view_name,
            "request_is_active": req.get("requestIsActive"),
            "customer": customer,
            "season_code": season_code,
            "brand": brand,
            "parse_ok": parse_ok,
            "msg": msg,
        }
    
    def get_max_row_index(self, sheet_id: str, view_id: str) -> int:
        """
        Get maximum rowIndex for a sheet.
        
        Args:
            sheet_id: DTC sheet ID
            view_id: DTC view ID
        
        Returns:
            Max rowIndex (int), or 0 if sheet is empty
        """
        logger.info(f"Getting max row index for sheet {sheet_id}")
        
        sheet = self.get_sheet(sheet_id, view_id)
        rows = sheet.get("sheetData", [])
        
        if not rows:
            return 0
        
        max_index = max(row.get("rowIndex", 0) for row in rows)
        logger.info(f"Max row index: {max_index}")
        return max_index
    
    def patch_row(
        self,
        sheet_id: str,
        view_id: str,
        column_values: Dict[str, Any],
        row_id: str = None,
        row_index: int = None
    ) -> Dict:
        """
        Update existing row (row_id) or insert a new row (row_index).

        Backwards-compatible single-row wrapper around patch_rows() using the
        validated PATCH/sheetData contract (see patch_rows docstring).

        Args:
            sheet_id: DTC sheet ID
            view_id: DTC view ID
            column_values: Dict of {DTC column name: value}
            row_id: For updating an existing row
            row_index: For inserting a new row (if row_id not provided)

        Returns:
            Response dict from DTC API ({"status_code": 204, ...} on success)
        """
        if not row_id and row_index is None:
            raise ValueError("Must provide either row_id or row_index")

        row = dict(column_values)
        if row_id:
            row["rowId"] = row_id
        else:
            row["rowIndex"] = row_index
        return self.patch_rows(sheet_id, view_id, [row])

    def close(self):
        """Close the connector."""
        self.client.close()
        logger.info("DTCConnector closed")
