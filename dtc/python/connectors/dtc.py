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
from urllib.parse import quote
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

    # ------------------------------------------------------------------
    # REQUEST SHARING
    # ------------------------------------------------------------------
    # A newly created request grants FULL rights to its creator only; for the
    # data to be visible to the team it must be explicitly SHARED. Validated
    # against the DTC Postman collection (Share Request):
    #
    #   POST /v1/requests/{requestId}/shares/{userEmail}
    #   POST /v1/requests/{requestId}/shares/usergroups/{userGroupName}
    #   body: {"viewNames": [...], "message": "...", "sendEmail": "Y"|"N"}
    #
    # The userEmail / userGroupName are PATH segments, so they are URL-encoded
    # (group names like "Fabric Group" contain spaces).
    # ------------------------------------------------------------------

    def get_request_shares(self, request_id: str) -> List[Dict[str, Any]]:
        """GET the users a request is shared with (best-effort; [] on error)."""
        try:
            resp = self.client.get(f"/v1/requests/{request_id}/shares")
            return resp.get("data", []) if isinstance(resp, dict) else (resp or [])
        except Exception as e:
            logger.warning(f"get_request_shares({request_id}) failed: {e}")
            return []

    def get_request_share_usergroups(self, request_id: str) -> List[Dict[str, Any]]:
        """GET the user groups a request is shared with (best-effort; [] on error)."""
        try:
            resp = self.client.get(f"/v1/requests/{request_id}/shares/usergroups")
            return resp.get("data", []) if isinstance(resp, dict) else (resp or [])
        except Exception as e:
            logger.warning(f"get_request_share_usergroups({request_id}) failed: {e}")
            return []

    def share_request_with_user(
        self,
        request_id: str,
        user_email: str,
        view_names: List[str],
        message: str = "",
        send_email: str = "N",
    ) -> Dict[str, Any]:
        """
        Share a request's views with a user (by email).

        POST /v1/requests/{requestId}/shares/{userEmail}

        Args:
            request_id: DTC request ID
            user_email: target user's email (path segment, URL-encoded)
            view_names: list of view display names to share
            message: optional notification message
            send_email: "Y" to email the user, "N" (default) to share silently

        Returns:
            Parsed response (or {"status_code": <code>}).
        """
        seg = quote(str(user_email), safe="")
        logger.info(
            f"Share request {request_id} -> user {user_email}: views={view_names}"
        )
        return self.client.post(
            f"/v1/requests/{request_id}/shares/{seg}",
            data={"viewNames": list(view_names), "message": message,
                  "sendEmail": send_email},
        )

    def share_request_with_usergroup(
        self,
        request_id: str,
        user_group_name: str,
        view_names: List[str],
        message: str = "",
        send_email: str = "N",
    ) -> Dict[str, Any]:
        """
        Share a request's views with a user GROUP (by group name).

        POST /v1/requests/{requestId}/shares/usergroups/{userGroupName}

        Args:
            request_id: DTC request ID
            user_group_name: target group name (path segment, URL-encoded;
                             e.g. "Fabric Group")
            view_names: list of view display names to share
            message: optional notification message
            send_email: "Y" to email the group, "N" (default) to share silently

        Returns:
            Parsed response (or {"status_code": <code>}).
        """
        seg = quote(str(user_group_name), safe="")
        logger.info(
            f"Share request {request_id} -> group {user_group_name!r}: views={view_names}"
        )
        return self.client.post(
            f"/v1/requests/{request_id}/shares/usergroups/{seg}",
            data={"viewNames": list(view_names), "message": message,
                  "sendEmail": send_email},
        )

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
    # The sheet upsert API is a single endpoint:
    #
    #     PATCH /v1/sheets/{sheetId}/views/{viewId}
    #     body: {"sheetData": [ <rowObject>, ... ]}        -> HTTP 204 on success
    #
    # Each <rowObject> is a flat dict of {<DTC column display name>: value}, plus:
    #   - "rowId":   <uuid>  -> UPDATE that existing row
    #   - "rowIndex": <int>  -> INSERT a new row at that index
    # Provide exactly one of rowId / rowIndex per row object. A single PATCH must
    # not mix rowId and rowIndex keys; send updates and inserts as separate calls.
    #
    # IMPORTANT: every key in the row object MUST be a column that exists in the
    # view's mapping, otherwise the whole call fails with HTTP 400:
    #     "'<col>' is not found in the mapping."
    # Use get_view_column_names() (which reads the VIEW DEFINITION, not just the
    # populated sheet cells) to filter payloads before sending.
    #
    # Rows ARE deletable (validated live 2026-06-17 by inserting then removing a
    # dummy row on the sacrificial KTB FW26 Wrangler request):
    #
    #     DELETE /v1/sheets/{sheetId}/views/{viewId}/rows
    #     body: {"rowIndexes": [1, 2, 3, 4]}               -> HTTP 204 on success
    #
    # Note deletes key off rowIndex (not rowId). The older {"columnValues": ...}
    # PATCH body shape is REJECTED ("sheetData is required.").
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

    def delete_rows(
        self, sheet_id: str, view_id: str, row_indexes: List[int]
    ) -> Dict[str, Any]:
        """
        Delete one or more rows from a sheet by their rowIndex.

        Validated live against DTC UAT (2026-06-17):
            DELETE /v1/sheets/{sheetId}/views/{viewId}/rows
            body: {"rowIndexes": [...]}   -> HTTP 204 No Content

        Note: the DTC delete endpoint keys off rowIndex, NOT rowId. Callers that
        only have rowIds should resolve them to rowIndex first (e.g. from the
        current sheet data).

        Args:
            sheet_id: DTC sheet ID
            view_id: DTC view ID
            row_indexes: list of rowIndex values to delete

        Returns:
            {"status_code": 204, "rows": <n>} on success.
        """
        if not row_indexes:
            return {"status_code": 204, "rows": 0}
        logger.info(
            f"DELETE rows on sheet {sheet_id} view {view_id}: {row_indexes}"
        )
        self.client.delete(
            f"/v1/sheets/{sheet_id}/views/{view_id}/rows",
            data={"rowIndexes": list(row_indexes)},
        )
        return {"status_code": 204, "rows": len(row_indexes)}

    def delete_row(self, sheet_id: str, view_id: str, row_index: int) -> Dict[str, Any]:
        """Delete a single row by rowIndex (thin wrapper over delete_rows())."""
        return self.delete_rows(sheet_id, view_id, [row_index])

    # ------------------------------------------------------------------
    # IMAGE WRITE CONTRACT (Phase 3 — LIVE-VALIDATED 2026-06-17, 41 uploads OK)
    # ------------------------------------------------------------------
    # Cell images (e.g. the "Style Image" column) are NOT settable through the
    # JSON sheetData PATCH used for normal columns; they are binary and use a
    # dedicated multipart endpoint, keyed on rowIndex (not rowId):
    #
    #     POST /v1/sheets/{sheetId}/views/{viewId}/images
    #          ?rowindex={int}&columnname={display name}
    #     body: multipart/form-data with the image bytes as a file part
    #
    # CONFIRMED live: the query param name is lowercase "rowindex"; columnname is
    # the column DISPLAY name ("Style Image"); the multipart file PART NAME is
    # "file" (below). jpg and png upload successfully.
    # CAVEAT: DTC REJECTS webp with HTTP 400 — convert/skip unsupported types
    # before calling this. (Separately, some BeProduct CDN URLs 403 on download;
    # that is a CDN/SAS issue upstream of this method.)
    # ------------------------------------------------------------------

    def upload_row_image(
        self,
        sheet_id: str,
        view_id: str,
        row_index: int,
        image_bytes: bytes,
        column_name: str = "Style Image",
        filename: str = "image.jpg",
        content_type: str = "image/jpeg",
        file_field: str = "file",
    ) -> Dict[str, Any]:
        """
        Upload a binary image into a single sheet cell (Phase 3).

        Args:
            sheet_id: DTC sheet ID
            view_id: DTC view ID (WIP_ITS_USE)
            row_index: target row's rowIndex (the image endpoint keys off rowindex)
            image_bytes: raw image content (already downloaded from BeProduct CDN)
            column_name: DTC column display name (default "Style Image")
            filename: filename for the multipart part
            content_type: MIME type of the image (e.g. image/jpeg, image/png)
            file_field: multipart field name (UNVALIDATED; default "file")

        Returns:
            Parsed response (or {"status_code": <code>}).
        """
        files = {file_field: (filename, image_bytes, content_type)}
        params = {"rowindex": row_index, "columnname": column_name}
        logger.info(
            f"Upload image: sheet {sheet_id} view {view_id} "
            f"rowindex={row_index} column={column_name!r} ({len(image_bytes)} bytes)"
        )
        return self.client.post_multipart(
            f"/v1/sheets/{sheet_id}/views/{view_id}/images",
            params=params,
            files=files,
        )

    def get_view_definition(self, view_id: str) -> Dict[str, Any]:
        """
        Get a single view record (its schema), including dynamicFields.

        GET /v1/views/{viewId}

        Args:
            view_id: DTC view ID

        Returns:
            View dict (with a "dynamicFields" list of {fieldName, type, ...}).
        """
        logger.info(f"Fetching view definition: {view_id}")
        resp = self.client.get(f"/v1/views/{view_id}")
        data = resp.get("data", resp) if isinstance(resp, dict) else resp
        if isinstance(data, list):
            data = data[0] if data else {}
        return data or {}

    def get_view_column_names(
        self, sheet_id: str, view_id: str
    ) -> List[str]:
        """
        Return the column display names defined in a view's mapping.

        IMPORTANT: this reads the authoritative VIEW DEFINITION
        (GET /v1/views/{viewId} -> dynamicFields[].fieldName), NOT the populated
        sheet cells. Deriving columns from sheet data badly under-reports the set
        because columns that happen to be empty across every row do not appear in
        the row objects (validated 2026-06-17: WIP_ITS_USE has 178 view columns
        but only ~96 surfaced in sheetData). Filtering payloads against the sheet
        view would silently drop valid-but-empty columns such as "Garment Finish",
        "Tech Pack Stage", "Legacy Code" and "Main Vendor (Sampling)".

        Falls back to scanning sheet data only if the view definition cannot be
        read (so an unexpected schema response never hard-fails a sync).

        Args:
            sheet_id: DTC sheet ID (used only for the fallback scan)
            view_id: DTC view ID

        Returns:
            Sorted list of column display names.
        """
        try:
            view = self.get_view_definition(view_id)
            cols = {
                f.get("fieldName")
                for f in view.get("dynamicFields", [])
                if f.get("fieldName")
            }
            if cols:
                return sorted(cols)
            logger.warning(
                f"View {view_id} definition had no dynamicFields; "
                "falling back to sheet-data column scan"
            )
        except Exception as e:
            logger.warning(
                f"Could not read view definition for {view_id} ({e}); "
                "falling back to sheet-data column scan"
            )

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
        view_name: str = "WIP_ITS_USE",
        sharing_view_names: Optional[List[str]] = None,
        sheet_data: Optional[List[Dict[str, Any]]] = None,
        request_status_name: Optional[str] = None,
        request_assignee_email: Optional[str] = None,
        **kwargs,
    ) -> Dict[str, str]:
        """
        Create a new DTC Request + Sheet.

        POST /v1/sheets  (VALIDATED LIVE 2026-06-18, HTTP 201)

        The endpoint's required body shape (per the DTC Postman collection and
        confirmed live) is:
          - `requestReference`  (NOT `requestName` — the old payload 400'd with
            "Request reference is required.")
          - `requestDescription` MUST be non-empty (empty string 400'd with
            "Request description is required."); defaults to request_name here.
          - `viewName`
          - `requestAssigneeSharingViewNames` and `sheetData` MUST be present as
            ARRAYS — omitting them crashed the server with 400 "Cannot read
            properties of undefined (reading 'map')". Empty arrays are accepted.
        Optional: `requestStatusName` (e.g. "Factory Allocation"),
        `requestAssigneeEmail`.

        Response (201) nests ids under `data` and uses a CAPITAL S `SheetId`:
            {"data": {"requestId": "...", "SheetId": "..."}}
        This method normalises that to a flat {"requestId", "sheetId", "raw"}.

        Args:
            workspace_name: DTC workspace name (e.g. "KTB")
            document_name: Document name new request is created under (e.g. "KTB WIP")
            request_name: The request reference (e.g. "KTB SS26 Wrangler")
            request_description: Description; defaults to request_name if blank
                                 (the API rejects an empty description).
            view_name: View the request is created for (default "WIP_ITS_USE")
            sharing_view_names: assignee sharing view names (default [])
            sheet_data: initial rows (default []; Phase 1 inserts rows later)
            request_status_name: optional request status (e.g. "Factory Allocation")
            request_assignee_email: optional assignee email
            **kwargs: extra body fields (override defaults)

        Returns:
            {"requestId": str|None, "sheetId": str|None, "raw": <response>}
        """
        logger.info(
            f"Creating sheet: workspace={workspace_name}, document={document_name}, "
            f"reference={request_name}"
        )

        payload: Dict[str, Any] = {
            "workspaceName": workspace_name,
            "documentName": document_name,
            "requestReference": request_name,
            # API requires a non-empty description.
            "requestDescription": request_description or request_name,
            "viewName": view_name,
            # These MUST be arrays or the server 400s on .map(); empty is fine.
            "requestAssigneeSharingViewNames": sharing_view_names or [],
            "sheetData": sheet_data or [],
        }
        if request_status_name:
            payload["requestStatusName"] = request_status_name
        if request_assignee_email:
            payload["requestAssigneeEmail"] = request_assignee_email
        payload.update(kwargs)

        response = self.client.post("/v1/sheets", data=payload)

        # Normalise the nested/oddly-cased response. Success body is
        # {"data": {"requestId": "...", "SheetId": "..."}}; tolerate flat shapes too.
        data = response.get("data", response) if isinstance(response, dict) else {}
        if not isinstance(data, dict):
            data = {}
        request_id = data.get("requestId") or data.get("RequestId")
        sheet_id = data.get("sheetId") or data.get("SheetId")
        logger.info(f"Created sheet: requestId={request_id}, sheetId={sheet_id}")
        return {"requestId": request_id, "sheetId": sheet_id, "raw": response}
    
    def search_requests(
        self,
        workspace_name: str,
        document_name: str = None,
        filters: Optional[Dict[str, Any]] = None,
        pending_only: str = "N",
        request_only: str = "N",
    ) -> List[Dict]:
        """
        Search/list requests in a workspace via GET /v1/requests.

        NOTE (corrected 2026-06-17): this endpoint reads workspaceName + filters
        from the JSON request BODY, not from query parameters. The earlier
        conclusion that "the key cannot list requests" was a client bug - sending
        workspaceName as a query param returns HTTP 400 "Invalid workspaceName.",
        whereas the documented body shape works. Validated live: body
        {"workspaceName":"KTB","filters":{}} returns 824 requests.

        Request discovery may use this directly; the registry table
        (dtc/notebooks/00_init_request_registry.py) remains a valid design choice
        for an explicit, auditable in-scope list but is no longer forced by an API
        limitation.

        Args:
            workspace_name: DTC workspace name (the exact registered name)
            document_name: Optional document name (added to filters as documentName)
            filters: Optional additional filter dict (per spec: requestReference,
                     requestDescription, documentName, collectionName, ownerEmail,
                     assigneeEmail, requestStatusName, requestIsActive, ...)
            pending_only: "Y"/"N" - filter to pending records only
            request_only: "Y"/"N" - filter to my-request records only

        Returns:
            List of request dicts with requestId, requestReference, etc.
        """
        logger.info(
            f"Searching requests: workspace={workspace_name}, document={document_name}"
        )

        body_filters: Dict[str, Any] = dict(filters or {})
        if document_name:
            body_filters["documentName"] = document_name

        body = {
            "pendingOnly": pending_only,
            "requestOnly": request_only,
            "workspaceName": workspace_name,
            "filters": body_filters,
        }

        response = self.client.get("/v1/requests", data=body)
        requests = (
            response.get("data", []) if isinstance(response, dict) else (response or [])
        )

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
