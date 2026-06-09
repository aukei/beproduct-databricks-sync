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

    def create_row(self, sheet_id: str, row_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new row in DTC sheet.
        
        Args:
            sheet_id: DTC sheet ID
            row_data: Dict of column_name -> value to insert
            
        Returns:
            Response from DTC API (includes rowId of new row)
        """
        logger.info(f"Creating row in sheet {sheet_id}")
        payload = {"columnValues": row_data}
        return self.client.post(f"/v1/sheets/{sheet_id}/rows", payload)
    
    def update_row(
        self,
        sheet_id: str,
        row_id: str,
        updates: Dict[str, Dict[str, str]]
    ) -> Dict[str, Any]:
        """
        Update an existing row in DTC sheet.
        
        Args:
            sheet_id: DTC sheet ID
            row_id: DTC row ID
            updates: Dict of column_name -> {old_value, new_value}
                     Will extract new_value for the PATCH
            
        Returns:
            Response from DTC API
        """
        logger.info(f"Updating row {row_id} in sheet {sheet_id}")
        
        # Extract new values from the change dict
        column_values = {}
        for col_name, change_info in updates.items():
            if isinstance(change_info, dict) and 'new_value' in change_info:
                column_values[col_name] = change_info['new_value']
            else:
                # If it's not a change dict, use it directly
                column_values[col_name] = change_info
        
        payload = {"columnValues": column_values}
        return self.client.patch(f"/v1/sheets/{sheet_id}/rows/{row_id}", payload)
    
    def delete_row(self, sheet_id: str, row_id: str) -> Dict[str, Any]:
        """
        Delete a row from DTC sheet.
        
        Args:
            sheet_id: DTC sheet ID
            row_id: DTC row ID
            
        Returns:
            Response from DTC API
        """
        logger.info(f"Deleting row {row_id} from sheet {sheet_id}")
        return self.client.delete(f"/v1/sheets/{sheet_id}/rows/{row_id}")
    
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
        
        response = self.client.post("/v1/sheets", json=payload)
        logger.info(f"Created sheet: requestId={response.get('requestId')}, sheetId={response.get('sheetId')}")
        return response
    
    def search_requests(
        self,
        workspace_name: str,
        document_name: str = None
    ) -> List[Dict]:
        """
        Search for requests in a workspace.
        
        GET /v1/requests?workspace={name}&document={name}
        
        Args:
            workspace_name: DTC workspace name
            document_name: Optional document name to filter
        
        Returns:
            List of request dicts with requestId, requestReference, etc.
        """
        logger.info(f"Searching requests: workspace={workspace_name}, document={document_name}")
        
        params = {"workspace": workspace_name}
        if document_name:
            params["document"] = document_name
        
        response = self.client.get("/v1/requests", params=params)
        requests = response.get("data", [])
        
        logger.info(f"Found {len(requests)} requests")
        return requests
    
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
        Update existing row or create new row using PATCH API.
        
        PATCH /v1/sheets/{sheetId}/views/{viewId}
        
        Per requirements (lines 80-85):
        - For existing rows: use rowId
        - For new rows: use rowIndex (max + 1)
        
        Args:
            sheet_id: DTC sheet ID
            view_id: DTC view ID
            column_values: Dict of {columnName: value}
            row_id: For updating existing row
            row_index: For creating new row (if row_id not provided)
        
        Returns:
            Response dict from DTC API
        """
        if not row_id and not row_index:
            raise ValueError("Must provide either row_id or row_index")
        
        payload = {"columnValues": column_values}
        
        if row_id:
            payload["rowId"] = row_id
            logger.info(f"Patching existing row {row_id} in sheet {sheet_id}")
        elif row_index:
            payload["rowIndex"] = row_index
            logger.info(f"Creating new row at index {row_index} in sheet {sheet_id}")
        
        response = self.client.patch(
            f"/v1/sheets/{sheet_id}/views/{view_id}",
            json=payload
        )
        
        return response

    def close(self):
        """Close the connector."""
        self.client.close()
        logger.info("DTCConnector closed")
