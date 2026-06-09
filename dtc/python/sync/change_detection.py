"""
Change detection for DTC bi-directional sync.

Handles:
- Comparing current state with last snapshot
- Detecting INSERT/UPDATE/DELETE operations
- Logging changes to audit trail
"""

import pandas as pd
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple
from pyspark.sql import SparkSession


class ChangeDetector:
    """Detect and log changes between snapshots."""
    
    def __init__(self, spark: SparkSession, changes_table: str):
        """
        Initialize ChangeDetector.
        
        Args:
            spark: Spark session
            changes_table: Full table path (e.g., lft.beproduct.dtc_master_chart_changes_uat)
        """
        self.spark = spark
        self.changes_table = changes_table
    
    def detect_changes(
        self,
        request_id: str,
        current_df: pd.DataFrame,
        previous_df: pd.DataFrame
    ) -> List[Dict]:
        """
        Detect changes between current and previous state.
        
        Returns list of changes with operation type and column diffs.
        
        Args:
            request_id: DTC request ID
            current_df: Current data (Pandas DataFrame)
            previous_df: Previous snapshot data (Pandas DataFrame)
            
        Returns:
            List of change dicts with keys:
            - change_id (UUID)
            - row_id
            - operation ('INSERT', 'UPDATE', 'DELETE')
            - columns_changed (dict of {col: {old_value, new_value}})
            - detected_timestamp
        """
        
        changes = []
        
        # Ensure row_id is available for comparison
        if 'row_id' not in current_df.columns and 'row_id' not in previous_df.columns:
            raise ValueError("Neither current nor previous data has 'row_id' column")
        
        # Convert row_id to string for consistent comparison
        if 'row_id' in current_df.columns:
            current_df = current_df.copy()
            current_df['row_id'] = current_df['row_id'].astype(str)
        
        if 'row_id' in previous_df.columns:
            previous_df = previous_df.copy()
            previous_df['row_id'] = previous_df['row_id'].astype(str)
        
        # Build index of previous rows
        prev_index = {}
        if 'row_id' in previous_df.columns:
            for idx, row in previous_df.iterrows():
                prev_index[row['row_id']] = row
        
        # Columns to skip in comparison (metadata)
        skip_cols = {
            'sync_timestamp', 'sync_date', 'fetched_at',
            'request_id', 'request_reference', 'request_description',
            'document_name', 'request_status', 'request_is_active',
            'updated_at'
        }
        
        # Get data columns to compare
        data_cols = [c for c in current_df.columns if c not in skip_cols and c != 'row_id']
        
        # Process current rows (UPDATEs and INSERTs)
        if 'row_id' in current_df.columns:
            current_ids = set()
            
            for idx, current_row in current_df.iterrows():
                row_id = str(current_row['row_id'])
                current_ids.add(row_id)
                
                if row_id in prev_index:
                    # Existing row - check for updates
                    prev_row = prev_index[row_id]
                    changed_cols = {}
                    
                    for col in data_cols:
                        curr_val = current_row.get(col)
                        prev_val = prev_row.get(col)
                        
                        # Compare (handle NaN/None)
                        curr_is_null = pd.isna(curr_val)
                        prev_is_null = pd.isna(prev_val)
                        
                        if curr_is_null and prev_is_null:
                            continue  # Both null, no change
                        elif curr_is_null or prev_is_null or curr_val != prev_val:
                            # Changed
                            changed_cols[col] = {
                                'old_value': str(prev_val) if not prev_is_null else None,
                                'new_value': str(curr_val) if not curr_is_null else None
                            }
                    
                    if changed_cols:
                        changes.append({
                            'change_id': str(uuid.uuid4()),
                            'request_id': request_id,
                            'row_id': row_id,
                            'operation': 'UPDATE',
                            'detected_timestamp': datetime.now(timezone.utc),
                            'columns_changed': changed_cols,
                            'change_source': 'databricks',
                            'status': 'pending'
                        })
                else:
                    # New row (INSERT)
                    new_cols = {}
                    for col in data_cols:
                        new_cols[col] = {
                            'old_value': None,
                            'new_value': str(current_row.get(col))
                        }
                    
                    changes.append({
                        'change_id': str(uuid.uuid4()),
                        'request_id': request_id,
                        'row_id': row_id,
                        'operation': 'INSERT',
                        'detected_timestamp': datetime.now(timezone.utc),
                        'columns_changed': new_cols,
                        'change_source': 'databricks',
                        'status': 'pending'
                    })
        
        # Process deleted rows (DELETEs)
        prev_ids = set(prev_index.keys())
        current_ids_set = set()
        
        if 'row_id' in current_df.columns:
            current_ids_set = set(str(rid) for rid in current_df['row_id'].unique())
        
        for deleted_row_id in prev_ids - current_ids_set:
            changes.append({
                'change_id': str(uuid.uuid4()),
                'request_id': request_id,
                'row_id': deleted_row_id,
                'operation': 'DELETE',
                'detected_timestamp': datetime.now(timezone.utc),
                'columns_changed': {},
                'change_source': 'databricks',
                'status': 'pending'
            })
        
        return changes
    
    def store_changes(self, changes: List[Dict]) -> int:
        """
        Store detected changes in the change log table.
        
        Args:
            changes: List of change dicts
            
        Returns:
            Number of changes stored
        """
        if not changes:
            return 0
        
        # Convert to DataFrame
        changes_df = self.spark.createDataFrame(changes)
        
        # Append to changes table
        changes_df.write.mode("append").insertInto(self.changes_table)
        
        return len(changes)
    
    def get_pending_changes(self, request_id: str) -> List[Dict]:
        """
        Retrieve all pending changes for a request.
        
        Args:
            request_id: DTC request ID
            
        Returns:
            List of pending change dicts
        """
        query = f"""
        SELECT *
        FROM {self.changes_table}
        WHERE request_id = '{request_id}'
        AND status = 'pending'
        ORDER BY detected_timestamp ASC
        """
        
        result = self.spark.sql(query).collect()
        
        changes = []
        for row in result:
            changes.append({
                'change_id': row.change_id,
                'request_id': row.request_id,
                'row_id': row.row_id,
                'operation': row.operation,
                'detected_timestamp': row.detected_timestamp,
                'columns_changed': row.columns_changed or {},
                'change_source': row.change_source,
                'status': row.status
            })
        
        return changes
    
    def mark_as_pushed(
        self,
        change_id: str,
        response: Optional[Dict] = None
    ) -> None:
        """
        Mark a change as successfully pushed to DTC.
        
        Args:
            change_id: UUID of the change
            response: Optional response from DTC API
        """
        update_query = f"""
        UPDATE {self.changes_table}
        SET status = 'pushed',
            push_timestamp = current_timestamp(),
            push_response = {self.spark._jvm.java.lang.String(str(response or {}))}
        WHERE change_id = '{change_id}'
        """
        
        self.spark.sql(update_query)
    
    def mark_as_conflict(
        self,
        change_id: str,
        reason: str
    ) -> None:
        """
        Mark a change as having a conflict.
        
        Args:
            change_id: UUID of the change
            reason: Conflict reason
        """
        update_query = f"""
        UPDATE {self.changes_table}
        SET status = 'conflict',
            conflict_reason = '{reason}',
            push_timestamp = current_timestamp()
        WHERE change_id = '{change_id}'
        """
        
        self.spark.sql(update_query)
    
    def mark_as_rejected(
        self,
        change_id: str,
        reason: str
    ) -> None:
        """
        Mark a change as rejected (failed push).
        
        Args:
            change_id: UUID of the change
            reason: Rejection reason
        """
        update_query = f"""
        UPDATE {self.changes_table}
        SET status = 'rejected',
            conflict_reason = '{reason}',
            push_timestamp = current_timestamp()
        WHERE change_id = '{change_id}'
        """
        
        self.spark.sql(update_query)
    
    def get_change_summary(self, request_id: str) -> Dict:
        """
        Get summary of all changes by status and operation.
        
        Args:
            request_id: DTC request ID
            
        Returns:
            Dict with change counts
        """
        query = f"""
        SELECT
          status,
          operation,
          COUNT(*) as count
        FROM {self.changes_table}
        WHERE request_id = '{request_id}'
        GROUP BY status, operation
        """
        
        result = self.spark.sql(query).toPandas()
        
        summary = {
            'total': 0,
            'pending': 0,
            'pushed': 0,
            'conflict': 0,
            'rejected': 0,
            'by_operation': {
                'INSERT': 0,
                'UPDATE': 0,
                'DELETE': 0
            }
        }
        
        for _, row in result.iterrows():
            status = row['status']
            operation = row['operation']
            count = row['count']
            
            summary['total'] += count
            summary[status] = summary.get(status, 0) + count
            summary['by_operation'][operation] = summary['by_operation'].get(operation, 0) + count
        
        return summary
