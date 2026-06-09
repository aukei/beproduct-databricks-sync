"""
Snapshot management for DTC bi-directional sync.

Handles:
- Creating snapshots (hash of data after pull)
- Storing snapshot metadata
- Retrieving previous snapshots for comparison
"""

import hashlib
import pandas as pd
from datetime import datetime, timezone
from typing import Tuple, Dict, Optional
from pyspark.sql import SparkSession


class SnapshotManager:
    """Manage snapshots for change detection."""
    
    def __init__(self, spark: SparkSession, metadata_table: str):
        """
        Initialize SnapshotManager.
        
        Args:
            spark: Spark session
            metadata_table: Full table path (e.g., lft.beproduct.dtc_sync_metadata_uat)
        """
        self.spark = spark
        self.metadata_table = metadata_table
    
    def create_snapshot(
        self,
        request_id: str,
        environment: str,
        data_df,
        document_name: str,
        row_count: int,
        details: Optional[Dict] = None
    ) -> Tuple[str, Dict]:
        """
        Create a snapshot hash after pulling from DTC.
        
        This becomes the baseline for detecting future changes.
        
        Args:
            request_id: DTC request ID
            environment: 'uat' or 'prod'
            data_df: PySpark DataFrame with data
            document_name: Document name from DTC
            row_count: Number of rows pulled
            details: Optional details dict
            
        Returns:
            (snapshot_hash, metadata_record)
        """
        
        # Convert to Pandas for hashing
        pdf = data_df.toPandas()
        
        # Identify data columns (exclude sync metadata columns)
        sync_cols = {'sync_timestamp', 'sync_date', 'fetched_at', 'request_id', 
                     'request_reference', 'request_description', 'document_name',
                     'request_status', 'request_is_active', 'updated_at'}
        
        data_cols = [c for c in pdf.columns if c not in sync_cols]
        
        # Sort by row_id to ensure consistent hashing
        if 'row_id' in pdf.columns:
            pdf = pdf.sort_values('row_id')
        
        # Create hash of data columns as CSV
        content = pdf[data_cols].to_csv(index=False)
        snapshot_hash = hashlib.sha256(content.encode()).hexdigest()
        
        # Create metadata record
        metadata_record = {
            'request_id': request_id,
            'environment': environment,
            'sync_direction': 'pull',
            'sync_timestamp': datetime.now(timezone.utc),
            'snapshot_hash': snapshot_hash,
            'row_count': row_count,
            'document_name': document_name,
            'details': details or {}
        }
        
        return snapshot_hash, metadata_record
    
    def store_snapshot(self, metadata_record: Dict) -> None:
        """
        Store snapshot metadata in the metadata table.
        
        Args:
            metadata_record: Dict with snapshot data
        """
        # Convert to DataFrame for insert
        record_df = self.spark.createDataFrame([metadata_record])
        
        # Append to metadata table
        record_df.write.mode("append").insertInto(self.metadata_table)
    
    def get_last_snapshot(
        self,
        request_id: str,
        sync_direction: str = 'pull'
    ) -> Optional[Dict]:
        """
        Retrieve the most recent snapshot for a request.
        
        Args:
            request_id: DTC request ID
            sync_direction: 'pull' or 'push'
            
        Returns:
            Dict with snapshot metadata or None
        """
        query = f"""
        SELECT *
        FROM {self.metadata_table}
        WHERE request_id = '{request_id}'
        AND sync_direction = '{sync_direction}'
        ORDER BY sync_timestamp DESC
        LIMIT 1
        """
        
        result = self.spark.sql(query).collect()
        
        if not result:
            return None
        
        row = result[0]
        return {
            'request_id': row.request_id,
            'environment': row.environment,
            'sync_direction': row.sync_direction,
            'sync_timestamp': row.sync_timestamp,
            'snapshot_hash': row.snapshot_hash,
            'row_count': row.row_count,
            'document_name': row.document_name,
            'details': row.details or {}
        }
    
    def get_snapshot_data(
        self,
        request_id: str,
        data_table: str
    ) -> Optional[pd.DataFrame]:
        """
        Retrieve the actual data at a snapshot point.
        
        For change detection, we need to compare with row data from the snapshot.
        Since we store only the hash, we fetch from the current table
        (in production, you'd want to keep versioned snapshots).
        
        Args:
            request_id: DTC request ID
            data_table: Full path to data table
            
        Returns:
            Pandas DataFrame or None
        """
        # Get most recent snapshot
        snapshot = self.get_last_snapshot(request_id, 'pull')
        if not snapshot:
            return None
        
        # Fetch data from the data table
        query = f"""
        SELECT *
        FROM {data_table}
        WHERE request_id = '{request_id}'
        """
        
        df = self.spark.sql(query).toPandas()
        return df if len(df) > 0 else None
    
    def compare_snapshots(
        self,
        previous_hash: str,
        current_hash: str
    ) -> bool:
        """
        Compare two snapshot hashes.
        
        Args:
            previous_hash: Previous snapshot hash
            current_hash: Current snapshot hash
            
        Returns:
            True if hashes match (no changes), False otherwise
        """
        return previous_hash == current_hash
