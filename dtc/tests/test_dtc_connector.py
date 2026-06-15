#!/usr/bin/env python3
"""
Test script for DTCConnector.

Validates pulling data from DTC API and converting to DataFrame.
Can be run locally without Databricks.
"""

import sys
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add python modules to path
# Test is at databricks/dtc/tests/test_dtc_connector.py
# Modules are at databricks/dtc/python/
python_path = Path(__file__).parent.parent / "python"
sys.path.insert(0, str(python_path))

print("=" * 80)
print("DTC CONNECTOR TEST")
print("=" * 80)
print()

# Test 1: Import modules
print("[TEST 1] Import modules")
print("-" * 80)
try:
    from client.rest_client import RestClient
    from connectors.dtc import DTCConnector
    print("✅ Imports successful")
except ImportError as e:
    print(f"❌ Import failed: {e}")
    sys.exit(1)

# Test 2: Create connector
print("\n[TEST 2] Create DTCConnector")
print("-" * 80)
try:
    api_key = "49A127E0942071B4BD440DD00386C6B3"
    connector = DTCConnector(
        api_key=api_key,
        environment="uat",
        workspace_name="KTB"
    )
    print("✅ DTCConnector created")
except Exception as e:
    print(f"❌ Failed to create connector: {e}")
    sys.exit(1)

# Test 3: Get request
print("\n[TEST 3] Get request details")
print("-" * 80)
try:
    request_id = "69f076f0b7247a661226be9a"
    request = connector.get_request(request_id)
    print(f"✅ Request loaded")
    print(f"   ID: {request.get('requestId')}")
    print(f"   Reference: {request.get('requestReference')}")
    print(f"   Description: {request.get('requestDescription')}")
    print(f"   Status: {request.get('requestStatusName')}")
    print(f"   Sheet ID: {request.get('sheetId')}")
except Exception as e:
    print(f"❌ Failed to get request: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 4: Get views
print("\n[TEST 4] Get available views")
print("-" * 80)
try:
    views = connector.get_views(request_id)
    print(f"✅ Got {len(views)} views:")
    for i, view in enumerate(views[:5], 1):
        print(f"   {i}. {view.get('viewName')} ({view.get('viewId')})")
    if len(views) > 5:
        print(f"   ... and {len(views) - 5} more")
    
    # Use WIP_ITS_USE view
    view_id = None
    for v in views:
        if v.get("viewName") == "WIP_ITS_USE":
            view_id = v.get("viewId")
            break
    if not view_id and views:
        view_id = views[0].get("viewId")
    
    print(f"\n✅ Using view: {view_id}")
except Exception as e:
    print(f"❌ Failed to get views: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 5: Pull to DataFrame
print("\n[TEST 5] Pull to DataFrame")
print("-" * 80)
try:
    df, doc_metadata = connector.pull_request_to_dataframe(request_id, view_id)
    print(f"✅ DataFrame and Document metadata created")
    print(f"   Rows: {len(df)}")
    print(f"   Columns: {len(df.columns)}")
    print(f"\nDocument metadata:")
    print(f"   Document: {doc_metadata.get('document_name')}")
    print(f"   Request: {doc_metadata.get('request_reference')}")
    print(f"   Owner: {doc_metadata.get('owner_name')}")
    print(f"\nFirst few columns:")
    for col in list(df.columns)[:10]:
        print(f"   - {col}")
    if len(df.columns) > 10:
        print(f"   ... and {len(df.columns) - 10} more")
except Exception as e:
    print(f"❌ Failed to pull to DataFrame: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 6: Inspect data
print("\n[TEST 6] Inspect data")
print("-" * 80)
try:
    print(f"DataFrame info:")
    print(f"   Rows: {len(df)}")
    print(f"   Columns: {len(df.columns)}")
    print(f"   Memory: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
    
    # Show sample row
    print(f"\nFirst row sample:")
    row = df.iloc[0]
    for col in ["request_reference", "row_index", "row_id", "lf_style", "sync_timestamp", "fetched_at"]:
        if col in df.columns:
            print(f"   {col}: {row[col]}")
    
    # Count non-null values
    print(f"\nData sparsity (% non-null):")
    sparsity = (df.count() / len(df) * 100).sort_values(ascending=False)
    for col in sparsity.head(10).index:
        print(f"   {col}: {sparsity[col]:.1f}%")
        
except Exception as e:
    print(f"❌ Failed to inspect data: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 7: Save to CSV (optional)
print("\n[TEST 7] Save sample to CSV")
print("-" * 80)
try:
    csv_path = Path(__file__).parent / "data_samples" / "dtc_master_chart_sample.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Save first 3 rows and key columns
    key_cols = [col for col in df.columns if col in [
        "request_reference", "row_index", "row_id", "lf_style", 
        "style_description", "product_status", "quantity", "sync_timestamp"
    ]]
    if not key_cols:
        key_cols = list(df.columns)[:20]
    
    df[key_cols].head(3).to_csv(csv_path, index=False)
    print(f"✅ Saved sample to: {csv_path}")
except Exception as e:
    print(f"⚠️  Could not save CSV: {e}")

# Cleanup
print("\n[CLEANUP]")
print("-" * 80)
try:
    connector.close()
    print("✅ Connector closed")
except Exception as e:
    print(f"⚠️  Could not close connector: {e}")

print("\n" + "=" * 80)
print("✅ ALL TESTS PASSED")
print("=" * 80)
print("\nNext steps:")
print("1. Upload to Databricks workspace:")
print("   databricks workspace import-dir sync_hub /Workspace/Repos/YOUR_REPO/sync_hub")
print("\n2. Set up secrets:")
print("   databricks secrets create-scope --scope sync_hub")
print("   databricks secrets put --scope sync_hub --key dtc_api_key --string-value YOUR_KEY")
print("\n3. Run notebook in Databricks:")
print("   databricks runs submit --notebook-task notebook_path=/Workspace/Repos/YOUR_REPO/sync_hub/notebooks/pull_dtc_to_delta")
print()
