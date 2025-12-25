import sys
import os
import json
from pathlib import Path
from unittest.mock import MagicMock

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from dagster_dag_factory.factory.asset_factory import AssetFactory
from dagster_dag_factory.models.s3_info import S3Info
from dagster_dag_factory.factory.helpers.dynamic import Dynamic

def test_unified_metadata_v4_payload():
    print("Testing Unified Metadata V4 (Niagara-Inspired Payload)...")
    
    # 1. Simulate Sensor finding an item
    item = S3Info(
        bucket_name="my-bucket",
        key="raw/sales/data.csv",
        size=12345,
        modified_dt=None # We'll skip for mock
    )
    
    # Simulate SensorFactory serializing it
    payload = json.dumps(item.to_dict())
    tags = {
        "factory/source_metadata": payload
    }
    
    # 2. Simulate AssetFactory receiving the tags
    factory = AssetFactory(Path("/tmp"))
    
    mock_context = MagicMock()
    mock_context.run.tags = tags
    
    # Internal logic of AssetFactory._get_template_vars
    template_vars = factory._get_template_vars(mock_context)
    
    # Simulated internal logic of AssetFactory.logic
    metadata_json = template_vars.get("run_tags", {}).get("factory/source_metadata")
    if metadata_json:
        template_vars["source"] = {
            "item": Dynamic(json.loads(metadata_json))
        }
            
    # 3. Verify high-fidelity reconstruction
    source_item = template_vars.get("source", {}).get("item")
    assert source_item is not None
    assert source_item.key == "raw/sales/data.csv"
    assert source_item.bucket_name == "my-bucket"
    assert source_item.size == 12345
    assert source_item.object_name == "data.csv"
    
    print("✅ High-fidelity payload reconstruction successful")

if __name__ == "__main__":
    try:
        test_unified_metadata_v4_payload()
        print("\n✨ All V4 metadata payload tests passed!")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
