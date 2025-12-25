import sys
import os
import yaml
import json
from pathlib import Path
from unittest.mock import MagicMock

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from dagster_dag_factory.factory.asset_factory import AssetFactory
from dagster_dag_factory.factory.helpers.rendering import render_config
from dagster_dag_factory.factory.helpers.dynamic import Dynamic

def simulate_smart_sensor_test():
    print("🧪 Simulating 'smart_sensor_test.yaml' with V4 Pattern...")
    
    yaml_path = "/home/ukatru/github/dagster-pipelines/src/pipelines/defs/smart_sensor_test.yaml"
    with open(yaml_path, "r") as f:
        pipeline_def = yaml.safe_load(f)
    
    asset_conf = pipeline_def["assets"][0]
    
    # Simulation Data (Discovered by S3 Sensor)
    discovered_item = {
        "bucket_name": "my-dagster-poc",
        "key": "raw/regional_sales/region=US/date=2023-01-01/sales.csv",
        "object_name": "sales.csv",
        "size": 1024
    }
    
    # 1. State as it would be in AssetFactory.logic
    template_vars = {
        "run_tags": {
            "factory/source_metadata": json.dumps(discovered_item)
        }
    }
    
    # 2. Deserialization logic (AssetFactory.logic)
    metadata_json = template_vars.get("run_tags", {}).get("factory/source_metadata")
    if metadata_json:
        template_vars["source"] = {
            "item": Dynamic(json.loads(metadata_json))
        }
    
    print(f"📊 Initial Source Key Template: {asset_conf['source']['configs']['key']}")
    print(f"📊 Initial Target Key Template: {asset_conf['target']['configs']['key']}")

    # 3. Render
    rendered_source_key = render_config(asset_conf["source"]["configs"]["key"], template_vars)
    rendered_target_key = render_config(asset_conf["target"]["configs"]["key"], template_vars)
    
    print(f"\n✨ Rendered Results:")
    print(f"   Source Key: {rendered_source_key}")
    print(f"   Target Key: {rendered_target_key}")
    
    assert rendered_source_key == "raw/regional_sales/region=US/date=2023-01-01/sales.csv"
    assert rendered_target_key == "processed/regional_sales/sales.csv"
    
    print("\n✅ Verification SUCCESS: Analyst experience is perfect!")

if __name__ == "__main__":
    try:
        simulate_smart_sensor_test()
    except Exception as e:
        print(f"\n❌ Simulation failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
