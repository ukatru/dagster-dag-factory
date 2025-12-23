import sys
import yaml
from pathlib import Path
from dagster import AssetsDefinition

# Add src to path
sys.path.append("/home/ukatru/github/dagster-dag-factory/src")

from dagster_dag_factory.factory.asset_factory import AssetFactory

def verify():
    factory = AssetFactory(base_dir=".")
    
    yaml_path = Path("/home/ukatru/github/dagster-dag-factory/test_concurrency.yaml")
    with open(yaml_path) as f:
        config = yaml.safe_load(f)
        asset_conf = config["assets"][0]
        
    asset_defs = factory._create_asset(asset_conf)
    main_asset = asset_defs[0]
    
    if not isinstance(main_asset, AssetsDefinition):
        print(f"FAILURE: Expected AssetsDefinition, got {type(main_asset)}")
        sys.exit(1)
        
    print(f"Asset: {main_asset.key.to_user_string()}")
    
    # Inspect tags
    actual_tags = main_asset.tags_by_key.get(main_asset.key, {})
    print(f"Tags: {actual_tags}")
    
    if actual_tags.get("owner") != "cloud_ops":
        print(f"FAILURE: User tag 'owner' missing or incorrect. Got {actual_tags.get('owner')}")
        sys.exit(1)
        
    if actual_tags.get("dagster/concurrency_key") != "sftp_limited_pool":
        print(f"FAILURE: Concurrency tag missing or incorrect. Got {actual_tags.get('dagster/concurrency_key')}")
        sys.exit(1)
            
    print("SUCCESS: Concurrency Key correctly mapped to Dagster tags.")

if __name__ == "__main__":
    verify()
