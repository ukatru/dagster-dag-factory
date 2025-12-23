import sys
import os
import yaml
from pathlib import Path
from dagster import AssetsDefinition

# Add src to path
sys.path.append("/home/ukatru/github/dagster-dag-factory/src")

from dagster_dag_factory.factory.asset_factory import AssetFactory

def verify():
    factory = AssetFactory(base_dir=".")
    
    yaml_path = Path("/home/ukatru/github/dagster-dag-factory/test_metadata_tags.yaml")
    with open(yaml_path) as f:
        config = yaml.safe_load(f)
        asset_conf = config["assets"][0]
        
    asset_defs = factory._create_asset(asset_conf)
    
    # asset_defs is a list: [_generated_asset, ...checks]
    main_asset = asset_defs[0]
    
    if not isinstance(main_asset, AssetsDefinition):
        print(f"FAILURE: Expected AssetsDefinition, got {type(main_asset)}")
        sys.exit(1)
        
    print(f"Asset: {main_asset.key.to_user_string()}")
    
    # Inspect tags
    # main_asset.tags_by_key returns a dict Mapping[AssetKey, Mapping[str, str]]
    actual_tags = main_asset.tags_by_key.get(main_asset.key, {})
    print(f"Tags: {actual_tags}")
    expected_tags = {"owner": "platform_team", "pii": "false", "env": "production"}
    for k, v in expected_tags.items():
        if actual_tags.get(k) != v:
            print(f"FAILURE: Missing/incorrect tag {k}. Expected {v}, got {actual_tags.get(k)}")
            sys.exit(1)
            
    # Inspect metadata
    # main_asset.metadata_by_key returns a dict Mapping[AssetKey, Mapping[str, MetadataValue]]
    print(f"Metadata: {main_asset.metadata_by_key}")
    expected_metadata = {
        "documentation": "http://docs.example.com",
        "data_sensitivity": "public"
    }
    
    actual_metadata = main_asset.metadata_by_key.get(main_asset.key, {})
    print(f"Actual values: {actual_metadata}")
    
    for k, v in expected_metadata.items():
        if actual_metadata.get(k) != v:
            print(f"FAILURE: Missing/incorrect metadata {k}. Expected {v}, got {actual_metadata.get(k)}")
            sys.exit(1)
            
    print("SUCCESS: Metadata and Tags correctly applied to asset.")

if __name__ == "__main__":
    verify()
