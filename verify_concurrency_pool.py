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
    
    # Inspect pool
    # main_asset.node_def.pool is the parameter in Dagster 1.12+
    actual_pool = main_asset.node_def.pool
    print(f"Pool: {actual_pool}")
    
    if actual_pool != "sftp_limited_pool":
        print(f"FAILURE: Pool missing or incorrect. Expected 'sftp_limited_pool', got {actual_pool}")
        sys.exit(1)
            
    print("SUCCESS: Concurrency Key correctly mapped to Dagster Pool.")

if __name__ == "__main__":
    verify()
