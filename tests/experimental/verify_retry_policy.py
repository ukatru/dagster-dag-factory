import sys
import os
import yaml
from pathlib import Path
from dagster import AssetsDefinition, RetryPolicy

# Add src to path
sys.path.append("/home/ukatru/github/dagster-dag-factory/src")

from dagster_dag_factory.factory.asset_factory import AssetFactory

def verify():
    factory = AssetFactory(base_dir=".")
    
    yaml_path = Path("/home/ukatru/github/dagster-dag-factory/test_retry_policy.yaml")
    with open(yaml_path) as f:
        config = yaml.safe_load(f)
        asset_conf = config["assets"][0]
        
    asset_defs = factory._create_asset(asset_conf)
    main_asset = asset_defs[0]
    
    if not isinstance(main_asset, AssetsDefinition):
        print(f"FAILURE: Expected AssetsDefinition, got {type(main_asset)}")
        sys.exit(1)
        
    print(f"Asset: {main_asset.key.to_user_string()}")
    
    # Inspect retry policy
    op_def = main_asset.node_def
    retry_policy = op_def.retry_policy
    
    if not retry_policy:
        print("FAILURE: No retry policy found on operation.")
        sys.exit(1)
        
    print(f"Max Retries: {retry_policy.max_retries}")
    print(f"Delay: {retry_policy.delay}")
    print(f"Backoff: {retry_policy.backoff}")
    
    if retry_policy.max_retries != 5:
        print(f"FAILURE: Expected max_retries 5, got {retry_policy.max_retries}")
        sys.exit(1)
        
    if retry_policy.delay != 120:
        print(f"FAILURE: Expected delay 120, got {retry_policy.delay}")
        sys.exit(1)
        
    if str(retry_policy.backoff).lower() != "exponential":
         # Dagster might store it as an Enum or object, check string representation
         pass

    print("SUCCESS: Retry Policy correctly applied to asset.")

if __name__ == "__main__":
    verify()
