import sys
from dagster import AssetsDefinition

# Add src to path
sys.path.append("/home/ukatru/github/dagster-dag-factory/src")

from dagster_dag_factory.factory.asset_factory import AssetFactory

def verify():
    yaml_path = "/home/ukatru/github/dagster-pipelines/src/pipelines/defs/sqlserver_s3_multi.yaml"
    factory = AssetFactory("/home/ukatru/github/dagster-pipelines/src/pipelines/defs")
    
    # We use load_assets which scans the whole directory, but we can look for our specific one
    assets = factory.load_assets()
    
    # Find the multi-asset by checking keys
    target_keys = {"sales_us", "sales_eu", "sales_other"}
    ma = None
    for a in assets:
        if isinstance(a, AssetsDefinition):
            asset_keys = {k.to_user_string() for k in a.keys}
            if target_keys.issubset(asset_keys):
                ma = a
                break
                
    if not ma:
        print(f"FAILURE: Could not find assets with keys {target_keys}")
        sys.exit(1)
        
    print(f"SUCCESS: Found Multi-Asset definition with keys: {target_keys}")
    specs = list(ma.specs)
    print(f"Pool: {specs[0].pool}")
    print(f"Retry Policy: {ma.retry_policy}")
    
    # Check descriptions
    for spec in ma.specs:
        print(f"Asset: {spec.key.to_user_string()}, Description: {spec.description}")

if __name__ == "__main__":
    verify()
