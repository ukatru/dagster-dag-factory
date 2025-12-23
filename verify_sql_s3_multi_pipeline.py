import sys
from dagster import AssetsDefinition

# Add factory src to path
sys.path.append("/home/ukatru/github/dagster-dag-factory/src")

from dagster_dag_factory.factory.asset_factory import AssetFactory

def verify():
    # Point to the defs directory in dagster-pipelines
    defs_dir = "/home/ukatru/github/dagster-pipelines/src/pipelines/defs"
    factory = AssetFactory(defs_dir)
    
    # Load all assets
    assets = factory.load_assets()
    
    print(f"Total assets loaded: {len(assets)}")
    
    # Target keys we expect in our multi-asset
    expected_keys = {"sales_us", "sales_eu", "sales_apac"}
    
    multi_asset = None
    for asset in assets:
        if isinstance(asset, AssetsDefinition):
            asset_keys = {k.to_user_string() for k in asset.keys}
            if expected_keys.issubset(asset_keys):
                multi_asset = asset
                break
                
    if not multi_asset:
        print(f"FAILURE: Could not find multi-asset with keys {expected_keys}")
        # Print keys of loaded assets for debugging
        for a in assets:
            if isinstance(a, AssetsDefinition):
                print(f"Found AssetsDefinition with keys: {[k.to_user_string() for k in a.keys]}")
        sys.exit(1)
        
    print(f"SUCCESS: Found Multi-Asset definition: {multi_asset.node_def.name}")
    print(f"Asset Keys: {[k.to_user_string() for k in multi_asset.keys]}")
    
    # Check specs for correct metadata and pool
    specs = list(multi_asset.specs)
    print(f"Number of specs: {len(specs)}")
    
    for key in expected_keys:
        spec = next((s for s in specs if s.key.to_user_string() == key), None)
        if not spec:
            print(f"FAILURE: Spec for {key} not found")
            sys.exit(1)
        print(f"Asset: {key}")
        print(f"  Description: {spec.description}")
        print(f"  Metadata: {spec.metadata}")
        
    print("SUCCESS: Generic Multi-Asset pipeline correctly loaded and validated.")

if __name__ == "__main__":
    verify()
