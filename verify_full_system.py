from pathlib import Path
from dagster_dag_factory.factory.dagster_factory import DagsterFactory
import os

def verify_full_system():
    # Path to the pipelines project
    pipelines_dir = Path("/home/ukatru/github/dagster-pipelines/src/pipelines")
    
    print(f"--- System-Wide Integration Test ---")
    print(f"Target Project: {pipelines_dir}")
    
    # Ensure ENV is set
    if "ENV" not in os.environ:
        os.environ["ENV"] = "dev"
    print(f"Simulating Environment: {os.environ['ENV']}")

    try:
        # Initialize Factory
        factory = DagsterFactory(base_dir=pipelines_dir)
        
        # This triggers:
        # 1. Connection loading (Deep Merge test)
        # 2. Variable loading (Unified Syntax test)
        # 3. Asset Factory scan
        # 4. Definition building
        defs = factory.build_definitions()
        
        # Summary
        assets = list(defs.assets) if defs.assets else []
        checks = list(defs.asset_checks) if defs.asset_checks else []
        
        print(f"\nSUCCESS: Full system initialization complete.")
        print(f"  - Assets loaded: {len(assets)}")
        print(f"  - Checks loaded: {len(checks)}")
        print(f"  - Resources available: {list(defs.resources.keys())}")
        
        # Check for specific resources we fixed (SFTP, etc.)
        expected_resources = ['sftp_prod', 'snowflake_prod', 's3_prod', 'sqlserver_prod']
        for res in expected_resources:
            if res in defs.resources:
                print(f"    [OK] Resource '{res}' is registered.")
            else:
                print(f"    [MISSING] Resource '{res}' NOT FOUND!")
        
    except Exception as e:
        print(f"\nFATAL ERROR: System initialization failed!")
        print(f"Error Details: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

if __name__ == "__main__":
    verify_full_system()
