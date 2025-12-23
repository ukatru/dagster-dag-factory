import os
import yaml
from dagster_dag_factory.configs.s3 import S3Config
from pydantic import ValidationError

DEFS_DIR = "/home/ukatru/github/dagster-pipelines/src/pipelines/defs/"

def validate_yaml(file_path):
    print(f"Validating {os.path.basename(file_path)}...")
    try:
        with open(file_path, 'r') as f:
            data = yaml.safe_load(f)
        
        if not data or 'assets' not in data:
            return

        for asset in data['assets']:
            # Check source
            if asset.get('source', {}).get('type') == 'S3':
                print(f"  Validating source: {asset['name']}")
                S3Config(**asset['source'])
            
            # Check target
            if asset.get('target', {}).get('type') == 'S3':
                print(f"  Validating target: {asset['name']}")
                S3Config(**asset['target'])
                
        print(f"  Result: PASS")
    except ValidationError as e:
        print(f"  Result: FAIL (Validation Error)")
        for error in e.errors():
            print(f"    {error['loc']}: {error['msg']}")
    except Exception as e:
        print(f"  Result: FAIL (Unexpected Error: {e})")

yaml_files = [f for f in os.listdir(DEFS_DIR) if f.endswith('.yaml')]
for f in sorted(yaml_files):
    validate_yaml(os.path.join(DEFS_DIR, f))
