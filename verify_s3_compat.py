import yaml
from dagster_dag_factory.configs.s3 import S3Config
from pydantic import ValidationError

# Mock template variables for rendering simulation
template_vars = {"partition_date": "2023-01-01"}

def test_compat(name, target_conf):
    print(f"--- Testing {name} ---")
    try:
        # Simulate what AssetFactory._create_asset would do
        # For now, just test if S3Config can be instantiated with these keys
        s3 = S3Config(**target_conf)
        print(f"SUCCESS: {name} loaded.")
        print(f"  Key: {s3.key}")
        print(f"  Object Type: {s3.object_type}")
        print(f"  Mode: {s3.mode}")
        if s3.compress_options:
            print(f"  Compress Type: {s3.compress_options.compress_type}")
    except ValidationError as e:
        print(f"FAILED: {name} validation error:")
        for error in e.errors():
            print(f"  Field: {error['loc']} - {error['msg']} (type={error['type']})")
    except Exception as e:
        print(f"FAILED: {name} unexpected error: {type(e).__name__}: {e}")
    print()

# 1. Standard SQLServer-S3
sqlserver_s3_target = {
    "type": "S3",
    "connection": "s3_prod",
    "path": "raw/customers/test_customers.csv",
    "format": "csv"
}

# 2. Large File Split
large_file_split_target = {
    "type": "S3",
    "connection": "s3_prod",
    "path": "raw/large_datasets/huge_dataset.csv",
    "multi_file": True,
    "chunk_size_mb": 100,
    "compress_options": {
        "type": "GUNZIP",
        "action": "COMPRESS"
    }
}

test_compat("Standard SQLServer-S3", sqlserver_s3_target)
test_compat("Large File Split", large_file_split_target)
