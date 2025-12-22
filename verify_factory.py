import os
import yaml
from dagster import Definitions
from dagster_dag_factory.factory.asset_factory import AssetFactory

# Mock a YAML file
yaml_content = """
assets:
  - name: test_sql_to_s3
    group: test_group
    source:
      type: SQLSERVER
      connection: sqlserver_prod
      query: "SELECT * FROM users"
    target:
      type: S3
      connection: s3_prod
      path: "data/users.csv"
"""

os.makedirs("test_defs", exist_ok=True)
with open("test_defs/pipeline.yaml", "w") as f:
    f.write(yaml_content)

# Initialize Factory
factory = AssetFactory("test_defs")
assets = factory.load_assets()

print(f"Loaded {len(assets)} assets")
for asset in assets:
    print(f"Asset Name: {asset.key.path[0]}")
    print(f"Required Resources: {asset.required_resource_keys}")
    if asset.required_resource_keys == {"sqlserver_prod", "s3_prod"}:
        print("SUCCESS: Resource keys match")
    else:
        print(f"FAILURE: Resource keys mismatch: {asset.required_resource_keys}")
