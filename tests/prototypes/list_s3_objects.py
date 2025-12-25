import os
import sys
from pathlib import Path

# Setup paths
current_file = Path(__file__).resolve()
root_dir = current_file.parents[2]
pip_dir = root_dir.parent / "dagster-pipelines"
sys.path.append(str(root_dir / "src"))

def load_dotenv_manual(path):
    if not os.path.exists(path): return
    with open(path) as f:
        for line in f:
            if "=" in line:
                k, v = line.strip().split("=", 1)
                os.environ[k.strip()] = v.strip().strip("\"").strip("'")

load_dotenv_manual(pip_dir / ".env")

import boto3

# List S3 objects
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    region_name="us-west-2",
    endpoint_url=os.environ.get("ENDPOINT_URL") if os.environ.get("ENDPOINT_URL") else None
)

bucket = os.environ.get("S3_BUCKET_NAME")
prefix = "test_v1_multifile/"

print(f"Listing objects in s3://{bucket}/{prefix}")
print("="*70)

response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

if 'Contents' in response:
    for obj in response['Contents']:
        size_mb = obj['Size'] / (1024*1024)
        print(f"{obj['Key']} ({size_mb:.2f} MB)")
    print(f"\nTotal: {len(response['Contents'])} objects")
else:
    print("No objects found")
