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

from dagster_dag_factory.resources.sftp import SFTPResource
from dagster_dag_factory.resources.s3 import S3Resource

# Quick test: Does getfo() work with smart buffer?
print("Testing SFTP getfo() with S3 smart buffer...")

sftp_res = SFTPResource(
    host=os.environ.get("SFTP_HOST"),
    port=int(os.environ.get("SFTP_PORT", 22)),
    username=os.environ.get("SFTP_USERNAME"),
    password=os.environ.get("SFTP_PASSWORD"),
)

s3_res = S3Resource(
    bucket_name=os.environ.get("S3_BUCKET_NAME"),
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    region_name="us-west-2",
    endpoint_url=os.environ.get("ENDPOINT_URL")
)

# Test with one of the benchmark files we know exists
test_file = "/home/ukatru/data/benchmark_large/large_file_1.csv"

print(f"Transferring {test_file}...")

with sftp_res.get_client() as sftp:
    # Check file exists
    stat = sftp.stat(test_file)
    print(f"File size: {stat.st_size} bytes")
    
    # Create smart buffer
    buffer = s3_res.create_smart_buffer(
        bucket_name=os.environ.get("S3_BUCKET_NAME"),
        key="debug_test/test_getfo.csv",
        multi_file=False,
        min_size=5,
        compress_options=None,
        logger=None
    )
    
    print("Starting getfo()...")
    sftp.getfo(test_file, buffer)
    print("getfo() completed, closing buffer...")
    
    results = buffer.close()
    print(f"✅ Success! Results: {results}")
