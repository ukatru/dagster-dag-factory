import os, sys
from dagster_dag_factory.resources.sftp import SFTPResource

def load_dotenv_manual(path):
    if not os.path.exists(path): return
    with open(path) as f:
        for line in f:
            if "=" in line:
                k, v = line.strip().split("=", 1)
                os.environ[k.strip()] = v.strip().strip("\"").strip("'")

sys.path.append("/home/ukatru/github/dagster-dag-factory/src")
load_dotenv_manual("/home/ukatru/github/dagster-pipelines/.env")

res = SFTPResource(
    host=os.environ.get("SFTP_HOST"),
    username=os.environ.get("SFTP_USERNAME"),
    password=os.environ.get("SFTP_PASSWORD"),
    port=int(os.environ.get("SFTP_PORT", 22))
)

with res.get_client() as sftp:
    print("Files in /home/ukatru/data/large_datasets:")
    try:
        print(sftp.listdir("/home/ukatru/data/large_datasets"))
    except Exception as e:
        print(f"Error: {e}")
