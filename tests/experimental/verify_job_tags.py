import sys
from dagster import AssetSelection
from dagster._core.definitions.unresolved_asset_job_definition import UnresolvedAssetJobDefinition

# Add src to path
sys.path.append("/home/ukatru/github/dagster-dag-factory/src")

from dagster_dag_factory.factory.job_factory import JobFactory

def verify():
    factory = JobFactory()
    
    jobs_config = [
        {
            "name": "test_tagged_job",
            "tags": {
                "dagster/sensor_name": "my_test_sensor",
                "custom_tag": "active"
            },
            "selection": "*"
        }
    ]
    
    jobs = factory.create_jobs(jobs_config)
    job = jobs[0]
    
    if not isinstance(job, UnresolvedAssetJobDefinition):
        print(f"FAILURE: Expected UnresolvedAssetJobDefinition, got {type(job)}")
        sys.exit(1)
        
    print(f"Job: {job.name}")
    print(f"Tags: {job.tags}")
    
    expected_tags = {
        "dagster/sensor_name": "my_test_sensor",
        "custom_tag": "active"
    }
    
    for k, v in expected_tags.items():
        if job.tags.get(k) != v:
            print(f"FAILURE: Tag {k} mismatch. Expected {v}, got {job.tags.get(k)}")
            sys.exit(1)
            
    print("SUCCESS: Job-level tags correctly applied.")

if __name__ == "__main__":
    verify()
