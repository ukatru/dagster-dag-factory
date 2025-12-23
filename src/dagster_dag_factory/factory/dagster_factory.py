from pathlib import Path
import yaml
from dagster import Definitions
from dagster_dag_factory.factory.asset_factory import AssetFactory
from dagster_dag_factory.factory.resource_factory import ResourceFactory
from dagster_dag_factory.factory.job_factory import JobFactory
from dagster_dag_factory.factory.schedule_factory import ScheduleFactory

class DagsterFactory:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.asset_factory = AssetFactory(base_dir)
        self.resource_factory = ResourceFactory()
        self.job_factory = JobFactory()
        self.schedule_factory = ScheduleFactory()

    def build_definitions(self) -> Definitions:
        # 1. Load Resources
        resources = self.resource_factory.load_resources_from_dir(self.base_dir / "connections")
        
        # 2. Load Assets (and collect job/schedule definitions from same YAMLs)
        assets = []
        jobs_config = []
        schedules_config = []

        # We need to iterate YAMLs ourselves to separate concerns if they are mixed
        # Or delegate to sub-factories. 
        # For now, let's reuse AssetFactory's recursive logic but just extracting configs here mainly.
        # Actually simplest is to have AssetFactory return config or just iterate here.
        
        # Refactoring approach: Let's iterate files once here.
        for yaml_file in (self.base_dir / "defs").rglob("*.yaml"):
            with open(yaml_file) as f:
                config = yaml.safe_load(f) or {}
                
            if "assets" in config:
                # Use AssetFactory to create assets from this list
                for asset_conf in config["assets"]:
                     asset_defs = self.asset_factory._create_asset(asset_conf)
                     if isinstance(asset_defs, list):
                         assets.extend(asset_defs)
                     else:
                         assets.append(asset_defs)
            
            if "jobs" in config:
                jobs_config.extend(config["jobs"])
                

                
            if "schedules" in config:
                schedules_config.extend(config["schedules"])

            if "resources" in config:
                # Merge definition-local resources
                # We need to process them via ResourceFactory
                local_resources = self.resource_factory.load_resources_from_config(config["resources"])
                resources.update(local_resources)

        # 3. Create Jobs
        jobs = self.job_factory.create_jobs(jobs_config)
        jobs_map = {job.name: job for job in jobs}

        # 4. Create Schedules
        schedules = self.schedule_factory.create_schedules(schedules_config, jobs_map)
        
        return Definitions(
            assets=assets,
            resources=resources,
            jobs=jobs,
            schedules=schedules
        )
