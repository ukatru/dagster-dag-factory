from pathlib import Path
import yaml
from dagster import Definitions, AssetsDefinition, AssetChecksDefinition
from dagster_dag_factory.factory.asset_factory import AssetFactory
from dagster_dag_factory.factory.resource_factory import ResourceFactory
from dagster_dag_factory.factory.job_factory import JobFactory
from dagster_dag_factory.factory.schedule_factory import ScheduleFactory
import dagster_dag_factory.operators

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
        
        # 2. Load Assets and Checks
        assets = []
        asset_checks = []
        jobs_config = []
        schedules_config = []

        # Iterate YAMLs and separate assets from checks
        # NOTE: In this Dagster version, @asset_check returns AssetChecksDefinition
        for yaml_file in (self.base_dir / "defs").rglob("*.yaml"):
            with open(yaml_file) as f:
                config = yaml.safe_load(f) or {}
                
            if "assets" in config:
                for asset_conf in config["assets"]:
                     items = self.asset_factory._create_asset(asset_conf)
                     if not isinstance(items, list):
                         items = [items]
                     
                     for item in items:
                         # AssetChecksDefinition inherits from AssetsDefinition, so check it first
                         if isinstance(item, AssetChecksDefinition):
                             asset_checks.append(item)
                         elif isinstance(item, AssetsDefinition):
                             assets.append(item)
            
            if "jobs" in config:
                jobs_config.extend(config["jobs"])
                
            if "schedules" in config:
                schedules_config.extend(config["schedules"])

            if "resources" in config:
                local_resources = self.resource_factory.load_resources_from_config(config["resources"])
                resources.update(local_resources)

        # 3. Create Jobs
        jobs = self.job_factory.create_jobs(jobs_config)
        jobs_map = {job.name: job for job in jobs}

        # 4. Create Schedules
        schedules = self.schedule_factory.create_schedules(schedules_config, jobs_map)
        
        return Definitions(
            assets=assets,
            asset_checks=asset_checks,
            resources=resources,
            jobs=jobs,
            schedules=schedules
        )
