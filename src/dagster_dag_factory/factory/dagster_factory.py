from pathlib import Path
import yaml
import warnings
from dagster import Definitions, AssetsDefinition, AssetChecksDefinition, BetaWarning
from dagster_dag_factory.factory.asset_factory import AssetFactory
from dagster_dag_factory.factory.resource_factory import ResourceFactory
from dagster_dag_factory.factory.job_factory import JobFactory
from dagster_dag_factory.factory.schedule_factory import ScheduleFactory
import dagster_dag_factory.operators

# Suppress beta warnings for backfill_policy and other features
warnings.filterwarnings("ignore", category=BetaWarning)

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
        asset_partitions = {} # Track partitions per asset

        # Iterate YAMLs and separate assets from checks
        for yaml_file in (self.base_dir / "defs").rglob("*.yaml"):
            try:
                with open(yaml_file) as f:
                    config = yaml.safe_load(f) or {}
                    
                if "assets" in config:
                    for asset_conf in config["assets"]:
                        try:
                            items = self.asset_factory._create_asset(asset_conf)
                            if not isinstance(items, list):
                                items = [items]
                            
                            for item in items:
                                # AssetChecksDefinition inherits from AssetsDefinition, so check it first
                                if isinstance(item, AssetChecksDefinition):
                                    asset_checks.append(item)
                                elif isinstance(item, AssetsDefinition):
                                    assets.append(item)
                                    # Store partition info for later use in schedules
                                    if item.partitions_def:
                                        asset_partitions[item.key.to_user_string()] = item.partitions_def
                        except Exception as e:
                            # Re-raising with filename in the message ensures visibility in the UI
                            raise type(e)(f"Error in {yaml_file.name}: {e}") from e
                
                if "jobs" in config:
                    for job_conf in config["jobs"]:
                        try:
                            jobs_config.append(job_conf)
                            
                            # Determine if this job is partitioned
                            is_job_partitioned = False
                            job_p_def = None
                            job_sel = job_conf.get("selection", [])
                            if isinstance(job_sel, str):
                                job_sel = [job_sel]
                            
                            for asset_name in job_sel:
                                if asset_name in asset_partitions:
                                    is_job_partitioned = True
                                    job_p_def = asset_partitions[asset_name]
                                    break

                            # AUTO-INFER SCHEDULE: If 'cron' is in the job, implicitly add it to schedules
                            if "cron" in job_conf:
                                schedules_config.append({
                                    "name": f"{job_conf['name']}_schedule",
                                    "job": job_conf["name"],
                                    "cron": job_conf["cron"],
                                    "is_partitioned": is_job_partitioned,
                                    "partitions_def": job_p_def
                                })
                        except Exception as e:
                            print(f"ERROR: Failed to process job from {yaml_file}: {e}")
                            raise e
                    
                if "schedules" in config:
                    for s_conf in config["schedules"]:
                        # Also check if explicit schedules refer to partitioned jobs
                        s_job_name = s_conf.get("job")
                        is_p = False
                        p_d = None
                        if s_job_name:
                            # Search for the job config to find its selection
                            for j_c in jobs_config:
                                if j_c["name"] == s_job_name:
                                    j_sel = j_c.get("selection", [])
                                    if isinstance(j_sel, str): j_sel = [j_sel]
                                    for a_n in j_sel:
                                        if a_n in asset_partitions:
                                            is_p = True
                                            p_d = asset_partitions[a_n]
                                            break
                        
                        s_conf["is_partitioned"] = is_p
                        s_conf["partitions_def"] = p_d
                        schedules_config.append(s_conf)

                if "resources" in config:
                    local_resources = self.resource_factory.load_resources_from_config(config["resources"])
                    resources.update(local_resources)
            except Exception as e:
                raise type(e)(f"Critical failure loading {yaml_file.name}: {e}") from e

        # 3. Create Jobs
        jobs = self.job_factory.create_jobs(jobs_config, asset_partitions)
        jobs_map = {job.name: job for job in jobs}

        # 4. Create Schedules
        schedules = self.schedule_factory.create_schedules(schedules_config, jobs_map, asset_partitions)
        
        return Definitions(
            assets=assets,
            asset_checks=asset_checks,
            resources=resources,
            jobs=jobs,
            schedules=schedules
        )
