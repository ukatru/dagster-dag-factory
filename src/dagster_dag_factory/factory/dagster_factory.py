from pathlib import Path
import yaml
import warnings
from dagster import Definitions, AssetsDefinition, AssetChecksDefinition, BetaWarning
from dagster_dag_factory.factory.asset_factory import AssetFactory
from dagster_dag_factory.factory.resource_factory import ResourceFactory
from dagster_dag_factory.factory.job_factory import JobFactory
from dagster_dag_factory.factory.schedule_factory import ScheduleFactory
from dagster_dag_factory.factory.sensor_factory import SensorFactory
from dagster_dag_factory.factory.utils.logging import log_header, log_action, log_action_stats, log_marker
import time

# Suppress beta warnings for backfill_policy and other features
warnings.filterwarnings("ignore", category=BetaWarning)


class DagsterFactory:
    def __init__(self, base_dir: Path, verbose_build: bool = None):
        self.base_dir = Path(base_dir)
        self.asset_factory = AssetFactory(self.base_dir)
        self.resource_factory = ResourceFactory()
        self.job_factory = JobFactory()
        self.schedule_factory = ScheduleFactory()
        self.sensor_factory = SensorFactory()
        self.verbose_build = verbose_build

    def build_definitions(self) -> Definitions:
        import os
        
        # Decide whether to show build logs
        # True: always, False: never, None: skip if in a Dagster worker process
        show_logs = self.verbose_build
        if show_logs is None:
            # Silence logs in Dagster worker processes (Run or Step workers)
            is_worker = "DAGSTER_RUN_ID" in os.environ or "DAGSTER_STEP_KEY" in os.environ
            show_logs = not is_worker

        start_time = time.time()
        if show_logs:
            log_header("Dagster Factory | Starting Definition Build")

        # 1. Load Resources
        resources = self.resource_factory.load_resources_from_dir(
            self.base_dir / "connections"
        )
        if show_logs:
            log_action("LOAD_RESOURCES", count=len(resources), status="OK")

        # 2. Load Assets and Checks
        assets = []
        asset_checks = []
        jobs_config = []
        schedules_config = []
        sensors_config = []
        asset_partitions = {}  # Track partitions per asset

        # Iterate YAMLs and separate assets from checks
        for yaml_file in (self.base_dir / "defs").rglob("*.yaml"):
            file_assets = 0
            file_jobs = 0
            file_sensors = 0
            
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
                                    file_assets += 1
                                    # Store partition info for later use in schedules
                                    if item.partitions_def:
                                        asset_partitions[item.key.to_user_string()] = (
                                            item.partitions_def
                                        )

                                    # AUTO-INFER SCHEDULE for Asset-Level 'cron'
                                    if "cron" in asset_conf:
                                        schedules_config.append(
                                            {
                                                "name": f"{asset_conf['name']}_schedule",
                                                "job": f"{asset_conf['name']}_job",
                                                "cron": asset_conf["cron"],
                                                "is_partitioned": item.partitions_def
                                                is not None,
                                                "partitions_def": item.partitions_def,
                                            }
                                        )
                                        # We also need to ensure a job exists for this asset if it doesn't already
                                        # The job_factory will create it if we add it to jobs_config
                                        jobs_config.append(
                                            {
                                                "name": f"{asset_conf['name']}_job",
                                                "selection": [item.key.to_user_string()],
                                            }
                                        )
                        except Exception as e:
                            # Enhance exception with file name before re-raising
                            from dagster_dag_factory.utils.exceptions import DagsterFactoryError
                            error_msg = str(e)
                            if isinstance(e, DagsterFactoryError):
                                e.file_name = yaml_file.name
                                error_msg = str(e)
                            
                            if show_logs:
                                log_action("LOAD_FAILED", file=yaml_file.name, error=error_msg)
                            raise

                if "jobs" in config:
                    for job_conf in config["jobs"]:
                        try:
                            jobs_config.append(job_conf)
                            file_jobs += 1

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
                                schedules_config.append(
                                    {
                                        "name": f"{job_conf['name']}_schedule",
                                        "job": job_conf["name"],
                                        "cron": job_conf["cron"],
                                        "is_partitioned": is_job_partitioned,
                                        "partitions_def": job_p_def,
                                    }
                                )
                        except Exception as e:
                            if show_logs:
                                log_action("LOAD_FAILED", file=yaml_file.name, error=str(e))
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
                                    if isinstance(j_sel, str):
                                        j_sel = [j_sel]
                                    for a_n in j_sel:
                                        if a_n in asset_partitions:
                                            is_p = True
                                            p_d = asset_partitions[a_n]
                                            break

                        s_conf["is_partitioned"] = is_p
                        s_conf["partitions_def"] = p_d
                        schedules_config.append(s_conf)

                if "sensors" in config:
                    for sensor_conf in config["sensors"]:
                        sensors_config.append(sensor_conf)
                        file_sensors += 1

                if "resources" in config:
                    local_resources = self.resource_factory.load_resources_from_config(
                        config["resources"]
                    )
                    resources.update(local_resources)
                
                if show_logs:
                    log_action("LOAD_YAML", file=yaml_file.name, assets=file_assets, jobs=file_jobs, sensors=file_sensors)

            except Exception as e:
                # Catch all errors at file level and log them once
                if show_logs:
                    log_action("LOAD_FAILED", file=yaml_file.name, error=str(e))
                # Re-raise with filename context. We avoid calling type(e) directly 
                # because some exceptions (like DagsterInvalidPythonicConfigDefinitionError)
                # require specific arguments in __init__.
                raise RuntimeError(f"Critical failure loading {yaml_file.name}: {e}") from e

        # 3. Create Jobs
        jobs = self.job_factory.create_jobs(jobs_config, asset_partitions)
        jobs_map = {job.name: job for job in jobs}

        # 4. Create Schedules
        schedules = self.schedule_factory.create_schedules(
            schedules_config, jobs_map, asset_partitions
        )

        # 5. Create Sensors
        sensors = self.sensor_factory.create_sensors(
            sensors_config, jobs_map, self.asset_factory
        )
        
        if show_logs:
            log_marker("mini")
            log_action_stats("BUILD_STATS", start_time, 
                             assets=len(assets), 
                             checks=len(asset_checks), 
                             jobs=len(jobs), 
                             sensors=len(sensors))
            log_marker("strong")

        return Definitions(
            assets=assets,
            asset_checks=asset_checks,
            resources=resources,
            jobs=jobs,
            schedules=schedules,
            sensors=sensors,
        )
