from typing import List, Dict, Any, TYPE_CHECKING
from dagster import ScheduleDefinition

if TYPE_CHECKING:
    from dagster import JobDefinition

class ScheduleFactory:
    def create_schedules(
        self, 
        schedules_config: List[Dict[str, Any]], 
        jobs_map: Dict[str, Any],
        asset_partitions: Dict[str, Any]
    ):
        from dagster import build_schedule_from_partitioned_job, TimeWindowPartitionsDefinition
        schedules = []
        for sched_conf in schedules_config:
            name = sched_conf["name"]
            job_name = sched_conf["job"]
            cron_schedule = sched_conf["cron"]
            
            if job_name not in jobs_map:
                print(f"Warning: Job '{job_name}' not found for schedule '{name}'")
                continue
                
            job = jobs_map[job_name]
            
            # Determine if this job is partitioned
            is_partitioned = sched_conf.get("is_partitioned", False)
            p_def = sched_conf.get("partitions_def")
            
            # Check if job has partitions_def directly
            if not is_partitioned and hasattr(job, "partitions_def") and job.partitions_def:
                is_partitioned = True
                p_def = job.partitions_def

            if is_partitioned and p_def:
                # For time-based partitions, build_schedule_from_partitioned_job
                # infers the cron from the partition definition and rejects explicit cron_schedule
                is_time_partitioned = isinstance(p_def, TimeWindowPartitionsDefinition)
                
                if is_time_partitioned:
                    # Don't pass cron_schedule for time-partitioned jobs
                    schedules.append(build_schedule_from_partitioned_job(
                        name=name,
                        job=job
                    ))
                else:
                    # For non-time partitions (static, dynamic), we need the cron
                    schedules.append(build_schedule_from_partitioned_job(
                        name=name,
                        job=job,
                        cron_schedule=cron_schedule
                    ))
            else:
                # Non-partitioned job
                schedules.append(ScheduleDefinition(
                    name=name,
                    job=job,
                    cron_schedule=cron_schedule
                ))
        return schedules
