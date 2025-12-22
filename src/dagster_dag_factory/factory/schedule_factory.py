from typing import List, Dict, Any, TYPE_CHECKING
from dagster import ScheduleDefinition

if TYPE_CHECKING:
    from dagster import JobDefinition

class ScheduleFactory:
    def create_schedules(self, schedules_config: List[Dict[str, Any]], jobs_map: Dict[str, "JobDefinition"]):
        schedules = []
        for sched_conf in schedules_config:
            name = sched_conf["name"]
            job_name = sched_conf["job"]
            cron_schedule = sched_conf["cron"]
            
            if job_name not in jobs_map:
                print(f"Warning: Job '{job_name}' not found for schedule '{name}'")
                continue
                
            job = jobs_map[job_name]
            
            schedules.append(ScheduleDefinition(
                name=name,
                job=job,
                cron_schedule=cron_schedule
            ))
        return schedules
