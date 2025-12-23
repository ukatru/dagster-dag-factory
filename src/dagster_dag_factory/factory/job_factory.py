from typing import List, Dict, Any
from dagster import define_asset_job, AssetSelection

class JobFactory:
    def create_jobs(self, jobs_config: List[Dict[str, Any]]):
        jobs = []
        for job_conf in jobs_config:
            name = job_conf["name"]
            selection = job_conf.get("selection", "*")
            
            # Simple selection logic
            if isinstance(selection, list):
                # Select by asset names
                asset_sel = AssetSelection.assets(*selection)
            elif selection == "*":
                asset_sel = AssetSelection.all()
            else:
                 # Fallback/TODO: Support groups or other selection strings
                 asset_sel = AssetSelection.all()

            tags = job_conf.get("tags")
            jobs.append(define_asset_job(name=name, selection=asset_sel, tags=tags))
        return jobs
