from typing import List, Dict, Any
from dagster import define_asset_job, AssetSelection

class JobFactory:
    def create_jobs(self, jobs_config: List[Dict[str, Any]], asset_partitions: Dict[str, Any]):
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

            # Determine if we should set a partitions_def for this job
            # If the job explicitly selects partitioned assets, we should pass that partitions_def
            partitions_def = None
            if isinstance(selection, list):
                for asset_name in selection:
                    if asset_name in asset_partitions:
                        partitions_def = asset_partitions[asset_name]
                        break

            tags = job_conf.get("tags")
            
            jobs.append(define_asset_job(
                name=name, 
                selection=asset_sel, 
                partitions_def=partitions_def,
                tags=tags,
            ))
        return jobs
