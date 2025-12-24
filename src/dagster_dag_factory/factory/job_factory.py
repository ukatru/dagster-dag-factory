from typing import List, Dict, Any
from dagster import define_asset_job, AssetSelection


class JobFactory:
    def create_jobs(
        self, jobs_config: List[Dict[str, Any]], asset_partitions: Dict[str, Any]
    ):
        jobs = []
        for job_conf in jobs_config:
            name = job_conf["name"]
            selection = job_conf.get("selection", "*")

            # Advanced selection logic
            if isinstance(selection, list):
                # We union each item converted from string
                asset_sel = None
                for item in selection:
                    new_sel = AssetSelection.from_string(item)
                    asset_sel = (asset_sel | new_sel) if asset_sel else new_sel
            elif selection == "*":
                asset_sel = AssetSelection.all()
            elif isinstance(selection, str):
                asset_sel = AssetSelection.from_string(selection)
            else:
                asset_sel = AssetSelection.all()

            if asset_sel is None:
                asset_sel = AssetSelection.all()

            # Determine if we should set a partitions_def for this job
            # If the job explicitly selects partitioned assets, we should pass that partitions_def
            partitions_def = None
            # Extract simple names to check for partitions
            check_names = selection if isinstance(selection, list) else []
            if (
                isinstance(selection, str)
                and "*" not in selection
                and ":" not in selection
            ):
                check_names = [selection]

            for asset_name in check_names:
                if asset_name in asset_partitions:
                    partitions_def = asset_partitions[asset_name]
                    break

            tags = job_conf.get("tags")

            jobs.append(
                define_asset_job(
                    name=name,
                    selection=asset_sel,
                    partitions_def=partitions_def,
                    tags=tags,
                )
            )
        return jobs
