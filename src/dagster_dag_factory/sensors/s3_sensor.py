from typing import List, Any, Tuple, Optional
from dagster_dag_factory.sensors.base_sensor import BaseSensor, SensorRegistry
from dagster_dag_factory.resources.s3 import S3Resource, S3Info
from dagster_dag_factory.configs.s3 import S3Config
from dagster_dag_factory.factory.helpers.rendering import render_config


@SensorRegistry.register("S3")
class S3Sensor(BaseSensor):
    """
    Sensor for S3. Uses S3Resource to list objects matching criteria.
    """
    source_config_schema = S3Config

    def check(
        self, 
        context: Any, 
        source_config: S3Config, 
        resource: S3Resource, 
        cursor: Optional[str] = None,
        **kwargs
    ) -> Tuple[List[S3Info], Optional[str]]:
        """
        Checks for objects in S3 bucket matching prefix, pattern and predicate.
        Supports stateful polling via cursor (mtime).
        """
        last_mtime = float(cursor) if cursor else 0
        context.log.info(f"Checking S3 bucket: {source_config.bucket_name} with prefix: {source_config.key} (cursor: {last_mtime})")
        
        # Prepare predicate callback for Framework-style evaluation
        predicate_fn = None
        if source_config.predicate_template:
            template_vars = kwargs.get("template_vars", {})
            predicate_fn = lambda info: self._predicate(context, source_config.predicate_template, info, template_vars)

        # Use existing list_files method
        infos = resource.list_files(
            bucket_name=source_config.bucket_name,
            prefix=source_config.key,
            pattern=source_config.pattern,
            predicate=predicate_fn,
            check_is_modifying=source_config.check_is_modifying
        )
        
        # Filter by mtime
        new_items = [i for i in infos if i.modified_ts > last_mtime]
        
        # Calculate new cursor
        max_mtime = last_mtime
        if new_items:
            max_mtime = max(i.modified_ts for i in new_items)
            
        return new_items, str(max_mtime)
