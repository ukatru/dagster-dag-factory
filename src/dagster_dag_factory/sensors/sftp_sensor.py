from typing import List, Any, Tuple, Optional
from dagster_dag_factory.sensors.base_sensor import BaseSensor, SensorRegistry
from dagster_dag_factory.resources.sftp import SFTPResource, FileInfo
from dagster_dag_factory.configs.sftp import SFTPConfig
from dagster_dag_factory.factory.helpers.rendering import render_config


@SensorRegistry.register("SFTP")
class SftpSensor(BaseSensor):
    """
    Sensor for SFTP. Uses SFTPResource to list files matching criteria.
    """
    source_config_schema = SFTPConfig

    def check(
        self, 
        context: Any, 
        source_config: SFTPConfig, 
        resource: SFTPResource, 
        cursor: Optional[str] = None,
        **kwargs
    ) -> Tuple[List[FileInfo], Optional[str]]:
        """
        Checks for files on SFTP server matching path, pattern and predicate.
        """
        last_mtime = float(cursor) if cursor else 0
        context.log.info(f"Checking SFTP path: {source_config.path} with pattern: {source_config.pattern} (cursor: {last_mtime})")

        # Prepare predicate callback for Framework-style evaluation
        predicate_fn = None
        if source_config.predicate_template:
            template_vars = kwargs.get("template_vars", {})
            predicate_fn = lambda info: self._predicate(context, source_config.predicate_template, info, template_vars)

        with resource.get_client() as conn:
            # Use existing list_files method in SFTPResource
            infos = resource.list_files(
                conn=conn,
                path=source_config.path,
                pattern=source_config.pattern,
                recursive=source_config.recursive,
                check_is_modifying=source_config.check_is_modifying,
                predicate=predicate_fn
            )
        
        # Filter by mtime
        new_items = [i for i in infos if i.modified_ts > last_mtime]
        
        # Calculate new cursor
        max_mtime = last_mtime
        if new_items:
            max_mtime = max(i.modified_ts for i in new_items)
            
        return new_items, str(max_mtime)
