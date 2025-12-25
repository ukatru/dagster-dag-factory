from typing import List, Any, Tuple, Optional
from dagster_dag_factory.sensors.base_sensor import BaseSensor, SensorRegistry
from dagster_dag_factory.resources.sftp import SFTPResource, FileInfo
from dagster_dag_factory.configs.sftp import SFTPConfig


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
        Supports stateful polling via cursor (mtime).
        """
        last_mtime = float(cursor) if cursor else 0
        context.log.info(f"Checking SFTP path: {source_config.path} with pattern: {source_config.pattern} (cursor: {last_mtime})")
        
        with resource.get_client() as conn:
            # Use existing list_files method in SFTPResource
            infos = resource.list_files(
                conn=conn,
                path=source_config.path,
                pattern=source_config.pattern,
                recursive=source_config.recursive,
                check_is_modifying=source_config.check_is_modifying,
                predicate=source_config.predicate if callable(source_config.predicate) else None
            )
        
        # Filter by mtime
        new_items = [i for i in infos if i.modified_ts > last_mtime]
        
        # Calculate new cursor
        max_mtime = last_mtime
        if new_items:
            max_mtime = max(i.modified_ts for i in new_items)
            
        return new_items, str(max_mtime)
