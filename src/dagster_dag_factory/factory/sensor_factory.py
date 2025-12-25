from typing import List, Dict, Any, Optional, Type
from dagster import SensorDefinition, RunRequest, DefaultSensorStatus, AssetKey, AssetSelection, define_asset_job
from dagster_dag_factory.sensors.base_sensor import SensorRegistry
from dagster_dag_factory.factory.helpers.rendering import render_config
import logging

logger = logging.getLogger("dagster_dag_factory")

class SensorFactory:
    """
    Factory for creating native Dagster SensorDefinitions from YAML.
    """

    def create_sensors(
        self, 
        sensors_config: List[Dict[str, Any]], 
        jobs_map: Dict[str, Any],
        asset_factory_instance: Any
    ) -> List[SensorDefinition]:
        """
        Creates a list of SensorDefinitions from configuration.
        """
        sensors = []
        for sensor_conf in sensors_config:
            try:
                sensors.append(self._create_sensor(sensor_conf, jobs_map, asset_factory_instance))
            except Exception as e:
                logger.error(f"Failed to create sensor '{sensor_conf.get('name')}': {e}")
                raise e
        return sensors

    def _create_sensor(
        self, 
        config: Dict[str, Any], 
        jobs_map: Dict[str, Any],
        asset_factory_instance: Any
    ) -> SensorDefinition:
        name = config["name"]
        source_type = config.get("type")
        target_job_name = config.get("job")
        interval = config.get("minimum_interval_seconds", 30)
        status_str = config.get("default_status", "STOPPED").upper()
        source_configs = config.get("configs", {})

        # Resolve Default Status
        default_status = (
            DefaultSensorStatus.RUNNING 
            if status_str == "RUNNING" 
            else DefaultSensorStatus.STOPPED
        )

        # Get sensor class for logic reuse
        sensor_cls = SensorRegistry.get_sensor(source_type)
        if not sensor_cls:
            raise ValueError(f"No sensor logic registered for type: {source_type}")

        def _sensor_logic(context):
            # 1. Render config (for env vars, etc.)
            template_vars = asset_factory_instance._get_template_vars(context)
            rendered_source = render_config(source_configs, template_vars)

            # Inject connection from top-level if missing (required by Config models)
            if "connection" not in rendered_source and config.get("connection"):
                rendered_source["connection"] = config.get("connection")

            # 2. Validate using Pydantic if schema exists
            if sensor_cls.source_config_schema:
                source_model = sensor_cls.source_config_schema(**rendered_source)
            else:
                source_model = rendered_source

            # 3. Resolve Connection / Resource
            conn_name = config.get("connection") or rendered_source.get("connection")
            resource = getattr(context.resources, conn_name) if conn_name else None

            # 4. Run Check with Cursor
            sensor_instance = sensor_cls()
            found_items, new_cursor = sensor_instance.check(
                context=context,
                source_config=source_model,
                resource=resource,
                cursor=context.cursor
            )

            # 5. Handle Results
            if found_items:
                context.log.info(f"Sensor '{name}' found {len(found_items)} items. Triggering job '{target_job_name}'.")
                
                # Update cursor to progress
                if new_cursor:
                    context.update_cursor(new_cursor)

                # Yield RunRequests for each item (or batch them)
                # For now, let's yield one run per item to enable granular retries 
                # and item-specific run tags (e.g. filename)
                for item in found_items:
                    # Determine unique run key (e.g. filename + mtime)
                    item_name = getattr(item, "key", getattr(item, "filename", "unknown"))
                    item_mtime = getattr(item, "modified_ts", 0)
                    run_key = f"{name}:{item_name}:{item_mtime}"
                    
                    # Construct tags for the run
                    tags = {
                        "dagster/priority": "10",
                        "factory/sensor": name,
                        "factory/source_item": item_name,
                        "factory/source_mtime": str(item_mtime)
                    }
                    
                    yield RunRequest(
                        run_key=run_key,
                        job_name=target_job_name,
                        tags=tags
                    )
            
            return None

        return SensorDefinition(
            name=name,
            job_name=target_job_name,
            evaluation_fn=_sensor_logic,
            minimum_interval_seconds=interval,
            default_status=default_status,
            required_resource_keys={config.get("connection")} if config.get("connection") else set()
        )
