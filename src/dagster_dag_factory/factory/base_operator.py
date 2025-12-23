from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Type
from dagster_dag_factory.configs.base import BaseConfigModel

class BaseOperator(ABC):
    """
    Abstract base class for all operators.
    Operators handle the logic for moving data from a specific source type to a target type.
    """
    
    # Optional schema definitions for self-documentation and validation
    source_config_schema: Optional[Type[BaseConfigModel]] = None
    target_config_schema: Optional[Type[BaseConfigModel]] = None
    
    @abstractmethod
    def execute(self, context, source_config: Dict[str, Any], target_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the operator logic.
        """
        pass

    def log_configs(self, context, source_config: Any, target_config: Any):
        """Logs the rendered configurations for troubleshooting."""
        import json
        
        def get_masked_cfg(config):
            if hasattr(config, "to_masked_dict"):
                masked_cfg = config.to_masked_dict()
            elif isinstance(config, dict):
                masked_cfg = self._mask_dict(config)
            else:
                masked_cfg = {"config": str(config)}
            
            # Try to enrich with connection details
            connection_name = None
            if isinstance(config, dict):
                connection_name = config.get("connection")
            elif hasattr(config, "connection"):
                connection_name = config.connection
                
            if connection_name and hasattr(context, "resources"):
                # Dagster resources are accessed via attributes on context.resources
                resource = getattr(context.resources, connection_name, None)
                if resource and hasattr(resource, "to_masked_dict"):
                    masked_cfg["connection_details"] = resource.to_masked_dict()
                elif resource:
                    # Fallback for resources that don't inherit from our base
                    masked_cfg["connection_details"] = {"type": str(type(resource))}
            
            return masked_cfg

        # Log Source
        context.log.info(f"Source Configuration:\n{json.dumps(get_masked_cfg(source_config), indent=2, default=str)}")

        # Log Target
        context.log.info(f"Target Configuration:\n{json.dumps(get_masked_cfg(target_config), indent=2, default=str)}")

    def _mask_dict(self, d: Any) -> Any:
        if isinstance(d, dict):
            new_d = {}
            for k, v in d.items():
                if any(x in k.lower() for x in ["pass", "secret", "key", "token"]):
                    new_d[k] = "******" if v else v
                else:
                    new_d[k] = self._mask_dict(v)
            return new_d
        elif isinstance(d, list):
            return [self._mask_dict(x) for x in d]
        return d
