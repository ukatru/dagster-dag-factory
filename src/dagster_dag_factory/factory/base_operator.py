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
        
        # Log Source
        if isinstance(source_config, BaseConfigModel):
            context.log.info(f"Source Configuration:\n{source_config.to_masked_json()}")
        else:
            import json
            masked_source = self._mask_dict(source_config)
            context.log.info(f"Source Configuration:\n{json.dumps(masked_source, indent=2)}")

        # Log Target
        if isinstance(target_config, BaseConfigModel):
            context.log.info(f"Target Configuration:\n{target_config.to_masked_json()}")
        else:
            import json
            masked_target = self._mask_dict(target_config)
            context.log.info(f"Target Configuration:\n{json.dumps(masked_target, indent=2)}")

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
