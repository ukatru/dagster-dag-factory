from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Type
from dagster import AssetCheckExecutionContext, AssetCheckResult
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
    def execute(self, context, source_config: Any, target_config: Any, template_vars: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the operator logic.
        """
        pass

    def execute_check(self, context: AssetCheckExecutionContext, config: dict) -> AssetCheckResult:
        """
        A generic high-performance data quality check that compares persisted metadata 
        (Observations) from the asset materialization.
        """
        check_type = config.get("type", "observation_diff")
        
        if check_type == "observation_diff":
            return self._execute_observation_diff(context, config)
            
        # Default fallback for unknown check types. 
        # Operators can override this method to handle custom check types.
        context.log.warn(f"Unknown check type '{check_type}' for operator {self.__class__.__name__}")
        return AssetCheckResult(
            passed=False,
            metadata={"error": f"Operator {self.__class__.__name__} does not support check type '{check_type}'"}
        )

    def _execute_observation_diff(self, context: AssetCheckExecutionContext, config: dict) -> AssetCheckResult:
        asset_key = config.get("_asset_key")
        source_key = config["source_key"]
        target_key = config["target_key"]
        threshold = config.get("threshold", 0)
        
        # 1. Retrieve the latest materialization event for this asset
        event = context.instance.get_latest_materialization_event(asset_key)
        if not event:
            return AssetCheckResult(
                passed=False, 
                metadata={"error": "Check failed: No materialization event found for this asset."}
            )
            
        metadata = event.asset_materialization.metadata
        
        # 2. Extract raw values from Dagster Metadata
        def get_val(key):
            m_val = metadata.get(key)
            if m_val is None:
                return None
            if hasattr(m_val, "value"):
                return m_val.value
            return m_val

        source_val = get_val(source_key)
        target_val = get_val(target_key)
        
        # 3. Validation Logic
        if source_val is None or target_val is None:
            missing = []
            if source_val is None: missing.append(f"source:{source_key}")
            if target_val is None: missing.append(f"target:{target_key}")
            
            return AssetCheckResult(
                passed=False, 
                metadata={
                    "error": f"Missing observation keys: {', '.join(missing)}",
                    "available_keys": list(metadata.keys())
                }
            )
            
        try:
            diff = abs(float(source_val) - float(target_val))
            passed = diff <= threshold
            
            return AssetCheckResult(
                passed=passed,
                metadata={
                    "source_key": source_key,
                    "source_value": source_val,
                    "target_key": target_key,
                    "target_value": target_val,
                    "difference": diff,
                    "threshold": threshold,
                    "status": "PASS" if passed else "FAIL"
                }
            )
        except (ValueError, TypeError) as e:
            return AssetCheckResult(
                passed=False,
                metadata={
                    "error": f"Value comparison failed: {str(e)}",
                    "source_value": str(source_val),
                    "target_value": str(target_val)
                }
            )

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
