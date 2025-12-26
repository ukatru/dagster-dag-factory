"""
Base Operator for Streaming Pattern

Provides a consistent interface for all operators following proven streaming patterns:
- execute() orchestrates the flow
- pre_execute() for setup and validation
- _execute() for operator-specific logic (abstract)
- post_execute() for cleanup and stats

Factory contract is maintained - no changes needed to factory code.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict


class BaseOperator(ABC):
    """
    Base class for all streaming operators.
    
    Provides lifecycle hooks and maintains factory contract.
    Subclasses only need to implement _execute() with their specific logic.
    """
    
    def execute(
        self,
        context,
        source_config,
        target_config,
        template_vars: Dict[str, Any],
        max_workers: int = 5,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Main execution method called by the factory.
        
        This method orchestrates the execution flow:
        1. Pre-execution (setup, validation)
        2. Main execution (operator-specific logic)
        3. Post-execution (cleanup, stats)
        
        Args:
            context: Dagster context
            source_config: Source configuration model
            target_config: Target configuration model
            template_vars: Template variables for rendering
            **kwargs: Additional resources (source_resource, target_resource, etc.)
        
        Returns:
            Dict containing execution results and statistics
        """
        import time
        from dagster_dag_factory.factory.utils.logging import log_header, log_marker
        
        self.max_workers = max_workers
        
        # Determine asset name for logging
        asset_name = context.asset_key.to_user_string() if hasattr(context, "asset_key") else "unknown_asset"

        # 1. Log Operator Header & Configs (Grouped)
        log_block = []
        log_block.append(log_header(f"OPERATOR | {self.__class__.__name__} ({asset_name})", logger=None))
        log_block.append(self.log_operator_configs(context, source_config, target_config, logger=None))
        log_block.append(log_marker("mini", logger=None))
        
        # Log as a single cohesive unit in Dagster UI
        context.log.info("\n" + "\n".join(log_block))

        start_time = time.time()
        
        # 2. Pre-execution
        self.pre_execute(context, source_config, target_config, **kwargs)
        
        # 3. Main execution (operator-specific)
        result = self._execute(context, source_config, target_config, template_vars, **kwargs)
        
        # 4. Post-execution
        duration = time.time() - start_time
        self.post_execute(context, result, duration=duration)
        
        log_marker("strong")

        return result
    
    def pre_execute(
        self,
        context,
        source_config,
        target_config,
        **kwargs
    ) -> None:
        """
        Pre-execution hook for setup and validation.
        
        Override this method to add operator-specific setup logic:
        - Validate configurations
        - Initialize resources
        - Set up state
        
        Args:
            context: Dagster context
            source_config: Source configuration model
            target_config: Target configuration model
            **kwargs: Additional resources
        """
        pass
    
    @abstractmethod
    def _execute(
        self,
        context,
        source_config,
        target_config,
        template_vars: Dict[str, Any],
        **kwargs
    ) -> Dict[str, Any]:
        """
        Operator-specific execution logic.
        
        Subclasses MUST implement this method with their transfer logic.
        Use execute_streaming() utility for threaded operations.
        
        Args:
            context: Dagster context
            source_config: Source configuration model
            target_config: Target configuration model
            template_vars: Template variables for rendering
            **kwargs: Additional resources
        
        Returns:
            Dict containing execution results and statistics
        """
        raise NotImplementedError("Subclass must implement _execute()")
    
    def post_execute(
        self,
        context,
        result: Dict[str, Any],
        duration: float = 0.0
    ) -> None:
        """
        Post-execution hook for cleanup and stats.
        """
        from dagster_dag_factory.factory.utils.logging import log_action, convert_size, convert_speed
        
        # Extract stats from result
        stats = result.get("stats", {}) if result else {}
        
        # Standard transfer metrics
        files = stats.get("files_transferred")
        rows = stats.get("rows_processed")
        size_bytes = stats.get("total_bytes", 0)
        
        # Build the stats line
        log_kv = {}
        if files is not None:
             log_kv["files"] = files
        if rows is not None:
             log_kv["rows"] = rows
             
        log_kv.update({
            "size": convert_size(size_bytes),
            "duration": f"{round(duration, 2)}s",
            "speed": convert_speed(size_bytes, duration)
        })
        
        log_action("TRANSFER_STATS", logger=context.log, **log_kv)
    
    def log_operator_configs(self, context, source_config, target_config, logger=None):
        """
        Log configurations for troubleshooting.
        """
        from dagster_dag_factory.factory.utils.logging import log_action
        
        def _get_masked_summary(config):
            if hasattr(config, "to_masked_dict"):
                data = config.to_masked_dict()
            elif hasattr(config, "model_dump"):
                data = config.model_dump()
            else:
                data = str(config)
                
            if isinstance(data, dict):
                # Only log top-level fields for the summary table
                return " | ".join([f"{k}: {v}" for k, v in data.items() if v is not None])
            return data

        lines = []
        lines.append(log_action("SOURCE_CONFIG", summary=_get_masked_summary(source_config), logger=logger))
        lines.append(log_action("TARGET_CONFIG", summary=_get_masked_summary(target_config), logger=logger))
        return "\n".join(lines)
    
    def execute_check(self, context, config: dict):
        """
        Execute a data quality check.
        
        This method is called by the factory for asset checks.
        Default implementation supports observation_diff checks.
        """
        from dagster import AssetCheckResult
        
        check_type = config.get("type", "observation_diff")
        
        if check_type == "observation_diff":
            return self._execute_observation_diff(context, config)
        
        # Default fallback for unknown check types
        context.log.warn(
            f"Unknown check type '{check_type}' for operator {self.__class__.__name__}"
        )
        return AssetCheckResult(
            passed=False,
            metadata={
                "error": f"Operator {self.__class__.__name__} does not support check type '{check_type}'"
            },
        )
    
    def _execute_observation_diff(self, context, config: dict):
        """
        Execute an observation diff check comparing source and target counts.
        """
        from dagster import AssetCheckResult
        
        asset_key = config.get("_asset_key")
        source_key = config["source_key"]
        target_key = config["target_key"]
        threshold = config.get("threshold", 0)
        
        # Retrieve the latest materialization event
        event = context.instance.get_latest_materialization_event(asset_key)
        if not event:
            return AssetCheckResult(
                passed=False,
                metadata={
                    "error": "Check failed: No materialization event found for this asset."
                },
            )
        
        metadata = event.asset_materialization.metadata
        
        # Extract values from Dagster Metadata
        def get_val(key):
            m_val = metadata.get(key)
            if m_val is None:
                return None
            if hasattr(m_val, "value"):
                return m_val.value
            return m_val
        
        source_val = get_val(source_key)
        target_val = get_val(target_key)
        
        if source_val is None or target_val is None:
            return AssetCheckResult(
                passed=False,
                metadata={
                    "error": f"Missing observation keys: {source_key}={source_val}, {target_key}={target_val}"
                },
            )
        
        diff = abs(source_val - target_val)
        passed = diff <= threshold
        
        return AssetCheckResult(
            passed=passed,
            metadata={
                source_key: source_val,
                target_key: target_val,
                "diff": diff,
                "threshold": threshold,
            },
        )
