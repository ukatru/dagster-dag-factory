from dagster import (
    asset, 
    asset_check,
    AssetExecutionContext,
    AssetCheckExecutionContext,
    BackfillPolicy, 
    AutoMaterializePolicy, 
    AssetIn,
    FreshnessPolicy,
    IdentityPartitionMapping,
    LastPartitionMapping,
    AllPartitionMapping,
    TimeWindowPartitionMapping,
    MultiPartitionMapping,
    AssetCheckResult,
    SourceAsset,
    AssetKey,
    RetryPolicy,
    Backoff
)
import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional
from dagster_dag_factory.factory.registry import OperatorRegistry
# Import operators package to start registration
import dagster_dag_factory.operators  
from dagster_dag_factory.factory.partition_factory import PartitionFactory
from dagster_dag_factory.factory.helpers.rendering import render_config
from dagster_dag_factory.factory.helpers.config_loaders import load_env_vars
from dagster_dag_factory.factory.helpers.env_accessor import EnvVarAccessor
from dagster_dag_factory.factory.helpers.dagster_helpers import (
    get_backfill_policy,
    get_partition_mapping,
    get_automation_policy,
    get_freshness_policy,
    get_retry_policy
)

class AssetFactory:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.env_vars = load_env_vars(self.base_dir)


    def load_assets(self):
        all_defs = []
        defs_dir = self.base_dir / "defs"
        for yaml_file in defs_dir.rglob("*.yaml"):
            with open(yaml_file) as f:
                config = yaml.safe_load(f)
                
            if "assets" in config:
                for asset_conf in config["assets"]:
                    asset_defs = self._create_asset(asset_conf)
                    if isinstance(asset_defs, list):
                        all_defs.extend(asset_defs)
                    else:
                        all_defs.append(asset_defs)
            
            if "source_assets" in config:
                for sa_conf in config["source_assets"]:
                    all_defs.append(self._create_source_asset(sa_conf))
        return all_defs


    def _create_source_asset(self, config: Dict[str, Any]) -> SourceAsset:
        name = config["name"]
        description = config.get("description")
        partitions_def = PartitionFactory.get_partitions_def(config.get("partitions_def"))
        
        return SourceAsset(
            key=AssetKey(name),
            description=description,
            partitions_def=partitions_def
        )

    def _get_template_vars(self, context) -> Dict[str, Any]:
        template_vars = {}
        
        # In newer Dagster, AssetCheckExecutionContext is a wrapper around OpExecutionContext.
        # We need the inner context to check for partition keys reliably.
        inner_context = getattr(context, "op_execution_context", context)
        
        if hasattr(inner_context, "has_partition_key") and inner_context.has_partition_key:
            pk = inner_context.partition_key
            # Try to get time-based window if available
            try:
                tw = inner_context.partition_time_window
                template_vars["partition_start"] = tw.start.isoformat()
                template_vars["partition_end"] = tw.end.isoformat()
            except Exception:
                pass

            # Handle Multi-dimensional keys
            from dagster import MultiPartitionKey
            if isinstance(pk, MultiPartitionKey):
                template_vars["partition_key"] = str(pk)
                for dim_name, dim_value in pk.keys_by_dimension.items():
                    template_vars[f"partition_key.{dim_name}"] = dim_value
            else:
                template_vars["partition_key"] = pk
        
        # Add vars and env
        template_vars["vars"] = self.env_vars
        template_vars["env"] = EnvVarAccessor()
        
        return template_vars


    def _create_checks(self, asset_key: AssetKey, config_list: List[Dict[str, Any]], required_resources: set):
        from dagster_dag_factory.factory.check_registry import CheckRegistry
        
        checks = []

        def make_check(check_conf):
            check_name = check_conf["name"]
            check_type = check_conf["type"]
            
            check_cls = CheckRegistry.get_check(check_type)
            if not check_cls:
                return None
                
            check_obj = check_cls()
            
            # Extract resources needed by this check
            check_resources = set()
            if "connection" in check_conf:
                check_resources.add(check_conf["connection"])
            
            @asset_check(asset=asset_key, name=check_name, required_resource_keys=check_resources)
            def _generated_check(context: AssetCheckExecutionContext):
                template_vars = self._get_template_vars(context)
                rendered_conf = render_config(check_conf, template_vars)
                return check_obj.execute(context, rendered_conf)
            
            return _generated_check

        for conf in config_list:
            check_def = make_check(conf)
            if check_def:
                checks.append(check_def)
                
        return checks

    def _create_asset(self, config):
        name = config["name"]
        group = config.get("group", "default")
        
        source = config.get("source", {})
        target = config.get("target", {})
        deps = config.get("deps", [])
        
        # Metadata and Tags
        metadata = config.get("metadata")
        tags = config.get("tags") or {}
        
        # Concurrency support
        pool = config.get("concurrency_key")

        # Partition Support
        partitions_def = PartitionFactory.get_partitions_def(config.get("partitions_def"))
        
        # Backfill Policy
        backfill_policy = get_backfill_policy(config.get("backfill_policy"))
        
        # Automation Policy
        automation_policy = get_automation_policy(config.get("automation_policy"))

        # Freshness Policy
        freshness_policy = get_freshness_policy(config.get("freshness_policy"))

        # Retry Policy
        retry_policy = get_retry_policy(config.get("retry_policy"))

        # Input dependencies with Partition Mappings
        ins = {}
        ins_config = config.get("ins", {})
        for dep_name, dep_conf in ins_config.items():
            partition_mapping = get_partition_mapping(dep_conf.get("partition_mapping"))
            ins[dep_name] = AssetIn(partition_mapping=partition_mapping)

        # Remove assets in 'ins' from 'deps' to avoid duplication error
        deps = [d for d in deps if d not in ins]

        # Determine required resources
        required_resources = set()
        if "connection" in source:
            required_resources.add(source["connection"])
        if "connection" in target:
            required_resources.add(target["connection"])
            
        # Select logic based on types
        source_type = source.get("type")
        target_type = target.get("type")
        
        # Dynamic Operator Lookup
        operator_class = OperatorRegistry.get_operator(source_type, target_type)
        
        if not operator_class:
            def logic(*args, **kwargs): 
                raise NotImplementedError(f"No operator registered for {source_type}->{target_type}")
        else:
            # Instantiate operator
            operator = operator_class()
            def logic(context, source_conf, target_conf):
                template_vars = self._get_template_vars(context)
                
                # Render source and target configs
                rendered_source = render_config(source_conf, template_vars)
                rendered_target = render_config(target_conf, template_vars)

                # Validate using Op schemas if provided
                if operator.source_config_schema:
                    try:
                        validated_source = operator.source_config_schema(**rendered_source)
                    except Exception as e:
                        context.log.error(f"Source configuration validation failed: {e}")
                        raise
                else:
                    validated_source = rendered_source

                if operator.target_config_schema:
                    try:
                        validated_target = operator.target_config_schema(**rendered_target)
                    except Exception as e:
                        context.log.error(f"Target configuration validation failed: {e}")
                        raise
                else:
                    validated_target = rendered_target

                # Log configurations for troubleshooting
                operator.log_configs(context, validated_source, validated_target)

                operator.execute(context, validated_source, validated_target)
                return None

        @asset(
            name=name, 
            group_name=group, 
            required_resource_keys=required_resources,
            deps=deps,
            ins=ins,
            partitions_def=partitions_def,
            backfill_policy=backfill_policy,
            auto_materialize_policy=automation_policy,
            freshness_policy=freshness_policy,
            metadata=metadata,
            tags=tags,
            retry_policy=retry_policy,
            pool=pool
        )
        def _generated_asset(context: AssetExecutionContext, **kwargs):
            return logic(context, source, target)
            
        # Create checks
        checks = self._create_checks(AssetKey(name), config.get("checks", []), required_resources)
        
        return [_generated_asset, *checks]
