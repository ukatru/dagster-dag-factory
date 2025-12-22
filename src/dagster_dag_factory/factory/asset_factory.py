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
    AssetKey
)
import yaml
from pathlib import Path
from datetime import timedelta
from typing import Dict, Any, List, Optional
from dagster_dag_factory.factory.registry import OperatorRegistry
# Import operators package to start registration
import dagster_dag_factory.operators  
from dagster_dag_factory.factory.partition_factory import PartitionFactory

class AssetFactory:
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)

    def load_assets(self):
        all_defs = []
        for yaml_file in self.base_dir.rglob("*.yaml"):
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

    def _get_backfill_policy(self, config: Optional[Dict[str, Any]]) -> Optional[BackfillPolicy]:
        if not config:
            return None
        p_type = config.get("type", "").lower()
        if p_type == "single_run":
            return BackfillPolicy.single_run()
        elif p_type == "multi_run":
            max_partitions_per_run = config.get("max_partitions_per_run", 1)
            return BackfillPolicy.multi_run(max_partitions_per_run)
        return None

    def _get_partition_mapping(self, config: Optional[Dict[str, Any]]):
        if not config:
            return None
        p_type = config.get("type", "").lower()
        if p_type == "identity":
            return IdentityPartitionMapping()
        elif p_type == "last":
            return LastPartitionMapping()
        elif p_type == "all":
            return AllPartitionMapping()
        elif p_type == "time_window":
            return TimeWindowPartitionMapping(
                allow_incomplete_mappings=config.get("allow_incomplete_mappings", False),
                start_offset=config.get("start_offset", 0),
                end_offset=config.get("end_offset", 0)
            )
        elif p_type == "multi":
            mappings_config = config.get("mappings", {})
            mappings = {}
            for dim_name, mapping_conf in mappings_config.items():
                mappings[dim_name] = self._get_partition_mapping(mapping_conf)
            return MultiPartitionMapping(mappings)
        return None

    def _get_automation_policy(self, policy_name: Optional[str]) -> Optional[AutoMaterializePolicy]:
        if not policy_name:
            return None
        if policy_name.lower() == "eager":
            return AutoMaterializePolicy.eager()
        elif policy_name.lower() == "lazy":
            return AutoMaterializePolicy.lazy()
        return None

    def _get_freshness_policy(self, config: Optional[Dict[str, Any]]) -> Optional[FreshnessPolicy]:
        if not config:
            return None
        
        cron = config.get("cron")
        lag = config.get("maximum_lag_minutes", 0)
        
        if cron:
            return FreshnessPolicy.cron(
                deadline_cron=cron,
                lower_bound_delta=timedelta(minutes=lag)
            )
        else:
            return FreshnessPolicy.time_window(
                fail_window=timedelta(minutes=lag)
            )

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
        return template_vars

    def _render_config(self, d: Any, template_vars: Dict[str, Any]) -> Any:
        if isinstance(d, dict):
            new_d = {}
            for k, v in d.items():
                new_d[k] = self._render_config(v, template_vars)
            return new_d
        elif isinstance(d, list):
            return [self._render_config(x, template_vars) for x in d]
        elif isinstance(d, str):
            v = d
            for var_k, var_v in template_vars.items():
                v = v.replace(f"{{{{ {var_k} }}}}", str(var_v))
                v = v.replace(f"{{{{{var_k}}}}}", str(var_v))
            return v
        else:
            return d

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
                rendered_conf = self._render_config(check_conf, template_vars)
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
        
        # Partition Support
        partitions_def = PartitionFactory.get_partitions_def(config.get("partitions_def"))
        
        # Backfill Policy
        backfill_policy = self._get_backfill_policy(config.get("backfill_policy"))
        
        # Automation Policy
        automation_policy = self._get_automation_policy(config.get("automation_policy"))

        # Freshness Policy
        freshness_policy = self._get_freshness_policy(config.get("freshness_policy"))

        # Input dependencies with Partition Mappings
        ins = {}
        ins_config = config.get("ins", {})
        for dep_name, dep_conf in ins_config.items():
            partition_mapping = self._get_partition_mapping(dep_conf.get("partition_mapping"))
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
                rendered_source = self._render_config(source_conf, template_vars)
                rendered_target = self._render_config(target_conf, template_vars)

                operator.execute(context, rendered_source, rendered_target)
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
            freshness_policy=freshness_policy
        )
        def _generated_asset(context: AssetExecutionContext, **kwargs):
            return logic(context, source, target)
            
        # Create checks
        checks = self._create_checks(AssetKey(name), config.get("checks", []), required_resources)
        
        return [_generated_asset, *checks]
