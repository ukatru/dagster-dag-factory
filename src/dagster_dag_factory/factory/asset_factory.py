from dagster import (
    asset,
    asset_check,
    AssetExecutionContext,
    AssetCheckExecutionContext,
    AssetIn,
    SourceAsset,
    AssetKey,
)
import yaml
from pathlib import Path
from typing import Dict, Any, List
from dagster_dag_factory.factory.registry import OperatorRegistry

# Import operators package to start registration
import dagster_dag_factory.operators as _operators  # noqa: F401
from dagster_dag_factory.factory.partition_factory import PartitionFactory
from dagster_dag_factory.factory.helpers.rendering import render_config
from dagster_dag_factory.factory.helpers.config_loaders import load_env_vars
from dagster_dag_factory.factory.helpers.env_accessor import EnvVarAccessor
from dagster_dag_factory.factory.helpers.dynamic import Dynamic
from dagster_dag_factory.factory.helpers.macros import get_macros
from dagster_dag_factory.factory.helpers.dagster_compat import (
    FRESHNESS_POLICY_KEY,
)
from dagster_dag_factory.factory.helpers.auto_materialize import (
    get_auto_materialize_policy,
)
from dagster_dag_factory.factory.helpers.dagster_helpers import (
    get_backfill_policy,
    get_partition_mapping,
    get_automation_policy,
    get_freshness_policy,
    get_retry_policy,
)


class AssetFactory:
    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)
        self.env_vars = load_env_vars(self.base_dir)

    def load_assets(self):
        all_defs = []
        defs_dir = self.base_dir / "defs"
        for yaml_file in defs_dir.rglob("*.yaml"):
            try:
                with open(yaml_file) as f:
                    config = yaml.safe_load(f)

                if not config:
                    continue

                if "assets" in config:
                    for asset_conf in config["assets"]:
                        try:
                            asset_defs = self._create_asset(asset_conf)
                            if isinstance(asset_defs, list):
                                all_defs.extend(asset_defs)
                            else:
                                all_defs.append(asset_defs)
                        except Exception as e:
                            print(
                                f"ERROR: Failed to create asset from {yaml_file}: {e}"
                            )
                            raise e

                if "source_assets" in config:
                    for sa_conf in config["source_assets"]:
                        try:
                            all_defs.append(self._create_source_asset(sa_conf))
                        except Exception as e:
                            print(
                                f"ERROR: Failed to create source asset from {yaml_file}: {e}"
                            )
                            raise e
            except Exception as e:
                print(f"ERROR: Critical failure loading {yaml_file}: {e}")
                raise e
        return all_defs

    def _create_source_asset(self, config: Dict[str, Any]) -> SourceAsset:
        name = config["name"]
        description = config.get("description")
        partitions_def = PartitionFactory.get_partitions_def(
            config.get("partitions_def")
        )

        return SourceAsset(
            key=AssetKey(name), description=description, partitions_def=partitions_def
        )

    def _get_template_vars(self, context) -> Dict[str, Any]:
        template_vars = {}

        # In newer Dagster, AssetCheckExecutionContext is a wrapper around OpExecutionContext.
        # We need the inner context to check for partition keys reliably.
        inner_context = getattr(context, "op_execution_context", context)

        if (
            hasattr(inner_context, "has_partition_key")
            and inner_context.has_partition_key
        ):
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
                template_vars["partition_key"] = Dynamic(pk.keys_by_dimension)
                # Also provide flat keys for backward compatibility if needed,
                # but Jinja2 prefers the nested dict for {{ partition_key.dim }}
                for dim_name, dim_value in pk.keys_by_dimension.items():
                    template_vars[f"partition_key_{dim_name}"] = dim_value
            else:
                template_vars["partition_key"] = pk
        else:
            # Fallback for ad-hoc or non-partitioned runs to prevent NameError in macros
            template_vars["partition_key"] = None

        # Add vars and env
        template_vars["vars"] = Dynamic(self.env_vars)
        template_vars["env"] = EnvVarAccessor()
        template_vars.update(get_macros(context))

        return template_vars

    def _create_checks(
        self,
        asset_key: AssetKey,
        config_list: List[Dict[str, Any]],
        required_resources: set,
        operator,
    ):
        checks = []

        def make_check(check_conf):
            check_name = check_conf["name"]

            # Extract resources needed by this check
            check_resources = set()
            if "connection" in check_conf:
                check_resources.add(check_conf["connection"])

            @asset_check(
                asset=asset_key, name=check_name, required_resource_keys=check_resources
            )
            def _generated_check(context: AssetCheckExecutionContext):
                template_vars = self._get_template_vars(context)
                rendered_conf = render_config(check_conf, template_vars)
                # Pass asset_key to the check object
                rendered_conf["_asset_key"] = asset_key
                return operator.execute_check(context, rendered_conf)

            return _generated_check

        for conf in config_list:
            check_def = make_check(conf)
            if check_def:
                checks.append(check_def)

        return checks

    def _create_asset(self, config):
        name = config["name"]
        group = config.get("group", "default")
        description = config.get("description")

        source = config.get("source", {})
        target = config.get("target", {})
        deps = config.get("deps", [])

        # Metadata and Tags
        metadata = config.get("metadata")
        tags = config.get("tags") or {}

        # Concurrency support
        pool = config.get("concurrency_key")

        # Partition Support
        partitions_def = PartitionFactory.get_partitions_def(
            config.get("partitions_def")
        )

        # Backfill Policy
        backfill_policy = get_backfill_policy(config.get("backfill_policy"))

        # Automation Policy
        automation_policy = get_automation_policy(config.get("automation_policy"))

        # Freshness Policy
        freshness_policy = get_freshness_policy(config.get("freshness_policy"))

        # Auto-Materialize Policy
        auto_materialize_policy = get_auto_materialize_policy(
            config.get("auto_materialize_policy")
        )

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

        # Prepare asset arguments
        asset_kwargs = {
            "name": name,
            "group_name": group,
            "description": description,
            "required_resource_keys": required_resources,
            "deps": deps,
            "ins": ins,
            "partitions_def": partitions_def,
            "metadata": metadata,
            "tags": tags,
            "retry_policy": retry_policy,
            "pool": pool,
        }
        if backfill_policy:
            asset_kwargs["backfill_policy"] = backfill_policy
        if automation_policy:
            asset_kwargs["auto_materialize_policy"] = automation_policy
        elif auto_materialize_policy:
            asset_kwargs["auto_materialize_policy"] = auto_materialize_policy
        if freshness_policy:
            asset_kwargs[FRESHNESS_POLICY_KEY] = freshness_policy

        # Select logic based on types
        source_type = source.get("type")
        target_type = target.get("type")

        # Dynamic Operator Lookup
        operator_class = OperatorRegistry.get_operator(source_type, target_type)

        if not operator_class:

            def _generated_asset(context: AssetExecutionContext, **kwargs):
                raise NotImplementedError(
                    f"No operator registered for {source_type}->{target_type}"
                )

            return [asset(**asset_kwargs)(_generated_asset)]
        else:
            # Instantiate operator
            operator = operator_class()

            def logic(context, source_conf, target_conf, operator=operator):
                template_vars = self._get_template_vars(context)

                # Resolve Resources for injection (Dagster style)
                # We extract the 'connection' string from the raw config before validation
                source_conn_name = source_conf.get("connection")
                target_conn_name = target_conf.get("connection")

                source_res = (
                    getattr(context.resources, source_conn_name)
                    if source_conn_name
                    else None
                )
                target_res = (
                    getattr(context.resources, target_conn_name)
                    if target_conn_name
                    else None
                )

                # Render source FIRST
                rendered_source = render_config(source_conf, template_vars)

                # Validate source if schema exists
                if operator.source_config_schema:
                    try:
                        source_model = operator.source_config_schema(**rendered_source)
                        template_vars["source"] = source_model
                    except Exception as e:
                        context.log.error(
                            f"Source configuration validation failed: {e}"
                        )
                        raise
                else:
                    dynamic_source = Dynamic(rendered_source)
                    template_vars["source"] = dynamic_source
                    source_model = dynamic_source

                # Render target SECOND (now has access to source model)
                rendered_target = render_config(target_conf, template_vars)

                # Validate target if schema exists
                if operator.target_config_schema:
                    try:
                        target_model = operator.target_config_schema(**rendered_target)
                    except Exception as e:
                        context.log.error(
                            f"Target configuration validation failed: {e}"
                        )
                        raise
                else:
                    target_model = rendered_target

                # Log configurations for troubleshooting
                operator.log_configs(context, source_model, target_model)

                results = operator.execute(
                    context=context,
                    source_config=source_model,
                    target_config=target_model,
                    template_vars=template_vars,
                    source_resource=source_res,
                    target_resource=target_res,
                )

                # Automatically harvest observations as Dagster Metadata
                if results and isinstance(results, dict) and "observations" in results:
                    context.add_output_metadata(results["observations"])

                return None

            def _generated_asset(context: AssetExecutionContext, **kwargs):
                source = config.get("source", {})
                target = config.get("target", {})
                return logic(context, source, target)

            # Create checks using the operator instance
            checks = self._create_checks(
                AssetKey(name), config.get("checks", []), required_resources, operator
            )

            return [asset(**asset_kwargs)(_generated_asset), *checks]
