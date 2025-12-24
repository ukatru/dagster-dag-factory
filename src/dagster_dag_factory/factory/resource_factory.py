from pathlib import Path
from typing import Dict, Any, Type, Optional
from dagster import ConfigurableResource
import dagster_dag_factory.resources as resources_module
from dagster_dag_factory.factory.helpers.config_loaders import load_env_config


class ResourceFactory:
    """
    Factory class to load Dagster resources dynamically from YAML configurations.
    Supports hierarchical environment-based loading and 'env:' variable substitution.
    """

    @staticmethod
    def load_resources_from_dir(directory: Path) -> Dict[str, Any]:
        """
        Loads resources following the environment hierarchy (common.yaml + <ENV>.yaml).
        """
        resources = {}
        if not directory.exists():
            return resources

        # Use the generalized hierarchical loader
        merged_config = load_env_config(directory)

        if "resources" in merged_config:
            for res_name, res_def in merged_config["resources"].items():
                resources[res_name] = ResourceFactory._create_resource(res_def)

        return resources

    @staticmethod
    def load_resources_from_config(resources_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Loads resources provided as a dictionary configuration.
        """
        resources = {}
        if not resources_config:
            return resources

        for res_name, res_def in resources_config.items():
            resources[res_name] = ResourceFactory._create_resource(res_def)

        return resources

    @staticmethod
    def _create_resource(
        res_def: Dict[str, Any], template_vars: Optional[Dict[str, Any]] = None
    ) -> Any:
        res_type_name = res_def.get("type")
        res_config = res_def.get("config", {})

        # If no template vars provided (base case), we still provide env accessor
        if template_vars is None:
            from dagster_dag_factory.factory.helpers.env_accessor import EnvVarAccessor

            template_vars = {"env": EnvVarAccessor()}

        # Parse config using unified rendering
        from dagster_dag_factory.factory.helpers.rendering import render_config

        parsed_config = render_config(res_config, template_vars)

        # Dynamically find the resource class
        res_class = ResourceFactory._get_resource_class(res_type_name)

        if not res_class:
            raise ValueError(
                f"Resource type '{res_type_name}' not found in dagster_dag_factory.resources"
            )

        return res_class(**parsed_config)

    @staticmethod
    def _get_resource_class(type_name: str) -> Type[ConfigurableResource]:
        """
        Looks up the resource class component in dagster_dag_factory.resources.
        """
        if hasattr(resources_module, type_name):
            return getattr(resources_module, type_name)
        return None
