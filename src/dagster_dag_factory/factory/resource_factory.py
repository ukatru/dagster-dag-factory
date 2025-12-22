import yaml
import os
import importlib
import inspect
from pathlib import Path
from typing import Dict, Any, Type
from dagster import EnvVar, ConfigurableResource
import dagster_dag_factory.resources as resources_module

class ResourceFactory:
    """
    Factory class to load Dagster resources dynamically from YAML configurations.
    Supports environment variable substitution for values starting with 'env:'.
    """
    
    @staticmethod
    def load_resources_from_dir(directory: Path) -> Dict[str, Any]:
        """
        Loads all resources defined in YAML files within the specified directory.
        """
        resources = {}
        if not directory.exists():
            return resources

        for yaml_file in directory.rglob("*.yaml"):
            with open(yaml_file) as f:
                config = yaml.safe_load(f)
                if not config or "resources" not in config:
                    continue
                
                for res_name, res_def in config["resources"].items():
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
    def _create_resource(res_def: Dict[str, Any]) -> Any:
        res_type_name = res_def.get("type")
        res_config = res_def.get("config", {})
        
        # Parse config for EnvVars
        parsed_config = ResourceFactory._parse_config_values(res_config)
        
        # Dynamically find the resource class
        res_class = ResourceFactory._get_resource_class(res_type_name)
        
        if not res_class:
            raise ValueError(f"Resource type '{res_type_name}' not found in dagster_dag_factory.resources")
            
        return res_class(**parsed_config)

    @staticmethod
    def _get_resource_class(type_name: str) -> Type[ConfigurableResource]:
        """
        Looks up the resource class component in dagster_dag_factory.resources.
        """
        if hasattr(resources_module, type_name):
            return getattr(resources_module, type_name)
        return None

    @staticmethod
    def _parse_config_values(config: Any) -> Any:
        """
        recursively parses dictionary values to replace 'env:VAR_NAME' with dagster.EnvVar.
        """
        if isinstance(config, dict):
            return {k: ResourceFactory._parse_config_values(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [ResourceFactory._parse_config_values(v) for v in config]
        elif isinstance(config, str) and config.startswith("env:"):
            env_var_name = config[4:]
            return EnvVar(env_var_name)
        else:
            return config
