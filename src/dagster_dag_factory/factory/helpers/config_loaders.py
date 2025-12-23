import os
import yaml
from typing import Dict, Any
from pathlib import Path

def _deep_merge(base: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merges two dictionaries."""
    for key, value in overrides.items():
        if isinstance(value, dict) and key in base and isinstance(base[key], dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base

def load_env_config(directory: Path) -> Dict[str, Any]:
    """
    Loads common and environment-specific configurations from the specified directory.
    Follows the pattern: common.yaml -> <ENV>.yaml (deep merged).
    """
    env = os.getenv("ENV", "dev")
    all_config = {}
    
    # 1. Load common.yaml
    common_path = directory / "common.yaml"
    if common_path.exists():
        with open(common_path) as f:
            common_vars = yaml.safe_load(f) or {}
            _deep_merge(all_config, common_vars)
    
    # 2. Load env specific
    env_path = directory / f"{env}.yaml"
    if env_path.exists():
        with open(env_path) as f:
            env_vars = yaml.safe_load(f) or {}
            _deep_merge(all_config, env_vars)
    
    return all_config

def load_env_vars(base_dir: Path) -> Dict[str, Any]:
    """Backwards compatibility wrapper for loading variables from the 'vars' folder."""
    return load_env_config(base_dir / "vars")
