import os
import yaml
from typing import Dict, Any
from pathlib import Path

def load_env_vars(base_dir: Path) -> Dict[str, Any]:
    """Loads common and environment-specific variables from the vars directory."""
    vars_dir = base_dir / "vars"
    env = os.getenv("ENV", "dev")
    
    all_vars = {}
    
    # 1. Load common.yaml
    common_path = vars_dir / "common.yaml"
    if common_path.exists():
        with open(common_path) as f:
            common_vars = yaml.safe_load(f) or {}
            all_vars.update(common_vars)
    
    # 2. Load env specific
    env_path = vars_dir / f"{env}.yaml"
    if env_path.exists():
        with open(env_path) as f:
            env_vars = yaml.safe_load(f) or {}
            all_vars.update(env_vars)
    
    return all_vars
