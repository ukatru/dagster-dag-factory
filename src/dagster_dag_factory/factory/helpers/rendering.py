import re
from typing import Any, Dict, TypeVar
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)

def render_config_model(model: T, template_vars: Dict[str, Any]) -> T:
    """
    Renders an existing Pydantic model by converting to dict, rendering, 
    and re-instantiating. Useful for runtime overrides (e.g., inside a loop).
    """
    # Convert to dict, including extras
    model_dict = model.model_dump()
    rendered_dict = render_config(model_dict, template_vars)
    # Re-instantiate as the same type
    return type(model)(**rendered_dict)

def render_config(d: Any, template_vars: Dict[str, Any]) -> Any:
    """
    Recursively renders configuration values using template variables.
    Supports:
    1. Full match: "{{ env.VAR }}" -> returns EnvVar object
    2. Interpolation: "Path: {{ vars.BASE }}/file" -> returns string
    """
    if isinstance(d, dict):
        return {k: render_config(v, template_vars) for k, v in d.items()}
    elif isinstance(d, list):
        return [render_config(x, template_vars) for x in d]
    elif isinstance(d, str):
        v = d
        # Pattern for {{ path.to.var }}
        pattern = re.compile(r"\{\{\s*([^}\s]+)\s*\}\}")
        
        # 1. Full match check
        full_match = pattern.fullmatch(v.strip())
        if full_match:
            return _get_value_at_path(full_match.group(1), template_vars, interpolate=False)

        # 2. String interpolation
        def replace_match(match):
            return str(_get_value_at_path(match.group(1), template_vars, interpolate=True))

        return pattern.sub(replace_match, v)
    else:
        return d

def _get_value_at_path(path: str, template_vars: Dict[str, Any], interpolate: bool) -> Any:
    parts = path.split('.')
    curr = template_vars
    for i, part in enumerate(parts):
        is_last = (i == len(parts) - 1)
        
        if isinstance(curr, dict) and part in curr:
            curr = curr[part]
        elif hasattr(curr, part):
            # Special handling for EnvVarAccessor (env.)
            if hasattr(curr, "get_raw") and interpolate and not isinstance(getattr(curr, part), (dict, list)):
                # If we are at the target property and it's an EnvVarAccessor, get raw value
                # but only if it's the leaf node or we want interpolation
                if is_last:
                    return curr.get_raw(part)
            
            curr = getattr(curr, part)
        else:
            return f"{{{{{path}}}}}"
    return curr
