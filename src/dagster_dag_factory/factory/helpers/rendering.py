import re
from typing import Any, Dict

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
    for part in parts:
        if isinstance(curr, dict) and part in curr:
            curr = curr[part]
        elif hasattr(curr, part):
            # If it's the 'env' accessor and we are interpolating, use get_raw
            if part == parts[-1] and hasattr(curr, "get_raw") and interpolate:
                 # This is a bit specific to EnvVarAccessor but helps with "host: {{env.HOST}}"
                 # Actually, better to checks if we are at the end
                 pass 
            
            # Use getattr for EnvVarAccessor or regular objects
            res = getattr(curr, part)
            if interpolate and hasattr(curr, "get_raw") and not isinstance(res, (dict, list)):
                # If we have a get_raw method, use it for interpolation
                return curr.get_raw(part)
            return res
        else:
            return f"{{{{{path}}}}}"
    return curr
