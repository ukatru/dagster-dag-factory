import re
from typing import Any, Dict

def render_config(d: Any, template_vars: Dict[str, Any]) -> Any:
    """
    Recursively renders configuration values using template variables.
    Supports nested access like {{env.VAR_NAME}} and {{partition_key.dimension}}.
    """
    if isinstance(d, dict):
        new_d = {}
        for k, v in d.items():
            new_d[k] = render_config(v, template_vars)
        return new_d
    elif isinstance(d, list):
        return [render_config(x, template_vars) for x in d]
    elif isinstance(d, str):
        v = d
        # Handle complex object access like {{env.PATH}} or {{partition_key.date}}
        # We look for {{ ... }} patterns
        pattern = re.compile(r"\{\{\s*([^}\s]+)\s*\}\}")
        
        def replace_match(match):
            path = match.group(1)
            # Split by dot for nested access
            parts = path.split('.')
            curr = template_vars
            for part in parts:
                if isinstance(curr, dict) and part in curr:
                    curr = curr[part]
                else:
                    return match.group(0) # Keep literal if not found
            return str(curr)

        return pattern.sub(replace_match, v)
    else:
        return d
