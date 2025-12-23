import re
from typing import Any, Dict, TypeVar
from enum import Enum
from pydantic import BaseModel
import jinja2

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

# Create a Jinja2 environment for rendering
_jinja_env = jinja2.Environment(
    variable_start_string="{{",
    variable_end_string="}}",
    undefined=jinja2.StrictUndefined # Fail on missing variables
)

def render_config(d: Any, template_vars: Dict[str, Any]) -> Any:
    """
    Recursively renders configuration values using Jinja2.
    Supports:
    1. Full match: "{{ env.VAR }}" -> returns EnvVar object or value
    2. Interpolation: "Path: {{ vars.BASE }}/file" -> returns string
    3. Macros: "{{ fn.date.to_date_nodash(partition_key) }}"
    """
    if isinstance(d, dict):
        return {k: render_config(v, template_vars) for k, v in d.items()}
    elif isinstance(d, list):
        return [render_config(x, template_vars) for x in d]
    elif isinstance(d, str) and not hasattr(d, "__enum_cls__") and not isinstance(d, Enum):
        v = d.strip()
        # Pattern for exact {{ ... }} matches to return non-string types
        full_match_pattern = re.compile(r"\{\{\s*([^}]*)\s*\}\}")
        
        # 1. Full match check for returning raw objects (like EnvVars)
        if full_match_pattern.fullmatch(v):
            try:
                # Use jinja to evaluate the expression directly
                return _jinja_env.compile_expression(v[2:-2].strip())(**template_vars)
            except Exception:
                # If evaluation fails or is complex, fall back to string rendering
                pass

        # 2. String interpolation
        try:
            template = _jinja_env.from_string(d)
            return template.render(**template_vars)
        except Exception as e:
            # Fallback for complex paths or missing vars
            return d
    else:
        return d
