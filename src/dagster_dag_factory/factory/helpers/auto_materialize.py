from typing import Optional, Dict, Any
from dagster import AutoMaterializePolicy


def get_auto_materialize_policy(
    config: Optional[Dict[str, Any]],
) -> Optional[AutoMaterializePolicy]:
    """
    Convert YAML auto-materialize config to Dagster AutoMaterializePolicy.

    Supports:
    - eager: AutoMaterializePolicy.eager()
    - lazy: AutoMaterializePolicy.lazy()
    """
    if not config:
        return None

    policy_type = config.get("type", "").lower()
    max_materializations_per_minute = config.get("max_materializations_per_minute")

    if policy_type == "eager":
        policy = AutoMaterializePolicy.eager()
    elif policy_type == "lazy":
        policy = AutoMaterializePolicy.lazy()
    else:
        raise ValueError(
            f"Unsupported auto_materialize_policy type: {policy_type}. Use 'eager' or 'lazy'."
        )

    # Apply rate limiting if specified
    if max_materializations_per_minute:
        policy = policy.with_max_materializations_per_minute(
            max_materializations_per_minute
        )

    return policy
