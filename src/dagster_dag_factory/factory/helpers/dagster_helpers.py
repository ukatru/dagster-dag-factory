from typing import Optional, Dict, Any
from datetime import timedelta
from dagster import (
    BackfillPolicy, 
    AutoMaterializePolicy, 
    RetryPolicy, 
    Backoff,
    IdentityPartitionMapping,
    LastPartitionMapping,
    AllPartitionMapping,
    TimeWindowPartitionMapping,
    MultiPartitionMapping
)
from dagster_dag_factory.factory.helpers.dagster_compat import FreshnessPolicy

def get_backfill_policy(config: Optional[Dict[str, Any]]) -> Optional[BackfillPolicy]:
    if not config:
        return None
    p_type = config.get("type", "").lower()
    if p_type == "single_run":
        return BackfillPolicy.single_run()
    elif p_type == "multi_run":
        max_partitions_per_run = config.get("max_partitions_per_run", 1)
        return BackfillPolicy.multi_run(max_partitions_per_run)
    return None

def get_partition_mapping(config: Optional[Dict[str, Any]]):
    if not config:
        return None
    p_type = config.get("type", "").lower()
    if p_type == "identity":
        return IdentityPartitionMapping()
    elif p_type == "last":
        return LastPartitionMapping()
    elif p_type == "all":
        return AllPartitionMapping()
    elif p_type == "time_window":
        return TimeWindowPartitionMapping(
            allow_incomplete_mappings=config.get("allow_incomplete_mappings", False),
            start_offset=config.get("start_offset", 0),
            end_offset=config.get("end_offset", 0)
        )
    elif p_type == "multi":
        mappings_config = config.get("mappings", {})
        mappings = {}
        for dim_name, mapping_conf in mappings_config.items():
            mappings[dim_name] = get_partition_mapping(mapping_conf)
        return MultiPartitionMapping(mappings)
    return None

def get_automation_policy(policy_name: Optional[str]) -> Optional[AutoMaterializePolicy]:
    if not policy_name:
        return None
    if policy_name.lower() == "eager":
        return AutoMaterializePolicy.eager()
    elif policy_name.lower() == "lazy":
        return AutoMaterializePolicy.lazy()
    return None

def get_freshness_policy(config: Optional[Dict[str, Any]]) -> Optional[FreshnessPolicy]:
    if not config:
        return None
    
    cron = config.get("cron")
    lag = config.get("maximum_lag_minutes", 0)
    
    # Use direct constructor for LegacyFreshnessPolicy compatibility (1.11.1)
    return FreshnessPolicy(
        maximum_lag_minutes=float(lag),
        cron_schedule=cron
    )

def get_retry_policy(config: Optional[Dict[str, Any]]) -> Optional[RetryPolicy]:
    if not config:
        return None
        
    backoff_str = str(config.get("backoff_type", "constant")).upper()
    if backoff_str == "EXPONENTIAL":
        backoff = Backoff.EXPONENTIAL
    else:
        backoff = Backoff.LINEAR 

    return RetryPolicy(
        max_retries=config.get("max_retries", 0),
        delay=config.get("delay_seconds"),
        backoff=backoff
    )
