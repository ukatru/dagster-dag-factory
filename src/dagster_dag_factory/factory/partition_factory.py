from typing import Any, Dict, Optional, Union
from dagster import (
    DailyPartitionsDefinition, 
    HourlyPartitionsDefinition, 
    WeeklyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    StaticPartitionsDefinition,
    DynamicPartitionsDefinition,
    MultiPartitionsDefinition,
    TimeWindowPartitionsDefinition,
    PartitionsDefinition,
    MultiPartitionKey
)

class PartitionFactory:
    """
    Creates Dagster PartitionsDefinition objects from YAML configuration.
    """
    _cache: Dict[str, PartitionsDefinition] = {}

    @classmethod
    def get_partitions_def(cls, config: Dict[str, Any]) -> Optional[PartitionsDefinition]:
        if not config:
            return None
            
        # Use a stable JSON string as the cache key
        import json
        cache_key = json.dumps(config, sort_keys=True)
        if cache_key in cls._cache:
            return cls._cache[cache_key]
            
        partitions_def = cls._get_partitions_def_inner(config)
        if partitions_def:
            cls._cache[cache_key] = partitions_def
        return partitions_def

    @staticmethod
    def _get_partitions_def_inner(config: Dict[str, Any]) -> Optional[PartitionsDefinition]:
            
        p_type = config.get("type", "").lower()
        
        # Time-based partitions
        if p_type in ["daily", "hourly", "weekly", "monthly"]:
            start_date = config.get("start_date")
            end_date = config.get("end_date")
            hour_offset = config.get("hour_offset", 0)
            minute_offset = config.get("minute_offset", 0)
            timezone = config.get("timezone", "UTC")
            fmt = config.get("fmt") # Custom format for partition keys
            
            if p_type == "daily":
                return DailyPartitionsDefinition(
                    start_date=start_date, 
                    end_date=end_date,
                    hour_offset=hour_offset,
                    minute_offset=minute_offset,
                    timezone=timezone,
                    fmt=fmt
                )
            elif p_type == "hourly":
                return HourlyPartitionsDefinition(
                    start_date=start_date,
                    end_date=end_date,
                    minute_offset=minute_offset,
                    timezone=timezone,
                    fmt=fmt
                )
            elif p_type == "weekly":
                day_offset = config.get("day_offset", 0)
                return WeeklyPartitionsDefinition(
                    start_date=start_date,
                    end_date=end_date,
                    hour_offset=hour_offset,
                    minute_offset=minute_offset,
                    day_offset=day_offset,
                    timezone=timezone,
                    fmt=fmt
                )
            elif p_type == "monthly":
                day_offset = config.get("day_offset", 1) # Day of month
                return MonthlyPartitionsDefinition(
                    start_date=start_date,
                    end_date=end_date,
                    hour_offset=hour_offset,
                    minute_offset=minute_offset,
                    day_offset=day_offset,
                    timezone=timezone,
                    fmt=fmt
                )
        
        # Cron-based (TimeWindow)
        elif p_type == "cron":
            cron_schedule = config.get("cron_schedule")
            start_val = config.get("start") or config.get("start_date")
            timezone = config.get("timezone", "UTC")
            
            # Use a format that includes timezone to be safe with Dagster 1.11.1
            fmt = config.get("fmt", "%Y-%m-%d %H:%M%z")
            
            if start_val:
                import pendulum
                # Parse to pendulum object
                if hasattr(start_val, "isoformat"):
                    dt = pendulum.instance(start_val)
                else:
                    dt = pendulum.parse(str(start_val))
                
                # Ensure it has the correct timezone and format it to match fmt
                start = dt.in_timezone(timezone).strftime(fmt)
            else:
                start = None
                
            return TimeWindowPartitionsDefinition(
                cron_schedule=cron_schedule,
                start=start,
                fmt=fmt,
                timezone=timezone
            )
            
        # Static partitions
        elif p_type == "static":
            values = config.get("values", [])
            return StaticPartitionsDefinition(values)
            
        # Dynamic partitions
        elif p_type == "dynamic":
            name = config.get("name")
            if not name:
                raise ValueError("Dynamic partitions require a 'name' to be specified.")
            return DynamicPartitionsDefinition(name=name)
            
        # Multi-dimensional partitions
        elif p_type == "multi":
            dimensions_config = config.get("dimensions", {})
            partitions_defs = {}
            for dim_name, dim_conf in dimensions_config.items():
                dim_def = PartitionFactory.get_partitions_def(dim_conf)
                if dim_def:
                    partitions_defs[dim_name] = dim_def
            
            if not partitions_defs:
                return None
                
            return MultiPartitionsDefinition(partitions_defs)
            
        return None
