from typing import Any, Dict, Optional, List, Union
import pendulum
from datetime import datetime

import math
from croniter import croniter, croniter_range

def _normalize_date(value: Any) -> Any:
    """Helper to extract a date string if the value is a dict or Dynamic object."""
    if not value:
        return value
    if isinstance(value, dict) and "date" in value:
        return value["date"]
    if hasattr(value, "date") and not callable(getattr(value, "date")):
        return getattr(value, "date")
    return value

class DateMacros:
    """Helper functions for date manipulation in templates using Pendulum."""
    
    @staticmethod
    def to_date_nodash(value: Any) -> str:
        """Converts yyyy-mm-dd or any parseable date to yyyymmdd."""
        value = _normalize_date(value)
        if not value:
            return ""
        try:
            # Handle pendulum objects or strings
            dt = pendulum.instance(value) if hasattr(value, "isoformat") else pendulum.parse(str(value))
            return dt.format("YYYYMMDD")
        except Exception:
            return str(value)

    @staticmethod
    def format(value: Any, fmt: str) -> Union[str, int]:
        """Formats a date string or object using a given format."""
        value = _normalize_date(value)
        if not value:
            return ""
        try:
            dt = pendulum.instance(value) if hasattr(value, "isoformat") else pendulum.parse(str(value))
            result = dt.format(fmt)
            # If requesting Unix timestamp, return as integer for direct comparison in predicates
            if fmt in ("X", "x"):
                return int(result)
            return result
        except Exception:
            return str(value)

class CronMacros:
    """Helper functions for cron-based date calculations."""

    @staticmethod
    def diff(start_date: Any, end_date: Any, interval: str) -> int:
        """Difference between starts and ends based on interval (days, week, month, year)."""
        start_date = _normalize_date(start_date)
        end_date = _normalize_date(end_date)
        
        dt_start = pendulum.instance(start_date) if hasattr(start_date, "isoformat") else pendulum.parse(str(start_date))
        dt_end = pendulum.instance(end_date) if hasattr(end_date, "isoformat") else pendulum.parse(str(end_date))
        
        if dt_end < dt_start:
            raise ValueError('end_date is less than start_date')

        diff = dt_end - dt_start
        
        if interval == 'days':
            return diff.days + 1
        elif interval == 'week':
            return math.ceil((diff.days + 1) / 7)
        elif interval == 'month':
            return diff.months + (diff.years * 12)
        elif interval == 'year':
            return diff.years
        return 0

    @staticmethod
    def next(schedule: str, base_date: Any, count: int = 1) -> str:
        """Returns the next execution date(s) for a cron schedule."""
        base_date = _normalize_date(base_date)
        dt = pendulum.instance(base_date) if hasattr(base_date, "isoformat") else pendulum.parse(str(base_date))
        ret_val = dt
        for _ in range(count):
            ret_val = croniter(schedule, ret_val).get_next(datetime)
        return str(ret_val)

    @staticmethod
    def prev(schedule: str, base_date: Any, count: int = 1) -> str:
        """Returns the previous execution date(s) for a cron schedule."""
        base_date = _normalize_date(base_date)
        dt = pendulum.instance(base_date) if hasattr(base_date, "isoformat") else pendulum.parse(str(base_date))
        ret_val = dt
        for _ in range(count):
            ret_val = croniter(schedule, ret_val).get_prev(datetime)
        return str(ret_val)

    @staticmethod
    def range(start_date: Any, end_date: Any, schedule: str) -> List[str]:
        """Returns a list of scheduled dates between start and end."""
        start_date = _normalize_date(start_date)
        end_date = _normalize_date(end_date)
        dt_start = pendulum.instance(start_date) if hasattr(start_date, "isoformat") else pendulum.parse(str(start_date))
        dt_end = pendulum.instance(end_date) if hasattr(end_date, "isoformat") else pendulum.parse(str(end_date))
        return [str(d) for d in croniter_range(dt_start, dt_end, schedule)]

def get_macros(context: Optional[Any] = None) -> Dict[str, Any]:
    """Returns a dictionary of functions to be exposed in templates."""
    date_helpers = DateMacros()
    cron_helpers = CronMacros()
    return {
        "date": date_helpers,
        "cron": cron_helpers,
        "fn": {
            "date": date_helpers,
            "cron": cron_helpers
        }
    }
