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
            dt = (
                pendulum.instance(value)
                if hasattr(value, "isoformat")
                else pendulum.parse(str(value))
            )
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
            dt = (
                pendulum.instance(value)
                if hasattr(value, "isoformat")
                else pendulum.parse(str(value))
            )
            result = dt.format(fmt)
            # If requesting Unix timestamp, return as integer for direct comparison in predicates
            if fmt in ("X", "x"):
                return int(result)
            return result
        except Exception:
            return str(value)

    @staticmethod
    def between_date(value: Any, start: Any, end: Any) -> str:
        """Generates SQL 'BETWEEN' for dates."""
        return f"{value} BETWEEN '{start}' AND '{end}'"

    @staticmethod
    def between_datetime(value: Any, start: Any, end: Any) -> str:
        """Generates SQL 'BETWEEN' for datetimes."""
        # Standard format with milliseconds
        import pendulum
        s_dt = pendulum.parse(str(start)).format("YYYY-MM-DD HH:mm:ss.SSS")
        e_dt = pendulum.parse(str(end)).format("YYYY-MM-DD HH:mm:ss.SSS")
        return f"{value} BETWEEN '{s_dt}' AND '{e_dt}'"


class CronMacros:
    """Helper functions for cron-based date calculations."""

    @staticmethod
    def diff(start_date: Any, end_date: Any, interval: str) -> int:
        """Difference between starts and ends based on interval (days, week, month, year)."""
        start_date = _normalize_date(start_date)
        end_date = _normalize_date(end_date)

        dt_start = (
            pendulum.instance(start_date)
            if hasattr(start_date, "isoformat")
            else pendulum.parse(str(start_date))
        )
        dt_end = (
            pendulum.instance(end_date)
            if hasattr(end_date, "isoformat")
            else pendulum.parse(str(end_date))
        )

        if dt_end < dt_start:
            raise ValueError("end_date is less than start_date")

        diff = dt_end - dt_start

        if interval == "days":
            return diff.days + 1
        elif interval == "week":
            return math.ceil((diff.days + 1) / 7)
        elif interval == "month":
            return diff.months + (diff.years * 12)
        elif interval == "year":
            return diff.years
        return 0

    @staticmethod
    def next(schedule: str, base_date: Any, count: int = 1) -> str:
        """Returns the next execution date(s) for a cron schedule."""
        base_date = _normalize_date(base_date)
        dt = (
            pendulum.instance(base_date)
            if hasattr(base_date, "isoformat")
            else pendulum.parse(str(base_date))
        )
        ret_val = dt
        for _ in range(count):
            ret_val = croniter(schedule, ret_val).get_next(datetime)
        return str(ret_val)

    @staticmethod
    def prev(schedule: str, base_date: Any, count: int = 1) -> str:
        """Returns the previous execution date(s) for a cron schedule."""
        base_date = _normalize_date(base_date)
        dt = (
            pendulum.instance(base_date)
            if hasattr(base_date, "isoformat")
            else pendulum.parse(str(base_date))
        )
        ret_val = dt
        for _ in range(count):
            ret_val = croniter(schedule, ret_val).get_prev(datetime)
        return str(ret_val)

    @staticmethod
    def range(start_date: Any, end_date: Any, schedule: str) -> List[str]:
        """Returns a list of scheduled dates between start and end."""
        start_date = _normalize_date(start_date)
        end_date = _normalize_date(end_date)
        dt_start = (
            pendulum.instance(start_date)
            if hasattr(start_date, "isoformat")
            else pendulum.parse(str(start_date))
        )
        dt_end = (
            pendulum.instance(end_date)
            if hasattr(end_date, "isoformat")
            else pendulum.parse(str(end_date))
        )
        return [str(d) for d in croniter_range(dt_start, dt_end, schedule)]


class SqlserverMacros:
    """Helper functions for SQL Server generation mirroring Framework patterns."""
    @staticmethod
    def to_datetime(value: Any) -> str:
        """Formats a value for SQL Server datetime."""
        import pendulum
        try:
            dt = pendulum.parse(str(value)) if isinstance(value, str) else value
            # Standard SQL Server format with milliseconds (121)
            return dt.format("YYYY-MM-DD HH:mm:ss.SSS")
        except:
            return str(value)

class JsonMacros:
    """Helper functions for JSON manipulation in templates."""
    @staticmethod
    def from_str(value: Any) -> Any:
        """Parses a JSON string or literal into a Python object."""
        if not value:
            return value
        import json
        import ast
        try:
            # Try as literal first (handles single quotes, etc.)
            return ast.literal_eval(str(value))
        except:
            try:
                return json.loads(str(value))
            except:
                return value

    @staticmethod
    def to_str(value: Any) -> str:
        """Serializes a Python object to a JSON string."""
        import json
        try:
            return json.dumps(value, default=str)
        except:
            return str(value)

class FileMacros:
    """Helper functions for file operations in templates."""
    @staticmethod
    def read(path: str, context: Optional[Any] = None) -> str:
        """Reads the content of a file."""
        import os
        from pathlib import Path
        
        if not path:
            return ""
            
        # If relative path, try to resolve relative to the pipeline file if available
        # This is a bit tricky as context.dag is Airflow-specific, so we might need
        # to pass the pipeline path explicitly from the factory.
        # For now, we support absolute paths and current-working-directory relative.
        try:
            if os.path.isfile(path):
                return Path(path).read_text()
            return f"Error: File not found: {path}"
        except Exception as e:
            return f"Error reading file {path}: {e}"

class SecretMacros:
    """Helper functions for secure secret retrieval."""
    @staticmethod
    def get(key: str, default: Optional[str] = None) -> Optional[str]:
        """Fetches a secret value from environment variables."""
        import os
        return os.environ.get(key, default)

class AppMacros:
    """Application-specific helper functions inspired by Framework's da_app."""
    @staticmethod
    def filename(pattern: str, base_date: Any = None) -> str:
        """
        Formats a filename pattern using a base date.
        Replaces <yyyymmdd>, <yyyy>, <mm>, <dd>, etc.
        """
        import pendulum
        dt = (
            pendulum.instance(base_date)
            if hasattr(base_date, "isoformat")
            else pendulum.parse(str(base_date)) if base_date else pendulum.now("UTC")
        )
        
        mapping = {
            "yyyymmdd": dt.format("YYYYMMDD"),
            "yyyy": dt.format("YYYY"),
            "mm": dt.format("MM"),
            "dd": dt.format("DD"),
        }
        
        result = pattern
        for k, v in mapping.items():
            result = result.replace(f"<{k}>", v)
        return result

def get_macros(context: Optional[Any] = None) -> Dict[str, Any]:
    """Returns a dictionary of functions to be exposed in templates."""
    date_helpers = DateMacros()
    cron_helpers = CronMacros()
    sqlserver_helpers = SqlserverMacros()
    json_helpers = JsonMacros()
    file_helpers = FileMacros()
    secret_helpers = SecretMacros()
    app_helpers = AppMacros()
    
    return {
        "date": date_helpers,
        "cron": cron_helpers,
        "sqlserver": sqlserver_helpers,
        "json": json_helpers,
        "file": file_helpers,
        "secrets": secret_helpers,
        "app": app_helpers,
        "fn": {
            "date": date_helpers, 
            "cron": cron_helpers,
            "sqlserver": sqlserver_helpers,
            "json": json_helpers,
            "file": file_helpers,
            "secrets": secret_helpers,
            "app": app_helpers,
        },
    }
