from typing import Any, Dict, Optional
import pendulum

class DateMacros:
    """Helper functions for date manipulation in templates using Pendulum."""
    
    @staticmethod
    def to_date_nodash(value: Any) -> str:
        """Converts yyyy-mm-dd or any parseable date to yyyymmdd."""
        if not value:
            return ""
        try:
            # Handle pendulum objects or strings
            dt = pendulum.instance(value) if hasattr(value, "isoformat") else pendulum.parse(str(value))
            return dt.format("YYYYMMDD")
        except Exception:
            return str(value)

    @staticmethod
    def format(value: Any, fmt: str) -> str:
        """Formats a date string or object using a given format."""
        if not value:
            return ""
        try:
            dt = pendulum.instance(value) if hasattr(value, "isoformat") else pendulum.parse(str(value))
            return dt.format(fmt)
        except Exception:
            return str(value)

def get_macros(context: Optional[Any] = None) -> Dict[str, Any]:
    """Returns a dictionary of functions to be exposed in templates."""
    date_helpers = DateMacros()
    return {
        "date": date_helpers,
        "fn": {
            "date": date_helpers
        }
    }
