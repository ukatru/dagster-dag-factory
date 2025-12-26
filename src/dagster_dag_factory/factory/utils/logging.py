import logging
import time
from typing import Any, List, Tuple, Union

_log = logging.getLogger("dagster_dag_factory")
_log.setLevel(logging.INFO)

# Ensure logs are visible in console during discovery phase
if not _log.handlers:
    handler = logging.StreamHandler()
    # Use a simpler format for build-phase logs
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    _log.addHandler(handler)
    # Prevent propagation to avoid double logging if the root logger also has a handler
    _log.propagate = False

def log_header(header: str, logger=None):
    """Log a strong visual header."""
    lines = []
    lines.append(log_marker("strong", logger=logger))
    lines.append(log_action(header.upper(), logger=logger))
    lines.append(log_marker("normal", logger=logger))
    return "\n".join(lines)


def log_action(action: str, *args: Tuple[str, Any], logger=None, **kwargs):
    """Log a structured action with key-values.
    Supports both ordered tuples and keyword arguments.
    """
    # Combine positional tuples and keyword arguments
    kv_pairs = list(args)
    for k, v in kwargs.items():
        kv_pairs.append((k, v))

    values = [f"{str(k).ljust(12)} : {v}" for k, v in kv_pairs]
    msg = f"{action.rjust(20)} | " + " | ".join(values) if values else action
    
    if logger:
        logger.info(msg)
    else:
        _log.info(msg)
    return msg


def log_action_stats(action: str, start_time: float, *args, logger=None, **kwargs):
    """Log an action with elapsed time."""
    elapsed = round(time.time() - start_time, 3)
    kwargs["duration"] = f"{elapsed}s"
    return log_action(action, *args, logger=logger, **kwargs)


def log_marker(style: str = "normal", logger=None):
    """Log a visual break."""
    if style == "strong":
        msg = "=" * 80
    elif style == "mini":
        msg = "-" * 40
    else:
        msg = "-" * 80
    
    if logger:
        logger.info(msg)
    else:
        _log.info(msg)
    return msg

def log_message(msg: str, level: int = logging.INFO):
    """Simple message logging."""
    _log.log(level, msg)


def convert_size(size_bytes: int) -> str:
    """Convert bytes to human readable format."""
    if size_bytes == 0:
        return "0 B"
    import math

    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])


def convert_speed(size_bytes: int, duration_seconds: float) -> str:
    """Calculate and format speed."""
    if duration_seconds <= 0:
        return "-"
    speed_bytes = size_bytes / duration_seconds
    return f"{convert_size(int(speed_bytes))}/s"
