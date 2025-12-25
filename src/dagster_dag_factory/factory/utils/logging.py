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

def log_header(header: str):
    """Log a strong visual header."""
    log_marker("strong")
    log_action(header.upper())
    log_marker("normal")

def log_action(action: str, *args: Tuple[str, Any], **kwargs):
    """Log a structured action with key-values.
    Supports both ordered tuples and keyword arguments.
    """
    # Combine positional tuples and keyword arguments
    kv_pairs = list(args)
    for k, v in kwargs.items():
        kv_pairs.append((k, v))
        
    values = [f"{str(k).ljust(12)} : {v}" for k, v in kv_pairs]
    msg = f"{action.rjust(20)} | " + " | ".join(values) if values else action
    _log.info(msg)

def log_action_stats(action: str, start_time: float, *args, **kwargs):
    """Log an action with elapsed time."""
    elapsed = round(time.time() - start_time, 3)
    kwargs["duration"] = f"{elapsed}s"
    log_action(action, *args, **kwargs)

def log_marker(style: str = "normal"):
    """Log a visual break."""
    if style == "strong":
        _log.info("=" * 80)
    elif style == "mini":
        _log.info("-" * 40)
    else:
        _log.info("-" * 80)

def log_message(msg: str, level: int = logging.INFO):
    """Simple message logging."""
    _log.log(level, msg)
