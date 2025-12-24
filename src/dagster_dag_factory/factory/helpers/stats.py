from typing import Dict, Any, List


def format_size(size_bytes: int) -> str:
    """Formats a size in bytes into a human-readable string."""
    if size_bytes == 0:
        return "0 B"
    units = ("B", "KB", "MB", "GB", "TB", "PB", "EB")
    i = 0
    while size_bytes >= 1024 and i < len(units) - 1:
        size_bytes /= 1024
        i += 1
    f = ("%.2f" % size_bytes).rstrip("0").rstrip(".")
    return "%s %s" % (f, units[i])


def format_throughput(total_bytes: int, duration_seconds: float) -> str:
    """Formats throughput (bytes per second) into a human-readable string."""
    if duration_seconds <= 0 or total_bytes == 0:
        return "0 B/s"
    bps = total_bytes / duration_seconds
    return f"{format_size(int(bps))}/s"


def generate_transfer_stats(
    transferred_items: List[Dict[str, Any]], duration: float
) -> Dict[str, Any]:
    """Generates a summary of transfer statistics."""
    total_bytes = sum(item.get("size", 0) for item in transferred_items)
    total_files = len(transferred_items)

    return {
        "total_files": total_files,
        "total_bytes": total_bytes,
        "total_size_human": format_size(total_bytes),
        "duration_seconds": round(duration, 2),
        "throughput": format_throughput(total_bytes, duration),
    }
