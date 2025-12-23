import base64
from typing import Any

def decode(value: Any) -> bytes:
    if not value:
        return None
    
    if isinstance(value, str):
        value = value.encode('utf-8')
        
    try:
        return base64.b64decode(value)
    except Exception:
        return value

def from_b64(value: Any) -> str:
    if not value:
        return None
    try:
        decoded = decode(value)
        return decoded.decode('utf-8')
    except Exception:
        if isinstance(value, bytes):
            return value.decode('utf-8')
        return str(value)

def from_b64_str(value: str) -> str:
    """Decodes a base64 string and returns the decoded string."""
    if not value:
        return value
    return from_b64(value)
