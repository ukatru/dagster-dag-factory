from typing import Any, Dict

class Dynamic:
    """
    A class that converts a dictionary into an object with dot notation.
    """
    def __init__(self, data: Dict[str, Any]):
        for key, value in data.items():
            if isinstance(value, dict):
                setattr(self, key, Dynamic(value))
            elif isinstance(value, list):
                setattr(self, key, [Dynamic(i) if isinstance(i, dict) else i for i in value])
            else:
                setattr(self, key, value)

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)
    
    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def __repr__(self):
        return f"Dynamic({self.__dict__})"
