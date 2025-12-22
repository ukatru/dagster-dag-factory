from typing import Dict, Type, Optional

class CheckRegistry:
    _checks: Dict[str, Type] = {}

    @classmethod
    def register(cls, check_type: str):
        def wrapper(check_cls: Type):
            cls._checks[check_type] = check_cls
            return check_cls
        return wrapper

    @classmethod
    def get_check(cls, check_type: str) -> Optional[Type]:
        return cls._checks.get(check_type)
