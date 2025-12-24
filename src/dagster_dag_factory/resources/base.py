from typing import List, Any, ClassVar
from dagster import ConfigurableResource
import json


class BaseConfigurableResource(ConfigurableResource):
    """
    Base resource class that supports sensitive field masking for logging.
    Following the Niagara mask_fields pattern.
    """

    mask_fields: ClassVar[List[str]] = [
        "password",
        "secret",
        "key",
        "token",
        "private_key",
    ]

    def resolve(self, field_name: str) -> Any:
        """Resolves a field value, handling Dagster EnvVar objects if present."""
        val = getattr(self, field_name)
        return val.get_value() if hasattr(val, "get_value") else val

    def to_masked_dict(self) -> dict:
        """Returns a dict of resource config with sensitive fields masked."""
        # ConfigurableResource.model_dump() is available because it inherits from Pydantic BaseModel
        data = self.model_dump()
        return self._recursive_mask(data)

    def _recursive_mask(self, data: Any) -> Any:
        if isinstance(data, dict):
            new_data = {}
            for k, v in data.items():
                if any(m in k.lower() for m in self.mask_fields):
                    new_data[k] = "******" if v else v
                else:
                    new_data[k] = self._recursive_mask(v)
            return new_data
        elif isinstance(data, list):
            return [self._recursive_mask(x) for x in data]
        return data

    def __repr__(self):
        """Pretty-print masked configuration."""
        return json.dumps(self.to_masked_dict(), indent=2, default=str)
