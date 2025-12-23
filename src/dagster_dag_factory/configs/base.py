from pydantic import BaseModel, ConfigDict
from typing import Any, List, ClassVar
import json

class BaseConfigModel(BaseModel):
    """
    Base Pydantic model for configurations with automated masking for logging.
    """
    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True,
        arbitrary_types_allowed=True
    )
    
    # Sensible defaults for sensitive fields to mask in logs
    mask_fields: ClassVar[List[str]] = ["password", "secret", "token", "token_file"]

    def to_masked_dict(self) -> dict:
        """Returns a dictionary with sensitive fields masked."""
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

    def to_masked_json(self) -> str:
        """Returns a pretty-printed JSON string of the masked configuration."""
        return json.dumps(self.to_masked_dict(), indent=2, default=str)
