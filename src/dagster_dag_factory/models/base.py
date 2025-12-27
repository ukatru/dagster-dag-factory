from typing import List
import json


class ModelBase:
    """
    Base model for all configuration objects, providing sensitive field masking.
    Following the Framework pattern.
    """

    mask_fields: List[str] = []

    def __init__(self, **kwargs):
        pass

    def __repr__(self):
        obj = self._mask_fields()
        return json.dumps(obj, indent=4, default=str)

    def _mask_fields(self) -> dict:
        """
        Recursively masks sensitive fields defined in mask_fields.
        """
        obj = {**self.__dict__}

        if self.mask_fields:
            for field in self.mask_fields:
                if field in obj and obj[field]:
                    # Mask with asterisks, handle if it's not a string
                    val = str(obj[field])
                    obj[field] = "*" * len(val)

        for k, v in obj.items():
            if isinstance(v, ModelBase):
                obj[k] = v._mask_fields()
            elif isinstance(v, list):
                obj[k] = [
                    x._mask_fields() if isinstance(x, ModelBase) else x for x in v
                ]
            elif isinstance(v, dict):
                obj[k] = {
                    nk: nv._mask_fields() if isinstance(nv, ModelBase) else nv
                    for nk, nv in v.items()
                }

        return obj

    def to_dict(self) -> dict:
        """
        Converts the object to a dictionary (unmasked).
        """
        obj = {**self.__dict__}
        for k, v in obj.items():
            if hasattr(v, "to_dict"):
                obj[k] = v.to_dict()
            elif isinstance(v, list):
                obj[k] = [x.to_dict() if hasattr(x, "to_dict") else x for x in v]
            elif isinstance(v, dict):
                obj[k] = {
                    nk: nv.to_dict() if hasattr(nv, "to_dict") else nv
                    for nk, nv in v.items()
                }
        return obj
