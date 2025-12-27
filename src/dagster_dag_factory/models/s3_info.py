from pydantic import BaseModel
from typing import Optional
import os
from datetime import datetime


class S3Info(BaseModel):
    """
    Metadata representation of an S3 object.
    Mirrors the Framework S3Info model for consistent listing and filtering logic.
    """

    bucket_name: str
    key: str
    prefix: Optional[str] = None
    size: int = 0
    modified_dt: Optional[datetime] = None
    storage_class: Optional[str] = None

    @property
    def object_name(self) -> str:
        """The last part of the key (filename)."""
        return os.path.basename(self.key)

    @property
    def name(self) -> str:
        """Filename without extension."""
        return os.path.splitext(self.object_name)[0]

    @property
    def ext(self) -> str:
        """File extension with dot."""
        return os.path.splitext(self.object_name)[1]

    @property
    def path(self) -> str:
        """The directory portion of the key relative to the bucket."""
        return os.path.dirname(self.key)

    @property
    def object_path(self) -> str:
        """The path within the prefix if prefix is specified, otherwise same as key."""
        if self.prefix and self.key.startswith(self.prefix):
            return self.key[len(self.prefix) :].lstrip("/")
        return self.key

    @property
    def modified_ts(self) -> Optional[float]:
        """Unix timestamp of modification."""
        return self.modified_dt.timestamp() if self.modified_dt else None

    def to_dict(self):
        return {
            "bucket_name": self.bucket_name,
            "key": self.key,
            "prefix": self.prefix,
            "size": self.size,
            "modified_dt": str(self.modified_dt) if self.modified_dt else None,
            "storage_class": self.storage_class,
            "object_name": self.object_name,
            "name": self.name,
            "ext": self.ext,
            "path": self.path,
            "object_path": self.object_path,
        }
