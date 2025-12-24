from dataclasses import dataclass, field
from typing import Any, Dict
import os
from datetime import datetime


@dataclass
class FileInfo:
    """
    Standardized file metadata, aligned with Niagara's FileInfo model.
    """

    file_name: str
    full_file_path: str
    root_path: str = ""
    file_size: int = 0
    modified_ts: float = 0.0
    # Additional attributes for dot-notation flexibility
    extra: Dict[str, Any] = field(default_factory=dict)

    @property
    def file_path(self) -> str:
        """
        Relative path including filename, starting from root_path.
        """
        return os.path.relpath(self.full_file_path, self.root_path)

    @property
    def path(self) -> str:
        """
        Directory path relative to root_path.
        """
        rel_path = os.path.dirname(self.file_path)
        return rel_path if rel_path != "." else ""

    @property
    def name(self) -> str:
        """File name without extension."""
        return os.path.splitext(self.file_name)[0]

    @property
    def ext(self) -> str:
        """File extension with dot."""
        return os.path.splitext(self.file_name)[1]

    @property
    def modified_date(self) -> str:
        """Human readable modified date."""
        return datetime.fromtimestamp(self.modified_ts).isoformat()

    def to_dict(self):
        return {
            "file_name": self.file_name,
            "file_path": self.file_path,
            "path": self.path,
            "full_file_path": self.full_file_path,
            "root_path": self.root_path,
            "file_size": self.file_size,
            "modified_ts": self.modified_ts,
            "modified_date": self.modified_date,
            "name": self.name,
            "ext": self.ext,
            **self.extra,
        }

    def __repr__(self):
        # Concise repr to avoid flooding logs
        return f"FileInfo(file_name='{self.file_name}', file_path='{self.file_path}')"
