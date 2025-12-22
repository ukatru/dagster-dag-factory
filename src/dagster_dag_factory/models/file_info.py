from dataclasses import dataclass
from typing import Optional
import os
from datetime import datetime

@dataclass
class FileInfo:
    file_name: str
    file_path: str  # Relative path including filename
    full_file_path: str  # Absolute path on server
    file_size: int
    modified_ts: float  # Unix timestamp
    
    @property
    def name(self) -> str:
        """File name without extension."""
        return os.path.splitext(self.file_name)[0]
        
    @property
    def ext(self) -> str:
        """File extension with dot."""
        return os.path.splitext(self.file_name)[1]
        
    @property
    def path(self) -> str:
        """Directory path."""
        return os.path.dirname(self.file_path)

    def to_dict(self):
        return {
            "file_name": self.file_name,
            "file_path": self.file_path,
            "full_file_path": self.full_file_path,
            "file_size": self.file_size,
            "modified_ts": self.modified_ts,
            "modified_date": str(datetime.fromtimestamp(self.modified_ts))
        }
