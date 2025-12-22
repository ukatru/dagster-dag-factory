from typing import Optional
from pydantic import Field
from .base import BaseConfigModel

class SFTPConfig(BaseConfigModel):
    """Configuration for SFTP as a source."""
    connection: str = Field(description="Name of the SFTP resource")
    path: str = Field(description="Remote directory or file path")
    pattern: Optional[str] = Field(default=".*", description="Regex pattern to match files")
    recursive: bool = Field(default=False, description="Search subdirectories recursively")
    predicate: Optional[str] = Field(default=None, description="Python expression for filtering files")
