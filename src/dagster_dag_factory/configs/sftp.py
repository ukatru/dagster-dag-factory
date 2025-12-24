from typing import Optional
from pydantic import Field
from dagster_dag_factory.configs.base import BaseConfigModel
from dagster_dag_factory.configs.compression import CompressConfig


class SFTPConfig(BaseConfigModel):
    """Configuration for SFTP as a source."""

    connection: str = Field(description="Name of the SFTP resource")
    path: str = Field(description="Remote directory or file path")
    pattern: Optional[str] = Field(
        default=".*", description="Regex pattern to match files"
    )
    file_name: Optional[str] = Field(
        default=None,
        description="Specific file name to match (overrides pattern). Supports templating.",
    )
    recursive: bool = Field(
        default=False, description="Search subdirectories recursively"
    )
    predicate: Optional[str] = Field(
        default=None, description="Python expression for filtering files"
    )
    check_is_modifying: bool = Field(
        default=False,
        description="Check if file is still being modified before transfer",
    )
    compress_options: Optional[CompressConfig] = Field(
        default=None, description="Compression settings for upload"
    )
