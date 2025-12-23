from typing import Optional
from pydantic import Field
from .base import BaseConfigModel
from .compression import CompressConfig

class S3Config(BaseConfigModel):
    """Configuration for S3 as a source or target."""
    connection: str = Field(description="Name of the S3 resource")
    path: str = Field(description="S3 Path (e.g. raw/sales/ or bucket/path)")
    format: Optional[str] = Field(default="CSV", description="File format (CSV, PARQUET, JSON)")
    delimiter: Optional[str] = Field(default=",", description="CSV Delimiter (if applicable)")
    skip_header: Optional[int] = Field(default=1, description="Number of header rows to skip")
    compression: Optional[str] = Field(default=None, description="Compression type (gzip, snappy, etc.)")
    compress_options: Optional[CompressConfig] = Field(default=None, description="Detailed compression settings (ZIP/GUNZIP)")
