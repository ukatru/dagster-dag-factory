from typing import Optional, List, ClassVar
from pydantic import Field
from dagster_dag_factory.configs.base import BaseConfigModel
from dagster_dag_factory.configs.enums import CompressionType, CompressionAction


class CompressConfig(BaseConfigModel):
    """
    Configuration for file compression/decompression.
    Adapted from Framework legacy configs.
    """

    template_fields: ClassVar[List[str]] = []

    compress_type: Optional[CompressionType] = Field(
        default=None, description="Type of compression (ZIPFILE, GUNZIP)"
    )
    action: Optional[CompressionAction] = Field(
        default=None, description="Action to perform (COMPRESS, DECOMPRESS)"
    )
