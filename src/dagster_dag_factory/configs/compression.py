from enum import Enum
from typing import Optional
from pydantic import Field
from dagster_dag_factory.configs.base import BaseConfigModel

class CompressionType(str, Enum):
    ZIP = 'ZIPFILE'
    GUNZIP = 'GUNZIP'

class CompressionAction(str, Enum):
    COMPRESS = 'COMPRESS'
    DECOMPRESS = 'DECOMPRESS'

class CompressConfig(BaseConfigModel):
    """
    Configuration for file compression/decompression.
    Adapted from Niagara legacy configs.
    """
    type: CompressionType = Field(default=CompressionType.GUNZIP, description="Type of compression (ZIPFILE, GUNZIP)")
    action: CompressionAction = Field(default=CompressionAction.COMPRESS, description="Action to perform (COMPRESS, DECOMPRESS)")
