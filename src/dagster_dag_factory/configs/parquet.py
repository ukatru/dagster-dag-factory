from typing import List, Dict, Optional
from enum import Enum
from pydantic import Field
from dagster_dag_factory.configs.base import BaseConfigModel

class ParquetCompression(str, Enum):
    SNAPPY = 'snappy'
    GZIP = 'gzip'
    BROTLI = 'brotli'
    NONE = 'none'

class ParquetConfig(BaseConfigModel):
    """Configuration for Parquet file processing."""
    col_names: List[str] = Field(default_factory=list, description="Explicit column names")
    infer_schema: bool = Field(default=False, description="Whether to infer schema")
    schema_overrides: Dict[str, str] = Field(default_factory=dict, description="Schema overrides (col_name: pyarrow_type)")
    compression: ParquetCompression = Field(default=ParquetCompression.SNAPPY, description="Compression type")
    cast_decimal_to_float: bool = Field(default=False, description="Whether to cast decimal to float")
