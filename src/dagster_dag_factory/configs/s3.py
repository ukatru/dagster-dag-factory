from typing import Optional, List
from enum import Enum
from pydantic import Field
from dagster_dag_factory.configs.base import BaseConfigModel
from dagster_dag_factory.configs.compression import CompressConfig
from dagster_dag_factory.configs.csv import CsvConfig
from dagster_dag_factory.configs.parquet import ParquetConfig
from dagster_dag_factory.configs.json_options import JSONOption

class S3Mode(str, Enum):
    COPY = 'COPY'
    MULTI_FILE = 'MULTI_FILE'

class S3ObjectType(str, Enum):
    CSV = 'CSV'
    JSON = 'JSON'
    PARQUET = 'PARQUET'

class S3Config(BaseConfigModel):
    """
    Standardized S3 Configuration.
    Follows the proven signature from legacy Niagara implementations.
    """
    connection: str = Field(description="Name of the S3 resource")
    
    bucket_name: str = Field(description="The name of the S3 bucket")
    key: Optional[str] = Field(default=None, description="S3 Key/Path")
    region: Optional[str] = Field(default=None, description="The AWS region")
    object_type: S3ObjectType = Field(default=S3ObjectType.CSV, description="S3 Object type (CSV, JSON, PARQUET)")
    
    mode: S3Mode = Field(default=S3Mode.COPY, description="Execution mode (COPY, MULTI_FILE)")
    min_size: int = Field(default=5, description="Minimum size threshold (if applicable)")
    
    prefix: str = Field(default='', description="Key prefix for S3 operations")
    pattern: Optional[str] = Field(default=None, description="Pattern for file matching (often defaults to key)")
    delimiter: Optional[str] = Field(default=None, description="Delimiter (fallback if not in csv_options)")
    check_is_modifing: bool = Field(default=False, description="Verify if file is being modified")
    predicate: Optional[str] = Field(default=None, description="Predicate for filtering")
    
    compress_options: Optional[CompressConfig] = Field(default=None, description="Compression settings")
    csv_options: Optional[CsvConfig] = Field(default=None, description="CSV specific options")
    parquet_options: Optional[ParquetConfig] = Field(default=None, description="Parquet specific options")
    json_options: Optional[JSONOption] = Field(default=None, description="JSON specific options")

    def model_post_init(self, __context) -> None:
        if not self.pattern and self.key:
            self.pattern = self.key
        
        # Ensure sub-configs are initialized if they match the object_type (Niagara pattern)
        if self.object_type == S3ObjectType.CSV and not self.csv_options:
            self.csv_options = CsvConfig()
        elif self.object_type == S3ObjectType.PARQUET and not self.parquet_options:
            self.parquet_options = ParquetConfig()
        elif self.object_type == S3ObjectType.JSON and not self.json_options:
            self.json_options = JSONOption()
