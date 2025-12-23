from typing import List, Optional
from enum import Enum
from pydantic import Field
from dagster_dag_factory.configs.base import BaseConfigModel

class CsvQuoting(str, Enum):
    NONE = 'NONE'
    ALL = 'ALL'
    MINIMAL = 'MINIMAL'
    NONNUMERIC = 'NONNUMERIC'

class CsvConfig(BaseConfigModel):
    """Configuration for CSV file processing."""
    delimiter: str = Field(default=',', description="The field delimiter")
    quoting: CsvQuoting = Field(default=CsvQuoting.MINIMAL, description="The quoting style")
    lineterminator: str = Field(default='\n', description="The line terminator")
    escapechar: Optional[str] = Field(default=None, description="The escape character")
    quotechar: Optional[str] = Field(default=None, description="The quote character")
    has_headers: bool = Field(default=True, description="Whether the file has headers")
    fields: List[str] = Field(default_factory=list, description="List of field names")
    indexes: List[int] = Field(default_factory=list, description="List of field indexes")

    def model_post_init(self, __context) -> None:
        if not self.escapechar and self.quoting == CsvQuoting.NONE:
            self.escapechar = '\\'
        if not self.quotechar and self.quoting != CsvQuoting.NONE:
            self.quotechar = '"'
