from typing import Optional
from pydantic import Field
from dagster_dag_factory.configs.base import BaseConfigModel

class SQLServerConfig(BaseConfigModel):
    """Configuration for SQL Server as a source."""
    connection: str = Field(description="Name of the SQL Server resource")
    query: Optional[str] = Field(default=None, description="SQL Query to extract data")
    table: Optional[str] = Field(default=None, description="Source table name")
