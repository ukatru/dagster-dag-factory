from typing import Optional, Any
from pydantic import Field, model_validator
from dagster_dag_factory.configs.base import BaseConfigModel

class SQLServerConfig(BaseConfigModel):
    """Configuration for SQL Server as a source."""
    connection: str = Field(description="Name of the SQL Server resource")
    query: Optional[str] = Field(default=None, description="SQL Query string")
    sql: Optional[str] = Field(default=None, description="Alias for query")
    table: Optional[str] = Field(default=None, description="Source table name")

    @model_validator(mode='after')
    def validate_query_fields(self) -> 'SQLServerConfig':
        if not self.query and self.sql:
            self.query = self.sql
        if not self.query and not self.table:
            raise ValueError("Either 'query' (or 'sql') or 'table' must be provided")
        return self
