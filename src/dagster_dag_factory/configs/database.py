from typing import Optional, List, Dict, Any, Union, ClassVar
from pydantic import Field
from dagster_dag_factory.configs.base import BaseConfigModel


class SqlConfig(BaseConfigModel):
    """Configuration for executing a raw SQL query."""

    template_fields: ClassVar[List[str]] = ["sql"]

    sql: Optional[str] = Field(None, description="The SQL query to execute")
    params: Optional[Dict[str, Any]] = Field(
        None, description="Parameters for the SQL query"
    )


class TableConfig(BaseConfigModel):
    """Configuration for operating on a database table."""

    template_fields: ClassVar[List[str]] = ["table_name", "schema_name"]

    table_name: Optional[str] = Field(None, description="The table name")
    schema_name: Optional[str] = Field(
        None, alias="schema", description="The schema name"
    )
    columns: Optional[List[str]] = Field(
        None, description="List of columns to select or insert"
    )


class DatabaseConfig(SqlConfig, TableConfig):
    """
    Standardized database configuration that inherits SQL and Table fields.
    Standardizes the lifecycle: sql_pre -> main operation -> sql_post.
    """

    template_fields: ClassVar[List[str]] = SqlConfig.template_fields + TableConfig.template_fields + [
        "sql_pre",
        "sql_post",
    ]

    rows_chunk: Optional[int] = Field(
        default=10000, description="Chunk size for reading/writing rows"
    )
    sql_pre: List[Union[str, SqlConfig]] = Field(
        default_factory=list,
        description="SQL commands to run before the main operation",
    )
    sql_post: List[Union[str, SqlConfig]] = Field(
        default_factory=list, description="SQL commands to run after the main operation"
    )
