from typing import Optional
from pydantic import Field
from dagster_dag_factory.configs.base import BaseConfigModel

class SnowflakeConfig(BaseConfigModel):
    """Configuration for Snowflake as a target."""
    connection: str = Field(description="Name of the Snowflake resource")
    table: str = Field(description="Target table name (fully qualified)")
    stage: Optional[str] = Field(default=None, description="External stage to use for COPY INTO")
    
    # Load Options (Operator specific to Snowflake)
    match_columns: bool = Field(default=False, description="Use MATCH_BY_COLUMN_NAME")
    force: bool = Field(default=True, description="Force re-load if file already loaded")
    on_error: Optional[str] = Field(default="SKIP_FILE", description="ON_ERROR strategy")
    
    # Schema Strategy (Pipeline specific logic)
    schema_strategy: Optional[str] = Field(default="fail", description="Strategy: fail, create, evolve, strict")
