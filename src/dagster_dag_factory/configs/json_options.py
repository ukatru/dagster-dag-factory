from typing import List, Optional, ClassVar
from pydantic import Field
from dagster_dag_factory.configs.base import BaseConfigModel


class JSONOption(BaseConfigModel):
    """Configuration for JSON file processing."""

    template_fields: ClassVar[List[str]] = ["orient"]

    orient: str = Field(default="records", description="Orientation of JSON file")
    fields: Optional[List[str]] = Field(
        default=None, description="List of column names"
    )
