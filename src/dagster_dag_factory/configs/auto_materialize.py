from typing import Optional
from pydantic import BaseModel, Field


class AutoMaterializePolicyConfig(BaseModel):
    """
    Configuration for Dagster Auto-Materialize Policies.

    Supports:
    - eager: Materialize as soon as upstream dependencies are met
    - lazy: Materialize only when requested or when downstream assets need it
    """

    type: str = Field(..., description="Policy type: 'eager' or 'lazy'")

    max_materializations_per_minute: Optional[int] = Field(
        None, description="Rate limit for materializations"
    )

    class Config:
        extra = "forbid"
