from abc import ABC, abstractmethod
from dagster import AssetCheckExecutionContext, AssetCheckResult

class BaseCheck(ABC):
    @abstractmethod
    def execute(self, context: AssetCheckExecutionContext, config: dict) -> AssetCheckResult:
        pass
