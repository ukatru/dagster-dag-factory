from abc import ABC, abstractmethod
from typing import Dict, Any

class BaseOperator(ABC):
    """
    Abstract base class for all operators.
    Operators handle the logic for moving data from a specific source type to a target type.
    """
    
    @abstractmethod
    def execute(self, context, source_config: Dict[str, Any], target_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the operator logic.
        
        Args:
            context: The Dagster asset execution context.
            source_config: Configuration dictionary for the source.
            target_config: Configuration dictionary for the target.
            
        Returns:
            Dict: Metadata about the execution (e.g. row counts, paths).
        """
        pass
