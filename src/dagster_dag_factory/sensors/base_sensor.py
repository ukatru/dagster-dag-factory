from abc import ABC, abstractmethod
from typing import Any, List, Dict, Optional, Type, Tuple


class BaseSensor(ABC):
    """
    Base class for all sensors.
    
    Provides a consistent interface for the factory to check for new data.
    """
    source_config_schema: Optional[Type[Any]] = None

    @abstractmethod
    def check(
        self, 
        context: Any, 
        source_config: Any, 
        resource: Any, 
        cursor: Optional[str] = None,
        **kwargs
    ) -> Tuple[List[Any], Optional[str]]:
        """
        Check for new data using the provided resource.
        
        Args:
            context: Dagster execution context.
            source_config: Validated source configuration.
            resource: The resource to use (e.g., S3Resource, SFTPResource).
            cursor: Current cursor value for stateful polling.
            **kwargs: Additional parameters.
            
        Returns:
            A tuple of (list of items found, new cursor value).
        """
        pass


class SensorRegistry:
    """Registry to map source types to sensor classes."""
    _sensors: Dict[str, Type[BaseSensor]] = {}

    @classmethod
    def register(cls, source_type: str):
        def decorator(sensor_cls: Type[BaseSensor]):
            cls._sensors[source_type.upper()] = sensor_cls
            return sensor_cls
        return decorator

    @classmethod
    def get_sensor(cls, source_type: str) -> Optional[Type[BaseSensor]]:
        return cls._sensors.get(source_type.upper())
