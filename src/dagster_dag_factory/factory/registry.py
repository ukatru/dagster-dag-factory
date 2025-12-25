from typing import Dict, Type, Tuple, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from dagster_dag_factory.operators.base_operator import BaseOperator


class OperatorRegistry:
    """
    Central registry for operators.
    Maps (source_type, target_type) -> OperatorClass.
    """

    _registry: Dict[Tuple[str, str], Type["BaseOperator"]] = {}

    @classmethod
    def register(cls, source: str, target: str):
        """
        Decorator to register an operator class.

        Usage:
            @OperatorRegistry.register(source="SQLSERVER", target="S3")
            class SqlServerToS3Operator(BaseOperator): ...
        """

        def wrapper(operator_class: Type["BaseOperator"]):
            cls._registry[(source.upper(), target.upper())] = operator_class
            return operator_class

        return wrapper

    @classmethod
    def get_operator(cls, source: Optional[str], target: Optional[str]) -> Optional[Type["BaseOperator"]]:
        """
        Retrieve a registered operator class.
        """
        if source is None or target is None:
            return None
        return cls._registry.get((source.upper(), target.upper()))
