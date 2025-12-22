from dagster import AssetCheckExecutionContext, AssetCheckResult
from dagster_dag_factory.factory.check_registry import CheckRegistry
from dagster_dag_factory.factory.base_check import BaseCheck

@CheckRegistry.register("sql_server_check")
class SQLServerCheck(BaseCheck):
    def execute(self, context: AssetCheckExecutionContext, config: dict) -> AssetCheckResult:
        query = config["query"]
        connection_name = config["connection"]
        threshold = config.get("threshold", 1)
        
        # Get resource
        resource = getattr(context.resources, connection_name)
        
        # Execute
        results = resource.execute_query(query)
        
        # Simple threshold check (assuming query returns a single value in first row/col)
        passed = False
        message = ""
        if results and len(results) > 0:
            first_row = results[0]
            val = list(first_row.values())[0] if isinstance(first_row, dict) else first_row[0]
            passed = float(val) >= threshold
            message = f"Check value: {val}, Threshold: {threshold}"
        else:
            message = "No results returned from check query."
            
        return AssetCheckResult(passed=passed, metadata={"value": val if 'val' in locals() else None, "message": message})
