from typing import Dict, Any
from dagster import AssetCheckExecutionContext, AssetCheckResult
from dagster_dag_factory.operators.base_operator import BaseOperator
from dagster_dag_factory.factory.registry import OperatorRegistry
from dagster_dag_factory.resources.sqlserver import SQLServerResource
from dagster_dag_factory.resources.s3 import S3Resource
from dagster_dag_factory.configs.sqlserver import SQLServerConfig
from dagster_dag_factory.configs.s3 import S3Config

@OperatorRegistry.register(source="SQLSERVER", target="S3")
class SqlServerS3Operator(BaseOperator):
    source_config_schema = SQLServerConfig
    target_config_schema = S3Config
    
    def execute(self, context, source_config: SQLServerConfig, target_config: S3Config, template_vars: Dict[str, Any]):
        # Retrieve injected resources via config key "connection"
        sql_resource: SQLServerResource = getattr(context.resources, source_config.connection)
        s3_resource: S3Resource = getattr(context.resources, target_config.connection)
        
        import time
        start_time = time.time()
        
        query = source_config.query
        
        # Resource handles detailed query logging now
        data = sql_resource.execute_query(query)
        
        s3_key = target_config.key
        s3_format = (target_config.object_type or "csv").lower()
        
        rows = len(data)
        context.log.info(f"Writing {rows} rows to {s3_key}")
        
        if s3_format == "parquet":
            s3_resource.write_parquet(target_config.bucket_name, s3_key, data)
        else:
            s3_resource.write_csv(target_config.bucket_name, s3_key, data)
            
        duration = time.time() - start_time
        rows_per_sec = round(rows / duration, 2) if duration > 0 else 0
        
        summary = {
            "rows_transferred": rows,
            "duration_seconds": round(duration, 2),
            "rows_per_second": rows_per_sec,
            "path": s3_key
        }
        
        context.log.info(f"Transfer complete. {rows} rows processed in {round(duration, 2)}s ({rows_per_sec} rows/s).")
        
        return {
            "summary": summary,
            "observations": {
                "rows_extracted": rows,
                "rows_written": rows
            },
            "source": "SQLSERVER",
            "target": "S3"
        }

    def execute_check(self, context: AssetCheckExecutionContext, config: dict) -> AssetCheckResult:
        check_type = config.get("type")
        
        if check_type == "sql_server_check":
            query = config["query"]
            connection_name = config["connection"]
            threshold = config.get("threshold", 1)
            
            # Get resource (we can reuse the same established pattern)
            resource = getattr(context.resources, connection_name)
            
            # Execute
            results = resource.execute_query(query)
            
            passed = False
            val = None
            if results and len(results) > 0:
                first_row = results[0]
                val = list(first_row.values())[0] if isinstance(first_row, dict) else first_row[0]
                passed = float(val) >= threshold
                message = f"Check value: {val}, Threshold: {threshold}"
            else:
                message = "No results returned from check query."
                
            return AssetCheckResult(
                passed=passed, 
                metadata={
                    "value": val if val is not None else None, 
                    "message": message,
                    "query": query
                }
            )
            
        # Fallback to BaseOperator for observation_diff etc.
        return super().execute_check(context, config)
