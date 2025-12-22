from dagster_dag_factory.factory.base_operator import BaseOperator
from dagster_dag_factory.factory.registry import OperatorRegistry
from dagster_dag_factory.resources.sqlserver import SQLServerResource
from dagster_dag_factory.resources.s3 import S3Resource
import json

@OperatorRegistry.register(source="SQLSERVER", target="S3")
class SqlServerToS3Operator(BaseOperator):
    
    def _mask_config(self, config: dict) -> dict:
        """Deep copy and mask sensitive keys."""
        masked = config.copy()
        sensitive_keys = ['password', 'secret', 'key', 'token', 'pwd']
        for k in masked:
            if isinstance(masked[k], str):
                if any(s in k.lower() for s in sensitive_keys):
                    masked[k] = "******"
            elif isinstance(masked[k], dict):
                 masked[k] = self._mask_config(masked[k])
        return masked

    def execute(self, context, source_config, target_config):
        # 1. Detailed Logging
        context.log.info("=== Operator Execution: SQLSERVER -> S3 ===")
        context.log.info(f"Source Config: {json.dumps(self._mask_config(source_config), indent=2)}")
        context.log.info(f"Target Config: {json.dumps(self._mask_config(target_config), indent=2)}")

        # Retrieve injected resources via config key "connection"
        source_conn_key = source_config.get("connection")
        target_conn_key = target_config.get("connection")
        
        sql_resource: SQLServerResource = getattr(context.resources, source_conn_key)
        s3_resource: S3Resource = getattr(context.resources, target_conn_key)
        
        query = source_config.get("query")
        
        # Resource handles detailed query logging now
        data = sql_resource.execute_query(query)
        
        s3_key = target_config.get("path")
        s3_format = target_config.get("format", "csv").lower()
        
        context.log.info(f"Writing {len(data)} rows to s3://{s3_resource.bucket_name}/{s3_key}")
        
        if s3_format == "parquet":
            s3_resource.write_parquet(s3_key, data)
        else:
            s3_resource.write_csv(s3_key, data)
            
        return {"rows": len(data), "path": s3_key, "source": "SQLSERVER", "target": "S3"}
