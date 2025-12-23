from dagster_dag_factory.factory.base_operator import BaseOperator
from dagster_dag_factory.factory.registry import OperatorRegistry
from dagster_dag_factory.resources.sqlserver import SQLServerResource
from dagster_dag_factory.resources.s3 import S3Resource
from dagster_dag_factory.configs.sqlserver import SQLServerConfig
from dagster_dag_factory.configs.s3 import S3Config

@OperatorRegistry.register(source="SQLSERVER", target="S3")
class SqlServerS3Operator(BaseOperator):
    source_config_schema = SQLServerConfig
    target_config_schema = S3Config
    
    def execute(self, context, source_config: SQLServerConfig, target_config: S3Config):
        # Retrieve injected resources via config key "connection"
        sql_resource: SQLServerResource = getattr(context.resources, source_config.connection)
        s3_resource: S3Resource = getattr(context.resources, target_config.connection)
        
        query = source_config.query
        
        # Resource handles detailed query logging now
        data = sql_resource.execute_query(query)
        
        s3_key = target_config.path
        s3_format = (target_config.format or "csv").lower()
        
        context.log.info(f"Writing {len(data)} rows to s3://{s3_resource.bucket_name}/{s3_key}")
        
        if s3_format == "parquet":
            s3_resource.write_parquet(s3_key, data)
        else:
            s3_resource.write_csv(s3_key, data)
            
        return {"rows": len(data), "path": s3_key, "source": "SQLSERVER", "target": "S3"}
