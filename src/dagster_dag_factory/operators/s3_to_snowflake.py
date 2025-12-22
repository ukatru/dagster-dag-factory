from dagster_dag_factory.factory.base_operator import BaseOperator
from dagster_dag_factory.factory.registry import OperatorRegistry
from dagster_dag_factory.resources.snowflake import SnowflakeResource
from dagster_dag_factory.resources.s3 import S3Resource
import time
import uuid

@OperatorRegistry.register(source="S3", target="SNOWFLAKE")
class S3ToSnowflakeOperator(BaseOperator):
    """
    Loads data from S3 to Snowflake using COPY INTO.
    Creates a temporary external stage with credentials if needed.
    """
    
    def execute(self, context, source_config, target_config):
        # Resources
        s3_resource: S3Resource = getattr(context.resources, source_config.get("connection"))
        snow_resource: SnowflakeResource = getattr(context.resources, target_config.get("connection"))
        context.log.info(f"Snow_resource type: {type(snow_resource)}")
        
        # Configs
        s3_path = source_config.get("path") or source_config.get("s3_path")
        s3_bucket = s3_resource.bucket_name
        
        table = target_config.get("table") or target_config.get("target_table")
        stage = target_config.get("stage") or target_config.get("external_stage")
        file_format_type = source_config.get("format", "CSV").upper()
        match_columns = source_config.get("match_columns", False)
        
        # 1. Build File Format String
        file_format_sql = f"TYPE = '{file_format_type}'"
        if file_format_type == 'CSV':
            delimiter = source_config.get("delimiter", ",")
            if match_columns:
                file_format_sql += " PARSE_HEADER = TRUE"
            else:
                skip_header = 1 if source_config.get("has_header", True) else 0
                file_format_sql += f" SKIP_HEADER = {skip_header}"
            
            file_format_sql += f" FIELD_DELIMITER = '{delimiter}'"
            file_format_sql += " FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE"

        force = target_config.get("force", True) # Default to true for factory logic mostly
        
        copy_sql = f"COPY INTO {table} FROM @{stage}/{s3_path}"
        copy_sql += f" FILE_FORMAT = ({file_format_sql})"
        
        if match_columns:
            copy_sql += " MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"
            
        if force:
            copy_sql += " FORCE = TRUE"
            
        copy_sql += " ON_ERROR = 'SKIP_FILE';"
        
        try:
            context.log.info(f"Executing COPY INTO {table} FROM @{stage}/{s3_path}")
            results = snow_resource.execute_query(copy_sql)
            
            rows_loaded = 0
            if results:
                for row in results:
                    # Snowflake column names can be uppercase or lowercase depending on driver/mode
                    keys = {k.lower(): v for k, v in row.items()}
                    rows_loaded += int(keys.get('rows_loaded', 0))
            
            context.log.info(f"Loaded {rows_loaded} rows into {table}")
            
            return {
                "source": f"@{stage}/{s3_path}",
                "target": table,
                "rows_loaded": rows_loaded
            }
            
        except Exception as e:
            context.log.error(f"Copy failed: {e}")
            raise
