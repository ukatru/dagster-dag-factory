from dagster_dag_factory.factory.base_operator import BaseOperator
from dagster_dag_factory.factory.registry import OperatorRegistry
from dagster_dag_factory.resources.snowflake import SnowflakeResource
from dagster_dag_factory.resources.s3 import S3Resource
import time
import uuid

from dagster_dag_factory.configs.s3 import S3Config
from dagster_dag_factory.configs.snowflake import SnowflakeConfig

@OperatorRegistry.register(source="S3", target="SNOWFLAKE")
class S3SnowflakeOperator(BaseOperator):
    """
    Loads data from S3 to Snowflake using COPY INTO.
    Creates a temporary external stage with credentials if needed.
    """
    source_config_schema = S3Config
    target_config_schema = SnowflakeConfig
    
    def execute(self, context, source_config: S3Config, target_config: SnowflakeConfig):
        # Resources
        s3_resource: S3Resource = getattr(context.resources, source_config.connection)
        snow_resource: SnowflakeResource = getattr(context.resources, target_config.connection)
        
        # Configs
        s3_path = source_config.key
        s3_bucket = source_config.bucket_name
        
        table = target_config.table
        stage = target_config.stage
        file_format_type = source_config.object_type.upper()
        match_columns = target_config.match_columns
        
        file_format_sql = f"TYPE = '{file_format_type}'"
        if file_format_type == 'CSV' and source_config.csv_options:
            delimiter = source_config.csv_options.delimiter or source_config.delimiter or ','
            if match_columns:
                file_format_sql += " PARSE_HEADER = TRUE"
            else:
                # Map has_headers to SKIP_HEADER for Snowflake if match_columns is false
                skip_header = 1 if source_config.csv_options.has_headers else 0
                file_format_sql += f" SKIP_HEADER = {skip_header}"
            
            file_format_sql += f" FIELD_DELIMITER = '{delimiter}'"
            file_format_sql += " FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE"

        force = target_config.force
        schema_strategy = target_config.schema_strategy.lower()
        delimiter = (source_config.csv_options.delimiter if source_config.csv_options else None) or source_config.delimiter or ','
        
        # 2. Handle Schema Strategy
        if schema_strategy != "fail":
            self._apply_schema_strategy(
                context, snow_resource, s3_resource, s3_bucket, table, stage, s3_path, 
                file_format_sql, file_format_type, schema_strategy, delimiter
            )

        copy_sql = f"COPY INTO {table} FROM @{stage}/{s3_path}"
        copy_sql += f" FILE_FORMAT = ({file_format_sql})"
        
        if match_columns or schema_strategy in ["evolve", "create"]:
            copy_sql += " MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"
            
        if force:
            copy_sql += " FORCE = TRUE"
            
        copy_sql += f" ON_ERROR = '{target_config.on_error}';"
        
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

    def _apply_schema_strategy(self, context, snow_resource: SnowflakeResource, s3_resource: S3Resource, bucket_name, table, stage, s3_path, file_format_sql, file_format_type, strategy, delimiter):
        context.log.info(f"Applying schema strategy: {strategy} for table {table}")
        
        # 1. Check if table exists
        table_exists = True
        existing_columns = {}
        try:
            existing_columns = snow_resource.get_table_columns(table)
        except Exception:
            table_exists = False
            context.log.info(f"Table {table} does not exist.")

        if not table_exists and strategy != "create":
            raise ValueError(f"Table {table} does not exist and schema_strategy is '{strategy}' (not 'create').")

        # 2. Infer Schema
        inferred_columns = {}
        
        if file_format_type == "CSV":
            # Native Snowflake INFER_SCHEMA doesn't support CSV properly.
            # We use Pandas to peek at the file in S3.
            try:
                df_sample = s3_resource.read_csv_sample(bucket_name, s3_path, delimiter=delimiter)
                # Map Pandas types to Snowflake types (simplified)
                # int64 -> NUMBER
                # float64 -> FLOAT
                # object -> VARCHAR
                # bool -> BOOLEAN
                # datetime64[ns] -> TIMESTAMP_NTZ
                type_map = {
                    "int64": "NUMBER",
                    "float64": "FLOAT",
                    "object": "VARCHAR",
                    "bool": "BOOLEAN",
                    "datetime64[ns]": "TIMESTAMP_NTZ"
                }
                for col_name, dtype in df_sample.dtypes.items():
                    snowflake_type = type_map.get(str(dtype), "VARCHAR")
                    inferred_columns[col_name.upper()] = snowflake_type
            except Exception as e:
                context.log.warn(f"Failed to infer CSV schema via Pandas: {e}. Falling back to VARCHAR for all columns.")
                # If we can't infer types, we might at least get column names if PARSE_HEADER is true
                # For now, just let it fail or log
                raise
        else:
            # For Parquet, Avro, etc., use Snowflake's INFER_SCHEMA
            temp_format_name = f"TEMP_FORMAT_{uuid.uuid4().hex.upper()}"
            snow_resource.execute_query(f"CREATE OR REPLACE FILE FORMAT {temp_format_name} {file_format_sql}")
            try:
                infer_sql = f"""
                SELECT COLUMN_NAME, TYPE 
                FROM TABLE(
                    INFER_SCHEMA(
                        LOCATION=>'@{stage}/{s3_path}',
                        FILE_FORMAT=>'{temp_format_name}'
                    )
                )
                """
                inferred_schema_list = snow_resource.execute_query(infer_sql)
                inferred_columns = {row["COLUMN_NAME"].upper(): row["TYPE"].upper() for row in inferred_schema_list}
            finally:
                snow_resource.execute_query(f"DROP FILE FORMAT IF EXISTS {temp_format_name}")
            
        if not inferred_columns:
            context.log.warn(f"No columns inferred from @{stage}/{s3_path}. Skipping schema management.")
            return

        if not table_exists:
            # Create Table
            cols_sql = ", ".join([f"{name} {dtype}" for name, dtype in inferred_columns.items()])
            create_sql = f"CREATE TABLE {table} ({cols_sql})"
            context.log.info(f"Creating table {table}: {create_sql}")
            snow_resource.execute_query(create_sql)
        else:
            # Table exists, compare schemas
            new_cols = {name: dtype for name, dtype in inferred_columns.items() if name not in existing_columns}
            
            if new_cols:
                if strategy == "evolve":
                    for col_name, col_type in new_cols.items():
                        context.log.info(f"Evolving schema: Adding column {col_name} ({col_type}) to {table}")
                        snow_resource.add_table_column(table, col_name, col_type)
                elif strategy == "strict":
                    raise ValueError(f"Schema mismatch detected in 'strict' mode. New columns found: {list(new_cols.keys())}")
            
            if strategy == "strict":
                missing_in_file = set(existing_columns.keys()) - set(inferred_columns.keys())
                if missing_in_file:
                    raise ValueError(f"Schema mismatch detected in 'strict' mode. Columns missing in file: {list(missing_in_file)}")
