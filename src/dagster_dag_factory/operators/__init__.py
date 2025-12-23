"""Configuration classes for operation-specific settings."""

import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)

# Import operators here to ensure they register themselves
from dagster_dag_factory.operators.sqlserver_s3 import SqlServerS3Operator
from dagster_dag_factory.operators.sftp_s3 import SftpS3Operator
from dagster_dag_factory.operators.s3_snowflake import S3SnowflakeOperator
from dagster_dag_factory.operators.sql_server_check import SQLServerCheck
