"""Configuration classes for operation-specific settings."""

import pkgutil

__path__ = pkgutil.extend_path(__path__, __name__)

# Standard resources
from dagster_dag_factory.resources.aws import AWSResource
from dagster_dag_factory.resources.s3 import S3Resource
from dagster_dag_factory.resources.sftp import SFTPResource
from dagster_dag_factory.resources.sqlserver import SQLServerResource
from dagster_dag_factory.resources.snowflake import SnowflakeResource

# Optional resources (handle missing drivers gracefully)
try:
    from dagster_dag_factory.resources.postgres import PostgresResource
except ImportError:
    PostgresResource = None

__all__ = [
    "AWSResource",
    "S3Resource",
    "SFTPResource",
    "SQLServerResource",
    "SnowflakeResource",
    "PostgresResource",
]
