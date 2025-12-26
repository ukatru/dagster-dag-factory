from enum import Enum


class CompressionType(str, Enum):
    ZIP = "ZIPFILE"
    GUNZIP = "GUNZIP"


class CompressionAction(str, Enum):
    COMPRESS = "COMPRESS"
    DECOMPRESS = "DECOMPRESS"


class CsvQuoting(str, Enum):
    NONE = "NONE"
    ALL = "ALL"
    MINIMAL = "MINIMAL"
    NONNUMERIC = "NONNUMERIC"


class S3Mode(str, Enum):
    COPY = "COPY"
    MULTI_FILE = "MULTI_FILE"


class S3ObjectType(str, Enum):
    CSV = "CSV"
    JSON = "JSON"
    PARQUET = "PARQUET"


class DagsterKind(str, Enum):
    """
    Standard Dagster Kind tags for UI icons and filtering.
    These map to Dagster's built-in icon pack for branded UI elements.
    """

    PYTHON = "python"
    SNOWFLAKE = "snowflake"
    S3 = "s3"
    MSSQL = "mssql"
    SFTP = "sftp"
    POSTGRES = "postgres"
    MYSQL = "mysql"
    DBT = "dbt"
    PANDAS = "pandas"
    DUCKDB = "duckdb"
    SQL = "sql"
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    AIRBYTE = "airbyte"
    FIVETRAN = "fivetran"
    EXCEL = "excel"
    SLACK = "slack"
    SQLSERVER = "sqlserver"


class OperationType(str, Enum):
    """
    Supported operation types for sources and targets.
    These are the canonical type identifiers used in YAML configurations
    and operator registrations.
    """

    SQLSERVER = "SQLSERVER"
    S3 = "S3"
    SNOWFLAKE = "SNOWFLAKE"
    SFTP = "SFTP"
    POSTGRES = "POSTGRES"
    MYSQL = "MYSQL"


# Centralized mapping from operation types to Dagster UI kinds
OPRN_TYPE_TO_KIND = {
    OperationType.SQLSERVER: DagsterKind.SQLSERVER,
    OperationType.S3: DagsterKind.S3,
    OperationType.SNOWFLAKE: DagsterKind.SNOWFLAKE,
    OperationType.SFTP: "file",  # Dagster may not have SFTP icon, use generic file icon
    OperationType.POSTGRES: DagsterKind.POSTGRES
}
