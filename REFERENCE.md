# Dagster Factory Reference

## Resources

Resources are used to define connections to external systems.

### `S3Resource`
Dagster resource for S3 operations with enterprise authentication support.
    Supports explicit keys, profiles, and EKS Web Identity (IRSA).

| Field | Type | Description |
| :--- | :--- | :--- |
| `bucket_name` | `string` | Default bucket name |
| `access_key` | `Any` | AWS Access Key ID |
| `secret_key` | `Any` | AWS Secret Access Key |
| `session_token` | `Any` | AWS Session Token |
| `assume_role_arn` | `Any` | ARN of role to assume |
| `aws_web_identity_token_file` | `Any` | Path to web identity token file (IRSA) |
| `assume_role_session_name` | `Any` | Session name for assumed role |
| `external_id` | `Any` | External ID for cross-account role assumption |
| `region_name` | `string` | AWS Region |
| `endpoint_url` | `Any` | Custom endpoint URL (e.g. for MinIO) |
| `profile_name` | `Any` | AWS Profile name |
| `use_unsigned_session` | `boolean` | Use unsigned session |
| `verify` | `boolean` | Verify SSL certificates |


### `SFTPResource`
Dagster resource for SFTP operations using Paramiko.

| Field | Type | Description |
| :--- | :--- | :--- |
| `host` | `string` | SFTP Hostname |
| `port` | `integer` | SFTP Port |
| `username` | `string` | SFTP Username |
| `password` | `Any` | SFTP Password |
| `private_key_path` | `Any` | Path to private key file |


### `SQLServerResource`
Dagster resource for SQL Server connections.
    Wraps pyodbc to provide a clean interface for executing queries.

| Field | Type | Description |
| :--- | :--- | :--- |
| `host` | `string` | SQL Server hostname |
| `database` | `string` | Database name |
| `user` | `Any` | Username (SQL Auth) |
| `password` | `Any` | Password (SQL Auth) |
| `port` | `integer` | SQL Server port |
| `driver` | `string` | ODBC driver name |
| `encrypt` | `boolean` | Encrypt connection |
| `trust_server_certificate` | `boolean` | Trust server certificate (useful for self-signed) |


### `SnowflakeResource`
Custom Snowflake Resource.

| Field | Type | Description |
| :--- | :--- | :--- |
| `account` | `string` |  |
| `user` | `string` |  |
| `password` | `Any` |  |
| `warehouse` | `Any` |  |
| `database` | `Any` |  |
| `schema` | `Any` |  |
| `role` | `Any` |  |
| `private_key` | `Any` | PEM encoded private key |
| `private_key_path` | `Any` | Path to private key file |
| `private_key_passphrase` | `Any` | Passphrase for private key |


## Operators

Operators define how data moves between a source and a target.

### `SQLSERVER` to `S3` (`SqlServerToS3Operator`)
#### Source Configuration
| Field | Type | Description |
| :--- | :--- | :--- |
| `connection` | `string` | Name of the SQL Server resource|
| `query` | `Any` | SQL Query to extract data|
| `table` | `Any` | Source table name|


#### Target Configuration
| Field | Type | Description |
| :--- | :--- | :--- |
| `connection` | `string` | Name of the S3 resource|
| `path` | `string` | S3 Path (e.g. raw/sales/ or bucket/path)|
| `format` | `Any` | File format (CSV, PARQUET, JSON)|
| `delimiter` | `Any` | CSV Delimiter (if applicable)|
| `skip_header` | `Any` | Number of header rows to skip|
| `compression` | `Any` | Compression type (gzip, snappy, etc.)|


### `SFTP` to `S3` (`SftpToS3Operator`)
#### Source Configuration
| Field | Type | Description |
| :--- | :--- | :--- |
| `connection` | `string` | Name of the SFTP resource|
| `path` | `string` | Remote directory or file path|
| `pattern` | `Any` | Regex pattern to match files|
| `recursive` | `boolean` | Search subdirectories recursively|
| `predicate` | `Any` | Python expression for filtering files|


#### Target Configuration
| Field | Type | Description |
| :--- | :--- | :--- |
| `connection` | `string` | Name of the S3 resource|
| `path` | `string` | S3 Path (e.g. raw/sales/ or bucket/path)|
| `format` | `Any` | File format (CSV, PARQUET, JSON)|
| `delimiter` | `Any` | CSV Delimiter (if applicable)|
| `skip_header` | `Any` | Number of header rows to skip|
| `compression` | `Any` | Compression type (gzip, snappy, etc.)|


### `S3` to `SNOWFLAKE` (`S3ToSnowflakeOperator`)
Loads data from S3 to Snowflake using COPY INTO.
    Creates a temporary external stage with credentials if needed.

#### Source Configuration
| Field | Type | Description |
| :--- | :--- | :--- |
| `connection` | `string` | Name of the S3 resource|
| `path` | `string` | S3 Path (e.g. raw/sales/ or bucket/path)|
| `format` | `Any` | File format (CSV, PARQUET, JSON)|
| `delimiter` | `Any` | CSV Delimiter (if applicable)|
| `skip_header` | `Any` | Number of header rows to skip|
| `compression` | `Any` | Compression type (gzip, snappy, etc.)|


#### Target Configuration
| Field | Type | Description |
| :--- | :--- | :--- |
| `connection` | `string` | Name of the Snowflake resource|
| `table` | `string` | Target table name (fully qualified)|
| `stage` | `Any` | External stage to use for COPY INTO|
| `match_columns` | `boolean` | Use MATCH_BY_COLUMN_NAME|
| `force` | `boolean` | Force re-load if file already loaded|
| `on_error` | `Any` | ON_ERROR strategy|
| `schema_strategy` | `Any` | Strategy: fail, create, evolve, strict|

