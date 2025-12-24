from typing import Optional, Iterator, Dict, Any, List, ClassVar
from contextlib import contextmanager
from dagster import get_dagster_logger
from pydantic import Field
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from dagster_dag_factory.resources.base import BaseConfigurableResource


class SnowflakeResource(BaseConfigurableResource):
    """
    Custom Snowflake Resource.
    """

    account: str
    user: str
    password: Optional[str] = None
    warehouse: Optional[str] = None
    database: Optional[str] = None
    schema_: Optional[str] = Field(default=None, alias="schema")
    role: Optional[str] = None

    private_key: Optional[str] = Field(
        default=None, description="PEM encoded private key"
    )
    private_key_path: Optional[str] = Field(
        default=None, description="Path to private key file"
    )
    private_key_passphrase: Optional[str] = Field(
        default=None, description="Passphrase for private key"
    )

    mask_fields: ClassVar[List[str]] = BaseConfigurableResource.mask_fields + [
        "password",
        "private_key",
        "private_key_passphrase",
    ]

    def _get_private_key_bytes(self):
        if self.private_key:
            return self.private_key.encode("utf-8")
        if self.private_key_path:
            with open(self.private_key_path, "rb") as key_file:
                return key_file.read()
        return None

    def get_connection_params(self):
        params = {
            "account": self.resolve("account"),
            "user": self.resolve("user"),
            "warehouse": self.resolve("warehouse"),
            "database": self.resolve("database"),
            "schema": self.resolve("schema_"),
            "role": self.resolve("role"),
        }

        pkey_bytes = self._get_private_key_bytes()
        if pkey_bytes:
            p_key = serialization.load_pem_private_key(
                pkey_bytes,
                password=self.resolve("private_key_passphrase").encode()
                if self.resolve("private_key_passphrase")
                else None,
                backend=default_backend(),
            )
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            params["private_key"] = pkb
        else:
            params["password"] = self.resolve("password")

        return params

    @contextmanager
    def get_connection(self) -> Iterator[snowflake.connector.SnowflakeConnection]:
        conn_params = self.get_connection_params()

        # Mask credentials for log
        log_params = conn_params.copy()
        if "password" in log_params:
            log_params["password"] = "******"
        if "private_key" in log_params:
            log_params["private_key"] = "******"

        get_dagster_logger().info(f"Connecting to Snowflake: {log_params}")

        conn = snowflake.connector.connect(**conn_params)
        try:
            # Explicitly set session context
            cursor = conn.cursor()
            role = self.resolve("role")
            wh = self.resolve("warehouse")
            db = self.resolve("database")
            sch = self.resolve("schema_")

            if role:
                cursor.execute(f"USE ROLE {role}")
            if wh:
                cursor.execute(f"USE WAREHOUSE {wh}")
            if db:
                cursor.execute(f"USE DATABASE {db}")
            if sch:
                cursor.execute(f"USE SCHEMA {sch}")
            cursor.close()

            yield conn
        finally:
            conn.close()

    def execute_query(self, sql: str, params: Optional[dict] = None) -> list:
        """Executes a query and returns list of dictionaries."""
        logger = get_dagster_logger()
        logger.info(f"Executing SQL:\n{sql}")

        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params)
                results = []
                try:
                    results = cursor.fetchall()
                    if cursor.description:
                        columns = [col[0] for col in cursor.description]
                        results = [dict(zip(columns, row)) for row in results]
                except Exception:
                    pass
                return results
            finally:
                cursor.close()

    def get_table_columns(self, table_name: str) -> Dict[str, str]:
        sql = f"DESCRIBE TABLE {table_name}"
        results = self.execute_query(sql)
        return {row["name"].upper(): row["type"].upper() for row in results}

    def add_table_column(self, table_name: str, col_name: str, col_type: str):
        sql = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}"
        self.execute_query(sql)

    def load_dataframe(
        self,
        df: Any,
        table_name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ):
        """
        Efficiently loads a pandas DataFrame into Snowflake using write_pandas.
        """
        from snowflake.connector.pandas_tools import write_pandas

        logger = get_dagster_logger()
        logger.info(f"Bulk loading {len(df)} rows into Snowflake table {table_name}")

        with self.get_connection() as conn:
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name=table_name,
                database=database or self.database,
                schema=schema or self.schema_,
                quote_identifiers=False,
            )

            if success:
                logger.info(f"Successfully loaded {nrows} rows in {nchunks} chunks.")
            else:
                raise Exception(
                    f"Failed to load data into Snowflake table {table_name}"
                )

            return nrows

    def bulk_insert_rows(
        self,
        table: str,
        rows: List[tuple],
        columns: Optional[List[str]] = None,
        commit_every: int = 5000,
        cursor: Optional[Any] = None,
    ) -> int:
        """
        High-performance bulk insert using executemany.
        """
        if not rows:
            return 0

        # Use provided cursor or manage a connection/cursor locally
        if cursor:
            self._bulk_insert_with_cursor(cursor, table, rows, columns, commit_every)
            return len(rows)

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                self._bulk_insert_with_cursor(cur, table, rows, columns, commit_every)
                conn.commit()
                return len(rows)

    def _bulk_insert_with_cursor(self, cursor, table, rows, columns, commit_every):
        # create the insert statement
        val_placeholders = ", ".join(["%s"] * len(rows[0]))
        col_clause = f"({', '.join(columns)})" if columns else ""
        sql = f"INSERT INTO {table} {col_clause} VALUES ({val_placeholders})"

        for i in range(0, len(rows), commit_every):
            chunk = rows[i : i + commit_every]
            cursor.executemany(sql, chunk)
            if not getattr(cursor.connection, "autocommit", False):
                cursor.connection.commit()
