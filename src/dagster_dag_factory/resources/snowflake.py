from typing import Optional, Iterator, ContextManager, Dict, Any, List, ClassVar
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
    
    private_key: Optional[str] = Field(default=None, description="PEM encoded private key")
    private_key_path: Optional[str] = Field(default=None, description="Path to private key file")
    private_key_passphrase: Optional[str] = Field(default=None, description="Passphrase for private key")
    
    mask_fields: ClassVar[List[str]] = BaseConfigurableResource.mask_fields + ["password", "private_key", "private_key_passphrase"]

    def _get_private_key_bytes(self):
        if self.private_key:
             return self.private_key.encode('utf-8')
        if self.private_key_path:
             with open(self.private_key_path, "rb") as key_file:
                  return key_file.read()
        return None

    def get_connection_params(self):
        params = {
            "account": self.account,
            "user": self.user,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema_,
            "role": self.role,
        }
        
        pkey_bytes = self._get_private_key_bytes()
        if pkey_bytes:
            p_key = serialization.load_pem_private_key(
                pkey_bytes,
                password=self.private_key_passphrase.encode() if self.private_key_passphrase else None,
                backend=default_backend()
            )
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            params["private_key"] = pkb
        else:
            params["password"] = self.password

        return params

    @contextmanager
    def get_connection(self) -> Iterator[snowflake.connector.SnowflakeConnection]:
        conn_params = self.get_connection_params()
        
        # Mask credentials for log
        log_params = conn_params.copy()
        if "password" in log_params: log_params["password"] = "******"
        if "private_key" in log_params: log_params["private_key"] = "******"
        
        get_dagster_logger().info(f"Connecting to Snowflake: {log_params}")

        conn = snowflake.connector.connect(**conn_params)
        try:
            # Explicitly set session context
            cursor = conn.cursor()
            if self.role: cursor.execute(f"USE ROLE {self.role}")
            if self.warehouse: cursor.execute(f"USE WAREHOUSE {self.warehouse}")
            if self.database: cursor.execute(f"USE DATABASE {self.database}")
            if self.schema_: cursor.execute(f"USE SCHEMA {self.schema_}")
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
