from typing import Optional, Iterator, List, ClassVar
from contextlib import contextmanager
import pyodbc
from dagster import get_dagster_logger
from pydantic import Field
from dagster_dag_factory.resources.base import BaseConfigurableResource

class SQLServerResource(BaseConfigurableResource):
    """
    Dagster resource for SQL Server connections.
    Wraps pyodbc to provide a clean interface for executing queries.
    """
    host: str = Field(description="SQL Server hostname")
    database: str = Field(description="Database name")
    user: Optional[str] = Field(default=None, description="Username (SQL Auth)")
    password: Optional[str] = Field(default=None, description="Password (SQL Auth)")
    port: int = Field(default=1433, description="SQL Server port")
    driver: str = Field(default="ODBC Driver 18 for SQL Server", description="ODBC driver name")
    encrypt: bool = Field(default=True, description="Encrypt connection")
    trust_server_certificate: bool = Field(default=True, description="Trust server certificate (useful for self-signed)")
    
    mask_fields: ClassVar[List[str]] = BaseConfigurableResource.mask_fields + ["password", "pwd"]

    @property
    def connection_string(self) -> str:
        conn_str = (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.host},{self.port};"
            f"DATABASE={self.database};"
        )
        
        if self.user:
            conn_str += f"UID={self.user};PWD={self.password};"
        else:
            conn_str += "Trusted_Connection=yes;"
            
        if self.encrypt:
            conn_str += "Encrypt=yes;"
        
        if self.trust_server_certificate:
            conn_str += "TrustServerCertificate=yes;"
            
        return conn_str

    def _mask_conn_string(self, conn_str: str) -> str:
        """Masks sensitive information in connection string."""
        if "PWD=" in conn_str:
            import re
            return re.sub(r'PWD=([^;]+)', 'PWD=******', conn_str)
        return conn_str

    @contextmanager
    def get_connection(self) -> Iterator[pyodbc.Connection]:
        """Yields a raw pyodbc connection."""
        logger = get_dagster_logger()
        # Benefit from BaseConfigurableResource masking if logging the whole object
        # logger.info(f"Connecting with resource: {self}")
        
        masked_conn = self._mask_conn_string(self.connection_string)
        logger.info(f"Connecting to SQL Server: {masked_conn}")
        
        conn = pyodbc.connect(self.connection_string)
        try:
            yield conn
        finally:
            conn.close()

    def execute_query(self, sql: str, params: Optional[tuple] = None) -> list:
        """Executes a query and returns list of dictionaries (one per row)."""
        logger = get_dagster_logger()
        logger.info(f"Executing query:\n{sql}")
        if params:
             logger.info(f"Query params: {params}")

        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            
            if cursor.description:
                columns = [column[0] for column in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
            return []
