import psycopg2
from typing import Optional, Iterator, List, ClassVar
from contextlib import contextmanager
from dagster import get_dagster_logger
from dagster_dag_factory.resources.base import BaseConfigurableResource


class PostgresResource(BaseConfigurableResource):
    """
    Standard Postgres Resource.
    """

    host: str
    user: str
    password: str
    database: str
    port: int = 5432

    mask_fields: ClassVar[List[str]] = BaseConfigurableResource.mask_fields + [
        "password"
    ]

    @contextmanager
    def get_connection(self) -> Iterator[psycopg2.extensions.connection]:
        get_dagster_logger().info(
            f"Connecting to Postgres: {self.host}:{self.port} (db: {self.database})"
        )

        conn = psycopg2.connect(
            host=self.host,
            user=self.resolve("user"),
            password=self.resolve("password"),
            database=self.database,
            port=self.port,
        )
        try:
            yield conn
        finally:
            conn.close()

    def execute_query(self, sql: str, params: Optional[dict] = None) -> list:
        """Executes a query and returns list of dictionaries."""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                get_dagster_logger().info(f"Executing SQL:\n{sql}")
                cursor.execute(sql, params)

                results = []
                if cursor.description:
                    columns = [col[0] for col in cursor.description]
                    results = [dict(zip(columns, row)) for row in cursor.fetchall()]

                return results

    def bulk_insert_rows(
        self,
        table: str,
        rows: List[tuple],
        columns: Optional[List[str]] = None,
        commit_every: int = 5000,
    ) -> int:
        """
        High-performance bulk insert using executemany.
        """
        if not rows:
            return 0

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                col_clause = f"({', '.join(columns)})" if columns else ""
                val_placeholders = ", ".join(["%s"] * len(rows[0]))
                sql = f"INSERT INTO {table} {col_clause} VALUES ({val_placeholders})"

                row_count = 0
                for i in range(0, len(rows), commit_every):
                    chunk = rows[i : i + commit_every]
                    cursor.executemany(sql, chunk)
                    row_count += len(chunk)
                    conn.commit()

                return row_count
