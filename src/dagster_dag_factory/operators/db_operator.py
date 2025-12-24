from typing import Dict, Any, List, Union
from abc import abstractmethod
from dagster_dag_factory.operators.base_operator import BaseOperator
from dagster_dag_factory.configs.database import DatabaseConfig
from dagster_dag_factory.configs.s3 import S3Config


class DbBaseOperator(BaseOperator):
    """
    Base operator providing SQL execution helpers for lifecycle hooks.
    Operators now receive resource objects directly, decoupling them from connection names.
    """

    def _execute_sql_list(self, context, resource, sql_list: List[Union[str, Any]]):
        if not sql_list:
            return

        with resource.get_connection() as conn:
            with conn.cursor() as cursor:
                for item in sql_list:
                    # Support both raw strings and SqlConfig objects
                    sql = item.sql if hasattr(item, "sql") else item
                    params = getattr(item, "params", None)
                    context.log.info(f"Executing lifecycle SQL: {sql}")
                    if params:
                        cursor.execute(sql, params)
                    else:
                        cursor.execute(sql)

    def _run_hooks(self, context, resource, config: DatabaseConfig, stage: str):
        """Runs sql_pre or sql_post hooks."""
        sql_list = config.sql_pre if stage == "pre" else config.sql_post
        if sql_list:
            context.log.info(f"Running sql_{stage} hooks...")
            self._execute_sql_list(context, resource, sql_list)


class DbToDbBaseOperator(DbBaseOperator):
    """
    Base class for transferring data between two databases.
    Standardizes the lifecycle: pre -> perform_transfer -> post.
    """

    def execute(
        self,
        context,
        source_config: DatabaseConfig,
        target_config: DatabaseConfig,
        source_resource: Any,
        target_resource: Any,
        template_vars: Dict[str, Any],
    ):
        # 1. Pre-SQL Hooks
        self._run_hooks(context, source_resource, source_config, "pre")
        self._run_hooks(context, target_resource, target_config, "pre")

        # 2. Extract & Load
        results = self.perform_transfer(
            context, source_resource, source_config, target_resource, target_config
        )

        # 3. Post-SQL Hooks
        self._run_hooks(context, source_resource, source_config, "post")
        self._run_hooks(context, target_resource, target_config, "post")

        return results

    @abstractmethod
    def perform_transfer(
        self,
        context,
        source_res,
        source_cfg: DatabaseConfig,
        target_res,
        target_cfg: DatabaseConfig,
    ) -> Dict[str, Any]:
        """Subclasses implement the specific extraction and loading logic."""
        pass


class SnowflakeBaseOperator(DbToDbBaseOperator):
    """Foundation for Snowflake-target transfers."""

    @abstractmethod
    def _execute_write(
        self,
        context,
        target_res,
        target_cfg,
        target_cursor,
        rows,
        columns,
        is_first_chunk,
    ):
        pass


class PostgresBaseOperator(DbToDbBaseOperator):
    """Foundation for Postgres-target transfers."""

    @abstractmethod
    def _execute_write(
        self,
        context,
        target_res,
        target_cfg,
        target_cursor,
        rows,
        columns,
        is_first_chunk,
    ):
        pass


class DbToS3BaseOperator(DbBaseOperator):
    """
    Base class for transferring data from a database to S3.
    Supports high-performance streaming via chunking.
    """

    def execute(
        self,
        context,
        source_config: DatabaseConfig,
        target_config: S3Config,
        source_resource: Any,
        target_resource: Any,
        template_vars: Dict[str, Any],
    ):
        # 1. Pre-SQL Hooks on Source
        self._run_hooks(context, source_resource, source_config, "pre")

        # 2. Extract & Load
        results = self.perform_transfer(
            context, source_resource, source_config, target_resource, target_config
        )

        # 3. Post-SQL Hooks on Source
        self._run_hooks(context, source_resource, source_config, "post")

        return results

    @abstractmethod
    def perform_transfer(
        self,
        context,
        source_res,
        source_cfg: DatabaseConfig,
        target_res: Any,
        target_cfg: S3Config,
    ) -> Dict[str, Any]:
        """Subclasses implement the specific streaming logic."""
        pass
