import time
from typing import Dict, Any
from dagster_dag_factory.operators.base_operator import BaseOperator
from dagster_dag_factory.factory.registry import OperatorRegistry
from dagster_dag_factory.resources.sqlserver import SQLServerResource
from dagster_dag_factory.resources.snowflake import SnowflakeResource
from dagster_dag_factory.configs.sqlserver import SQLServerConfig
from dagster_dag_factory.configs.snowflake import SnowflakeConfig


@OperatorRegistry.register(source="SQLSERVER", target="SNOWFLAKE")
class SqlServerSnowflakeOperator(BaseOperator):
    """
    High-performance SQL Server to Snowflake transfer.
    Uses chunked fetching and bulk uploads to prevent memory issues.
    """

    source_config_schema = SQLServerConfig
    target_config_schema = SnowflakeConfig

    def _execute(
        self,
        context,
        source_config: SQLServerConfig,
        target_config: SnowflakeConfig,
        template_vars: Dict[str, Any],
        **kwargs,
    ) -> Dict[str, Any]:
        source_res: SQLServerResource = kwargs.get("source_resource")
        target_res: SnowflakeResource = kwargs.get("target_resource")
        
        start_time = time.time()
        chunk_size = source_config.rows_chunk
        query = source_config.sql
        params = source_config.params

        if not query and source_config.table_name:
            cols = "*" if not source_config.columns else ", ".join(source_config.columns)
            query = f"SELECT {cols} FROM {f'{source_config.schema_name}.' if source_config.schema_name else ''}{source_config.table_name}"

        if not query:
            raise ValueError(
                "SqlServerSnowflakeOperator requires either 'sql' or 'table_name' in source_config"
            )

        total_rows = 0
        try:
            with source_res.get_cursor() as source_cursor:
                context.log.info(
                    f"Opening Snowflake connection for target: {target_config.table_name}"
                )
                with target_res.get_connection() as target_conn:
                    with target_conn.cursor() as target_cursor:
                        context.log.info(
                            f"Streaming query results in chunks of {chunk_size}..."
                        )
                        if params:
                            source_cursor.execute(
                                query,
                                list(params.values())
                                if isinstance(params, dict)
                                else params,
                            )
                        else:
                            source_cursor.execute(query)

                        columns = [col[0].upper() for col in source_cursor.description]
                        first_batch = True

                        while True:
                            rows = source_cursor.fetchmany(chunk_size)
                            if not rows:
                                break

                            # Convert to list of tuples if they aren't already
                            row_tuples = [tuple(r) for r in rows]

                            # Optimized Write
                            self._execute_write(
                                context=context,
                                target_res=target_res,
                                target_config=target_config,
                                target_cursor=target_cursor,
                                rows=row_tuples,
                                columns=columns,
                                is_first_chunk=first_batch,
                            )

                            total_rows += len(rows)
                            first_batch = False
                            context.log.info(
                                f"Transferred {len(rows)} rows (Total: {total_rows})"
                            )

        except Exception as e:
            context.log.error(f"Transfer failed: {e}")
            raise e

        duration = time.time() - start_time
        summary = {
            "rows_transferred": total_rows,
            "duration_seconds": round(duration, 2),
            "rows_per_second": round(total_rows / duration, 2) if duration > 0 else 0,
            "target_table": target_config.table_name,
        }

        result = {"summary": summary, "observations": {"rows_transferred": total_rows}}
        
        # Add structured stats for BaseOperator reporting
        result["stats"] = {
            "rows_processed": total_rows,
            "total_bytes": 0, # Bytes not easily tracked in direct bulk insert
        }
        
        return result

    def _execute_write(
        self,
        context,
        target_res: SnowflakeResource,
        target_config,
        target_cursor,
        rows,
        columns,
        is_first_chunk,
    ):
        """
        Implements optimized Snowflake writing.
        """
        table = target_config.table_name

        # Use resource bulk helper
        target_res.bulk_insert_rows(
            table=table,
            rows=rows,
            columns=columns,
            cursor=target_cursor,  # Pass cursor to keep transaction open if needed
        )


# Add explicit support for passing cursor to bulk_insert_rows in resource
