from typing import Dict, Any
from dagster import AssetCheckExecutionContext, AssetCheckResult
from dagster_dag_factory.operators.db_operator import DbToS3BaseOperator
from dagster_dag_factory.factory.registry import OperatorRegistry
from dagster_dag_factory.resources.sqlserver import SQLServerResource
from dagster_dag_factory.resources.s3 import S3Resource
from dagster_dag_factory.configs.sqlserver import SQLServerConfig
from dagster_dag_factory.configs.s3 import S3Config


@OperatorRegistry.register(source="SQLSERVER", target="S3")
class SqlServerS3Operator(DbToS3BaseOperator):
    source_config_schema = SQLServerConfig
    target_config_schema = S3Config

    def perform_transfer(
        self,
        context,
        source_res: SQLServerResource,
        source_cfg: SQLServerConfig,
        target_res: S3Resource,
        target_cfg: S3Config,
    ) -> Dict[str, Any]:
        import time
        import pandas as pd
        import io

        start_time = time.time()
        chunk_size = source_cfg.rows_chunk
        query = source_cfg.sql
        params = source_cfg.params

        # Initialize S3 Smart Buffer (Background Threaded)
        s3_format = (target_cfg.object_type or "csv").lower()
        buffer = target_res.create_smart_buffer(
            bucket_name=target_cfg.bucket_name,
            key=target_cfg.key,
            multi_file=target_cfg.multi_file,
            min_size=target_cfg.min_size or 5,
            compress_options=target_cfg.compress_options,
            logger=context.log,
        )

        total_rows = 0
        try:
            with source_res.get_cursor() as cursor:
                context.log.info(
                    f"Streaming query results in chunks of {chunk_size}..."
                )
                if params:
                    cursor.execute(
                        query,
                        list(params.values()) if isinstance(params, dict) else params,
                    )
                else:
                    cursor.execute(query)

                columns = [column[0] for column in cursor.description]
                first_chunk = True

                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        # For CSV, if the very first fetch is empty, we still write the header
                        if first_chunk and s3_format == "csv":
                            df = pd.DataFrame(columns=columns)
                            csv_buf = io.StringIO()
                            df.to_csv(csv_buf, index=False)
                            buffer.write(csv_buf.getvalue().encode())
                        break

                    # Efficiently convert batch to DataFrame
                    df = pd.DataFrame([tuple(r) for r in rows], columns=columns)

                    if s3_format == "csv":
                        csv_buf = io.StringIO()
                        # Only include header for the very first chunk.
                        df.to_csv(csv_buf, index=False, header=first_chunk)
                        buffer.write(csv_buf.getvalue().encode())
                    elif s3_format == "parquet":
                        parquet_buf = io.BytesIO()
                        df.to_parquet(parquet_buf, index=False)
                        buffer.write(parquet_buf.getvalue())

                    total_rows += len(rows)
                    first_chunk = False
                    context.log.info(
                        f"Pushed {len(rows)} rows to S3 (Total extracted: {total_rows})"
                    )
        except Exception as e:
            context.log.error(f"Extraction failed: {e}")
            buffer.abort()
            raise e
        finally:
            # wait for all background uploads to complete
            transferred_files = buffer.close()

        duration = time.time() - start_time
        rows_per_sec = round(total_rows / duration, 2) if duration > 0 else 0

        summary = {
            "rows_transferred": total_rows,
            "duration_seconds": round(duration, 2),
            "rows_per_second": rows_per_sec,
            "files_written": len(transferred_files),
            "path": target_cfg.key,
        }

        context.log.info(
            f"Transfer complete. {total_rows} rows processed in {round(duration, 2)}s ({rows_per_sec} rows/s)."
        )

        return {
            "summary": summary,
            "observations": {"rows_extracted": total_rows, "rows_written": total_rows},
            "source": "SQLSERVER",
            "target": "S3",
        }

    def execute_check(
        self, context: AssetCheckExecutionContext, config: dict
    ) -> AssetCheckResult:
        check_type = config.get("type")

        if check_type == "sql_server_check":
            query = config["query"]
            connection_name = config["connection"]
            threshold = config.get("threshold", 1)

            # Get resource (we can reuse the same established pattern)
            resource = getattr(context.resources, connection_name)

            # Execute
            results = resource.execute_query(query)

            passed = False
            val = None
            if results and len(results) > 0:
                first_row = results[0]
                val = (
                    list(first_row.values())[0]
                    if isinstance(first_row, dict)
                    else first_row[0]
                )
                passed = float(val) >= threshold
                message = f"Check value: {val}, Threshold: {threshold}"
            else:
                message = "No results returned from check query."

            return AssetCheckResult(
                passed=passed,
                metadata={
                    "value": val if val is not None else None,
                    "message": message,
                    "query": query,
                },
            )

        # Fallback to BaseOperator for observation_diff etc.
        return super().execute_check(context, config)
