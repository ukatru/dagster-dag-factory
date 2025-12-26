"""
SQL Server to S3 Operator

Transfers data from SQL Server to S3 with parallel processing using streaming pattern.
"""
import time
import pandas as pd
import io
from typing import Dict, Any

from dagster_dag_factory.operators.base_operator import BaseOperator
from dagster_dag_factory.utils.streaming import ProcessorItem, execute_streaming
from dagster_dag_factory.factory.registry import OperatorRegistry
from dagster_dag_factory.configs.sqlserver import SQLServerConfig
from dagster_dag_factory.configs.s3 import S3Config


@OperatorRegistry.register(source="SQLSERVER", target="S3")
class SqlServerS3Operator(BaseOperator):
    """
    SQL Server to S3 transfer operator using streaming pattern.
    
    Transfers query results from SQL Server to S3 with parallel processing.
    Uses producer-consumer streaming with self-feeding pattern for chunked reading.
    """
    source_config_schema = SQLServerConfig
    target_config_schema = S3Config
    
    def _execute(
        self,
        context,
        source_config: SQLServerConfig,
        target_config: S3Config,
        template_vars: Dict[str, Any],
        **kwargs
    ) -> Dict[str, Any]:
        """
        Execute SQL Server to S3 transfer using universal streaming pattern.
        
        Pattern:
        1. Producer (main thread): Kicks off first fetch
        2. Worker (worker thread): Fetches rows, writes to S3, queues next fetch (self-feeding)
        """
        source_resource = kwargs.get("source_resource")
        s3_resource = kwargs.get("target_resource")
        
        chunk_size = source_config.rows_chunk
        query = source_config.sql
        params = source_config.params
        s3_format = (target_config.object_type or "csv").lower()
        
        # Shared state for tracking progress
        state = {
            "total_rows": 0,
            "columns": None,
            "first_chunk": True,
            "cursor": None,
            "smart_buffer": None
        }
        
        # Producer: Main thread kicks off first fetch
        def producer(processor):
            """Initialize cursor and kick off first fetch"""
            # Open cursor using context manager properly
            cursor_cm = source_resource.get_cursor()
            state["cursor_cm"] = cursor_cm
            state["cursor"] = cursor_cm.__enter__()
            
            context.log.info(f"Executing query and fetching in chunks of {chunk_size}...")
            if params:
                state["cursor"].execute(query, list(params.values()) if isinstance(params, dict) else params)
            else:
                state["cursor"].execute(query)
            
            state["columns"] = [column[0] for column in state["cursor"].description]
            
            # Create smart buffer once
            state["smart_buffer"] = s3_resource.create_smart_buffer(
                bucket_name=target_config.bucket_name,
                key=target_config.key,
                multi_file=target_config.multi_file,
                min_size=target_config.min_size or 5,
                compress_options=target_config.compress_options,
                logger=context.log,
            )
            
            # Kick off first fetch
            processor.put(ProcessorItem(name='fetch[1]', data=1))
        
        # Worker: Fetch rows and write to S3 (self-feeding pattern)
        def worker(processor, item, index):
            """Fetch rows, write to S3, and queue next fetch"""
            cursor = state["cursor"]
            buffer = state["smart_buffer"]
            
            try:
                # Fetch rows
                rows = cursor.fetchmany(chunk_size)
                
                if not rows:
                    # No more rows - we're done
                    # Handle empty result set case
                    if state["first_chunk"] and s3_format == "csv" and state["columns"]:
                        df = pd.DataFrame(columns=state["columns"])
                        csv_buf = io.StringIO()
                        df.to_csv(csv_buf, index=False)
                        buffer.write(csv_buf.getvalue().encode())
                    return None
                
                # Convert to DataFrame (convert pyodbc Row objects to tuples)
                row_tuples = [tuple(row) for row in rows]
                df = pd.DataFrame(row_tuples, columns=state["columns"])
                
                # Write to S3
                if s3_format == "csv":
                    csv_buf = io.StringIO()
                    df.to_csv(csv_buf, index=False, header=state["first_chunk"])
                    buffer.write(csv_buf.getvalue().encode())
                elif s3_format == "parquet":
                    parquet_buf = io.BytesIO()
                    df.to_parquet(parquet_buf, index=False)
                    buffer.write(parquet_buf.getvalue())
                
                state["total_rows"] += len(rows)
                state["first_chunk"] = False
                
                # Count-based Milestone Logging (every 10 parts)
                if index % 10 == 0 and index > 0:
                    from dagster_dag_factory.factory.utils.logging import convert_size
                    context.log.info(
                        f"Database Extraction Progress: Part {index} processed | "
                        f"Rows: {state['total_rows']} | "
                        f"Size: {convert_size(state['smart_buffer']._uploaded_bytes if hasattr(state['smart_buffer'], '_uploaded_bytes') else 0)}"
                    )
                
                # Self-feeding: Queue next fetch
                processor.put(ProcessorItem(name=f'fetch[{index+1}]', data=index+1))
                
                return {"rows": len(rows)}
                
            except Exception as e:
                buffer.abort()
                raise e
        
        # Execute using universal streaming utility (single worker for DB)
        context.log.info("Using 1 worker for SQL Server transfer (DB connections aren't thread-safe)")
        start_time = time.time()
        
        try:
            execute_streaming(
                producer_callback=producer,
                worker_callback=worker,
                num_workers=1,  # DB connections aren't thread-safe
                logger=context.log
            )
        finally:
            # Cleanup: Close cursor and buffer
            if state.get("cursor_cm"):
                state["cursor_cm"].__exit__(None, None, None)
            
            if state["smart_buffer"]:
                transferred_files = state["smart_buffer"].close()
            else:
                transferred_files = []
        
        duration = time.time() - start_time
        total_rows = state["total_rows"]
        rows_per_sec = round(total_rows / duration, 2) if duration > 0 else 0
        
        summary = {
            "rows_transferred": total_rows,
            "duration_seconds": round(duration, 2),
            "rows_per_second": rows_per_sec,
            "files_written": len(transferred_files),
            "mode": "streaming_v2"
        }
        
        # Add structured stats for BaseOperator reporting
        result = {
            "summary": summary,
            "observations": {"rows_extracted": total_rows, "rows_written": total_rows},
            "source": "SQLSERVER",
            "target": "S3",
        }
        
        # Calculate total bytes from smart buffer results
        total_bytes = sum(f.get("size", 0) for f in transferred_files)
        
        result["stats"] = {
            "rows_processed": total_rows,
            "total_bytes": total_bytes,
            "files_transferred": len(transferred_files)
        }
        
        return result
