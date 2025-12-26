"""
SFTP to S3 Operator

Transfers files from SFTP to S3 with parallel processing using streaming pattern.
"""
import re
import time
from typing import Any, Dict
from jinja2 import Template

from dagster_dag_factory.operators.base_operator import BaseOperator
from dagster_dag_factory.utils.streaming import ProcessorItem, execute_streaming
from dagster_dag_factory.factory.helpers.rendering import render_config_model
from dagster_dag_factory.factory.registry import OperatorRegistry
from dagster_dag_factory.configs.sftp import SFTPConfig
from dagster_dag_factory.configs.s3 import S3Config


@OperatorRegistry.register(source="SFTP", target="S3")
class SftpS3Operator(BaseOperator):
    """
    SFTP to S3 transfer operator using streaming pattern.
    
    Transfers files from SFTP to S3 with parallel processing.
    Uses producer-consumer streaming for efficient concurrent transfers.
    """
    source_config_schema = SFTPConfig
    target_config_schema = S3Config
    
    def _execute(
        self,
        context,
        source_config: SFTPConfig,
        target_config: S3Config,
        template_vars: Dict[str, Any],
        **kwargs
    ) -> Dict[str, Any]:
        """
        Execute SFTP to S3 transfer using universal streaming pattern.
        
        Pattern:
        1. Producer (main thread): Scans SFTP and feeds files to queue
        2. Worker (worker threads): Transfer each file to S3
        """
        sftp_resource = kwargs.get("source_resource")
        s3_resource = kwargs.get("target_resource")
        
        # Extract config values
        sftp_path = source_config.path
        if source_config.file_name:
            pattern = f"^{re.escape(source_config.file_name)}$"
        else:
            pattern = source_config.pattern
            
        s3_prefix = target_config.prefix or ""
        num_workers = getattr(source_config, 'max_workers', 5)
        
        # Predicate function for filtering files
        predicate_fn = None
        if source_config.predicate:
            def predicate_fn(file_info):
                runtime_vars = {**template_vars, "source": {"item": file_info}}
                # Predicate is a string expression, render it as a template string
                rendered = Template(source_config.predicate).render(runtime_vars)
                return rendered == "True"
        
        # Render runtime config for each file
        def render_runtime_config(file_info):
            runtime_vars = template_vars.copy()
            # Set up source.item for template rendering
            runtime_vars["source"] = {"item": file_info, "path": sftp_path}
            return render_config_model(target_config, runtime_vars)
        
        # Producer: Main thread scans SFTP and feeds files to queue AS DISCOVERED
        # This allows workers to start processing while scanning continues (pipelined)
        def producer(processor):
            """Scan SFTP and feed files to worker queue as they're discovered"""
            def on_each_file(file_info, index):
                # Feed file to worker queue immediately as it's discovered
                # Workers can start processing while scan continues
                processor.put(ProcessorItem(
                    name=file_info.file_name,
                    data=file_info
                ))
            
            context.log.info(f"Scanning SFTP path: {sftp_path}")
            # Main thread scans SFTP (workers process in parallel)
            with sftp_resource.get_client() as sftp:
                sftp_resource.list_files(
                    conn=sftp,
                    path=sftp_path,
                    pattern=pattern,
                    recursive=source_config.recursive,
                    check_is_modifying=source_config.check_is_modifying,
                    predicate=predicate_fn,
                    on_each=on_each_file
                )
        
        # Worker: Process each file (runs in worker threads)
        def worker(processor, item, index):
            """Transfer one file from SFTP to S3"""
            file_info = item.data
            
            # Each worker has its own SFTP connection
            with sftp_resource.get_client() as sftp:
                # Render runtime config for this specific file
                runtime_target_config = render_runtime_config(file_info)
                
                # Determine S3 key
                target_key = runtime_target_config.key or (
                    f"{s3_prefix.rstrip('/')}/{file_info.file_name}" if s3_prefix else file_info.file_name
                )
                
                if context.log:
                    context.log.info(
                        f"Transferring {file_info.file_name} → s3://{runtime_target_config.bucket_name}/{target_key}"
                    )
                
                # Create smart buffer for S3 upload
                smart_buffer = s3_resource.create_smart_buffer(
                    bucket_name=runtime_target_config.bucket_name,
                    key=target_key,
                    multi_file=runtime_target_config.multi_file,
                    min_size=runtime_target_config.min_size,
                    compress_options=runtime_target_config.compress_options,
                    logger=context.log,
                )
                
                try:
                    # Transfer file using paramiko's getfo
                    sftp.getfo(file_info.full_file_path, smart_buffer)
                    results = smart_buffer.close()
                    
                    # Enrich results with source info
                    for res in results:
                        res["source"] = file_info.full_file_path
                    
                    return results
                    
                except Exception as e:
                    smart_buffer.abort()
                    if context.log:
                        context.log.error(f"Transfer failed for {file_info.file_name}: {e}")
                    raise e
        
        # Execute using universal streaming utility
        # Execute using universal streaming utility
        result = execute_streaming(
            producer_callback=producer,
            worker_callback=worker,
            num_workers=num_workers,
            logger=context.log
        )
        
        # Add structured stats for BaseOperator reporting
        result["stats"] = {
            "files_transferred": result["summary"].get("source_items_count"),
            "total_bytes": result["summary"].get("total_bytes", 0),
        }
        
        # Add observations for data quality checks
        result["observations"] = {
            "files_scanned": result["summary"].get("source_items_count", result["summary"]["total_files"]),
            "files_uploaded": result["summary"].get("source_items_count", result["summary"]["total_files"]),
            "bytes_transferred": result["summary"]["total_bytes"],
        }
        
        return result
