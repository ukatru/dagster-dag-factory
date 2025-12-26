"""
S3 to S3 Operator

Transfers/Copies files between S3 locations with parallel streaming.
Supports cross-bucket and same-bucket copies.
"""
import re
from typing import Any, Dict

from dagster_dag_factory.operators.base_operator import BaseOperator
from dagster_dag_factory.utils.streaming import ProcessorItem, execute_streaming
from dagster_dag_factory.factory.helpers.rendering import render_config_model
from dagster_dag_factory.factory.registry import OperatorRegistry
from dagster_dag_factory.configs.s3 import S3Config


@OperatorRegistry.register(source="S3", target="S3")
class S3ToS3Operator(BaseOperator):
    """
    S3 to S3 transfer operator.
    
    Supports parallel copies using the universal streaming pattern.
    Useful for moving data between buckets (e.g., Raw -> Processed) 
    while preserving or transforming metadata.
    """
    source_config_schema = S3Config
    target_config_schema = S3Config

    def _execute(
        self,
        context,
        source_config: S3Config,
        target_config: S3Config,
        template_vars: Dict[str, Any],
        **kwargs
    ) -> Dict[str, Any]:
        source_res = kwargs.get("source_resource")
        target_res = kwargs.get("target_resource")

        # Extract source params
        bucket = source_config.bucket_name
        prefix = source_config.key or source_config.prefix or ""
        pattern = source_config.pattern
        num_workers = getattr(source_config, 'max_workers', 5)

        # Render runtime config for each file
        def render_runtime_config(item_info):
            runtime_vars = template_vars.copy()
            runtime_vars["source"] = {"item": item_info, "bucket": bucket}
            return render_config_model(target_config, runtime_vars)

        # Producer: Scans source S3
        def producer(processor):
            def on_each_file(info, index):
                processor.put(ProcessorItem(name=info.key, data=info))

            context.log.info(f"Scanning S3 source: s3://{bucket}/{prefix}")
            source_res.list_files(
                bucket_name=bucket,
                prefix=prefix,
                pattern=pattern,
                check_is_modifying=source_config.check_is_modifying,
                predicate=source_config.predicate,
                on_each=on_each_file
            )

        # Worker: Performs the copy
        def worker(processor, item, index):
            info = item.data
            
            # Resolve target config for THIS specific file
            runtime_target_config = render_runtime_config(info)
            
            # Determine target key
            # If target has a specific key, use it. Otherwise, use prefix + filename.
            target_key = runtime_target_config.key or (
                f"{runtime_target_config.prefix.rstrip('/')}/{info.object_name}" 
                if runtime_target_config.prefix 
                else info.object_name
            )

            context.log.info(f"Copying s3://{bucket}/{info.key} -> s3://{runtime_target_config.bucket_name}/{target_key}")

            # Use boto3's copy_object through the client
            target_client = target_res.get_client()
            copy_source = {'Bucket': bucket, 'Key': info.key}
            
            target_client.copy(
                CopySource=copy_source,
                Bucket=runtime_target_config.bucket_name,
                Key=target_key
            )

            return [{
                "source": info.key,
                "target": target_key,
                "size": info.size
            }]

        # Execute
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

        return result
