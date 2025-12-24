from typing import Dict, Any
from dagster_dag_factory.operators.base_operator import BaseOperator
from dagster_dag_factory.factory.registry import OperatorRegistry
from dagster_dag_factory.resources.sftp import SFTPResource
from dagster_dag_factory.resources.s3 import S3Resource
from dagster_dag_factory.models.file_info import FileInfo
import io
import re
import os

from dagster_dag_factory.configs.sftp import SFTPConfig
from dagster_dag_factory.configs.s3 import S3Config
from dagster_dag_factory.configs.enums import S3Mode
from dagster_dag_factory.factory.helpers.dynamic import Dynamic

@OperatorRegistry.register(source="SFTP", target="S3")
class SftpS3Operator(BaseOperator):
    source_config_schema = SFTPConfig
    target_config_schema = S3Config
    
    def execute(self, context, source_config: SFTPConfig, target_config: S3Config, template_vars: Dict[str, Any]):
        import time
        sftp_resource: SFTPResource = getattr(context.resources, source_config.connection)
        s3_resource: S3Resource = getattr(context.resources, target_config.connection)
        
        # --- Source Configs ---
        sftp_path = source_config.path
        
        # Determine pattern: file_name overrides pattern if present
        if source_config.file_name:
            pattern = f"^{re.escape(source_config.file_name)}$"
            context.log.info(f"Using exact filename match: {source_config.file_name}")
        else:
            pattern = source_config.pattern

        recursive = source_config.recursive
        predicate_expr = source_config.predicate
        check_modifying = source_config.check_is_modifying
        
        # --- Target Configs ---
        # Niagara standard: key often acts as template or prefix
        s3_key_template = target_config.key 
        s3_prefix = target_config.prefix or (target_config.key if target_config.key and target_config.key.endswith('/') else None)

        # --- Logic ---
        
        # 1. Define Predicate Function
        def predicate_fn(info: FileInfo) -> bool:
            if not predicate_expr:
                return True
            try:
                from datetime import datetime, timedelta
                
                # Prepare evaluation context with Dynamic wrappers for dot-notation
                eval_context = {
                    "file_name": info.file_name,
                    "file_size": info.file_size,
                    "modified_ts": info.modified_ts,
                    "ext": info.ext,
                    "name": info.name,
                    "now_ts": time.time(),
                    # Modules for flexible logic
                    "datetime": datetime,
                    "timedelta": timedelta,
                    "re": re,
                    "info": info
                }
                
                # Wrap dictionaries from template_vars to allow dot-notation in eval()
                from dagster_dag_factory.factory.helpers.dynamic import Dynamic
                for k, v in template_vars.items():
                    if isinstance(v, dict):
                        eval_context[k] = Dynamic(v)
                    else:
                        eval_context[k] = v

                # Allow standard python builtins by not passing restricted __builtins__
                return eval(predicate_expr, eval_context)
            except Exception as e:
                context.log.warning(f"Predicate eval failed for {info.file_name}: {e}")
                return False

        # 2. Key Rendering Helper
        from dagster_dag_factory.factory.helpers.rendering import render_config_model
        
        def render_runtime_config(info: FileInfo) -> S3Config:
            # Prepare runtime context
            runtime_vars = template_vars.copy()
            
            # Legacy/Top-level 'item' removed for Niagara-style stricter routing
            # runtime_vars["item"] = info # REMOVED
            
            # Enable {{ source.item }} access for clearer enterprise referencing
            source_cfg = runtime_vars.get("source")
            if source_cfg is not None:
                if hasattr(source_cfg, "model_copy"):
                    # Pydantic model support
                    runtime_source = source_cfg.model_copy()
                    runtime_source.item = info # Pydantic extra="allow" enables this
                    runtime_vars["source"] = runtime_source
                elif isinstance(source_cfg, (dict, Dynamic)):
                    # Handle raw dict or already Dynamic objects
                    data = source_cfg.__dict__ if hasattr(source_cfg, "__dict__") else source_cfg
                    new_data = data.copy()
                    new_data["item"] = info
                    runtime_vars["source"] = Dynamic(new_data)
            
            # Re-render the target config with the runtime context
            return render_config_model(target_config, runtime_vars)

        transferred_files = []

        # 3. Define Callback for Transfer
        def transfer_callback(f_info: FileInfo, index: int) -> bool:
            try:
                # DYNAMICALLY RENDERED CONFIG for this specific file
                runtime_target_config = render_runtime_config(f_info)
                
                # If key is not provided and no suffix/prefix logic applied, fallback
                target_key = runtime_target_config.key
                if not target_key:
                    if s3_prefix:
                        target_key = f"{s3_prefix.rstrip('/')}/{f_info.file_name}"
                    else:
                        target_key = f_info.file_name

                context.log.info(f"Transferring {f_info.full_file_path} -> s3://{runtime_target_config.bucket_name}/{target_key}")
                
                if "FileInfo(" in target_key:
                    context.log.warning(f"Detected object repr in target key! Check your template for {{ source.item }}. Should probably be {{ source.item.file_name }}.")

                # Use Push Model (Smart Buffer + getfo)
                smart_buffer = s3_resource.create_smart_buffer(
                    bucket_name=runtime_target_config.bucket_name,
                    key=target_key,
                    multi_file=(runtime_target_config.mode == S3Mode.MULTI_FILE),
                    min_size=runtime_target_config.min_size,
                    compress_options=runtime_target_config.compress_options,
                    logger=context.log
                )
                
                try:
                    sftp.getfo(f_info.full_file_path, smart_buffer)
                finally:
                    # closing triggers upload of remainder and waiting for threads
                    results = smart_buffer.close()
                
                # Enrich results with source info
                for res in results:
                    res['source'] = f_info.full_file_path
                    transferred_files.append(res)

                return True
            except Exception as e:
                context.log.error(f"Failed to transfer {f_info.file_name}: {e}")
                raise e

        # 4. Execute Scan & Transfer
        start_time = time.time()
        with sftp_resource.get_client() as sftp:
            scan_msg = f"Scanning {sftp_path} (Pattern: {pattern}, Recurse: {recursive})"
            
            # Show the partition key if we are running in a partitioned context
            pk = template_vars.get("partition_key")
            if pk:
                scan_msg += f" [Partition: {pk}]"

            if predicate_expr:
                scan_msg += f" with Predicate: {predicate_expr}"
                
            context.log.info(scan_msg)

            # MACRO DEBUG: Try to resolve the 'threshold' if it's using a standard look-back pattern
            # This makes the "invisible" calculation visible in your logs!
            if predicate_expr and "fn.cron.prev" in predicate_expr:
                try:
                    import pendulum
                    from dagster_dag_factory.factory.helpers.dynamic import Dynamic
                    from datetime import datetime
                    
                    # Prepare a dummy eval context for pre-calculation
                    pre_eval_context = {
                        "datetime": datetime,
                        "pendulum": pendulum,
                        "now_ts": time.time(),
                    }
                    for k, v in template_vars.items():
                        if isinstance(v, dict):
                            pre_eval_context[k] = Dynamic(v)
                        else:
                            pre_eval_context[k] = v
                    
                    # Try to extract and evaluate the Right-Hand-Side after '>=' or '>'
                    if ">=" in predicate_expr:
                        rhs = predicate_expr.split(">=")[-1].strip()
                        threshold_val = eval(rhs, pre_eval_context)
                        if isinstance(threshold_val, (int, float)):
                            dt_str = pendulum.from_timestamp(threshold_val).to_datetime_string()
                            context.log.info(f"Predicate Threshold (Resolved): {threshold_val} ({dt_str} UTC)")
                except Exception:
                    pass
            
            sftp_resource.list_files(
                conn=sftp,
                path=sftp_path,
                pattern=pattern,
                recursive=recursive,
                check_is_modifing=check_modifying,
                predicate=predicate_fn,
                on_each=transfer_callback
            )
            
            duration = time.time() - start_time
            from dagster_dag_factory.factory.helpers.stats import generate_transfer_stats
            summary = generate_transfer_stats(transferred_files, duration)
            
            context.log.info(f"Transfer complete. {summary['total_files']} files ({summary['total_size_human']}) processed in {summary['duration_seconds']}s.")

        return {
            "summary": summary,
            "observations": {
                "files_scanned": summary["total_files"],
                "files_uploaded": summary["total_files"],
                "bytes_transferred": summary["total_bytes"]
            },
            "files": transferred_files
        }
