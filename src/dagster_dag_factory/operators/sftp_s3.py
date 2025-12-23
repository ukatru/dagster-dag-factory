from dagster_dag_factory.factory.base_operator import BaseOperator
from dagster_dag_factory.factory.registry import OperatorRegistry
from dagster_dag_factory.resources.sftp import SFTPResource
from dagster_dag_factory.resources.s3 import S3Resource
from dagster_dag_factory.models.file_info import FileInfo
import io
import re
import os

from ..configs.sftp import SFTPConfig
from ..configs.s3 import S3Config

@OperatorRegistry.register(source="SFTP", target="S3")
class SftpS3Operator(BaseOperator):
    source_config_schema = SFTPConfig
    target_config_schema = S3Config
    
    def execute(self, context, source_config: SFTPConfig, target_config: S3Config):
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
        compress_cfg = target_config.compress_options
        s3_key_template = target_config.path 
        s3_prefix = target_config.path if target_config.path.endswith('/') else None

        # --- Logic ---
        
        # 1. Define Predicate Function
        def predicate_fn(info: FileInfo) -> bool:
            if not predicate_expr:
                return True
            try:
                from datetime import datetime, timedelta
                
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
                # Allow standard python builtins by not passing restricted __builtins__
                return eval(predicate_expr, eval_context)
            except Exception as e:
                context.log.warning(f"Predicate eval failed for {info.file_name}: {e}")
                return False

        # 2. Key Rendering Helper
        def render_s3_key(info: FileInfo) -> str:
            render_ctx = {
                "file_name": info.file_name,
                "name": info.name,
                "ext": info.ext,
                "date": context.run_id,
            }
            
            if s3_key_template and ("{{" in s3_key_template):
                key = s3_key_template
                for k, v in render_ctx.items():
                    key = key.replace(f"{{{{ {k} }}}}", str(v))
                    key = key.replace(f"{{{{{k}}}}}", str(v))
                return key
            elif s3_prefix:
                return f"{s3_prefix.rstrip('/')}/{info.file_name}"
            else:
                return target_config.path 

        transferred_files = []

        # 3. Define Callback for Transfer
        def transfer_callback(f_info: FileInfo, index: int) -> bool:
            try:
                base_target_key = render_s3_key(f_info)
                context.log.info(f"Processing {f_info.full_file_path} -> s3://{s3_resource.bucket_name}/{base_target_key}")

                # Use Push Model (Smart Buffer + getfo)
                smart_buffer = s3_resource.create_smart_buffer(
                    key=base_target_key,
                    multi_file=target_config.multi_file,
                    chunk_size_mb=target_config.chunk_size_mb,
                    compress_options=target_config.compress_options,
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

        with sftp_resource.get_client() as sftp:
            context.log.info(f"Scanning {sftp_path} (Pattern: {pattern}, Recurse: {recursive})")
            
            # Pass 'sftp' client implicitly to callback via closure? 
            # Yes, 'sftp' variable from context manager is available in 'transfer_callback' scope.
            
            sftp_resource.list_files(
                conn=sftp,
                path=sftp_path,
                pattern=pattern,
                recursive=recursive,
                check_is_modifing=check_modifying,
                predicate=predicate_fn,
                on_each=transfer_callback
            )
            
            context.log.info(f"Transfer complete. {len(transferred_files)} files processed.")

        return {
            "transferred_count": len(transferred_files),
            "files": transferred_files
        }
