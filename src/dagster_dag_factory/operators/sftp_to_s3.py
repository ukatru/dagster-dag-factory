from dagster_dag_factory.factory.base_operator import BaseOperator
from dagster_dag_factory.factory.registry import OperatorRegistry
from dagster_dag_factory.resources.sftp import SFTPResource
from dagster_dag_factory.resources.s3 import S3Resource
from dagster_dag_factory.models.file_info import FileInfo
import io
import re
import os

@OperatorRegistry.register(source="SFTP", target="S3")
class SftpToS3Operator(BaseOperator):
    def execute(self, context, source_config, target_config):
        sftp_resource: SFTPResource = getattr(context.resources, source_config.get("connection"))
        s3_resource: S3Resource = getattr(context.resources, target_config.get("connection"))
        
        # --- Source Configs ---
        # standardize on 'path' but fallback to 'sftp_path'
        sftp_path = source_config.get("path") or source_config.get("sftp_path")
        if not sftp_path:
             raise ValueError("Source must specify 'path'")
             
        pattern = source_config.get("pattern")
        recursive = source_config.get("recursive", False)
        predicate_expr = source_config.get("predicate") # e.g. "file_size > 1024"

        # --- Target Configs ---
        # standardize on 'key' vs 'prefix'
        # s3_key: Explicit template or exact key
        # s3_prefix: Directory prefix
        s3_key_template = target_config.get("key") or target_config.get("s3_key")
        s3_prefix = target_config.get("prefix") or target_config.get("s3_prefix")

        # --- Logic ---
        
        # 1. Define Predicate Function
        def predicate_fn(info: FileInfo) -> bool:
            if not predicate_expr:
                return True
            try:
                # Safe-ish eval context
                eval_context = {
                    "file_name": info.file_name,
                    "file_size": info.file_size,
                    "modified_ts": info.modified_ts,
                    "ext": info.ext,
                    "name": info.name
                }
                return eval(predicate_expr, {"__builtins__": {}}, eval_context)
            except Exception as e:
                context.log.warning(f"Predicate eval failed for {info.file_name}: {e}")
                return False

        # 2. Key Rendering Helper
        def render_s3_key(info: FileInfo) -> str:
            # Template context
            render_ctx = {
                "file_name": info.file_name,
                "name": info.name,
                "ext": info.ext,
                "date": context.run_id, # Simplified, could be execution date
                # Add more vars as needed
            }
            
            if s3_key_template:
                # Render template {{ var }}
                key = s3_key_template
                for k, v in render_ctx.items():
                    key = key.replace(f"{{{{ {k} }}}}", str(v))
                    key = key.replace(f"{{{{{k}}}}}", str(v))
                return key
            elif s3_prefix:
                return f"{s3_prefix.rstrip('/')}/{info.file_name}"
            else:
                return info.file_name

        transferred_files = []
        
        with sftp_resource.get_client() as sftp:
            context.log.info(f"Scanning {sftp_path} (Pattern: {pattern}, Recurse: {recursive})")
            
            files = sftp_resource.list_files(
                conn=sftp,
                path=sftp_path,
                pattern=pattern,
                recursive=recursive,
                predicate=predicate_fn
            )
            
            context.log.info(f"Found {len(files)} files to transfer.")
            
            for f_info in files:
                target_key = render_s3_key(f_info)
                context.log.info(f"Transferring {f_info.full_file_path} -> s3://{s3_resource.bucket_name}/{target_key}")
                
                # Streaming transfer
                # Optimization: Use sftp.getfo directly to s3.upload_fileobj if possible? 
                # Boto3 upload_fileobj accepts a file-like object.
                # However, sftp.getfo writes TO a file-like object. 
                # So we need a buffer or a pipe. Memory buffer is safest for small/medium files.
                # For huge files, multipart upload with pipes is complex. Sticking to memory buffer for now.
                
                mem_file = io.BytesIO()
                sftp.getfo(f_info.full_file_path, mem_file)
                mem_file.seek(0)
                
                s3_resource.get_client().put_object(
                    Bucket=s3_resource.bucket_name,
                    Key=target_key,
                    Body=mem_file.getvalue()
                )
                
                transferred_files.append({
                    "source": f_info.full_file_path,
                    "target": target_key,
                    "size": f_info.file_size
                })

        return {
            "transferred_count": len(transferred_files),
            "files": transferred_files
        }
