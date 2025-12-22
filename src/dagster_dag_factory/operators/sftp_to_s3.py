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
class SftpToS3Operator(BaseOperator):
    source_config_schema = SFTPConfig
    target_config_schema = S3Config
    
    def execute(self, context, source_config: SFTPConfig, target_config: S3Config):
        sftp_resource: SFTPResource = getattr(context.resources, source_config.connection)
        s3_resource: S3Resource = getattr(context.resources, target_config.connection)
        
        # --- Source Configs ---
        sftp_path = source_config.path
        pattern = source_config.pattern
        recursive = source_config.recursive
        predicate_expr = source_config.predicate

        # --- Target Configs ---
        # For S3 target, we use the path from S3Config
        s3_key_template = target_config.path  # In SftpToS3, 'path' can be a template
        # We don't have 'prefix' in S3Config anymore, but we can infer it or use path as prefix if it ends with /
        s3_prefix = target_config.path if target_config.path.endswith('/') else None

        # --- Logic ---
        
        # 1. Define Predicate Function
        def predicate_fn(info: FileInfo) -> bool:
            if not predicate_expr:
                return True
            try:
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
                return target_config.path # default use path as is

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
