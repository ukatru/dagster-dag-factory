from typing import Optional, Any, List, ClassVar, Union, IO
import boto3
import os
import uuid
import sys
import threading
from pydantic import Field
import io
from .base import BaseConfigurableResource
from ..configs.compression import CompressConfig

class ProgressPercentage(object):
    def __init__(self, filename: str, size: float, logger=None):
        self._filename = filename
        self._size = size
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self._logger = logger

    def __call__(self, bytes_amount):
        # To simplify we just lock and update
        with self._lock:
            self._seen_so_far += bytes_amount
            # If we wanted to log percentage:
            # if self._size > 0:
            #     percentage = (self._seen_so_far / self._size) * 100
            #     if self._logger:
            #         self._logger.debug(f"{self._filename}  {self._seen_so_far} / {self._size}  ({percentage:.2f}%)")
            #     else:
            #         sys.stdout.write(
            #             "\r%s  %s / %s  (%.2f%%)" % (
            #                 self._filename, self._seen_so_far, self._size,
            #                 percentage))
            #         sys.stdout.flush()

class S3Resource(BaseConfigurableResource):
    """
    Dagster resource for S3 operations with enterprise authentication support.
    Supports explicit keys, profiles, and EKS Web Identity (IRSA).
    """
    mask_fields: ClassVar[List[str]] = BaseConfigurableResource.mask_fields + ["access_key", "secret_key", "session_token"]
    
    bucket_name: str = Field(description="Default bucket name")
    
    # Auth: Explicit Keys
    access_key: Optional[str] = Field(default=None, description="AWS Access Key ID")
    secret_key: Optional[str] = Field(default=None, description="AWS Secret Access Key")
    session_token: Optional[str] = Field(default=None, description="AWS Session Token")
    
    # Auth: Role Assumption & IRSA
    assume_role_arn: Optional[str] = Field(default=None, description="ARN of role to assume")
    aws_web_identity_token_file: Optional[str] = Field(default=None, description="Path to web identity token file (IRSA)")
    assume_role_session_name: Optional[str] = Field(default=None, description="Session name for assumed role")
    external_id: Optional[str] = Field(default=None, description="External ID for cross-account role assumption")
    
    # Configuration
    region_name: str = Field(default="us-east-1", description="AWS Region")
    endpoint_url: Optional[str] = Field(default=None, description="Custom endpoint URL (e.g. for MinIO)")
    profile_name: Optional[str] = Field(default=None, description="AWS Profile name")
    use_unsigned_session: bool = Field(default=False, description="Use unsigned session")
    verify: bool = Field(default=True, description="Verify SSL certificates")

    def get_session(self) -> boto3.Session:
        """
        Creates a boto3 Session with priority:
        1. Explicit Keys
        2. Profile
        3. Web Identity (IRSA) / Role Assumption
        4. Default Chain (Env vars, Instance Profile)
        """
        session_kwargs = {
            "region_name": self.region_name,
            "profile_name": self.profile_name
        }
        
        if self.access_key and self.secret_key:
            session_kwargs.update({
                "aws_access_key_id": self.access_key,
                "aws_secret_access_key": self.secret_key,
                "aws_session_token": self.session_token
            })
            
        session = boto3.Session(**session_kwargs)

        if self.assume_role_arn:
            sts_client = session.client('sts')
            role_session_name = self.assume_role_session_name or f"dagster-{uuid.uuid4()}"
            
            web_identity_token = None
            if self.aws_web_identity_token_file:
                with open(self.aws_web_identity_token_file, 'r') as f:
                    web_identity_token = f.read()
            elif os.environ.get("AWS_WEB_IDENTITY_TOKEN_FILE") and not self.access_key:
                with open(os.environ["AWS_WEB_IDENTITY_TOKEN_FILE"], 'r') as f:
                    web_identity_token = f.read()

            if web_identity_token:
                sts_response = sts_client.assume_role_with_web_identity(
                    RoleArn=self.assume_role_arn,
                    RoleSessionName=role_session_name,
                    WebIdentityToken=web_identity_token
                )
            else:
                assume_kwargs = {
                    "RoleArn": self.assume_role_arn,
                    "RoleSessionName": role_session_name
                }
                if self.external_id:
                    assume_kwargs["ExternalId"] = self.external_id
                    
                sts_response = sts_client.assume_role(**assume_kwargs)

            credentials = sts_response['Credentials']
            return boto3.Session(
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken'],
                region_name=self.region_name
            )

        return session

    def get_client(self) -> Any:
        session = self.get_session()
        config = None
        if self.use_unsigned_session:
            from botocore import UNSIGNED
            from botocore.config import Config
            config = Config(signature_version=UNSIGNED)

        return session.client(
            "s3",
            endpoint_url=self.endpoint_url,
            verify=self.verify,
            config=config
        )

    def write_csv(self, key: str, data: list, headers: list = None) -> None:
        if not data:
            return
        import pandas as pd
        df = pd.DataFrame(data)
        if headers:
            df = df[headers]
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        self.get_client().put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=csv_buffer.getvalue()
        )

    def write_parquet(self, key: str, data: list) -> None:
        if not data:
            return
        import pandas as pd
        df = pd.DataFrame(data)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        self.get_client().put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=parquet_buffer.getvalue()
        )

    def read_csv_sample(self, key: str, nrows: int = 10, delimiter: str = ",") -> Any:
        import pandas as pd
        response = self.get_client().get_object(Bucket=self.bucket_name, Key=key, Range='bytes=0-10240')
        return pd.read_csv(response["Body"], nrows=nrows, sep=delimiter)

    def upload_stream_chunks(
        self, 
        stream: IO, 
        key: str, 
        multi_file: bool = False, 
        chunk_size_mb: int = 100,
        compress_options: Optional[CompressConfig] = None,
        logger: Any = None,
        max_workers: int = 5
    ) -> List[dict]:
        """
        Reads from stream in chunks and uploads to S3 using threads.
        Handles:
        - Multi-file splitting with SAFE newline detection
        - Header retention (prepends header to all splits)
        - Compression
        - Parallel Uploads
        """
        import concurrent.futures
        
        s3_client = self.get_client()
        transferred_files = []
        lock = threading.Lock() # Protect transferred_files append
        
        def upload_data(data: bytes, part_key: str, part_num: int):
            try:
                final_data = data
                final_key = part_key
                
                # Compress
                if compress_options and compress_options.action == "COMPRESS":
                    if compress_options.type == "GUNZIP":
                        import gzip
                        gzip_buffer = io.BytesIO()
                        with gzip.GzipFile(fileobj=gzip_buffer, mode='wb') as gz:
                            gz.write(data)
                        gzip_buffer.seek(0)
                        final_data = gzip_buffer.getvalue()
                        if not final_key.endswith(".gz"):
                            final_key += ".gz"
                
                # Upload
                s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=final_key,
                    Body=final_data
                )
                
                # Record result
                with lock:
                    transferred_files.append({
                        "target": final_key,
                        "size": len(final_data),
                        "part": part_num
                    })
                return True
            except Exception as e:
                if logger:
                     logger.error(f"Failed to upload part {part_num} ({part_key}): {e}")
                raise e

        if multi_file:
            chunk_size = chunk_size_mb * 1024 * 1024
            if logger:
                logger.info(f"Splitting stream into {chunk_size_mb}MB chunks (safe split with header). Parallelism: {max_workers}")
            
            part_num = 1
            leftover = b""
            header = b""
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                
                while True:
                    read_data = stream.read(chunk_size)
                    
                    if not read_data:
                        # EOF
                        if leftover:
                            name, ext = os.path.splitext(key)
                            part_key = f"{name}_part_{part_num}{ext}"
                            
                            final_payload = (header + leftover) if part_num > 1 else leftover
                            
                            # Submit final part
                            futures.append(executor.submit(upload_data, final_payload, part_key, part_num))
                        break
                    
                    current_block = leftover + read_data
                    
                    # Detect Header
                    if part_num == 1 and not header:
                        first_newline = current_block.find(b'\n')
                        if first_newline != -1:
                            header = current_block[:first_newline + 1]
                            if logger:
                                logger.info(f"Detected header: {header.strip()}")
                    
                    last_newline = current_block.rfind(b'\n')
                    
                    if last_newline == -1:
                        if logger:
                             logger.warning(f"No newline found in chunk {part_num}. Accumulating...")
                        leftover = current_block
                        continue
                    
                    data_to_upload = current_block[:last_newline + 1]
                    leftover = current_block[last_newline + 1:]
                    
                    if part_num > 1 and header:
                        data_to_upload = header + data_to_upload
                    
                    name, ext = os.path.splitext(key)
                    part_key = f"{name}_part_{part_num}{ext}"
                    
                    # Submit task
                    futures.append(executor.submit(upload_data, data_to_upload, part_key, part_num))
                    part_num += 1
                
                # Wait for all uploads to complete
                for future in concurrent.futures.as_completed(futures):
                    future.result() # Raise exceptions if any
        else:
            # Single File - no threading needed for single part? 
            # Or use multipart upload? For now keeping simple.
            content = stream.read()
            final_key = key
            # Re-use upload_data logic but synchronous is fine for single file
            upload_data(content, final_key, 1)

        return transferred_files
