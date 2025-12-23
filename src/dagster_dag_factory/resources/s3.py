from typing import Optional, Any, List, ClassVar, Union, IO, Callable
import boto3
import os
import uuid
import sys
import threading
import re
import time
from pydantic import Field
import io
from dagster_dag_factory.resources.base import BaseConfigurableResource
from dagster_dag_factory.configs.compression import CompressConfig
from dagster_dag_factory.models.s3_info import S3Info

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
            # If we wanted to log percentage details, we could here

class SafeSplitBuffer:
    """
    A smart buffer that accepts writes (from sftp.getfo), performs safe newline splitting,
    and uploads chunks to S3 in parallel background threads.
    """
    def __init__(
        self,
        s3_resource: 'S3Resource',
        bucket_name: str,
        key: str,
        multi_file: bool = False,
        min_size: int = 5,
        compress_options: Optional[CompressConfig] = None,
        logger: Any = None,
        max_workers: int = 5
    ):
        self.s3_resource = s3_resource
        self.bucket_name = bucket_name
        self.key = key
        self.multi_file = multi_file
        self.chunk_size = min_size * 1024 * 1024
        self.compress_options = compress_options
        self.logger = logger
        
        # State
        self.buffer = b""
        self.header = b""
        self.part_num = 1
        self.transferred_files = []
        
        # Threading
        import concurrent.futures
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.futures = []
        self.lock = threading.Lock() # Protect transferred_files
        
        self.s3_client = s3_resource.get_client()

    def write(self, data: bytes):
        if not data:
            return
            
        self.buffer += data
        
        if self.multi_file:
            # Check if buffer is big enough to split
            while len(self.buffer) >= self.chunk_size:
                # Strategy: Look at the first 'chunk_size' bytes. 
                # Find last newline there.
                
                window = self.buffer[:self.chunk_size]
                last_newline = window.rfind(b'\n')
                
                if last_newline == -1:
                    # No newline in this chunk.
                    # We might accumulate unless buffer is HUGE
                    if len(self.buffer) > 2 * self.chunk_size:
                        if self.logger:
                             self.logger.warning(f"No newline found in {len(self.buffer)} bytes! Force splitting risks data.")
                    return # Wait for more data
                
                # We found a split point!
                chunk_data = self.buffer[:last_newline + 1]
                self.buffer = self.buffer[last_newline + 1:] # Carry over the rest
                
                self._submit_chunk(chunk_data)

    def _submit_chunk(self, data: bytes):
        # Capture header if first part
        if self.part_num == 1 and not self.header:
            first_newline = data.find(b'\n')
            if first_newline != -1:
                self.header = data[:first_newline + 1]
                if self.logger:
                    self.logger.info(f"Detected header: {self.header.strip()}")

        # Prepend header if needed
        data_to_upload = data
        if self.part_num > 1 and self.header:
            data_to_upload = self.header + data
            
        # Determine Key
        if self.multi_file:
            name, ext = os.path.splitext(self.key)
            part_key = f"{name}_part_{self.part_num}{ext}"
        else:
            part_key = self.key

        # Submit to thread
        future = self.executor.submit(
            self._upload_worker, 
            data_to_upload, 
            part_key, 
            self.part_num
        )
        self.futures.append(future)
        self.part_num += 1

    def _upload_worker(self, data: bytes, key: str, part_num: int):
        try:
            final_data = data
            final_key = key
            
            # Compress
            if self.compress_options and self.compress_options.action == "COMPRESS":
                if self.compress_options.type == "GUNZIP":
                    import gzip
                    gzip_buffer = io.BytesIO()
                    with gzip.GzipFile(fileobj=gzip_buffer, mode='wb') as gz:
                        gz.write(data)
                    gzip_buffer.seek(0)
                    final_data = gzip_buffer.getvalue()
                    if not final_key.endswith(".gz"):
                        final_key += ".gz"
            
            # Upload
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=final_key,
                Body=final_data
            )
            
            with self.lock:
                self.transferred_files.append({
                    "target": final_key,
                    "size": len(final_data),
                    "part": part_num
                })
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to upload part {part_num}: {e}")
            raise e

    def close(self):
        """
        Finalize: upload remaining buffer and wait for all threads.
        """
        if self.buffer:
            self._submit_chunk(self.buffer)
            self.buffer = b""
            
        # Wait for threads
        import concurrent.futures
        for future in concurrent.futures.as_completed(self.futures):
            future.result()
        
        self.executor.shutdown(wait=True)
        return self.transferred_files

    def flush(self):
        pass
    
    def tell(self):
        return 0

class S3Resource(BaseConfigurableResource):
    """
    Dagster resource for S3 operations with enterprise authentication support.
    Supports explicit keys, profiles, and EKS Web Identity (IRSA).
    """
    mask_fields: ClassVar[List[str]] = BaseConfigurableResource.mask_fields + ["access_key", "secret_key", "session_token"]
    
    
    
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
    endpoint_url: Optional[str] = Field(default="http://localhost:9000", description="Custom endpoint URL (e.g. for MinIO)")
    profile_name: Optional[str] = Field(default=None, description="AWS Profile name")
    use_unsigned_session: bool = Field(default=False, description="Use unsigned session")
    verify: bool = Field(default=False, description="Verify SSL certificates")

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

    def write_csv(self, bucket_name: str, key: str, data: list, headers: list = None) -> None:
        if not data:
            return
        import pandas as pd
        df = pd.DataFrame(data)
        if headers:
            df = df[headers]
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        self.get_client().put_object(
            Bucket=bucket_name,
            Key=key,
            Body=csv_buffer.getvalue()
        )

    def write_parquet(self, bucket_name: str, key: str, data: list) -> None:
        if not data:
            return
        import pandas as pd
        df = pd.DataFrame(data)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        self.get_client().put_object(
            Bucket=bucket_name,
            Key=key,
            Body=parquet_buffer.getvalue()
        )

    def read_csv_sample(self, bucket_name: str, key: str, nrows: int = 10, delimiter: str = ",") -> Any:
        import pandas as pd
        response = self.get_client().get_object(Bucket=bucket_name, Key=key, Range='bytes=0-10240')
        return pd.read_csv(response["Body"], nrows=nrows, sep=delimiter)

    def create_smart_buffer(
        self,
        bucket_name: str,
        key: str,
        multi_file: bool = False,
        min_size: int = 5,
        compress_options: Optional[CompressConfig] = None,
        logger: Any = None,
        max_workers: int = 5
    ):
        """
        Creates a SafeSplitBuffer that behaves like a file object for writing.
        Best used with sftp.getfo() for high-performance 'push' transfers.
        """
        return SafeSplitBuffer(
            s3_resource=self,
            bucket_name=bucket_name,
            key=key,
            multi_file=multi_file,
            min_size=min_size,
            compress_options=compress_options,
            logger=logger,
            max_workers=max_workers
        )

    def list_files(
        self,
        bucket_name: Optional[str] = None,
        prefix: str = "",
        pattern: str = None,
        delimiter: str = None,
        check_is_modifing: bool = False,
        predicate: Union[str, Callable[[S3Info], bool]] = None,
        on_each: Callable[[S3Info, int], bool] = None
    ) -> List[S3Info]:
        """
        List objects in an S3 bucket with filtering and callbacks.
        
        :param bucket_name: Bucket to list from (defaults to resource default)
        :param prefix: Key prefix
        :param pattern: Regex pattern to match keys
        :param delimiter: Delimiter for hierarchy
        :param check_is_modifing: If True, skips objects modified in the last 60s
        :param predicate: Python expression (str) or callable for filtering
        :param on_each: Callback for each matched object. If returns False, stops listing.
        """
        bucket = bucket_name
        if not bucket:
            raise ValueError("bucket_name must be provided for list_files")
        client = self.get_client()
        
        regex = re.compile(pattern) if pattern else None
        infos: List[S3Info] = []
        
        paginator = client.get_paginator('list_objects_v2')
        paginate_kwargs = {'Bucket': bucket, 'Prefix': prefix}
        if delimiter:
            paginate_kwargs['Delimiter'] = delimiter
            
        for page in paginator.paginate(**paginate_kwargs):
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                key = obj['Key']
                
                # Regex match
                if regex and not regex.match(key):
                    continue
                    
                info = S3Info(
                    bucket_name=bucket,
                    key=key,
                    prefix=prefix,
                    size=obj['Size'],
                    modified_dt=obj['LastModified'],
                    storage_class=obj.get('StorageClass')
                )
                
                # Check if modifying (min 60s idle)
                if check_is_modifing:
                    if (time.time() - info.modified_ts) < 60:
                        continue
                
                # Predicate filter
                if predicate:
                    if isinstance(predicate, str):
                        # Evaluate string predicate with info context
                        if not eval(predicate, {"info": info, "re": re}):
                            continue
                    elif callable(predicate):
                        if not predicate(info):
                            continue
                
                # Callback and collection
                index = len(infos) + 1
                if on_each:
                    should_continue = on_each(info, index)
                    infos.append(info)
                    if should_continue is False:
                        return infos
                else:
                    infos.append(info)
                    
        return infos
