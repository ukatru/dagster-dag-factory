from typing import Optional, Any, List, Union, Callable
import os
import threading
import re
import time
from pydantic import Field
import io
from dagster_dag_factory.configs.compression import CompressConfig
from dagster_dag_factory.resources.aws import AWSResource
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
        s3_resource: "S3Resource",
        bucket_name: str,
        key: str,
        multi_file: bool = False,
        min_size: int = 5,
        compress_options: Optional[CompressConfig] = None,
        logger: Any = None,
        max_workers: int = 5,
        total_size: Optional[int] = None,
    ):
        self.s3_resource = s3_resource
        self.bucket_name = bucket_name
        self.key = key
        self.multi_file = multi_file
        self.chunk_size = min_size * 1024 * 1024
        self.compress_options = compress_options
        self.logger = logger
        self.total_size = total_size

        # State
        self.buffer = b""
        self.header = b""
        self.part_num = 1
        self.transferred_files = []
        self._uploaded_bytes = 0
        self._last_logged_pct = 0

        # Threading
        import concurrent.futures

        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.futures = []
        import threading

        self.lock = threading.Lock()
        self.s3_client = s3_resource.get_client()
        self.upload_id = None
        self.etags = []
        self._aborted = False

        if not self.multi_file:
            # Initialize multipart upload for single-file mode
            try:
                # Add .gz extension if compressing
                upload_key = self.key
                if self.compress_options and self.compress_options.action == "COMPRESS":
                    if (
                        self.compress_options.type == "GUNZIP"
                        and not upload_key.endswith(".gz")
                    ):
                        upload_key += ".gz"

                response = self.s3_client.create_multipart_upload(
                    Bucket=self.bucket_name, Key=upload_key
                )
                self.upload_id = response["UploadId"]
                self.key = upload_key  # Update key to include .gz if applied
                if self.logger:
                    self.logger.info(
                        f"Initialized Multipart Upload for {self.key} (ID: {self.upload_id})"
                    )
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Failed to create multipart upload: {e}")
                raise

    def write(self, data: bytes):
        if not data:
            return

        if self.logger and len(self.buffer) == 0:
            # Log first write to show data is flowing at DEBUG level
            self.logger.debug(f"Smart buffer receiving data ({len(data)} bytes)...")

        self.buffer += data

        if self.multi_file:
            # Multi-file Mode: Split at newlines to ensure valid CSV parts
            while len(self.buffer) >= self.chunk_size:
                window = self.buffer[: self.chunk_size]
                last_newline = window.rfind(b"\n")

                if last_newline == -1:
                    # No newline found? If buffer is much bigger than chunk, we must force a split
                    # to prevent memory blow-up, though this risks breaking a record.
                    if len(self.buffer) > 2 * self.chunk_size:
                        if self.logger:
                            self.logger.warning(
                                f"No newline found at {self.chunk_size}. Force splitting."
                            )
                        chunk_data = self.buffer[: self.chunk_size]
                        self.buffer = self.buffer[self.chunk_size :]
                        self._submit_chunk(chunk_data)
                    return

                chunk_data = self.buffer[: last_newline + 1]
                self.buffer = self.buffer[last_newline + 1 :]
                self._submit_chunk(chunk_data)
        else:
            # Single-file Multipart Mode: Just split at the chunk boundary (new-lines don't matter)
            while len(self.buffer) >= self.chunk_size:
                chunk_data = self.buffer[: self.chunk_size]
                self.buffer = self.buffer[self.chunk_size :]
                self._submit_chunk(chunk_data)

    def _submit_chunk(self, data: bytes):
        # Capture header if first part
        if self.part_num == 1 and not self.header:
            first_newline = data.find(b"\n")
            if first_newline != -1:
                self.header = data[: first_newline + 1]
                if self.logger:
                    self.logger.debug(f"Detected header: {self.header.strip()}")

        # Prepend header if in multi_file mode (where each part is a new file)
        # For multipart single-file, the header is already in the stream from part 1.
        data_to_upload = data
        if self.multi_file and self.part_num > 1 and self.header:
            data_to_upload = self.header + data

        # Determine Key
        if self.multi_file:
            name, ext = os.path.splitext(self.key)
            part_key = f"{name}_part_{self.part_num}{ext}"
        else:
            part_key = self.key

        # Submit to thread
        future = self.executor.submit(
            self._upload_worker, data_to_upload, part_key, self.part_num
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
                    with gzip.GzipFile(fileobj=gzip_buffer, mode="wb") as gz:
                        gz.write(data)
                    gzip_buffer.seek(0)
                    final_data = gzip_buffer.getvalue()
                    if not final_key.endswith(".gz"):
                        final_key += ".gz"

            # Upload
            if self.multi_file:
                if self.logger:
                    self.logger.debug(f"[{final_key}] Uploading part {part_num} ({len(final_data) / (1024*1024):.2f} MB)")
                self.s3_client.put_object(
                    Bucket=self.bucket_name, Key=final_key, Body=final_data
                )
                if self.logger:
                    self.logger.debug(f"[{final_key}] Completed upload of part {part_num}")
            else:
                if self.logger:
                    self.logger.debug(f"[{self.key}] Uploading part {part_num} ({len(final_data) / (1024*1024):.2f} MB)")
                resp = self.s3_client.upload_part(
                    Bucket=self.bucket_name,
                    Key=self.key,
                    PartNumber=part_num,
                    UploadId=self.upload_id,
                    Body=final_data,
                )
                with self.lock:
                    self.etags.append({"PartNumber": part_num, "ETag": resp["ETag"]})
                if self.logger:
                    self.logger.debug(f"[{self.key}] Completed upload of part {part_num}")

            with self.lock:
                self.transferred_files.append(
                    {"target": final_key, "size": len(final_data), "part": part_num}
                )
                self._uploaded_bytes += len(data) # Use original data size for progress
                
                # Milestone Logging (every 10%)
                if self.total_size and self.total_size > 0:
                    current_pct = int((self._uploaded_bytes / self.total_size) * 100)
                    if current_pct >= self._last_logged_pct + 10:
                        self._last_logged_pct = (current_pct // 10) * 100 # Reset milestone
                        # Just used as floor for int division
                        self._last_logged_pct = (current_pct // 10) * 10
                        
                        est_total_parts = max(part_num, int(self.total_size / self.chunk_size))
                        from dagster_dag_factory.factory.utils.logging import convert_size
                        self.logger.info(
                            f"[{self.key}] Progress: {current_pct}% | "
                            f"Part {part_num}/{est_total_parts} | "
                            f"{convert_size(self._uploaded_bytes)} / {convert_size(self.total_size)}"
                        )

        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to upload part {part_num}: {e}")
            raise e

    def abort(self):
        """Signals that the transfer should be discarded."""
        self._aborted = True
        if self.upload_id:
            try:
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket_name, Key=self.key, UploadId=self.upload_id
                )
                if self.logger:
                    self.logger.info(f"Aborted Multipart Upload for {self.key}")
            except Exception:
                pass

    def close(self):
        """
        Finalize: upload remaining buffer and wait for all threads.
        """
        if self._aborted:
            self.executor.shutdown(wait=True)
            return []

        if self.buffer:
            self._submit_chunk(self.buffer)
            self.buffer = b""

        # Wait for threads
        import concurrent.futures

        for future in concurrent.futures.as_completed(self.futures):
            future.result()

        if self.upload_id:
            # Complete multipart upload
            try:
                if not self.etags:
                    # S3 cannot complete multipart with 0 parts.
                    # Abort and just put an empty object if no data was ever sent.
                    if self.logger:
                        self.logger.info(
                            f"No parts generated for {self.key}. Completing as empty object."
                        )
                    try:
                        # Use abort() helper but don't log it as an error-case abort
                        self.s3_client.abort_multipart_upload(
                            Bucket=self.bucket_name,
                            Key=self.key,
                            UploadId=self.upload_id,
                        )
                    except Exception:
                        pass

                    self.s3_client.put_object(
                        Bucket=self.bucket_name, Key=self.key, Body=b""
                    )
                else:
                    # Sorted ETags are required by S3
                    sorted_etags = sorted(self.etags, key=lambda x: x["PartNumber"])
                    self.s3_client.complete_multipart_upload(
                        Bucket=self.bucket_name,
                        Key=self.key,
                        MultipartUpload={"Parts": sorted_etags},
                        UploadId=self.upload_id,
                    )
                    if self.logger:
                        from dagster_dag_factory.factory.utils.logging import convert_size
                        self.logger.info(
                            f"Completed Multipart Upload for {self.key} "
                            f"(Total Parts: {len(self.transferred_files)}, Size: {convert_size(self._uploaded_bytes)})"
                        )
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Failed to complete multipart upload: {e}")
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket_name, Key=self.key, UploadId=self.upload_id
                )
                raise

        self.executor.shutdown(wait=True)
        return self.transferred_files

    def flush(self):
        pass

    def tell(self):
        return 0


class S3Resource(AWSResource):
    """
    Dagster resource for S3 operations, extending AWSBaseResource for unified authentication.
    Supports high-performance streaming, partitioning, and automated cleanup.
    """

    use_unsigned_session: bool = Field(
        default=False, description="Use unsigned session"
    )

    def get_client(self, **kwargs) -> Any:
        # Override to ensure we use S3 by default and handle UNSIGNED session
        config = None
        if self.use_unsigned_session:
            from botocore import UNSIGNED
            from botocore.config import Config

            config = Config(signature_version=UNSIGNED)

        return super().get_client("s3", config=config, **kwargs)

    def write_csv(
        self, bucket_name: str, key: str, data: list, headers: list = None
    ) -> None:
        if not data:
            return
        import pandas as pd

        df = pd.DataFrame(data)
        if headers:
            df = df[headers]
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        self.get_client().put_object(
            Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue()
        )

    def write_parquet(self, bucket_name: str, key: str, data: list) -> None:
        if not data:
            return
        import pandas as pd

        df = pd.DataFrame(data)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        self.get_client().put_object(
            Bucket=bucket_name, Key=key, Body=parquet_buffer.getvalue()
        )

    def read_csv_sample(
        self, bucket_name: str, key: str, nrows: int = 10, delimiter: str = ","
    ) -> Any:
        import pandas as pd

        response = self.get_client().get_object(
            Bucket=bucket_name, Key=key, Range="bytes=0-10240"
        )
        return pd.read_csv(response["Body"], nrows=nrows, sep=delimiter)

    def create_smart_buffer(
        self,
        bucket_name: str,
        key: str,
        multi_file: bool = False,
        min_size: int = 5,
        compress_options: Optional[CompressConfig] = None,
        logger: Any = None,
        max_workers: int = 5,
        total_size: Optional[int] = None,
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
            max_workers=max_workers,
            total_size=total_size,
        )

    def list_files(
        self,
        bucket_name: Optional[str] = None,
        prefix: str = "",
        pattern: str = None,
        delimiter: str = None,
        check_is_modifying: bool = False,
        predicate: Union[str, Callable[[S3Info], bool]] = None,
        on_each: Callable[[S3Info, int], bool] = None,
    ) -> List[S3Info]:
        """
        List objects in an S3 bucket with filtering and callbacks.

        :param bucket_name: Bucket to list from (defaults to resource default)
        :param prefix: Key prefix
        :param pattern: Regex pattern to match keys
        :param delimiter: Delimiter for hierarchy
        :param check_is_modifying: If True, skips objects modified in the last 60s
        :param predicate: Python expression (str) or callable for filtering
        :param on_each: Callback for each matched object. If returns False, stops listing.
        """
        bucket = bucket_name
        if not bucket:
            raise ValueError("bucket_name must be provided for list_files")
        client = self.get_client()

        regex = re.compile(pattern) if pattern else None
        infos: List[S3Info] = []

        paginator = client.get_paginator("list_objects_v2")
        paginate_kwargs = {"Bucket": bucket, "Prefix": prefix}
        if delimiter:
            paginate_kwargs["Delimiter"] = delimiter

        for page in paginator.paginate(**paginate_kwargs):
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]

                # Regex match
                if regex and not regex.match(key):
                    continue

                info = S3Info(
                    bucket_name=bucket,
                    key=key,
                    prefix=prefix,
                    size=obj["Size"],
                    modified_dt=obj["LastModified"],
                    storage_class=obj.get("StorageClass"),
                )

                # Check if modifying (min 60s idle)
                if check_is_modifying:
                    if (time.time() - info.modified_ts) < 60:
                        continue

                # Predicate filter
                if predicate:
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
