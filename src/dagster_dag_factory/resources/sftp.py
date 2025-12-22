from typing import Optional, Any, List, Callable, ClassVar
from pydantic import Field
import paramiko
import re
import os
import stat
from contextlib import contextmanager
from dagster_dag_factory.models.file_info import FileInfo
from .base import BaseConfigurableResource

class SFTPResource(BaseConfigurableResource):
    """
    Dagster resource for SFTP operations using Paramiko.
    """
    host: str = Field(description="SFTP Hostname")
    port: int = Field(default=22, description="SFTP Port")
    username: str = Field(description="SFTP Username")
    password: Optional[str] = Field(default=None, description="SFTP Password")
    private_key_path: Optional[str] = Field(default=None, description="Path to private key file")
    
    mask_fields: ClassVar[List[str]] = BaseConfigurableResource.mask_fields + ["password"]

    @contextmanager
    def get_client(self):
        """Yields an SFTP client."""
        transport = paramiko.Transport((self.host, self.port))
        try:
            if self.private_key_path:
                pkey = paramiko.RSAKey.from_private_key_file(self.private_key_path)
                transport.connect(username=self.username, pkey=pkey)
            else:
                transport.connect(username=self.username, password=self.password)
                
            sftp = paramiko.SFTPClient.from_transport(transport)
            yield sftp
        finally:
            if transport:
                transport.close()

    def list_files(
        self,
        conn: paramiko.SFTPClient,
        path: str,
        pattern: str = None,
        recursive: bool = False,
        predicate: Callable[[FileInfo], bool] = None,
    ) -> List[FileInfo]:
        """
        List files in directory with advanced filtering.
        """
        files: List[FileInfo] = []
        regex = re.compile(pattern) if pattern else None

        try:
            items = conn.listdir_attr(path)
        except FileNotFoundError:
            try:
                attr = conn.stat(path)
                if not stat.S_ISDIR(attr.st_mode):
                    file_name = os.path.basename(path)
                    finfo = FileInfo(
                        file_name=file_name,
                        file_path=file_name,
                        full_file_path=path,
                        file_size=attr.st_size,
                        modified_ts=attr.st_mtime
                    )
                    if predicate and not predicate(finfo):
                        return []
                    return [finfo]
                else:
                    raise
            except Exception:
                raise FileNotFoundError(f"Path {path} not found or not accessible")

        for item in items:
            filename = item.filename
            full_path = os.path.join(path, filename)
            is_dir = stat.S_ISDIR(item.st_mode)

            if is_dir:
                if recursive and filename not in ['.', '..']:
                     files.extend(self.list_files(conn, full_path, pattern, recursive, predicate))
                continue

            if regex and not regex.match(filename):
                continue
                
            finfo = FileInfo(
                file_name=filename,
                file_path=os.path.relpath(full_path, path) if recursive else filename, 
                full_file_path=full_path,
                file_size=item.st_size,
                modified_ts=item.st_mtime
            )
            
            if predicate and not predicate(finfo):
                continue
                
            files.append(finfo)
            
        return files
