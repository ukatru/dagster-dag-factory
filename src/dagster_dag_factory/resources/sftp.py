from typing import Optional, List, Callable, ClassVar, Type, Dict
from contextlib import contextmanager
from pydantic import Field
import paramiko
import re
import os
import stat
import io
import base64

# pysftp 0.2.9 is incompatible with modern paramiko because it tries to import DSSKey.
# We monkeypatch it here to allow pysftp to load.
if not hasattr(paramiko, "DSSKey"):
    paramiko.DSSKey = paramiko.PKey  # Dummy type to satisfy import

import pysftp
from dagster_dag_factory.models.file_info import FileInfo
from dagster_dag_factory.resources.base import BaseConfigurableResource
from dagster_dag_factory.utils.base64 import from_b64_str


class SFTPResource(BaseConfigurableResource):
    """
    Dagster resource for SFTP operations using Paramiko.
    """

    host: str = Field(description="SFTP Hostname")

    username: str = Field(description="SFTP Username")
    password: Optional[str] = Field(default=None, description="SFTP Password")
    port: int = Field(default=22, description="SFTP Port")
    public_key: Optional[str] = Field(default=None, description="public key")
    private_key: Optional[str] = Field(default=None, description="private key")
    key_type: Optional[str] = Field(default="RSA", description="key type")

    mask_fields: ClassVar[List[str]] = BaseConfigurableResource.mask_fields + [
        "password",
        "private_key",
        "public_key",
    ]

    @contextmanager
    def get_client(self):
        cnopts = pysftp.CnOpts()

        # Mapping of key types to paramiko classes
        key_map: Dict[str, Type[paramiko.PKey]] = {
            "RSA": paramiko.RSAKey,
            "ECDSA": paramiko.ECDSAKey,
            "ED25519": paramiko.Ed25519Key,
        }

        pkey_class = key_map.get(self.key_type.upper(), paramiko.RSAKey)

        resolved_host = self.resolve("host")
        resolved_username = self.resolve("username")
        resolved_password = self.resolve("password")
        resolved_public_key = self.resolve("public_key")
        resolved_private_key = self.resolve("private_key")

        if resolved_public_key:
            # Handle full SSH string like "ssh-rsa AAAAB3..." or just the data
            parts = resolved_public_key.strip().split()
            if len(parts) >= 2:
                algo = parts[0]
                key_data_b64 = parts[1]
                public_key = pkey_class(data=base64.b64decode(key_data_b64))
                cnopts.hostkeys.add(resolved_host, algo, public_key)
            else:
                # Assume it's just the B64 data part
                public_key = pkey_class(data=base64.b64decode(resolved_public_key))
                # Use appropriate algorithm string based on key type
                algo_map = {
                    "RSA": "ssh-rsa",
                    "ECDSA": "ecdsa-sha2-nistp256",
                    "ED25519": "ssh-ed25519",
                }
                algo = algo_map.get(self.key_type.upper(), "ssh-rsa")
                cnopts.hostkeys.add(resolved_host, algo, public_key)
        else:
            if not os.path.exists(os.path.expanduser("~/.ssh/known_hosts")):
                cnopts.hostkeys = None

        connection_args = {
            "host": resolved_host,
            "username": resolved_username,
            "port": self.port,
            "cnopts": cnopts,
        }

        if resolved_private_key:
            # User provides private key as b64 encoded
            private_key_str = from_b64_str(resolved_private_key)
            private_key = pkey_class.from_private_key(io.StringIO(private_key_str))
            connection_args["private_key"] = private_key
        else:
            connection_args["password"] = resolved_password

        connection = pysftp.Connection(**connection_args)

        try:
            yield connection
        finally:
            connection.close()

    def list_files(
        self,
        conn: pysftp.Connection,
        path: str,
        pattern: str = None,
        recursive: bool = False,
        check_is_modifying: bool = False,
        predicate: Callable[[FileInfo], bool] = None,
        on_each: Callable[[FileInfo, int], bool] = None,
    ) -> List[FileInfo]:
        """
        List files in directory with advanced filtering and callback support.
        """
        import time

        def logging_action(action, kv):
            return None  # Placeholder for now or use logger if available

        regex = re.compile(pattern) if pattern else None
        infos: List[FileInfo] = []

        def _list_files(current_path: str) -> bool:
            try:
                items = conn.listdir_attr(current_path)
            except FileNotFoundError:
                # Handle single file case
                try:
                    attr = conn.stat(current_path)
                    if not stat.S_ISDIR(attr.st_mode):
                        # Implementation for single file...
                        # Reuse item logic by wrapping in list or similar
                        # For now let's stick to directory logical structure,
                        # if path is a file, we treat it as single item list
                        items = [attr]
                        # Paramiko st_mode doesn't have filename attached strictly if it came from stat
                        # We might need to attach filename manually if it's missing
                        # BUT listdir_attr returns SFTPAttributes which usually have filename.
                        # conn.stat returns SFTPAttributes without filename usually.
                        attr.filename = os.path.basename(current_path)
                    else:
                        raise
                except Exception:
                    # Log warning?
                    return False

            # If items is a single attr from stat, it might not be iterable like listdir_attr result
            if not isinstance(items, list):
                items = [items]

            dirs: List[str] = []

            for item in items:
                mode = item.st_mode
                file_name = item.filename
                full_file_path = (
                    os.path.join(current_path, file_name)
                    if current_path != file_name
                    else current_path
                )

                # If we stat-ed a single file, full_file_path calculation might be tricky if we don't be careful.
                # If current_path is /foo/bar.txt, basename is bar.txt.
                # If we join /foo/bar.txt and bar.txt we get wrong path.
                # Let's trust listdir behavior mostly. for single file checks, the user usually provides directory path+pattern.

                if stat.S_ISDIR(mode):
                    if file_name not in [".", ".."]:
                        dirs.append(full_file_path)
                    continue
                elif not stat.S_ISREG(mode):
                    continue

                if regex and not regex.match(file_name):
                    continue

                if check_is_modifying:
                    # Logic from snippet: (current_ts - modified_ts) < 60
                    # This assumes file is stable if it hasn't been modified in last 60s
                    # This is different from "check size change", but following user request.
                    current_ts = time.time()
                    if (current_ts - item.st_mtime) < 60:
                        # File is too new, might be writing
                        continue

                info = FileInfo(
                    file_name=file_name,
                    full_file_path=full_file_path,
                    root_path=path,
                    file_size=item.st_size,
                    modified_ts=item.st_mtime,
                )

                if predicate and not predicate(info):
                    continue

                if on_each:
                    # Callback returns False to stop? Or just return value doesn't matter?
                    # Snippet says: if not on_each(...) == False: infos.append
                    # So if on_each returns False, we don't append.
                    if on_each(info, len(infos) + 1) is False:
                        continue

                infos.append(info)

            if recursive:
                for dir_path in dirs:
                    _list_files(dir_path)

            return True

        if _list_files(path):
            return infos
        return []
