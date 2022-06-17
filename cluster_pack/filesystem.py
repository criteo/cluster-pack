import logging
import os
import tempfile
import pyarrow

import types

from typing import Tuple, Any, List, Iterator
from fsspec.implementations.arrow import ArrowFSWrapper
from fsspec.implementations.hdfs import PyArrowHDFS
from fsspec.implementations.local import LocalFileSystem
from fsspec.spec import AbstractFileSystem
from pyarrow import fs as filesys
from urllib.parse import urlparse

try:
    from s3fs import S3FileSystem

    # pyarrow calls mkdirs which is an alias to makedirs which is implemented as no-op
    # remove this once https://github.com/dask/s3fs/pull/331 is released
    def _makedirs(self: Any, path: str, exist_ok: bool = False) -> None:
        bucket, _, _ = self.split_path(path)
        if not self.exists(bucket):
            self.mkdir(bucket)

    S3FileSystem.makedirs = _makedirs
except (ModuleNotFoundError, ImportError):
    pass


_logger = logging.getLogger(__name__)


def _make_function(base_fs: Any, method_name: str) -> Any:
    def f(*args: Any, **kwargs: Any) -> Any:
        func = getattr(base_fs, method_name)
        return func(*args, **kwargs)

    return f


def _expose_methods(child_class: Any, base_class: Any, ignored: List[str] = []) -> None:
    """
    expose all methods from base_class to child_class

    :param base_class: instance of base class
    :param child_class: instance of child class to add the methods to
    :param ignored: methods that will be redefined manually
    """
    method_list = [
        func
        for func in dir(base_class)
        if hasattr(base_class, func) and callable(getattr(base_class, func))
        and not func.startswith("__")
        and not [f for f in ignored if func.startswith(f)]
    ]
    for method_name in method_list:
        _logger.debug(
            f"add method impl from {type(base_class)}.{method_name}" f" to {type(child_class)}"
            )
        setattr(child_class, method_name, _make_function(base_class, method_name))


def _chmod(self: Any, path: str, mode: int) -> None:
    os.chmod(path, mode)


def _st_mode(self: Any, path: str) -> int:
    return os.stat(path).st_mode


def _hdfs_st_mode(self: Any, path: str) -> int:
    st_mode = self.ls(path, True)[0]["permissions"]
    return int(st_mode)


class EnhancedHdfsFile(pyarrow.HdfsFile):
    def __init__(self, base_hdfs_file: pyarrow.HdfsFile):
        self.base_hdfs_file = base_hdfs_file
        _expose_methods(self, base_hdfs_file, ignored=["write", "closed"])

    def ensure_bytes(self, s: Any) -> bytes:
        if isinstance(s, bytes):
            return s
        if hasattr(s, "encode"):
            return s.encode()
        if hasattr(s, "tobytes"):
            return s.tobytes()
        if isinstance(s, bytearray):
            return bytes(s)
        return s

    def write(self, data: Any) -> None:
        self.base_hdfs_file.write(self.ensure_bytes(data))

    def _genline(self) -> Iterator[bytes]:
        while True:
            out = self.readline()
            if out:
                yield out
            else:
                raise StopIteration

    def __iter__(self) -> Iterator[bytes]:
        return self._genline()


class EnhancedFileSystem(AbstractFileSystem):
    def __init__(self, base_fs: AbstractFileSystem):
        self.base_fs = base_fs
        overidden_methods = ["open", "put", "move"]
        if isinstance(base_fs, LocalFileSystem):
            self.chmod = types.MethodType(_chmod, base_fs)
            self.st_mode = types.MethodType(_st_mode, base_fs)
            overidden_methods += ["chmod", "st_mode"]
        elif isinstance(base_fs, PyArrowHDFS):
            self.st_mode = types.MethodType(_hdfs_st_mode, base_fs)
            overidden_methods += ["st_mode"]
        _expose_methods(self, base_fs, ignored=overidden_methods)

    def _preserve_acls(self, local_file: str, remote_file: str) -> None:
        # this is useful for keeing pex executable rights
        if (
            isinstance(self.base_fs, LocalFileSystem)
            or isinstance(self.base_fs, PyArrowHDFS)
        ):
            st = os.stat(local_file)
            self.chmod(remote_file, st.st_mode & 0o777)

        # not supported for S3 yet

    def put(self, filename: str, path: str, chunk: int = 2**16) -> None:
        with self.open(path, mode="wb") as target:
            with open(filename, "rb") as source:
                while True:
                    out = source.read(chunk)
                    if len(out) == 0:
                        break
                    target.write(out)

        self._preserve_acls(filename, path)

    def move(self, path: str, new_path: str) -> None:
        st_mode = self.st_mode(path)
        self.base_fs.move(path, new_path)
        self.chmod(new_path, st_mode)

    def get(self, filename: str, path: str, chunk: int = 2**16) -> None:
        with open(path, "wb") as target:
            with self.open(filename) as source:
                while True:
                    out = source.read(chunk)
                    if len(out) == 0:
                        break
                    target.write(out)

    def open(self, path: str, mode: str = "rb") -> EnhancedHdfsFile:
        if isinstance(self.base_fs, LocalFileSystem):
            return EnhancedHdfsFile(open(path, mode))
        return EnhancedHdfsFile(self.base_fs.open(path, mode))


def resolve_filesystem_and_path(uri: str, **kwargs: Any) -> Tuple[EnhancedFileSystem, str]:
    parsed_uri = urlparse(uri)
    fs_path = parsed_uri.path
    # from https://github.com/apache/arrow/blob/master/python/pyarrow/filesystem.py#L419
    # with viewfs support
    if parsed_uri.scheme == "hdfs" or parsed_uri.scheme == "viewfs":
        netloc_split = parsed_uri.netloc.split(":")
        host = netloc_split[0]
        if host == "":
            host = "default"
        else:
            host = parsed_uri.scheme + "://" + host
        port = 0
        if len(netloc_split) == 2 and netloc_split[1].isnumeric():
            port = int(netloc_split[1])

        fs = PyArrowHDFS(host=host, port=port)
    elif parsed_uri.scheme == "s3" or parsed_uri.scheme == "s3a":
        fs = ArrowFSWrapper(filesys.S3FSWrapper(S3FileSystem(**kwargs)))
    else:
        # Input is local path such as /home/user/myfile.parquet
        fs = LocalFileSystem()

    _logger.info(f"Resolved base filesystem: {type(fs)}")
    return EnhancedFileSystem(fs), fs_path
