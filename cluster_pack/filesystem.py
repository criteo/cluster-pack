import logging
import pyarrow


from typing import Dict, Tuple, Any, List
from pyarrow import filesystem, util
from urllib.parse import urlparse

try:
    from s3fs import S3FileSystem
except (ModuleNotFoundError, ImportError):
    pass


_logger = logging.getLogger(__name__)


def _make_function(base_fs, method_name):
    def f(*args, **kwargs):
        func = getattr(base_fs, method_name)
        return func(*args, **kwargs)
    return f


def _expose_methods(child_class: Any, base_class: Any, ignored: List[str] = []):
    """
    expose all methods from base_class to child_class

    :param base_class: instance of base class
    :param child_class: instance of child class to add the methods to
    :param ignored: methods that will be redefined manually
    """
    method_list = [func for func in dir(base_class)
                   if callable(getattr(base_class, func))
                   and not func.startswith("__")
                   and not [f for f in ignored if func.startswith(f)]]
    for method_name in method_list:
        _logger.debug(f"add method impl from {type(base_class)}.{method_name}"
                      f" to {type(child_class)}")
        setattr(child_class, method_name, _make_function(base_class, method_name))


class EnhancedFileSystem(filesystem.FileSystem):

    def __init__(self, base_fs):
        self.base_fs = base_fs
        _expose_methods(self, base_fs, ignored=["open"])

    def put(self, filename, path, chunk=2**16):
        with self.base_fs.open(path, 'wb') as target:
            with open(filename, 'rb') as source:
                while True:
                    out = source.read(chunk)
                    if len(out) == 0:
                        break
                    target.write(out)

    def get(self, filename, path, chunk=2**16):
        with open(path, 'wb') as target:
            with self.base_fs.open(filename, 'rb') as source:
                while True:
                    out = source.read(chunk)
                    if len(out) == 0:
                        break
                    target.write(out)

    def open(self, path, mode='rb'):
        return EnhancedHdfsFile(self.base_fs.open(path, mode))


class EnhancedHdfsFile(pyarrow.HdfsFile):

    def __init__(self, base_hdfs_file):
        self.base_hdfs_file = base_hdfs_file
        _expose_methods(self, base_hdfs_file, ignored=["write"])

    def ensure_bytes(self, s):
        if isinstance(s, bytes):
            return s
        if hasattr(s, 'encode'):
            return s.encode()
        if hasattr(s, 'tobytes'):
            return s.tobytes()
        if isinstance(s, bytearray):
            return bytes(s)
        return s

    def write(self, data):
        self.base_hdfs_file.write(self.ensure_bytes(data))


def resolve_filesystem_and_path(uri: str, **kwargs) -> Tuple[EnhancedFileSystem, str]:
    parsed_uri = urlparse(uri)
    fs_path = parsed_uri.path
    # from https://github.com/apache/arrow/blob/master/python/pyarrow/filesystem.py#L419
    # with viewfs support
    if parsed_uri.scheme == 'hdfs' or parsed_uri.scheme == 'viewfs':
        netloc_split = parsed_uri.netloc.split(':')
        host = netloc_split[0]
        if host == '':
            host = 'default'
        else:
            host = parsed_uri.scheme + "://" + host
        port = 0
        if len(netloc_split) == 2 and netloc_split[1].isnumeric():
            port = int(netloc_split[1])

        fs = EnhancedFileSystem(pyarrow.hdfs.connect(host=host, port=port))
    elif parsed_uri.scheme == 's3' or parsed_uri.scheme == 's3a':
        fs = EnhancedFileSystem(pyarrow.filesystem.S3FSWrapper(S3FileSystem(**kwargs)))
    else:
        # Input is local path such as /home/user/myfile.parquet
        fs = EnhancedFileSystem(pyarrow.filesystem.LocalFileSystem.get_instance())

    _logger.info(f"Resolved base filesystem: {type(fs.base_fs)}")
    return fs, fs_path
