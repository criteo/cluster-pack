import logging
import pyarrow


from typing import Dict, Tuple
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


class EnhancedFileSystem(filesystem.FileSystem):

    def __init__(self, base_fs):
        self.base_fs = base_fs
        # expose all methods from base_fs
        method_list = [func for func in dir(base_fs)
                       if callable(getattr(base_fs, func)) and not func.startswith("__")]
        for method_name in method_list:
            _logger.debug(f"add method impl from {type(base_fs)}.{method_name}"
                          " to EnhancedFileSystem")
            setattr(self, method_name, _make_function(base_fs, method_name))

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
