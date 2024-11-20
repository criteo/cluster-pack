import logging
from typing import Tuple, Any

import fsspec
import fsspec.implementations.arrow

# Support viewfs:// protocol for HDFS
fsspec.register_implementation("viewfs", fsspec.implementations.arrow.HadoopFileSystem)

_logger = logging.getLogger(__name__)


def resolve_filesystem_and_path(uri: str, **kwargs: Any) -> Tuple[fsspec.AbstractFileSystem, str]:
    fs, fs_path = fsspec.url_to_fs(uri, **kwargs)
    _logger.info(f"Resolved base filesystem: {type(fs)}")
    return fs, fs_path
