import logging
from typing import Tuple, Any
from urllib.parse import urlparse

import fsspec

_logger = logging.getLogger(__name__)


def resolve_filesystem_and_path(
    uri: str, **kwargs: Any
) -> Tuple[fsspec.AbstractFileSystem, str]:
    parsed_uri = urlparse(uri)
    fs_path = parsed_uri.path
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

        fs = fsspec.filesystem("hdfs", host=host, port=port, **kwargs)
    elif parsed_uri.scheme == "":
        # Input is local path such as /home/user/myfile.parquet
        fs = fsspec.filesystem("file", **kwargs)
    else:
        fs = fsspec.filesystem(parsed_uri.scheme, **kwargs)

    _logger.info(f"Resolved base filesystem: {type(fs)}")
    return fs, fs_path
