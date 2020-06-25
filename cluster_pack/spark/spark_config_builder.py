
import os
import logging
import pyspark
from pyspark.sql import SparkSession

from cluster_pack import packaging, uploader

from typing import Dict, Optional, Any

_logger = logging.getLogger(__name__)


def add_packaged_environment(ssb: SparkSession.Builder, archive: str):
    archive = _make_path_hadoop_compatible(archive)
    if archive.endswith('pex'):
        _add_or_merge(ssb, "spark.yarn.dist.files", f"{archive}")
        ssb.config("spark.executorEnv.PEX_ROOT", "./.pex")
        os.environ['PYSPARK_PYTHON'] = './' + archive.split('/')[-1]
        os.environ['PYSPARK_DRIVER_PYTHON'] = archive.split('/')[-1]
    else:
        _add_archive(ssb, f"{archive}#condaenv")
        os.environ['PYSPARK_PYTHON'] = "./condaenv/bin/python"
        os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'

    if not _get_value(ssb, "spark.master") == "yarn":
        _logger.info("Not running on yarn. Adding archive to spark.files")
        _add_or_merge(ssb, "spark.files", f"{archive}")


def add_editable_requirements(ssb: SparkSession.Builder):
    for requirement_dir in packaging.get_editable_requirements().values():
        py_archive = packaging.zip_path(requirement_dir)
        _add_archive(ssb, py_archive)


def add_s3_params(ssb: SparkSession.Builder,  fs_args: Dict[str, Any] = {}):
    ssb.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    ssb.config("spark.hadoop.fs.s3a.path.style.access", "true")
    if "key" in fs_args:
        ssb.config("spark.hadoop.fs.s3a.access.key", fs_args["key"])
    if "secret" in fs_args:
        ssb.config("spark.hadoop.fs.s3a.secret.key", fs_args["secret"])
    if "client_kwargs" in fs_args:
        ssb.config("spark.hadoop.fs.s3a.endpoint", fs_args["client_kwargs"]["endpoint_url"])


def _add_archive(ssb: SparkSession.Builder, path):
    _add_or_merge(ssb, "spark.yarn.dist.archives", path)


def _get_value(ssb: SparkSession.Builder, key: str) -> Optional[str]:
    if key in ssb._options:
        return ssb._options[key]
    return None


def _add_or_merge(ssb: SparkSession.Builder, key: str, value: str):
    if key in ssb._options:
        old_value = ssb._options[key]
        ssb.config(key, f"{old_value},{value}")
    else:
        ssb.config(key, value)


# https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#How_S3A_writes_data_to_S3
# Hadoop uses s3a where aws seems to use S3 now
# See https://github.com/dask/s3fs/pull/269 for s3fs
def _make_path_hadoop_compatible(path):
    if path.startswith('s3://'):
        path = path[5:]
        path = path.rstrip('/').lstrip('/')
        return f"s3a://{path}"
    return path
