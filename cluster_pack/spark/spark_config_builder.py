
import os
import logging
from pyspark.sql import SparkSession

from cluster_pack import packaging, get_pyenv_usage_from_archive

from typing import Dict, Optional, Any

_logger = logging.getLogger(__name__)


def add_packaged_environment(ssb: SparkSession.Builder, archive: str) -> None:
    archive = _make_path_hadoop_compatible(archive)

    usage = get_pyenv_usage_from_archive(archive)
    os.environ['PYSPARK_PYTHON'] = usage.interpreter_cmd

    if usage.must_unpack:
        _add_archive(ssb, f"{archive}#{usage.dest_path}")
    else:
        ssb.config("spark.executorEnv.PEX_ROOT", "./.pex")
        _add_or_merge(ssb, "spark.yarn.dist.files", f"{archive}")

    if _get_value(ssb, "spark.submit.deployMode") == "cluster":
        os.environ['PYSPARK_DRIVER_PYTHON'] = usage.interpreter_cmd
    else:
        os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'

    if not _get_value(ssb, "spark.master") == "yarn":
        _logger.info("Not running on yarn. Adding archive to spark.files")
        _add_or_merge(ssb, "spark.files", f"{archive}")


def add_editable_requirements(ssb: SparkSession.Builder) -> None:
    for requirement_dir in packaging.get_editable_requirements().values():
        py_archive = packaging.zip_path(requirement_dir)
        _add_archive(ssb, py_archive)


def add_s3_params(ssb: SparkSession.Builder,  fs_args: Dict[str, Any] = {}) -> None:
    ssb.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    ssb.config("spark.hadoop.fs.s3a.path.style.access", "true")
    if "key" in fs_args:
        ssb.config("spark.hadoop.fs.s3a.access.key", fs_args["key"])
    if "secret" in fs_args:
        ssb.config("spark.hadoop.fs.s3a.secret.key", fs_args["secret"])
    if "client_kwargs" in fs_args:
        ssb.config("spark.hadoop.fs.s3a.endpoint", fs_args["client_kwargs"]["endpoint_url"])


def _add_archive(ssb: SparkSession.Builder, path: str) -> None:
    _add_or_merge(ssb, "spark.yarn.dist.archives", path)


def _get_value(ssb: SparkSession.Builder, key: str) -> Optional[str]:
    if key in ssb._options:  # type: ignore [attr-defined]
        return ssb._options[key]  # type: ignore [attr-defined]
    return None


def _add_or_merge(ssb: SparkSession.Builder, key: str, value: str) -> None:
    sep = ','
    if key in ssb._options:  # type: ignore [attr-defined]
        old_value = ssb._options[key]  # type: ignore [attr-defined]
        old_value_set = set(old_value.split(sep))
        old_value_set.add(value)
        ssb.config(key, sep.join(old_value_set))
    else:
        ssb.config(key, value)


# https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#How_S3A_writes_data_to_S3
# Hadoop uses s3a where aws seems to use S3 now
# See https://github.com/dask/s3fs/pull/269 for s3fs
def _make_path_hadoop_compatible(path: str) -> str:
    if path.startswith('s3://'):
        path = path[5:]
        path = path.rstrip('/').lstrip('/')
        return f"s3a://{path}"
    return path
