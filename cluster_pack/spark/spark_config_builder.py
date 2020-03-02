
import os

import pyspark
from pyspark.sql import SparkSession

from cluster_pack import packaging, uploader

from typing import Optional


def add_packaged_environment(ssb: SparkSession.Builder, archive: str):
    if archive.endswith('pex'):
        _add_or_merge(ssb, "spark.yarn.dist.files", f"{archive}")
        ssb.config("spark.executorEnv.PEX_ROOT", "./.pex")
        os.environ['PYSPARK_PYTHON'] = './' + archive.split('/')[-1]
        os.environ['PYSPARK_DRIVER_PYTHON'] = archive.split('/')[-1]
    else:
        _add_archive(ssb, f"{archive}#condaenv")
        os.environ['PYSPARK_PYTHON'] = f"./condaenv/bin/python"
        os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'


def add_editable_requirements(ssb: SparkSession.Builder):
    for requirement_dir in packaging.get_editable_requirements().values():
        py_archive = packaging.zip_path(requirement_dir)
        _add_archive(ssb, py_archive)


def _add_archive(ssb: SparkSession.Builder, path):
    _add_or_merge(ssb, "spark.yarn.dist.archives", path)


def _add_or_merge(ssb: SparkSession.Builder, key: str, value: str):
    if key in ssb._options:
        old_value = ssb._options[key]
        ssb.config(key, f"{old_value},{value}")
    else:
        ssb.config(key, value)
