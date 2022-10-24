
import os
import sys
import pytest
from pyspark.sql import SparkSession
import tempfile

from cluster_pack.spark import spark_config_builder


@pytest.fixture()
def local_spark_session_builder():
    if "SPARK_HOME" in os.environ.keys():
        del os.environ["SPARK_HOME"]
    os.environ["PYSPARK_PYTHON"] = sys.executable
    SparkSession.builder._options = {}
    ssb = SparkSession.builder.master("local[1]").config("spark.submit.deployMode", "client")
    return ssb


def test__add_or_merge(local_spark_session_builder):
    def _add_file(spark_session_builder, path):
        with open(path, 'w'):
            pass
        spark_config_builder._add_or_merge(
            spark_session_builder, "spark.files", path)

    with tempfile.TemporaryDirectory() as tempdir:
        _add_file(local_spark_session_builder, f"{tempdir}/path1")
        _add_file(local_spark_session_builder, f"{tempdir}/path1")  # should be deduplicated
        _add_file(local_spark_session_builder, f"{tempdir}/path2")
        _add_file(local_spark_session_builder, f"{tempdir}/path3")

        ss = local_spark_session_builder.getOrCreate()

        actual_files = set(ss.sparkContext.getConf().get("spark.files").split(','))
        assert actual_files == {f"{tempdir}/path1", f"{tempdir}/path2", f"{tempdir}/path3"}

        ss.stop()


def test_add_packaged_environment(local_spark_session_builder):
    with tempfile.TemporaryDirectory() as tempdir:
        with open(f"{tempdir}/myenv.pex", 'w'):
            pass
        spark_config_builder.add_packaged_environment(
            local_spark_session_builder,
            f"{tempdir}/myenv.pex")

        ss = local_spark_session_builder.getOrCreate()

        assert ss.sparkContext.getConf().get("spark.files") == f"{tempdir}/myenv.pex"

        ss.stop()
