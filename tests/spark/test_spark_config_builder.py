
import os
import sys
import pytest
from pyspark.sql import SparkSession

from cluster_pack.spark import spark_config_builder


@pytest.fixture()
def local_spark_session_builder():
    if "SPARK_HOME" in os.environ.keys():
        del os.environ["SPARK_HOME"]
    os.environ["PYSPARK_PYTHON"] = sys.executable
    ssb = SparkSession.builder.master(f"local[1]").config("spark.submit.deployMode", "client")
    return ssb


def test__add_or_merge(local_spark_session_builder):
    spark_config_builder._add_or_merge(
        local_spark_session_builder, "spark.yarn.dist.archives", "path")
    spark_config_builder._add_or_merge(
        local_spark_session_builder, "spark.yarn.dist.archives", "path1")
    spark_config_builder._add_or_merge(
        local_spark_session_builder, "spark.yarn.dist.archives", "path2")

    ss = local_spark_session_builder.getOrCreate()

    assert ss.sparkContext.getConf().get("spark.yarn.dist.archives") == "path,path1,path2"

    ss.stop()


def test_add_packaged_environment(local_spark_session_builder):
    spark_config_builder.add_packaged_environment(local_spark_session_builder, "myenv.pex")

    ss = local_spark_session_builder.getOrCreate()

    assert ss.sparkContext.getConf().get("spark.yarn.dist.files") == "myenv.pex"

    ss.stop()
