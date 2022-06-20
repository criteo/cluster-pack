
import logging
import pandas as pd

import cluster_pack
from cluster_pack.spark import spark_config_builder

from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql import SparkSession

logging.basicConfig(level="INFO")

_logger = logging.getLogger(__name__)


if __name__ == "__main__":

    # use local minio S3 instance
    # allowed parameters are here:
    # https://s3fs.readthedocs.io/en/latest/api.html#s3fs.core.S3FileSystem
    s3_args = {"scheme": "http",
               "endpoint_override": "127.0.0.1:9000",
               "access_key": "AAA",
               "secret_key": "BBBBBBBB"}
    archive, _ = cluster_pack.upload_env(package_path="s3://test/envs/myenv.pex", fs_args=s3_args)

    ssb = SparkSession.builder
    spark_config_builder.add_s3_params(ssb, s3_args)
    spark_config_builder.add_packaged_environment(ssb, archive)
    spark_config_builder.add_editable_requirements(ssb)
    spark = ssb.getOrCreate()

    # https://spark.apache.org/docs/2.4.4/sql-pyspark-pandas-with-arrow.html#grouped-map
    # cluster-pack will ship pyarrow & pandas to the executor

    df = spark.createDataFrame(
        [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        ("id", "v"))

    @pandas_udf("double", PandasUDFType.GROUPED_AGG)
    def mean_udf(v: pd.Series) -> float:
        return v.mean()

    pd_df = df.groupby("id").agg(mean_udf(df['v'])).toPandas()

    _logger.info(pd_df)
