
import logging
import os
from pyspark.sql import SparkSession
import cluster_pack
from cluster_pack.spark import spark_config_builder

import numpy as np

logging.basicConfig(level="INFO")


if __name__ == "__main__":

    # use local minio S3 instance
    s3_args = {"use_ssl": False, "client_kwargs": {'endpoint_url': "http://s3:9000"}}
    archive, _ = cluster_pack.upload_env(package_path="s3://test/envs/myenv.pex", fs_args=s3_args)

    ssb = SparkSession.builder
    spark_config_builder.add_s3_params(ssb, s3_args)
    spark_config_builder.add_packaged_environment(ssb, archive)
    spark_config_builder.add_editable_requirements(ssb)
    spark = ssb.getOrCreate()

    # create 2 arrays with random ints range 0 to 100
    a = np.random.random_integers(0, 100, 100)
    b = np.random.random_integers(0, 100, 100)

    # compute intersection of 2 arrays on the worker
    def compute_intersection(x):
        first, second = x
        return np.intersect1d(first, second)

    rdd = spark.sparkContext.parallelize([(a, b)], numSlices=1)
    res = rdd.map(compute_intersection).collect()
    print(f"intersection of arrays len={len(res)} res={res}")
