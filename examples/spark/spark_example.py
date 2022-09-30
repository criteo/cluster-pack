import numpy as np

from pyspark.sql import SparkSession

import cluster_pack
from cluster_pack.spark import spark_config_builder

if __name__ == "__main__":
    package_path, _ = cluster_pack.upload_env()

    ssb = SparkSession.builder \
        .appName("spark_app") \
        .master("yarn") \
        .config("spark.submit.deployMode", "client") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.memoryOverhead", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.acls.enable", "true") \
        .config("spark.ui.view.acls", "*")

    spark_config_builder.add_packaged_environment(ssb, package_path)
    spark_config_builder.add_editable_requirements(ssb)

    ss = ssb.getOrCreate()

    # create 2 arrays with random ints range 0 to 100
    a = np.random.random_integers(0, 100, 100)
    b = np.random.random_integers(0, 100, 100)

    # compute intersection of 2 arrays on the worker
    def compute_intersection(x):
        first, second = x
        return np.intersect1d(first, second)

    rdd = ss.sparkContext.parallelize([(a, b)], numSlices=1)
    res = rdd.map(compute_intersection).collect()
    print(f"intersection of arrays len={len(res)} res={res}")
