## Docker example with Spark on S3 storage

Here is an example with Standalone Spark on S3 storage running with docker compose.

### Install

1) Build spark standalone docker containers (one master, one worker)

```bash
docker build -t spark-docker ./examples/spark-with-S3
```

2) Start standalone Spark & local S3 containers (using [minio](https://min.io/))

```bash
docker-compose -f ./examples/spark-with-S3/docker-compose.yml up -d
```

### Quick run

```bash
docker exec spark-master ./examples/spark-with-S3/scripts/run_spark_example.sh
```

### Step by step


#### Define the workload to be executed remotely

This function uses numpy as a dependency that won't be available on the cluster

```python
def compute_intersection():
    a = np.random.random_integers(0, 100, 100)
    b = np.random.random_integers(0, 100, 100)
    print("Computed intersection of two arrays:")
    print(np.intersect1d(a, b))
```

#### Upload current virtual environment as a self contained zip file to the distributed storage 

The self contained zip file contains all installed external packages i.e numpy.

```python
import cluster_pack
s3_args = {"use_ssl": False, "client_kwargs": {'endpoint_url': "http://s3:9000"}}
archive, _ = cluster_pack.upload_env(package_path="s3://test/envs/myenv.pex", fs_args=s3_args)
```

#### Call spark config helper to generate the SparkConfig set up to use this executable zip file on the executors

```python
from cluster_pack.spark import spark_config_builder
ssb = SparkSession.builder
spark_config_builder.add_s3_params(ssb, s3_args)
spark_config_builder.add_packaged_environment(ssb, archive)
spark_config_builder.add_editable_requirements(ssb)
spark = ssb.getOrCreate()
```

#### Submit the Spark application to the cluster

```python
# create 2 arrays with random ints range 0 to 100
a = np.random.random_integers(0, 100, 100)
b = np.random.random_integers(0, 100, 100)
rdd = spark.sparkContext.parallelize([(a, b)], numSlices=1)
res = rdd.map(compute_intersection).collect()
print(f"intersection of arrays len={len(res)} res={res}")
```

 