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

We want to execute the pandas udf example from [PySpark Pandas UDFs (a.k.a. Vectorized UDFs)](https://spark.apache.org/docs/3.2.2/api/python/user_guide/sql/arrow_pandas.html#grouped-map).

As Spark uses pandas & pyarrow under the hood we need them to be installed on the executor. cluster-pack will take care making everything easily available on the cluster.

#### Create the current virtual environment

```bash
python3.9 -m venv /tmp/pyspark_env
. /tmp/pyspark_env/bin/activate
pip install -U pip setuptools
pip install pypandoc
pip install s3fs pandas pyspark==3.2.2
pip install cluster-pack
```

#### Upload current virtual environment as a self contained zip file to the distributed storage 

The self contained zip file contains all installed external packages pandas & pyarrow
```python
import cluster_pack
s3_args = {"use_ssl": False, "client_kwargs": {'endpoint_url': "http://s3:9000"}}
archive, _ = cluster_pack.upload_env(package_path="s3://test/envs/myenv.pex", fs_args=s3_args)
```

#### Call spark config helper to generate the SparkConfig set up to use this executable zip file on the executors

```python
from pyspark.sql import SparkSession
from cluster_pack.spark import spark_config_builder
ssb = SparkSession.builder
spark_config_builder.add_s3_params(ssb, s3_args)
spark_config_builder.add_packaged_environment(ssb, archive)
spark_config_builder.add_editable_requirements(ssb)
spark = ssb.getOrCreate()
```

#### Submit the Spark application to the cluster

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType

df = spark.createDataFrame(
    [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
    ("id", "v"))

@pandas_udf("double", PandasUDFType.GROUPED_AGG)
def mean_udf(v: pd.Series) -> float:
    return v.mean()

df.groupby("id").agg(mean_udf(df['v'])).toPandas()
```

 