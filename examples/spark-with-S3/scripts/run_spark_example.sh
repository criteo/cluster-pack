#!/bin/bash
rm -rf /tmp/pyspark_env
python3.6 -m venv /tmp/pyspark_env
. /tmp/pyspark_env/bin/activate
pip install -U pip setuptools
pip install 'pypandoc<1.8'
# pandas pinned to < 1.0.0 because https://github.com/pantsbuild/pex/issues/1017
pip install s3fs 'pandas<1.0.0' pyarrow==6.0.1 pyspark==2.4.4
pip install -e /cluster-pack

curr_dir=$(dirname "$0")
python $curr_dir/spark_example.py
