#!/bin/bash
python3.6 -m venv pyspark_env
. pyspark_env/bin/activate
pip install -U pip setuptools
pip install pypandoc
pip install s3fs numpy pyspark==2.4.4
pip install -e /cluster-pack

curr_dir=$(dirname "$0")
python $curr_dir/spark_example.py