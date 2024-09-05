#!/bin/bash
rm -rf /tmp/pyspark_env
$1 -m venv /tmp/pyspark_env
. /tmp/pyspark_env/bin/activate
pip install -U pip setuptools wheel
pip install pypandoc<1.8
pip install s3fs pandas pyspark==$2
pip install -e /cluster-pack

curr_dir=$(dirname "$0")
python -V
python $curr_dir/spark_example.py
