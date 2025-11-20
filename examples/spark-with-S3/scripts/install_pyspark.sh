#!/bin/bash
rm -rf /tmp/pyspark_env
uv venv /tmp/pyspark_env
source /tmp/pyspark_env/bin/activate

uv pip install -U pip setuptools wheel
uv pip install "pypandoc<1.8" s3fs pandas pyspark=="$1"
uv pip install -e /cluster-pack

python -V
