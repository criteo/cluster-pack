#!/bin/bash
source /tmp/pyspark_env/bin/activate
curr_dir=$(dirname "$0")
python "$curr_dir"/spark_example.py
