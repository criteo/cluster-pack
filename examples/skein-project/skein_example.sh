#!/bin/bash

# install venv
python3.6 -m venv skein_env
. skein_env/bin/activate
pip install --upgrade pip
pip install -e .

# execute client script, all dependencies are shipped to the cluster
python -m skein_project.client