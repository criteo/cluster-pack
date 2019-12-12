#!/bin/bash

# install venv
python3.6 -m venv skein_project_env
. skein_project_env/bin/activate
pip install --upgrade pip setuptools
pip install -e .

# execute client script, all dependencies are shipped to the cluster
python -m skein_project.client