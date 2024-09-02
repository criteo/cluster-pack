#!/bin/bash

set -e

pushd cluster-pack
    rm -rf ~/venv
    python3.9 -m venv ~/venv
    source ~/venv/bin/activate
    pip install -U wheel pip setuptools
    pip install -e .
    pip install -r tests-requirements.txt

    pytest -m hadoop -s tests --log-cli-level=INFO
popd
