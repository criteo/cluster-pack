#!/usr/bin/env python3

import setuptools

setuptools.setup(
    name='skein_project',
    version='0.0.1',
    packages=setuptools.find_packages(),
    python_requires=">=3.6",
    install_requires=[
        "skein",
        "numpy",
        "cluster-pack@git+git://github.com/criteo/cluster-pack@master#egg=cluster-pack"
    ])
