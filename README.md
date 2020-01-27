# cluster-pack

cluster-pack is a library on top of either [pex][pex] or [conda-pack][conda-pack] to make your Python code easily available on a cluster.

Its goal is to make your prod/dev Python code & libraries easiliy available on any cluster. cluster-pack supports HDFS/S3 as a distributed storage.

The first examples use [Skein][skein] (a simple library for deploying applications on Apache YARN) with HDFS storage. We intend to add more examples for other applications (like [PySpark](https://spark.apache.org/docs/latest/quick-start.html), [Dask](https://dask.org/), [Ray](https://ray.readthedocs.io/en/latest/index.html)) and S3 storage.


## Installation

### Install with Pip

```bash
$ pip install cluster-pack
```

### Install from source

```bash
$ git clone https://github.com/criteo/cluster-pack
$ cd cluster-pack
$ pip install .
```

## Prerequisites

cluster-pack supports Python ≥3.6.

## Features

- Ships a package with all the dependencies from your current virtual environment or your conda environment

- Stores metadata for an environment

- Supports "under development" mode by taking advantage of pip's [editable installs mode][editable_installs_mode], all editable requirements will be uploaded all the time, making local changes directly visible on the cluster

- Interactive (Jupyter notebook) mode

- Provides config helpers to directly use the uploaded zip file inside your application

- Launching jobs from jobs by propagating all artifacts


## Basic examples with [skein][skein]

1) [Interactive mode](https://github.com/criteo/cluster-pack/blob/master/examples/interactive-mode/README.md)

2) [Self shipping project](https://github.com/criteo/cluster-pack/blob/master/examples/skein-project/README.md)

[pex]: https://github.com/pantsbuild/pex
[conda-pack]: https://github.com/conda/conda-pack
[editable_installs_mode]: https://pip.pypa.io/en/stable/reference/pip_install/#editable-installs
[skein]: https://jcrist.github.io/skein/
