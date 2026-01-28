# cluster-pack

cluster-pack is a library on top of either [pex][pex] to make your Python code easily available on a cluster.

Its goal is to make your prod/dev Python code & libraries easiliy available on any cluster. cluster-pack supports HDFS/S3 as a distributed storage.

The first examples use [Skein][skein] (a simple library for deploying applications on Apache YARN) and [PySpark](https://spark.apache.org/docs/latest/quick-start.html) with HDFS storage. We intend to add more examples for other applications (like [Dask](https://dask.org/), [Ray](https://ray.readthedocs.io/en/latest/index.html)) and S3 storage.

An introducing blog post can be found [here](https://medium.com/criteo-labs/open-sourcing-cluster-pack-700f46c139a).

![cluster-pack](https://github.com/criteo/cluster-pack/blob/master/cluster_pack.png?raw=true)

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

cluster-pack supports Python ≥3.9.
Cluster-pack can speed up pex creation by using uv if available.

## Feature flags
- C_PACK_USER: override the current user for HDFS path generation and Skein impersonation
  - When set, this value is used instead of the system user (from `getpass.getuser()`)
  - Useful for running jobs as a different user or in environments where the system user doesn't match the HDFS user
  - If not set or empty, falls back to the current system user

- C_PACK_VENV_OPTIMIZATION_LEVEL (default 1): uses an existing venv to speed up pex creation
  - This will work if no additional packages (not already in the venv) are requested at the pex creation, or fallbacks to standard pex creation
  - If uv is installed, it will be used to speed up the previous use case by creating a transient venv with additional requirements, 
which is faster than pex's default way of building the pex
  - also adds an optimization to use multithreading when running the pex the first time (`--max-install-jobs 0`)
  - Can be deactivated by setting it to 0 in case of issues

- C_PACK_USE_ZIPFILE (default 1): use zipfile module to create the zip archive instead of shutil.make_archive
  - This is another python lib to create zip files, which allows to control the compression level.
  - Can be deactivated by setting it to 0 in case of issues

- C_PACK_ZIP_COMPRESSION_LEVEL (default 0, i.e. no compression): compression level to use when creating the zip archive
  - Currently, we only explicitly zip pex files that have been built with `--layout packed` which already produces compressed files.
  - In that case, there is no advantage in compressing again, so the default level is 0 which speeds up a lot `large_pex` creation.
  - Only used if C_PACK_USE_ZIPFILE is set to 1

## Features

- Ships a package with all the dependencies from your current virtual environment

- Stores metadata for an environment

- Supports "under development" mode by taking advantage of pip's [editable installs mode][editable_installs_mode], all editable requirements will be uploaded all the time, making local changes directly visible on the cluster

- Interactive (Jupyter notebook) mode

- Provides config helpers to directly use the uploaded zip file inside your application

- Launching jobs from jobs by propagating all artifacts


## Basic examples with [skein][skein]

1) [Interactive mode](https://github.com/criteo/cluster-pack/blob/master/examples/interactive-mode/README.md)

2) [Self shipping project](https://github.com/criteo/cluster-pack/blob/master/examples/skein-project/README.md)


## Basic examples with [PySpark](https://spark.apache.org/docs/latest/quick-start.html)

1) [PySpark with HDFS on Yarn](https://github.com/criteo/cluster-pack/blob/master/examples/spark/spark_example.py)

2) [Docker with PySpark on S3](https://github.com/criteo/cluster-pack/blob/master/examples/spark-with-S3/README.md)

[pex]: https://github.com/pantsbuild/pex
[editable_installs_mode]: https://pip.pypa.io/en/stable/reference/pip_install/#editable-installs
[skein]: https://jcrist.github.io/skein/


## Info

Conda is no longer supported