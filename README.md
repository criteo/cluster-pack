# cluster-pack

cluster-pack is a library on top of either [pex][pex] (Conda support has been discontinued)to make your Python code easily
available on a cluster.

Its goal is to easily deploy your prod/dev Python code & libraries on any cluster. cluster-pack supports HDFS/S3 as a distributed storage.

The first examples use [Skein][skein] (a simple library for deploying applications on Apache YARN) and [PySpark](https://spark.apache.org/docs/latest/quick-start.html) with HDFS storage.
We intend to add more examples for other applications (like [Dask](https://dask.org/), [Ray](https://ray.readthedocs.io/en/latest/index.html)) and S3 storage.

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

- Cluster-pack supports Python ≥3.9.
- Cluster-pack can speed up pex creation by using uv if available.

## Feature flags
- C_PACK_USER: override the current user for HDFS path generation and Skein impersonation
  - When set, this value is used instead of the system user (from `getpass.getuser()`)
  - Useful for running jobs as a different user or in environments where the system user doesn't match the HDFS user
  - If not set or empty, falls back to the current system user


- C_PACK_VENV_OPTIMIZATION_LEVEL (default 1): uses an existing venv to speed up pex creation
  - A benchmark of level 1 is available below, which shows a reduction of pex creation time from ~760s to ~260s with an existing venv (not taking compression into account)
    - Benchmarks for level 2 pending
  - This will work if no additional packages (not already in the venv) are requested at the pex creation, or fallbacks to standard pex creation
  - If uv is installed, it will be used to speed up the previous use case by creating a transient venv with additional requirements, 
which is faster than pex's default way of building the pex
  - also adds an optimization to use multithreading when running the pex the first time (`--max-install-jobs 0`)
  - Can be deactivated by setting it to 0 in case of issues


- C_PACK_LAYOUT_OPTIMIZATION (default: SLOW_FAST_SMALL): controls pex layout and zip compression for `allow_large_pex=True`
  - **SLOW_FAST_SMALL**: layout packed, no additional compression - slowest build, fastest 1st execution, small artifact
  - **FAST_MID_BIG**: layout loose + no additional compression - fastest build, intermediate 1st execution time, biggest artifact
  - **MID_SLOW_SMALL**: layout loose + compression - intermediate build time, slowest 1st execution, small artifact
  - **DISABLED**: legacy behavior, slower build time than all other options, fast 1st execution (still slower than SLOW_FAST_SMALL, small artifact
    - Should only be used if other modes cause issues

| Mode | Creation | 1st Exec | Size |
|------|----------|----------|------|
| **SLOW_FAST_SMALL** (default) | 348.27s | 30.60s | 3737.0MB |
| **FAST_MID_BIG** | 27.65s | 40.36s | 6386.3MB |
| **MID_SLOW_SMALL** | 167.17s | 54.77s | 3877.9MB |
| **DISABLED** | 442.79s | 34.10s | 3726.4MB |

### Benchmarks
#### impact of C_PACK_VENV_OPTIMIZATION_LEVEL=1 on pex creation time
Performed on Intel(R) Xeon(R) Gold 6146 CPU with 8 Hyperthreads, all caches cold before each experiment:

**Disabled**
```shell
$ time pex -r requirements.txt --layout packed  -o test.pex
real    12m40.294s
user    7m4.401s
sys     0m25.296s
```
Total pex Creation time: 760s

**pex creation with intermediate uv venv creation**
```shell
$ uv venv --python 3.11
$ source .venv/bin/activate
(venv)$ time uv pip install -r requirements.txt
real    1m4.487s
user    1m36.739s
sys     0m11.801s
(venv)$ time pex --venv-repository  -r requirements.txt --layout packed  -o test.pex
real    4m18.429s
user    4m21.615s
```
Total pex Creation time: 64 + 258 = 322s
If the venv already exists, pex creation time is only 258s

#### ZIP methods comparison across PEX layouts
Performed on Intel(R) Xeon(R) Gold 6146 CPU with 8 Hyperthreads, with C_PACK_VENV_OPTIMIZATION_LEVEL=1:
(Note, this is not the same requirements.txt as above, this one is smaller)
```shell
========================================================================================================================
SUMMARY: ZIP METHODS COMPARISON ACROSS PEX LAYOUTS
========================================================================================================================

Layout       Zip Method               Pex Create   Compress       Size      Ratio    Extract   1st Exec        Total
--------------------------------------------------------------------------------------------------------------------------------------------
zipapp       N/A (already zipped)       337.86s     0.00s       3738.1MB      N/A     0.00s     17.36s        355.22s
packed       shutil                     342.51s   100.28s       3726.4MB    99.7%    17.44s     16.66s        476.89s
packed       zipfile_cl0                342.51s     5.76s       3737.0MB   100.0%    16.03s     14.57s        378.87s
packed       zipfile_cl1                342.51s    88.98s       3726.7MB    99.7%    17.22s     14.84s        463.54s
loose        shutil                      12.82s   329.91s       3738.2MB    58.6%    51.69s      4.16s        398.59s
loose        zipfile_cl0                 12.82s    14.83s       6386.3MB   100.1%    35.87s      4.49s         68.01s
loose        zipfile_cl1                 12.82s   154.35s       3877.9MB    60.8%    50.99s      3.78s        221.94s
```


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

