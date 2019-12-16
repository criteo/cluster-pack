# cluster-pack

cluster-pack is a library on top of either [pex][pex] or [conda-pack][conda-pack] to make your Python code easily available on a cluster.

Its goal is to make your prod/dev Python code & libraries easiliy available on any cluster. cluster-pack supports HDFS/S3 as a distributed storage.

The first examples use [skein][skein-github]. We will add more examples for other applications (like Pyspark, Dask) with other compute clusters (like mesos, kubernetes) soon.


## Installation

```bash
$ git clone https://github.com/criteo/cluster-pack
$ cd cluster-pack
$ pip install .
```

## Prerequisites

cluster-pack supports Python ≥3.6.

## Features

- ships a package with all the dependencies from your current virtual environment or your conda environment
- provides config helpers to inject those dependencies to your application
- when using pip with pex cluster-pack takes advantage of pip's [editable installs mode][editable installs mode], all editable requirements will be uploaded all the time separatly, making local changes direclty visible on the cluster, and not requiring to regenerate the packacke with all the dependencies again


## Basic examples with [skein][skein]

1) [Interactive mode](https://github.com/criteo/cluster-pack/blob/master/examples/skein-project/README.md) 

2) [Self shipping project](https://github.com/criteo/cluster-pack/blob/master/examples/interactive-mode/README.md) 

[pex]: https://github.com/pantsbuild/pex
[conda-pack]: https://github.com/conda/conda-pack
[editable installs mode]: https://pip.pypa.io/en/stable/reference/pip_install/#editable-installs
[skein]: https://jcrist.github.io/skein/quickstart.html
