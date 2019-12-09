# cluster-pack

cluster-pack is a library on top of either [pex][pex] or [conda-pack][conda-pack] to make your Python code easily available on a cluster.

Its goal is to make your prod/dev Python code & libraries easiliy available on any cluster. cluster-pack supports HDFS/S3 as a distributed storage.

The first example uses [skein][skein-github]. We will add more examples for other applications (like Pyspark, Dask) with other compute clusters (like mesos, kubernetes) soon.


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


## Basic example with [skein][skein]


1) Prepare a [virtual environment][virtual environment] and install the sample project using skein

```bash
$ cd examples/skein-project
$ python3.6 -m venv skein_env
$ . skein_env/bin/activate
$ pip install --upgrade pip
$ pip install .
python
```

2) Upload current virtual environment to the distributed storage

```python
package_path, _ = packaging.upload_env_to_hdfs()
```

3) Call skein config helper to get the config that easily accesses those uploaded packages on the cluster,
   [`skein_project.worker`][skein_project.worker] is the module we want to call remotly (it has been shipped by cluster-pack)

```python
script = skein_config_builder.get_script(
    package_path, 
    module_name="skein_project.worker")
files = skein_config_builder.get_files(package_path)
```

4) Submit a simple skein application

```python
 with skein.Client() as client:
        service = skein.Service(
            resources=skein.model.Resources("1 GiB", 1),
            files=files,
            script=script
        )
        spec = skein.ApplicationSpec(services={"service": service})
        app_id = client.submit(spec)
```

[pex]: (https://github.com/pantsbuild/pex)
[conda-pack]: (https://github.com/conda/conda-pack)
[editable installs mode]: (https://pip.pypa.io/en/stable/reference/pip_install/#editable-installs)
[skein_project.worker]: https://github.com/criteo/cluster-pack/blob/master/cluster_pack/examples/skein-project/skein_project/worker.py
[virtual environment]: (https://docs.python.org/3/tutorial/venv.html)
[skein-github]: (https://github.com/jcrist/skein)
[skein]: (https://jcrist.github.io/skein/quickstart.html)
