## Self shipping project with [skein][skein]


1) Prepare a [virtual environment][virtual environment] and install the [sample project](https://github.com/criteo/cluster-pack/tree/master/examples/skein-project) using skein

```bash
$ cd examples/skein-project
$ python3.6 -m venv skein_env
$ . skein_env/bin/activate
$ pip install --upgrade pip setuptools
$ pip install -e .
python
```

2) Upload current virtual environment to the distributed storage (HDFS in this case)

```python
import cluster_pack
package_path, _ = cluster_pack.upload_env()
```

3) Call skein config helper to get the config that easily accesses those uploaded packages on the cluster, [`skein_project.worker`][skein_project.worker] is the module we want to call remotely. It has been shipped by cluster-pack

```python
from cluster_pack.skein import skein_config_builder
skein_config = skein_config_builder.build(
    module_name="skein_project.worker",
    package_path=package_path
)
```

4) Submit a simple skein application

```python
import skein
with skein.Client() as client:
    service = skein.Service(
        resources=skein.model.Resources("1 GiB", 1),
        files=skein_config.files,
        script=skein_config.script
    )
    spec = skein.ApplicationSpec(services={"service": service})
    app_id = client.submit(spec)
```

[skein_project.worker]: https://github.com/criteo/cluster-pack/blob/master/cluster_pack/examples/skein-project/skein_project/worker.py
[virtual environment]: https://docs.python.org/3/tutorial/venv.html
[skein]: https://jcrist.github.io/skein/quickstart.html
