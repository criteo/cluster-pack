## Interactive example with [skein][skein]

Here is an interactive example Skein and HDFS storage with a virtual environment. You can also execute it directly in a Jupyter notebook.

1) Prepare a [virtual environment][virtual environment] with skein & numpy

```bash
$ cd examples/interactive-mode
$ python3.6 -m venv venv
$ . venv/bin/activate
$ pip install cluster-pack numpy skein
python
```

2) Define the workload to execute remotely

```python
def compute_intersection():
    a = np.random.random_integers(0, 100, 100)
    b = np.random.random_integers(0, 100, 100)
    print("Computed intersection of two arrays:")
    print(np.intersect1d(a, b))
```

3) Upload current virtual environment to the distributed storage (HDFS in this case)

```python
import cluster_pack
package_path, _ = cluster_pack.upload_env()
```

4) Call skein config helper to get the config that easily executes this function on the cluster

```python
from cluster_pack.skein import skein_config_builder
skein_config = skein_config_builder.build_with_func(
    func=compute_intersection,
    package_path=package_path
)
```

5) Submit a simple skein application

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

[virtual environment]: https://docs.python.org/3/tutorial/venv.html
[skein]: https://jcrist.github.io/skein/quickstart.html
