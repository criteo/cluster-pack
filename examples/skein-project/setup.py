import setuptools

setuptools.setup(
    name='skein_project',
    version='0.0.1',
    packages=setuptools.find_packages(),
    python_requires=">=3.7",
    install_requires=[
        "skein",
        "numpy",
        "cluster-pack"
    ],
    maintainer="Criteo",
    maintainer_email="github@criteo.com",
    url="https://github.com/criteo/cluster-pack"
)
