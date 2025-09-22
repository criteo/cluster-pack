import os
import setuptools
import versioneer

here = os.path.abspath(os.path.dirname(__file__))

DESCRIPTION = (
    "A library on top of pex to make your Python code easily available on a cluster"
)

try:
    LONG_DESCRIPTION = open(os.path.join(here, "README.md"), encoding="utf-8").read()
except Exception:
    LONG_DESCRIPTION = ""


def _read_reqs(relpath):
    fullpath = os.path.join(os.path.dirname(__file__), relpath)
    with open(fullpath) as f:
        return [
            s.strip() for s in f.readlines() if (s.strip() and not s.startswith("#"))
        ]


REQUIREMENTS = _read_reqs("requirements.txt")

CLASSIFIERS = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "Intended Audience :: Science/Research",
    "Operating System :: POSIX :: Linux",
    "Environment :: Console",
    "License :: OSI Approved :: Apache Software License",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Topic :: Software Development :: Libraries",
]


setuptools.setup(
    name="cluster-pack",
    packages=setuptools.find_packages(),
    version=versioneer.get_version(),
    install_requires=REQUIREMENTS,
    tests_require=["pytest"],
    python_requires=">=3.9",
    maintainer="Criteo",
    maintainer_email="github@criteo.com",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    classifiers=CLASSIFIERS,
    keywords="hadoop distributed cluster S3 HDFS",
    url="https://github.com/criteo/cluster-pack",
)
