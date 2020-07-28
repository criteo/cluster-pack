import contextlib
import json
import os
import pyarrow
import subprocess
from subprocess import check_output
import sys
import shutil
import tempfile
from unittest import mock
import zipfile

import pytest

from pex.pex_info import PexInfo

from cluster_pack import packaging, filesystem


MODULE_TO_TEST = "cluster_pack.packaging"
MYARCHIVE_FILENAME = "myarchive.pex"
MYARCHIVE_METADATA = "myarchive.json"
VARNAME = 'VARNAME'


def test_get_virtualenv_name():
    with mock.patch.dict('os.environ'):
        os.environ[VARNAME] = '/path/to/my_venv'
        assert 'my_venv' == packaging.get_env_name(VARNAME)


def test_get_virtualenv_empty_returns_default():
    with mock.patch.dict('os.environ'):
        if VARNAME in os.environ:
            del os.environ[VARNAME]
        assert 'default' == packaging.get_env_name(VARNAME)


def test_get_empty_editable_requirements():
    with tempfile.TemporaryDirectory() as tempdir:
        _create_venv(tempdir)
        subprocess.check_call([
                        f"{tempdir}/bin/python", "-m", "pip", "install",
                        "cloudpickle", _get_editable_package_name(), "pip==18.1"
                        ])
        editable_requirements = packaging._get_editable_requirements(f"{tempdir}/bin/python")
        assert len(editable_requirements) == 0


def test_get_empty_non_editable_requirements():
    with tempfile.TemporaryDirectory() as tempdir:
        _create_venv(tempdir)
        subprocess.check_call([
                    f"{tempdir}/bin/python", "-m", "pip", "install",
                    "-e", _get_editable_package_name(), "pip==18.1"
                    ])
        non_editable_requirements = packaging.get_non_editable_requirements(
            f"{tempdir}/bin/python")
        assert len(non_editable_requirements) == 0


def test__get_editable_requirements():
    with tempfile.TemporaryDirectory() as tempdir:
        _create_venv(tempdir)
        _pip_install(tempdir)
        editable_requirements = packaging._get_editable_requirements(f"{tempdir}/bin/python")
        assert len(editable_requirements) == 1
        assert os.path.basename(editable_requirements[0]) == "user_lib"


def test_get_non_editable_requirements():
    with tempfile.TemporaryDirectory() as tempdir:
        _create_venv(tempdir)
        _pip_install(tempdir)
        non_editable_requirements = packaging.get_non_editable_requirements(
            f"{tempdir}/bin/python")
        assert len(non_editable_requirements) == 1
        assert list(non_editable_requirements.keys())[0] == "cloudpickle"


def _create_venv(tempdir: str):
    subprocess.check_call([sys.executable, "-m", "venv", f"{tempdir}"])


def _pip_install(tempdir: str):
    subprocess.check_call([f"{tempdir}/bin/python", "-m", "pip", "install",
                           "cloudpickle", "pip==18.1"])
    pkg = _get_editable_package_name()
    subprocess.check_call([f"{tempdir}/bin/python", "-m", "pip", "install", "-e", pkg])
    if pkg not in sys.path:
        sys.path.append(pkg)


def _get_editable_package_name():
    return os.path.join(os.path.dirname(__file__), "user-lib")


def test_get_current_pex_filepath():
    mock_pex = mock.Mock()
    mock_pex.__file__ = './current_directory/filename.pex/.bootstrap/_pex/__init__.pyc'
    sys.modules['_pex'] = mock_pex
    assert packaging.get_current_pex_filepath() == \
        os.path.join(os.getcwd(), 'current_directory/filename.pex')
    del sys.modules['_pex']


def test_get_editable_requirements():
    with mock.patch(f"{MODULE_TO_TEST}._running_from_pex") as mock_running_from_pex:
        mock_running_from_pex.return_value = True
        with tempfile.TemporaryDirectory() as tempdir:
            pkg = _get_editable_package_name()
            _create_editable_files(tempdir, os.path.basename(pkg))
            shutil.copytree(pkg, f"{tempdir}/{os.path.basename(pkg)}")

            editable_requirements = packaging.get_editable_requirements(
                editable_packages_dir=tempdir
            )
            assert editable_requirements == {os.path.basename(pkg): pkg}


def test_zip_path(tmpdir):
    s = "Hello, world!"
    tmpdir.mkdir("foo").join("bar.txt").write_text(s, encoding="utf-8")
    tmpdir.mkdir("py-lib").join("bar.py").write_text(s, encoding="utf-8")
    b = 0xffff.to_bytes(4, "little")
    tmpdir.join("boo.bin").write_binary(b)

    with tempfile.TemporaryDirectory() as tempdirpath:
        zipped_path = packaging.zip_path(str(tmpdir), False, tempdirpath)
        assert os.path.isfile(zipped_path)
        assert zipped_path.endswith(".zip")
        assert zipfile.is_zipfile(zipped_path)
        with zipfile.ZipFile(zipped_path) as zf:
            zipped = {zi.filename for zi in zf.filelist}
            assert "foo/bar.txt" in zipped
            assert "py-lib/bar.py" in zipped
            assert "boo.bin" in zipped

            assert zf.read("foo/bar.txt") == s.encode()
            assert zf.read("py-lib/bar.py") == s.encode()
            assert zf.read("boo.bin") == b


def _create_editable_files(tempdir, pkg):
    with open(f"{tempdir}/{packaging.EDITABLE_PACKAGES_INDEX}", "w") as file:
        for repo in [pkg, "not-existing-pgk"]:
            file.write(repo + "\n")


@contextlib.contextmanager
def does_not_raise():
    yield


# check fix of https://issues.apache.org/jira/browse/ARROW-5130
# which only works with pulled manylinux2010 wheel
@pytest.mark.parametrize(
    "pyarrow_version,expectation",
    [
        ("0.14.1", does_not_raise()),
        ("0.13.0", pytest.raises(subprocess.CalledProcessError)),
    ]
)
def test_pack_in_pex(pyarrow_version, expectation):
    with tempfile.TemporaryDirectory() as tempdir:
        requirements = ["tensorflow==1.15.0", f"pyarrow=={pyarrow_version}"]
        packaging.pack_in_pex(
            requirements,
            f"{tempdir}/out.pex",
            # make isolated pex from current pytest virtual env
            pex_inherit_path="false")
        assert os.path.exists(f"{tempdir}/out.pex")
        with expectation:
            print(subprocess.check_output([
                f"{tempdir}/out.pex",
                "-c",
                ("""print("Start importing pyarrow and tensorflow..");"""
                 """import pyarrow; import tensorflow;"""
                 """print("Successfully imported pyarrow and tensorflow!")""")]
            ))


def test_pack_in_pex_include_editable_requirements():
    requirements = {}
    requirement_dir = _get_editable_package_name()
    with tempfile.TemporaryDirectory() as tempdir:
        packaging.pack_in_pex(
            requirements,
            f"{tempdir}/out.pex",
            # make isolated pex from current pytest virtual env
            pex_inherit_path="false",
            editable_requirements={os.path.basename(requirement_dir): requirement_dir})
        assert os.path.exists(f"{tempdir}/out.pex")
        with does_not_raise():
            print(subprocess.check_output([
                f"{tempdir}/out.pex",
                "-c",
                ("""print("Start importing user-lib..");import user_lib;"""
                 """print("Successfully imported user-lib!")""")]
            ))


def test_pack_in_pex_from_spec():
    with tempfile.TemporaryDirectory() as tempdir:
        spec_file = os.path.join(os.path.dirname(__file__), "resources", "requirements.txt")
        packaging.pack_spec_in_pex(
            spec_file,
            f"{tempdir}/out.pex",
            # make isolated pex from current pytest virtual env
            pex_inherit_path="false")
        assert os.path.exists(f"{tempdir}/out.pex")
        with does_not_raise():
            print(subprocess.check_output([
                f"{tempdir}/out.pex",
                "-c",
                ("print('Start importing cloudpickle..');import cloudpickle;"
                 "assert cloudpickle.__version__ == '1.4.1'")]
            ))
