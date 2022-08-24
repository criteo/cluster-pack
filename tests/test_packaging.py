import contextlib
import os
import stat
import subprocess
import sys
import shutil
import tempfile
from unittest import mock
import zipfile

import pytest

from cluster_pack import packaging


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
        assert len(non_editable_requirements) == 2
        assert list(non_editable_requirements.keys()) == ["pip", "setuptools"]


def test__get_editable_requirements():
    with tempfile.TemporaryDirectory() as tempdir:
        _create_venv(tempdir)
        _pip_install(tempdir)
        editable_requirements = packaging._get_editable_requirements(f"{tempdir}/bin/python")
        assert len(editable_requirements) == 2
        pkg_names = [os.path.basename(req) for req in editable_requirements]
        assert "user_lib" in pkg_names
        assert "user_lib2" in pkg_names


def test_get_non_editable_requirements():
    with tempfile.TemporaryDirectory() as tempdir:
        _create_venv(tempdir)
        _pip_install(tempdir)
        non_editable_requirements = packaging.get_non_editable_requirements(
            f"{tempdir}/bin/python")
        assert len(non_editable_requirements) == 3
        assert list(non_editable_requirements.keys()) == ["cloudpickle", "pip", "setuptools"]


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
    with tempfile.TemporaryDirectory() as tempdir:
        path_to_pex = f"{tempdir}/out.pex"
        packaging.pack_in_pex(
            ["numpy"],
            path_to_pex,
            # make isolated pex from current pytest virtual env
            pex_inherit_path="false")
        assert os.path.exists(path_to_pex)
        subprocess.check_output([
            path_to_pex,
            "-c",
            ("""import os;"""
             """assert "PEX" in os.environ;""")]
        )


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


def test_pack_in_pex_with_allow_large():
    with tempfile.TemporaryDirectory() as tempdir:
        requirements = ["pyarrow==0.14.1"]
        packaging.pack_in_pex(
            requirements,
            f"{tempdir}/out.pex",
            # make isolated pex from current pytest virtual env
            pex_inherit_path="false",
            allow_large_pex=True)
        assert os.path.exists(f"{tempdir}/out.pex")

        with tempfile.TemporaryDirectory() as temp_pex_dir:
            shutil.unpack_archive(f"{tempdir}/out.pex", temp_pex_dir, 'zip')
            st = os.stat(f"{temp_pex_dir}/__main__.py")
            os.chmod(f"{temp_pex_dir}/__main__.py", st.st_mode | stat.S_IEXEC)

            with does_not_raise():
                print(subprocess.check_output([
                    f"{temp_pex_dir}/__main__.py",
                    "-c",
                    ("""print("Start importing pyarrow..");"""
                     """import pyarrow;"""
                     """print("Successfully imported pyarrow!")""")]
                ))


def test_pack_in_pex_with_additional_repo():
    with tempfile.TemporaryDirectory() as tempdir:
        requirements = ["setuptools", "torch==1.10.1.0"]
        packaging.pack_in_pex(
            requirements,
            f"{tempdir}/out.pex",
            # make isolated pex from current pytest virtual env
            pex_inherit_path="false",
            additional_repo="https://download.pytorch.org/whl/cu113")

        assert os.path.exists(f"{tempdir}/out.pex")
        with does_not_raise():
            print(subprocess.check_output([
                f"{tempdir}/out.pex",
                "-c",
                ("""print("Start importing torch..");"""
                 """import torch;"""
                 """print("Successfully imported torch!")""")]
            ))


def test_pack_in_pex_include_editable_requirements():
    requirements = {}
    requirement_dir = os.path.join(os.path.dirname(__file__), "user-lib", "user_lib")
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


def test_get_packages():
    subprocess.check_output = mock.Mock(return_value='{"key": "value"}'.encode())
    packages = packaging._get_packages(False)
    expected_packages = {"key": "value"}
    assert packages == expected_packages


def test_get_packages_with_warning():
    subprocess.check_output = mock.Mock(return_value='{"key": "value"}\nwarning'.encode())
    packages = packaging._get_packages(False)
    expected_packages = {"key": "value"}
    assert packages == expected_packages
