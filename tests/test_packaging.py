import contextlib
import os
import stat
import subprocess
import sys
import shutil
import tempfile
import getpass
from unittest import mock
import zipfile

import pytest

from cluster_pack import packaging, get_pyenv_usage_from_archive, uploader
from cluster_pack.packaging import (
    UNPACKED_ENV_NAME,
    resolve_zip_from_pex_dir,
)

MODULE_TO_TEST = "cluster_pack.packaging"
MYARCHIVE_FILENAME = "myarchive.pex"
MYARCHIVE_METADATA = "myarchive.json"
VARNAME = "VARNAME"
PINNED_VERSIONS_FOR_COMPATIBILITY_ISSUE = {"numpy": "numpy<2"}


def test_get_virtualenv_name():
    with mock.patch.dict("os.environ"):
        os.environ[VARNAME] = "/path/to/my_venv"
        assert "my_venv" == packaging.get_env_name(VARNAME)


def test_get_virtualenv_empty_returns_default():
    with mock.patch.dict("os.environ"):
        if VARNAME in os.environ:
            del os.environ[VARNAME]
        assert "default" == packaging.get_env_name(VARNAME)


def test_get_empty_editable_requirements():
    with tempfile.TemporaryDirectory() as tempdir:
        _create_venv(tempdir)
        subprocess.check_call(
            [
                f"{tempdir}/bin/python",
                "-m",
                "pip",
                "install",
                "cloudpickle",
                _get_editable_package_name(),
                "pip==22.0",
            ]
        )
        editable_requirements = packaging._get_editable_requirements(
            f"{tempdir}/bin/python"
        )
        assert len(editable_requirements) == 0


def test_get_empty_non_editable_requirements():
    with tempfile.TemporaryDirectory() as tempdir:
        _create_venv(tempdir)
        subprocess.check_call(
            [
                f"{tempdir}/bin/python",
                "-m",
                "pip",
                "install",
                "-e",
                _get_editable_package_name(),
                "pip==22.0",
            ]
        )
        non_editable_requirements = packaging.get_non_editable_requirements(
            f"{tempdir}/bin/python"
        )
        assert len(non_editable_requirements) == 2
        assert list(non_editable_requirements.keys()) == ["pip", "setuptools"]


def test__get_editable_requirements():
    with tempfile.TemporaryDirectory() as tempdir:
        _create_venv(tempdir)
        _pip_install(tempdir)
        editable_requirements = packaging._get_editable_requirements(
            f"{tempdir}/bin/python"
        )
        assert len(editable_requirements) == 2
        pkg_names = [os.path.basename(req) for req in editable_requirements]
        assert "user_lib" in pkg_names
        assert "user_lib2" in pkg_names


def test__get_editable_requirements_for_src_layout():
    with tempfile.TemporaryDirectory() as tempdir:
        _create_venv(tempdir)
        _pip_install(tempdir, use_src_layout=True)
        editable_requirements = packaging._get_editable_requirements(
            f"{tempdir}/bin/python"
        )
        assert len(editable_requirements) == 2
        pkg_names = [os.path.basename(req) for req in editable_requirements]
        assert "user_lib" in pkg_names
        assert "user_lib2" in pkg_names


def test__get_editable_requirements_withpip23():
    with tempfile.TemporaryDirectory() as tempdir:
        _create_venv(tempdir)
        _pip_install(tempdir, "23.1")
        editable_requirements = packaging._get_editable_requirements(
            f"{tempdir}/bin/python"
        )
        assert len(editable_requirements) == 2
        pkg_names = [os.path.basename(req) for req in editable_requirements]
        assert "user_lib" in pkg_names
        assert "user_lib2" in pkg_names


def test_get_non_editable_requirements():
    with tempfile.TemporaryDirectory() as tempdir:
        _create_venv(tempdir)
        _pip_install(tempdir)
        non_editable_requirements = packaging.get_non_editable_requirements(
            f"{tempdir}/bin/python"
        )
        assert len(non_editable_requirements) == 3
        assert list(non_editable_requirements.keys()) == [
            "cloudpickle",
            "pip",
            "setuptools",
        ]


def _create_venv(tempdir: str):
    subprocess.check_call([sys.executable, "-m", "venv", f"{tempdir}"])


def _pip_install(tempdir: str, pip_version: str = "22.0", use_src_layout: bool = False):
    subprocess.check_call(
        [
            f"{tempdir}/bin/python",
            "-m",
            "pip",
            "install",
            "cloudpickle",
            f"pip=={pip_version}",
        ]
    )
    pkg = (
        _get_editable_package_name_src_layout()
        if use_src_layout
        else _get_editable_package_name()
    )
    subprocess.check_call([f"{tempdir}/bin/python", "-m", "pip", "install", "-e", pkg])
    if pkg not in sys.path:
        sys.path.append(pkg)


def _get_editable_package_name():
    return os.path.join(os.path.dirname(__file__), "user-lib")


def _get_editable_package_name_src_layout():
    return os.path.join(os.path.dirname(__file__), "user-lib-src-layout")


def test_get_current_pex_filepath():
    with tempfile.TemporaryDirectory() as tempdir:
        path_to_pex = f"{tempdir}/out.pex"
        packaging.pack_in_pex(
            [PINNED_VERSIONS_FOR_COMPATIBILITY_ISSUE["numpy"]],
            path_to_pex,
            # make isolated pex from current pytest virtual env
            pex_inherit_path="false",
        )
        assert os.path.exists(path_to_pex)
        subprocess.check_output(
            [
                path_to_pex,
                "-c",
                (
                    """import os;"""
                    """assert "PEX" in os.environ;"""
                ),
            ]
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
    b = 0xFFFF.to_bytes(4, "little")
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
        ("6.0.1", does_not_raise()),
        ("0.13.0", pytest.raises(subprocess.CalledProcessError)),
    ],
)
def test_pack_in_pex(pyarrow_version, expectation):
    if sys.version_info.minor in {8, 9} and pyarrow_version == "0.13.0":
        return
    with tempfile.TemporaryDirectory() as tempdir:
        requirements = [
            "protobuf==3.19.6",
            "tensorflow==2.5.2",
            "tensorboard==2.10.1",
            f"pyarrow=={pyarrow_version}",
        ]
        packaging.pack_in_pex(
            requirements, f"{tempdir}/out.pex", pex_inherit_path="false"
        )
        assert os.path.exists(f"{tempdir}/out.pex")
        with expectation:
            print(
                subprocess.check_output(
                    [
                        f"{tempdir}/out.pex",
                        "-c",
                        (
                            """print("Start importing pyarrow and tensorflow..");"""
                            """import pyarrow; import tensorflow;"""
                            """print("Successfully imported pyarrow and tensorflow!")"""
                        ),
                    ]
                )
            )


def test_pack_in_pex_with_include_tools():
    with tempfile.TemporaryDirectory() as tempdir:
        requirements = [
            PINNED_VERSIONS_FOR_COMPATIBILITY_ISSUE["numpy"],
            "pyarrow==6.0.1",
        ]
        packaging.pack_in_pex(
            requirements,
            f"{tempdir}/out.pex",
            # make isolated pex from current pytest virtual env
            pex_inherit_path="false",
            include_pex_tools=True,
        )
        assert os.path.exists(f"{tempdir}/out.pex")
        cmd = (
            'print("Start importing pyarrow.."); '
            "import pyarrow; "
            'print("Successfully imported pyarrow!")'
        )

        with does_not_raise():
            print(
                subprocess.check_output(
                    (
                        f"PEX_TOOLS=1 {tempdir}/out.pex venv {tempdir}/pex_venv "
                        f"&& . {tempdir}/pex_venv/bin/activate "
                        f"&& python -c '{cmd}'"
                    ),
                    shell=True,
                )
            )


def test_pack_in_pex_with_additional_repo():
    with tempfile.TemporaryDirectory() as tempdir:
        requirements = [
            "torch",
            "typing-extensions<=3.7.4.3; python_version<'3.8'",
            "networkx<2.6; python_version<'3.9'",
        ]
        packaging.pack_in_pex(
            requirements,
            f"{tempdir}/out.pex",
            # make isolated pex from current pytest virtual env
            pex_inherit_path="false",
            additional_repo="https://download.pytorch.org/whl/cpu",
        )

        assert os.path.exists(f"{tempdir}/out.pex")
        with does_not_raise():
            print(
                subprocess.check_output(
                    [
                        f"{tempdir}/out.pex",
                        "-c",
                        (
                            """print("Start importing torch..");"""
                            """import torch;"""
                            """print("Successfully imported torch!")"""
                        ),
                    ]
                )
            )


def test_pack_in_pex_include_editable_requirements():
    requirements = {}
    requirement_dir = os.path.join(os.path.dirname(__file__), "user-lib", "user_lib")
    with tempfile.TemporaryDirectory() as tempdir:
        packaging.pack_in_pex(
            requirements,
            f"{tempdir}/out.pex",
            # make isolated pex from current pytest virtual env
            pex_inherit_path="false",
            editable_requirements={os.path.basename(requirement_dir): requirement_dir},
        )
        assert os.path.exists(f"{tempdir}/out.pex")
        with does_not_raise():
            print(
                subprocess.check_output(
                    [
                        f"{tempdir}/out.pex",
                        "-c",
                        (
                            """print("Start importing user-lib..");import user_lib;"""
                            """print("Successfully imported user-lib!")"""
                        ),
                    ]
                )
            )


def test_pack_in_pex_from_spec():
    with tempfile.TemporaryDirectory() as tempdir:
        spec_file = os.path.join(
            os.path.dirname(__file__), "resources", "requirements.txt"
        )
        packaging.pack_spec_in_pex(
            spec_file,
            f"{tempdir}/out.pex",
            # make isolated pex from current pytest virtual env
            pex_inherit_path="false",
        )
        assert os.path.exists(f"{tempdir}/out.pex")
        with does_not_raise():
            print(
                subprocess.check_output(
                    [
                        f"{tempdir}/out.pex",
                        "-c",
                        (
                            "print('Start importing cloudpickle..');import cloudpickle;"
                            "assert cloudpickle.__version__ == '1.4.1'"
                        ),
                    ]
                )
            )


def test_get_packages():
    subprocess.check_output = mock.Mock(return_value='{"key": "value"}'.encode())
    packages = packaging._get_packages(False)
    expected_packages = {"key": "value"}
    assert packages == expected_packages


def test_get_packages_with_warning():
    subprocess.check_output = mock.Mock(
        return_value='{"key": "value"}\nwarning'.encode()
    )
    packages = packaging._get_packages(False)
    expected_packages = {"key": "value"}
    assert packages == expected_packages


test_data = [
    ("/path/to/myenv.pex", "./myenv.pex", "myenv.pex"),
]


@pytest.mark.parametrize("path_to_archive, expected_cmd, expected_dest_path", test_data)
def test_gen_pyenvs_from_existing_env(
    path_to_archive, expected_cmd, expected_dest_path
):
    result = get_pyenv_usage_from_archive(path_to_archive)
    assert result.path_to_archive == path_to_archive
    assert result.interpreter_cmd == expected_cmd
    assert result.dest_path == expected_dest_path


def test_gen_pyenvs_from_unknown_format():
    with pytest.raises(ValueError):
        get_pyenv_usage_from_archive("/path/to/pack.tar.bz2")


archive_test_data = [
    (False, "dummy/path/exe.pex", False, False, "dummy/path/exe.pex"),
    (False, "dummy/path/exe.pex", True, False, "dummy/path/exe.pex.zip"),
    (True, "dummy/path/exe.pex", False, False, "dummy/path/exe.pex"),
    (True, "dummy/path/exe.pex", True, False, "dummy/path/exe.pex.zip"),
    (False, None, False, False, f"hdfs:///user/{getpass.getuser()}/envs/venv_exe.pex"),
    (False, None, None, False, f"hdfs:///user/{getpass.getuser()}/envs/venv_exe.pex"),
    (
        False,
        None,
        True,
        False,
        f"hdfs:///user/{getpass.getuser()}/envs/venv_exe.pex.zip",
    ),
    (True, None, False, False, f"hdfs:///user/{getpass.getuser()}/envs/pex_exe.pex"),
    (True, None, True, False, f"hdfs:///user/{getpass.getuser()}/envs/pex_exe.pex.zip"),
    (True, None, None, False, f"hdfs:///user/{getpass.getuser()}/envs/pex_exe.pex"),
    (True, None, None, True, f"hdfs:///user/{getpass.getuser()}/envs/pex_exe.pex.zip"),
]


@pytest.mark.parametrize(
    "running_from_pex, package_path, is_dir, expected",
    archive_test_data,
)
def test_detect_archive_names(running_from_pex, package_path, is_dir, expected):
    with contextlib.ExitStack() as stack:
        mock_running_from_pex = stack.enter_context(
            mock.patch(f"{MODULE_TO_TEST}._running_from_pex")
        )
        mock_current_filepath = stack.enter_context(
            mock.patch(f"{MODULE_TO_TEST}.get_current_pex_filepath")
        )
        mock_fs = stack.enter_context(mock.patch(f"{MODULE_TO_TEST}.get_default_fs"))
        mock_venv = stack.enter_context(mock.patch(f"{MODULE_TO_TEST}.get_env_name"))
        mock_is_dir = stack.enter_context(mock.patch("os.path.isdir"))
        mock_glob = stack.enter_context(mock.patch("glob.glob"))

        mock_running_from_pex.return_value = running_from_pex
        mock_current_filepath.return_value = "pex_exe.pex"
        mock_fs.return_value = "hdfs://"
        mock_venv.return_value = "venv_exe"
        mock_is_dir.return_value = is_dir
        mock_glob.return_value = ["pex_exe.pex.zip"]
        actual, _, _ = packaging.detect_archive_names(
            packaging.PEX_PACKER, package_path
        )
        assert actual == expected


def test_resolve_zip_from_pex_dir_with_1_pex():
    # the function should work even if file and dir names are different if we handle only 1 pex
    with tempfile.TemporaryDirectory() as tempdir:
        open(os.path.join(tempdir, "pex1.pex.zip"), "a").close()
        pex_dir = os.path.join(tempdir, "pex_dir")
        os.mkdir(pex_dir)

        resolved = resolve_zip_from_pex_dir(pex_dir)

    assert resolved == os.path.join(tempdir, "pex1.pex.zip")


def test_resolve_zip_from_pex_dir_with_2_pexes_with_correct_names():
    with tempfile.TemporaryDirectory() as tempdir:
        open(os.path.join(tempdir, "pex1.pex.zip"), "a").close()
        pex1_dir = os.path.join(tempdir, "pex1.pex")
        os.mkdir(pex1_dir)

        open(os.path.join(tempdir, "pex2.pex.zip"), "a").close()
        pex2_dir = os.path.join(tempdir, "pex2.pex")
        os.mkdir(pex2_dir)

        assert resolve_zip_from_pex_dir(pex1_dir) == os.path.join(
            tempdir, "pex1.pex.zip"
        )
        assert resolve_zip_from_pex_dir(pex2_dir) == os.path.join(
            tempdir, "pex2.pex.zip"
        )


def test_resolve_zip_from_pex_dir_with_2_pexes_with_overlapping_names():
    with tempfile.TemporaryDirectory() as tempdir:
        open(os.path.join(tempdir, "pex1.pex.zip"), "a").close()
        pex1_dir = os.path.join(tempdir, "pex1.pex")
        os.mkdir(pex1_dir)

        open(os.path.join(tempdir, "other_pex1.pex.zip"), "a").close()
        pex2_dir = os.path.join(tempdir, "other_pex1.pex")
        os.mkdir(pex2_dir)

        assert resolve_zip_from_pex_dir(pex1_dir) == os.path.join(
            tempdir, "pex1.pex.zip"
        )
        assert resolve_zip_from_pex_dir(pex2_dir) == os.path.join(
            tempdir, "other_pex1.pex.zip"
        )


def test_resolve_zip_from_pex_dir_with_2_pexes_with_wrong_names():
    with tempfile.TemporaryDirectory() as tempdir:
        open(os.path.join(tempdir, "pex1.pex.zip"), "a").close()
        pex1_dir = os.path.join(tempdir, "pex_dir.pex")
        os.mkdir(pex1_dir)

        open(os.path.join(tempdir, "pex2.pex.zip"), "a").close()

        with pytest.raises(ValueError, match=r".zip not found"):
            resolve_zip_from_pex_dir(pex1_dir)


def test_resolve_zip_from_pex_dir_with_no_zip_found():
    with tempfile.TemporaryDirectory() as tempdir:
        pex1_dir = os.path.join(tempdir, "pex_dir.pex")
        os.mkdir(pex1_dir)

        with pytest.raises(ValueError, match=r".zip not found"):
            resolve_zip_from_pex_dir(pex1_dir)
