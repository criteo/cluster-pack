import contextlib
import getpass
import hashlib
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import zipfile
from importlib.metadata import version as pkg_version
from typing import List
from unittest import mock

import pytest

from cluster_pack import (
    packaging,
    get_pyenv_usage_from_archive,
    uploader,
)
from cluster_pack.packaging import (
    UNPACKED_ENV_NAME,
    LARGE_PEX_CMD,
    resolve_zip_from_pex_dir,
    check_large_pex,
    PexTooLargeError,
    UV_AVAILABLE,
)

MODULE_TO_TEST = "cluster_pack.packaging"
MYARCHIVE_FILENAME = "myarchive.pex"
MYARCHIVE_METADATA = "myarchive.json"
VARNAME = "VARNAME"


@pytest.fixture(params=[0, 1, 2])
def venv_optimization_level(request):
    """Fixture to test with all venv optimization levels."""
    original_level = packaging.VENV_OPTIMIZATION_LEVEL
    packaging.set_venv_optimization_level(request.param)
    yield request.param
    packaging.set_venv_optimization_level(original_level)


@pytest.fixture(scope="module")
def large_pex_unzipped():
    """Build a large pex once and share across tests, cleanup at the end."""
    tempdir = tempfile.mkdtemp()
    try:
        current_packages = packaging.get_non_editable_requirements(sys.executable)
        reqs = uploader._build_reqs_from_venv({}, current_packages, [])
        local_package_path = uploader._pack_from_venv(
            sys.executable, reqs, tempdir, include_editable=True, allow_large_pex=True
        )
        assert os.path.exists(local_package_path)

        unzipped_pex_path = local_package_path.replace(".zip", "")
        os.mkdir(unzipped_pex_path)
        shutil.unpack_archive(local_package_path, unzipped_pex_path)
        st = os.stat(f"{unzipped_pex_path}/__main__.py")
        os.chmod(f"{unzipped_pex_path}/__main__.py", st.st_mode | stat.S_IEXEC)
        yield unzipped_pex_path
    finally:
        shutil.rmtree(tempdir)


def test_get_virtualenv_name():
    with mock.patch.dict("os.environ"):
        os.environ[VARNAME] = "/path/to/my_venv"
        expected_hash = hashlib.sha1("/path/to/my_venv".encode("utf-8")).hexdigest()[:7]
        assert f"my_venv_{expected_hash}" == packaging.get_env_name(VARNAME)


def test_get_virtualenv_empty_returns_default():
    with mock.patch.dict("os.environ"):
        if VARNAME in os.environ:
            del os.environ[VARNAME]
        assert "default" == packaging.get_env_name(VARNAME)


def test_default_hdfs_pex_name_includes_venv_path_hash():
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch(f"{MODULE_TO_TEST}._running_from_pex", return_value=False))
        stack.enter_context(mock.patch(f"{MODULE_TO_TEST}.get_default_fs", return_value="hdfs://"))
        stack.enter_context(mock.patch(f"{MODULE_TO_TEST}.getpass.getuser", return_value="alice"))
        with mock.patch.dict("os.environ"):
            os.environ["VIRTUAL_ENV"] = "/home/alice/workspaces/proj/.venv"
            expected_hash = hashlib.sha1(
                "/home/alice/workspaces/proj/.venv".encode("utf-8")
            ).hexdigest()[:7]
            expected_env_name = f".venv_{expected_hash}"
            expected_path = f"hdfs:///user/alice/envs/{expected_env_name}.pex"

            actual_path, actual_env_name, actual_pex_file = packaging.detect_archive_names(
                packaging.PEX_PACKER, package_path=None, allow_large_pex=False
            )

            assert actual_pex_file == ""
            assert actual_env_name == expected_env_name
            assert actual_path == expected_path


def test_get_empty_editable_requirements():
    with tempfile.TemporaryDirectory() as tempdir:
        _create_venv(tempdir)
        _install_packages(tempdir, ["cloudpickle", _get_editable_package_name(), "pip==22.0"])
        editable_requirements = packaging._get_editable_requirements(
            f"{tempdir}/bin/python"
        )
        assert len(editable_requirements) == 0


def test_get_empty_non_editable_requirements():
    with tempfile.TemporaryDirectory() as tempdir:
        _create_venv(tempdir)
        _install_packages(tempdir, [_get_editable_package_name(), "pip==22.0"], editable=True)
        non_editable_requirements = packaging.get_non_editable_requirements(
            f"{tempdir}/bin/python"
        )
        assert list(non_editable_requirements.keys()) == ["packaging", "pip", "setuptools", "wheel"]


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
        assert list(non_editable_requirements.keys()) == [
            "cloudpickle",
            "packaging",
            "pip",
            "setuptools",
            "wheel"
        ]


def _install_packages(tempdir: str, packages: List[str], editable: bool = False):
    """Install packages into the venv, using uv if available."""
    cmd = (
        ["uv", "pip", "install", "--python", f"{tempdir}/bin/python"] if UV_AVAILABLE
        else [f"{tempdir}/bin/python", "-m", "pip", "install"]
    )
    if editable:
        cmd.append("-e")

    cmd.extend(packages)
    subprocess.check_call(cmd)


def _create_venv(tempdir: str):
    if UV_AVAILABLE:
        subprocess.check_call(["uv", "venv", tempdir, "--python", sys.executable])
    else:
        subprocess.check_call([sys.executable, "-m", "venv", tempdir])
    _install_packages(tempdir, ["setuptools", "wheel"])


def _pip_install(tempdir: str, pip_version: str = "22.0", use_src_layout: bool = False):
    _install_packages(tempdir, ["cloudpickle", f"pip=={pip_version}"])
    pkg = (
        _get_editable_package_name_src_layout()
        if use_src_layout
        else _get_editable_package_name()
    )
    _install_packages(tempdir, [pkg], editable=True)
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
            ["numpy"],
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


def test_pack_in_pex(venv_optimization_level):
    requirements = [
        "numpy",
        "pyarrow"
    ]
    with tempfile.TemporaryDirectory() as tempdir:
        packaging.pack_in_pex(
            requirements, f"{tempdir}/out.pex", pex_inherit_path="false"
        )
        assert os.path.exists(f"{tempdir}/out.pex")
        with does_not_raise():
            print(
                subprocess.check_output(
                    [
                        f"{tempdir}/out.pex",
                        "-c",
                        (
                            """print("Start importing pyarrow and numpy..");"""
                            """import pyarrow; import numpy;"""
                            """print("Successfully imported pyarrow and numpy!")"""
                        ),
                    ]
                )
            )


def test_pack_in_pex_with_allow_large(venv_optimization_level):
    with tempfile.TemporaryDirectory() as tempdir:
        requirements = [
            "numpy",
            "pyarrow",
        ]
        packaging.pack_in_pex(
            requirements,
            f"{tempdir}/out.pex",
            # make isolated pex from current pytest virtual env
            pex_inherit_path="false",
            allow_large_pex=True,
        )
        assert os.path.exists(f"{tempdir}/out.pex.zip")

        with tempfile.TemporaryDirectory() as temp_pex_dir:
            shutil.unpack_archive(f"{tempdir}/out.pex.zip", temp_pex_dir)
            st = os.stat(f"{temp_pex_dir}/__main__.py")
            os.chmod(f"{temp_pex_dir}/__main__.py", st.st_mode | stat.S_IEXEC)

            with does_not_raise():
                print(
                    subprocess.check_output(
                        [
                            f"{temp_pex_dir}/__main__.py",
                            "-c",
                            (
                                """print("Start importing pyarrow..");"""
                                """import pyarrow;"""
                                """print("Successfully imported pyarrow!")"""
                            ),
                        ]
                    )
                )


def test_pack_in_pex_with_include_tools(venv_optimization_level):
    with tempfile.TemporaryDirectory() as tempdir:
        requirements = [
            "numpy",
            "pyarrow",
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


@pytest.mark.parametrize(
    "is_large_pex,package_path",
    [
        (True, "hdfs://dummy/path/env.pex"),
        (None, "hdfs://dummy/path/env.pex"),
        (None, None),
    ],
)
def test_pack_in_pex_with_large_correctly_retrieves_zip_archive(
    large_pex_unzipped, is_large_pex, package_path
):
    unzipped_pex_path = large_pex_unzipped
    package_argument_as_string = (
        "None" if package_path is None else f"'{package_path}'"
    )
    expected_package_path = (
        f"hdfs:///user/{getpass.getuser()}/envs/{os.path.basename(unzipped_pex_path)}.zip"
        if is_large_pex is None
        else f"{package_path}.zip"
    )
    with does_not_raise():
        print(
            subprocess.check_output(
                [
                    f"{unzipped_pex_path}/__main__.py",
                    "-c",
                    (
                        """print("Start importing cluster-pack..");"""
                        """from cluster_pack import packaging;"""
                        """from unittest import mock;"""
                        """packer = packaging.detect_packer_from_env();"""
                        """packaging.get_default_fs = mock.Mock(return_value='hdfs://');"""
                        f"""package_path={package_argument_as_string};"""
                        f"""allow_large_pex={is_large_pex};"""
                        """package_path, env_name, pex_file = \
                    packaging.detect_archive_names(packer, package_path, allow_large_pex);"""
                        """print(f'package_path: {package_path}');"""
                        """print(f'pex_file: {pex_file}');"""
                        f"""assert(package_path == "{expected_package_path}");"""
                        """assert(pex_file.endswith('.pex'));"""
                    ),
                ]
            )
        )


def test_pack_in_pex_with_additional_repo(venv_optimization_level):
    with tempfile.TemporaryDirectory() as tempdir:
        requirements = [
            "torch",
            "networkx<3.2.2; python_version<='3.9'",
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


def test_pack_in_pex_include_editable_requirements(venv_optimization_level):
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
    ("/path/to/myenv.pex.zip", f"{LARGE_PEX_CMD}", UNPACKED_ENV_NAME),
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
    "running_from_pex, package_path, allow_large_pex, is_dir, expected",
    archive_test_data,
)
def test_detect_archive_names(
    running_from_pex, package_path, allow_large_pex, is_dir, expected
):
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
            packaging.PEX_PACKER, package_path, allow_large_pex
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


@pytest.mark.parametrize(
    "pex_size,pex_version,allow_large_pex,should_raise",
    [
        # Old large pex >2GB, disallow large pex => failure
        (2 * 1024 * 1024 * 1024 + 1, "2.40.0", False, True),
        # Old large pex >2GB, allow large pex => success
        (2 * 1024 * 1024 * 1024 + 1, "2.40.0", True, False),
        # Old small pex <=2GB, disallow large pex => success
        (2 * 1024 * 1024 * 1024, "2.40.0", False, False),
        # New small pex <=4GB, disallow large pex => success
        (4 * 1024 * 1024 * 1024, "2.41.1", False, False),
        # New large pex >4GB, disallow large pex => failure
        (4 * 1024 * 1024 * 1024 + 1, "2.41.1", False, True),
        # New large pex >4GB, allow large pex => success
        (4 * 1024 * 1024 * 1024 + 1, "2.41.1", True, False),
    ]
)
@mock.patch("cluster_pack.packaging.Version")
@mock.patch("cluster_pack.packaging.pkg_version")
@mock.patch("os.path.getsize")
def test_check_large_pex(
        mock_getsize, mock_pkg_version, mock_version,
        pex_size, pex_version, allow_large_pex, should_raise
):
    mock_getsize.return_value = pex_size
    mock_pkg_version.side_effect = lambda pkg: pex_version if pkg == "pex" else pkg_version(pkg)
    mock_version.side_effect = lambda v: v
    if should_raise:
        with pytest.raises(PexTooLargeError):
            check_large_pex(allow_large_pex, "dummy.pex")
    else:
        check_large_pex(allow_large_pex, "dummy.pex")
