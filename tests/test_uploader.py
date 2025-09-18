import re
import subprocess
import sys
import contextlib
import json
import os
import tempfile
from unittest import mock

import pytest

from pex.pex_info import PexInfo

import cluster_pack
from cluster_pack import packaging, uploader, filesystem


MODULE_TO_TEST = "cluster_pack.uploader"
MYARCHIVE_FILENAME = "myarchive.pex"
MYARCHIVE_METADATA = "myarchive.json"


def test_update_no_archive():
    map_is_exist = {MYARCHIVE_FILENAME: False}
    mock_fs = mock.MagicMock()
    mock_fs.exists = lambda arg: map_is_exist[arg]
    assert not uploader._is_archive_up_to_date(MYARCHIVE_FILENAME, [], mock_fs)


def test_update_no_metadata():
    map_is_exist = {MYARCHIVE_FILENAME: True, MYARCHIVE_METADATA: False}
    mock_fs = mock.MagicMock()
    mock_fs.exists = lambda arg: map_is_exist[arg]
    assert not uploader._is_archive_up_to_date(MYARCHIVE_FILENAME, [], mock_fs)


@pytest.mark.parametrize(
    "current_packages, metadata_packages, expected",
    [
        pytest.param(
            ["a==2.0", "b==1.0"],
            {
                "package_installed": ["a==2.0", "b==1.0"],
                "platform": "fake_platform",
                "python_version": "3.9.10",
            },
            True,
        ),
        pytest.param(
            ["a==2.0", "b==1.0"],
            {
                "package_installed": ["a==1.0", "b==1.0"],
                "platform": "fake_platform",
                "python_version": "3.9.10",
            },
            False,
        ),
        pytest.param(
            ["a==2.0", "b==1.0"],
            {
                "package_installed": ["a==2.0"],
                "platform": "fake_platform",
                "python_version": "3.9.10",
            },
            False,
        ),
        pytest.param(
            ["a==2.0"],
            {
                "package_installed": ["a==2.0", "b==1.0"],
                "platform": "fake_platform",
                "python_version": "3.9.10",
            },
            False,
        ),
        pytest.param(
            [],
            {
                "package_installed": ["a==2.0", "b==1.0"],
                "platform": "fake_platform",
                "python_version": "3.9.10",
            },
            False,
        ),
        pytest.param(
            ["a==2.0"],
            {
                "package_installed": ["c==1.0"],
                "platform": "fake_platform",
                "python_version": "3.9.10",
            },
            False,
        ),
        pytest.param(
            [],
            {
                "package_installed": [],
                "platform": "fake_platform",
                "python_version": "3.9.10",
            },
            True,
        ),
        pytest.param(["a==2.0", "b==1.0"], ["a==2.0", "b==1.0"], False),
        pytest.param(
            ["a==2.0", "b==1.0"],
            {
                "package_installed": ["a==2.0", "b==1.0"],
                "platform": "fake_platform2",
                "python_version": "3.9.10",
            },
            False,
        ),
    ],
)
@mock.patch(f"{MODULE_TO_TEST}.platform.platform")
@mock.patch(f"{MODULE_TO_TEST}.sys")
def test_update_version_comparaison(
    sys_mock, platform_mock, current_packages, metadata_packages, expected
):
    platform_mock.return_value = build_platform_mock()
    sys_mock.version_info = build_python_version_mock()
    map_is_exist = {MYARCHIVE_FILENAME: True, MYARCHIVE_METADATA: True}

    mock_fs = mock.MagicMock()
    mock_fs.exists = lambda arg: map_is_exist[arg]

    with mock.patch.object(
        mock_fs, "open", mock.mock_open(read_data=json.dumps(metadata_packages))
    ):
        assert (
            uploader._is_archive_up_to_date(
                MYARCHIVE_FILENAME, current_packages, mock_fs
            )
            == expected
        )


def Any(cls):
    class Any(cls):
        def __eq__(self, other):
            return isinstance(other, cls)

    return Any()


expected_file = """\
{
    "a": "1.0",
    "b": "2.0"
}"""


def build_python_version_mock():
    python_version_mock = mock.Mock()
    python_version_mock.major = 3
    python_version_mock.minor = 9
    python_version_mock.micro = 10
    return python_version_mock


def build_platform_mock():
    return "fake_platform"


def pack_spec_in_pex_mock(
    spec_file,
    output,
    pex_inherit_path="fallback",
    include_pex_tools=False,
    additional_repo=None,
    additional_indexes=None,
):
    # Write empty output file
    with open(output, "w"):
        pass
    return output


@mock.patch(f"{MODULE_TO_TEST}.platform.platform")
@mock.patch(f"{MODULE_TO_TEST}.sys")
def test_dump_metadata(sys_mock, platform_mock):
    platform_mock.return_value = build_platform_mock()
    sys_mock.version_info = build_python_version_mock()
    mock_fs = mock.Mock()
    mock_fs.rm.return_value = True
    mock_fs.exists.return_value = True

    def put_mock(src, dest):
        with open(src, "r") as f:
            assert f.read() == (
                '{\n    "package_installed": [\n        "a==1.0",\n        "b==2.0"\n    ],'
                + '\n    "platform": "fake_platform",\n    "python_version": "3.9.10"\n}'
            )

    mock_fs.put.side_effect = put_mock

    packages = ["a==1.0", "b==2.0"]
    uploader._dump_archive_metadata(MYARCHIVE_FILENAME, packages, mock_fs)
    # Check previous file has been deleted
    mock_fs.rm.assert_called_once_with(MYARCHIVE_METADATA)
    mock_fs.put.assert_called_once()


def test_upload_env():
    with contextlib.ExitStack() as stack:
        # Mock all objects
        mock_is_archive = stack.enter_context(
            mock.patch(f"{MODULE_TO_TEST}._is_archive_up_to_date")
        )
        mock_get_packages = stack.enter_context(
            mock.patch(f"{MODULE_TO_TEST}.packaging.get_non_editable_requirements")
        )

        mock_resolve_fs = stack.enter_context(
            mock.patch(f"{MODULE_TO_TEST}.filesystem.resolve_filesystem_and_path")
        )
        mock_fs = mock.MagicMock()
        mock_resolve_fs.return_value = mock_fs, ""

        stack.enter_context(mock.patch(f"{MODULE_TO_TEST}._dump_archive_metadata"))
        mock_packer = stack.enter_context(
            mock.patch(f"{MODULE_TO_TEST}.packaging.pack_in_pex")
        )

        # Regenerate archive
        mock_is_archive.return_value = False
        mock_get_packages.return_value = {"a": "1.0", "b": "2.0"}

        mock_packer.return_value = MYARCHIVE_FILENAME

        cluster_pack.upload_env(MYARCHIVE_FILENAME, cluster_pack.PEX_PACKER)
        mock_packer.assert_called_once_with(
            ["a==1.0", "b==2.0"],
            Any(str),
            [],
            additional_indexes=None,
            additional_repo=None,
            editable_requirements={},
            include_pex_tools=False,
        )
        mock_fs.put.assert_called_once_with(MYARCHIVE_FILENAME, MYARCHIVE_FILENAME)

        mock_packer.reset_mock()
        cluster_pack.upload_env(
            MYARCHIVE_FILENAME,
            cluster_pack.PEX_PACKER,
            additional_packages={"c": "3.0"},
            ignored_packages=["a"],
        )
        mock_packer.assert_called_once_with(
            ["b==2.0", "c==3.0"],
            Any(str),
            ["a"],
            additional_indexes=None,
            additional_repo=None,
            editable_requirements={},
            include_pex_tools=False,
        )


def test_upload_env_should_throw_error_if_wrong_extension():
    with pytest.raises(ValueError):
        cluster_pack.upload_env("myarchive.tar.gz", packer=cluster_pack.PEX_PACKER)


def test_upload_zip():
    home_fs_path = "/user/j.doe"
    with mock.patch(
        f"{MODULE_TO_TEST}.filesystem.resolve_filesystem_and_path"
    ) as mock_resolve_fs:
        mock_fs = mock.MagicMock()
        mock_resolve_fs.return_value = mock_fs, ""
        with mock.patch(f"{MODULE_TO_TEST}.request") as mock_request:
            with mock.patch(f"{MODULE_TO_TEST}.tempfile") as mock_tempfile:
                mock_fs.exists.return_value = False
                mock_tempfile.TemporaryDirectory.return_value.__enter__.return_value = (
                    "/tmp"
                )

                result = cluster_pack.upload_zip(
                    "http://myserver/mypex.pex", f"{home_fs_path}/blah.pex"
                )

                mock_request.urlretrieve.assert_called_once_with(
                    "http://myserver/mypex.pex", "/tmp/mypex.pex"
                )
                mock_fs.put.assert_any_call(
                    "/tmp/mypex.pex", f"{home_fs_path}/blah.pex"
                )

                assert "/user/j.doe/blah.pex" == result


def test_upload_env_in_a_pex():
    home_path = "/home/j.doe"
    home_fs_path = "/user/j.doe"
    with contextlib.ExitStack() as stack:
        mock_running_from_pex = stack.enter_context(
            mock.patch(f"{MODULE_TO_TEST}.packaging._running_from_pex")
        )
        mock_running_from_pex.return_value = True
        mock_pex_filepath = stack.enter_context(
            mock.patch(f"{MODULE_TO_TEST}.packaging.get_current_pex_filepath")
        )
        mock_pex_filepath.return_value = f"{home_path}/myapp.pex"

        mock_resolve_fs = stack.enter_context(
            mock.patch(f"{MODULE_TO_TEST}.filesystem.resolve_filesystem_and_path")
        )
        mock_fs = mock.MagicMock()
        mock_resolve_fs.return_value = mock_fs, ""

        mock__get_archive_metadata_path = stack.enter_context(
            mock.patch(f"{MODULE_TO_TEST}._get_archive_metadata_path")
        )
        mock__get_archive_metadata_path.return_value = (
            f"{home_fs_path}/blah-1.388585.133.497-review.json"
        )

        # metadata & pex already exists on fs
        mock_fs.exists.return_value = True

        mock_pex_info = stack.enter_context(mock.patch(f"{MODULE_TO_TEST}.PexInfo"))

        def _from_pex(arg):
            if arg == f"{home_path}/myapp.pex":
                return PexInfo({"code_hash": 1})
            else:
                return PexInfo({"code_hash": 2})

        mock_pex_info.from_pex.side_effect = _from_pex

        result = cluster_pack.upload_env(
            f"{home_fs_path}/blah-1.388585.133.497-review.pex"
        )

        # Check copy pex to remote
        mock_fs.put.assert_any_call(
            f"{home_path}/myapp.pex", f"{home_fs_path}/blah-1.388585.133.497-review.pex"
        )
        # Check metadata has been cleaned
        mock_fs.rm.assert_called_once_with(
            f"{home_fs_path}/blah-1.388585.133.497-review.json"
        )
        # check envname
        assert "myapp" == result[1]


@mock.patch(f"{MODULE_TO_TEST}._is_archive_up_to_date")
@mock.patch(f"{MODULE_TO_TEST}._dump_archive_metadata")
@mock.patch(f"{MODULE_TO_TEST}.filesystem.resolve_filesystem_and_path")
@mock.patch(f"{MODULE_TO_TEST}.packaging.pack_spec_in_pex")
@mock.patch(f"{MODULE_TO_TEST}.packaging.get_default_fs")
@mock.patch(f"{MODULE_TO_TEST}.getpass.getuser")
def test_upload_spec_hdfs(
    mock_get_user,
    mock_get_default_fs,
    mock_pack_spec_in_pex,
    mock_resolve_fs,
    mock_dump_archive_metadata,
    mock_is_archive_up_to_date,
):
    mock_is_archive_up_to_date.return_value = False
    mock_fs = mock.MagicMock()
    mock_resolve_fs.return_value = mock_fs, ""
    mock_fs.exists.return_value = True
    mock_get_default_fs.return_value = "hdfs://"
    mock_get_user.return_value = "testuser"

    spec_file = os.path.join(os.path.dirname(__file__), "resources", "requirements.txt")
    result_path = cluster_pack.upload_spec(
        spec_file, "hdfs:///user/testuser/envs/myenv.pex"
    )
    mock_pack_spec_in_pex.assert_called_once()
    assert result_path == "hdfs:///user/testuser/envs/myenv.pex"


@mock.patch(f"{MODULE_TO_TEST}.platform.platform")
@mock.patch(f"{MODULE_TO_TEST}.sys")
def test_upload_spec_local_fs(sys_mock, platform_mock):
    platform_mock.return_value = build_platform_mock()
    sys_mock.version_info = build_python_version_mock()
    spec_file = os.path.join(os.path.dirname(__file__), "resources", "requirements.txt")
    with tempfile.TemporaryDirectory() as tempdir:
        result_path = cluster_pack.upload_spec(spec_file, f"{tempdir}/package.pex")
        assert os.path.exists(result_path)
        _check_metadata(
            f"{tempdir}/package.json",
            {
                "package_installed": ["5a5f33b106aad8584345f5a0044a4188ce78b3f4"],
                "platform": "fake_platform",
                "python_version": "3.9.10",
            },
        )


@mock.patch(f"{MODULE_TO_TEST}.platform.platform")
@mock.patch(f"{MODULE_TO_TEST}.sys")
def test_upload_spec_unique_name(sys_mock, platform_mock):
    platform_mock.return_value = build_platform_mock()
    sys_mock.version_info = build_python_version_mock()
    with tempfile.TemporaryDirectory() as tempdir:
        spec_file = f"{tempdir}/myproject/requirements.txt"
        _write_spec_file(spec_file, ["cloudpickle==1.4.1"])

        result_path = cluster_pack.upload_spec(spec_file, f"{tempdir}")

        assert os.path.exists(result_path)
        assert result_path == f"{tempdir}/cluster_pack_myproject.pex"
        _check_metadata(
            f"{tempdir}/cluster_pack_myproject.json",
            {
                "package_installed": ["b8721a3c125d3f7edfa27d7b13236e696f652a16"],
                "platform": "fake_platform",
                "python_version": "3.9.10",
            },
        )


@pytest.mark.skipif(sys.version_info >= (3, 9), reason="fail on ci above python3.9")
@mock.patch(
    f"{MODULE_TO_TEST}.packaging.pack_spec_in_pex", side_effect=pack_spec_in_pex_mock
)
def test_upload_spec_local_fs_use_cache(mock_pack_spec_in_pex):
    with tempfile.TemporaryDirectory() as tempdir:
        spec_file = f"{tempdir}/myproject/requirements.txt"
        _write_spec_file(spec_file, ["cloudpickle==1.4.1"])

        pex_file = os.path.join(tempdir, "package.pex")
        result_path = cluster_pack.upload_spec(spec_file, pex_file)
        result_path1 = cluster_pack.upload_spec(spec_file, pex_file)

        mock_pack_spec_in_pex.assert_called_once()
        assert os.path.exists(result_path)
        assert result_path == result_path1 == pex_file


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="replacement test for python3.9 and above"
)
@mock.patch(f"{MODULE_TO_TEST}.platform.platform")
@mock.patch(
    f"{MODULE_TO_TEST}.packaging.pack_spec_in_pex", side_effect=pack_spec_in_pex_mock
)
def test_upload_spec_local_fs_use_cache_py39(mock_pack_spec_in_pex, platform_mock):
    platform_mock.return_value = build_platform_mock()
    with tempfile.TemporaryDirectory() as tempdir:
        spec_file = f"{tempdir}/myproject/requirements.txt"
        _write_spec_file(spec_file, ["cloudpickle==1.4.1"])

        pex_file = os.path.join(tempdir, "package.pex")
        result_path = cluster_pack.upload_spec(spec_file, pex_file)
        result_path1 = cluster_pack.upload_spec(spec_file, pex_file)

        mock_pack_spec_in_pex.assert_called_once()
        assert os.path.exists(result_path)
        assert result_path == result_path1 == pex_file


@mock.patch(f"{MODULE_TO_TEST}.platform.platform")
@mock.patch(f"{MODULE_TO_TEST}.sys")
@mock.patch(
    f"{MODULE_TO_TEST}.packaging.pack_spec_in_pex", side_effect=pack_spec_in_pex_mock
)
def test_upload_spec_local_fs_changed_reqs(
    mock_pack_spec_in_pex, sys_mock, platform_mock
):
    platform_mock.return_value = build_platform_mock()
    sys_mock.version_info = build_python_version_mock()

    with tempfile.TemporaryDirectory() as tempdir:
        spec_file = f"{tempdir}/myproject/requirements.txt"
        _write_spec_file(spec_file, ["cloudpickle==1.4.1"])

        pex_file = os.path.join(tempdir, "package.pex")

        result_path = cluster_pack.upload_spec(spec_file, pex_file)

        with open(spec_file, "a") as f:
            f.write("skein\n")

        result_path1 = cluster_pack.upload_spec(spec_file, pex_file)
        mock_pack_spec_in_pex.call_count == 2
        assert os.path.exists(result_path)
        assert os.path.exists(result_path1)
        _check_metadata(
            f"{tempdir}/package.json",
            {
                "package_installed": ["0fd17ced922a2387fa660fb0cb78e1c77fbe3349"],
                "platform": "fake_platform",
                "python_version": "3.9.10",
            },
        )


def test__handle_packages_use_local_wheel():
    current_packages = {"horovod": "0.18.2"}
    uploader._handle_packages(
        current_packages,
        additional_packages={"horovod-0.19.0-cp36-cp36m-linux_x86_64.whl": ""},
    )

    assert len(current_packages) == 1
    assert (
        next(iter(current_packages.keys()))
        == "horovod-0.19.0-cp36-cp36m-linux_x86_64.whl"
    )
    assert next(iter(current_packages.values())) == ""


def test__handle_packages_use_other_package():
    current_packages = {"tensorflow": "2.5.2"}
    uploader._handle_packages(
        current_packages,
        additional_packages={"tensorflow_gpu": "2.5.2"},
        ignored_packages="tensorflow",
    )

    print(current_packages)
    assert len(current_packages) == 1
    assert next(iter(current_packages.keys())) == "tensorflow_gpu"
    assert next(iter(current_packages.values())) == "2.5.2"


@pytest.mark.parametrize(
    "spec_file, expected",
    [
        pytest.param("/a/b/myproject/requirements.txt", "cluster_pack_myproject.pex"),
        pytest.param("myproject/requirements.txt", "cluster_pack_myproject.pex"),
        pytest.param("requirements.txt", "cluster_pack.pex"),
    ],
)
def test__unique_filename(spec_file, expected):
    assert expected == uploader._unique_filename(spec_file, packaging.PEX_PACKER)


def get_latest_pip_version() -> str:
    p = subprocess.Popen(
        "pip index versions pip",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    for line in p.stdout.readlines():
        result = re.match(r"pip \((.+)\)", line.decode("utf-8"))
        if result:
            return result.group(1)
    return None


def test_latest_pip():
    assert get_latest_pip_version() is not None


def test_format_pex_requirements():
    with tempfile.TemporaryDirectory() as tempdir:
        requirements = ["pipdeptree==2.0.0", "six==1.15.0"]
        packaging.pack_in_pex(
            requirements,
            f"{tempdir}/out.pex",
            # make isolated pex from current pytest virtual env
            pex_inherit_path="false",
        )
        pex_info = PexInfo.from_pex(f"{tempdir}/out.pex")
        cleaned_requirements = uploader._format_pex_requirements(pex_info)
        assert [
            f"pip=={get_latest_pip_version()}",
            "pipdeptree==2.0.0",
            "six==1.15.0",
        ] == cleaned_requirements


@pytest.mark.parametrize(
    "req, expected",
    [
        (
            [
                "pipdeptree==2.0.0",
                "GitPython==3.1.14",
                "six==1.15.0",
                "Cython==0.29.22",
            ],
            [
                "cython==0.29.22",
                "gitpython==3.1.14",
                "pipdeptree==2.0.0",
                "six==1.15.0",
            ],
        ),
        (
            [
                "pipdeptree==2.0.0",
                "six==1.15.0",
                "gitpython==3.1.14",
                "cython==0.29.22",
            ],
            [
                "cython==0.29.22",
                "gitpython==3.1.14",
                "pipdeptree==2.0.0",
                "six==1.15.0",
            ],
        ),
    ],
)
def test_sort_requirement(req, expected):
    assert uploader._sort_requirements(req) == expected


def test_normalize_requirement():
    assert ["tf-yarn", "typing-extension", "to-to"] == uploader._normalize_requirements(
        ["tf_yarn", "typing_extension", "to-to"]
    )


def _check_metadata(metadata_file, expected_json):
    with open(metadata_file, "r") as metadata_file:
        json_md = json.load(metadata_file)
        assert json_md == expected_json


def _write_spec_file(spec_file, reqs=[]):
    os.makedirs(os.path.dirname(spec_file))
    with open(spec_file, "w") as f:
        for req in reqs:
            f.write(req + "\n")
