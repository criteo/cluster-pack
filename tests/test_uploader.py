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
    map_is_exist = {MYARCHIVE_FILENAME: True,
                    MYARCHIVE_METADATA: False}
    mock_fs = mock.MagicMock()
    mock_fs.exists = lambda arg: map_is_exist[arg]
    assert not uploader._is_archive_up_to_date(MYARCHIVE_FILENAME, [], mock_fs)


@pytest.mark.parametrize("current_packages, metadata_packages, expected", [
    pytest.param({"a": "2.0", "b": "1.0"}, {"a": "2.0", "b": "1.0"}, True),
    pytest.param({"a": "2.0", "b": "1.0"}, {"a": "1.0", "b": "1.0"}, False),
    pytest.param({"a": "2.0", "b": "1.0"}, {"a": "2.0"}, False),
    pytest.param({"a": "2.0"}, {"a": "2.0", "b": "1.0"}, False),
    pytest.param({}, {"a": "2.0", "b": "1.0"}, False),
    pytest.param({"a": "2.0"}, {"c": "1.0"}, False),
    pytest.param({}, {}, True),
])
def test_update_version_comparaison(current_packages, metadata_packages,
                                    expected):

    map_is_exist = {MYARCHIVE_FILENAME: True,
                    MYARCHIVE_METADATA: True}

    mock_fs = mock.MagicMock()
    mock_fs.exists = lambda arg: map_is_exist[arg]

    with mock.patch.object(mock_fs, 'open',
        mock.mock_open(read_data=json.dumps(metadata_packages))
    ):
        assert uploader._is_archive_up_to_date(
            MYARCHIVE_FILENAME,
            current_packages,
            mock_fs) == expected


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


def test_dump_metadata():
    mock_fs = mock.Mock()
    mock_fs.rm.return_value = True
    mock_fs.exists.return_value = True
    mock_open = mock.mock_open()
    with mock.patch.object(mock_fs, 'open', mock_open):
        mock_fs.exists.return_value = True
        packages = {"a": "1.0", "b": "2.0"}
        uploader._dump_archive_metadata(
            MYARCHIVE_FILENAME,
            packages,
            filesystem.EnhancedFileSystem(mock_fs))
        # Check previous file has been deleted
        mock_fs.rm.assert_called_once_with(MYARCHIVE_METADATA)
        mock_open().write.assert_called_once_with(b'{\n    "a": "1.0",\n    "b": "2.0"\n}')


def test_upload_env():
    with contextlib.ExitStack() as stack:
        # Mock all objects
        mock_is_archive = stack.enter_context(
                mock.patch(f"{MODULE_TO_TEST}._is_archive_up_to_date"))
        mock_get_packages = stack.enter_context(
                mock.patch(f"{MODULE_TO_TEST}.packaging.get_non_editable_requirements"))

        mock_resolve_fs = stack.enter_context(
            mock.patch(f"{MODULE_TO_TEST}.filesystem.resolve_filesystem_and_path"))
        mock_fs = mock.MagicMock()
        mock_resolve_fs.return_value = mock_fs, ""

        stack.enter_context(mock.patch(f"{MODULE_TO_TEST}._dump_archive_metadata"))
        stack.enter_context(mock.patch(f"{MODULE_TO_TEST}.shutil.rmtree"))
        mock_packer = stack.enter_context(
            mock.patch(f"{MODULE_TO_TEST}.packaging.pack_in_pex")
        )

        # Regenerate archive
        mock_is_archive.return_value = False
        mock_get_packages.return_value = [{"name": "a", "version": "1.0"},
                                          {"name": "b", "version": "2.0"}]

        mock_packer.return_value = MYARCHIVE_FILENAME

        cluster_pack.upload_env(MYARCHIVE_FILENAME, cluster_pack.PEX_PACKER)
        mock_packer.assert_called_once_with(
            {"a": "1.0", "b": "2.0"}, Any(str), [], editable_requirements={}
        )
        mock_fs.put.assert_called_once_with(MYARCHIVE_FILENAME, MYARCHIVE_FILENAME)

        mock_packer.reset_mock()
        cluster_pack.upload_env(
            MYARCHIVE_FILENAME, cluster_pack.PEX_PACKER,
            additional_packages={"c": "3.0"},
            ignored_packages=["a"]
        )
        mock_packer.assert_called_once_with(
            {"c": "3.0", "b": "2.0"}, Any(str), ["a"], editable_requirements={}
        )


def test_upload_env_should_throw_error_if_wrong_extension():
    with pytest.raises(ValueError):
        cluster_pack.upload_env("myarchive.tar.gz", packer=cluster_pack.CONDA_PACKER)


def test_upload_zip():
    home_fs_path = '/user/j.doe'
    with mock.patch(
            f"{MODULE_TO_TEST}.filesystem.resolve_filesystem_and_path") as mock_resolve_fs:
        mock_fs = mock.MagicMock()
        mock_resolve_fs.return_value = mock_fs, ""
        with mock.patch(f"{MODULE_TO_TEST}.request") as mock_request:
            with mock.patch(f"{MODULE_TO_TEST}.tempfile") as mock_tempfile:

                mock_fs.exists.return_value = False
                mock_tempfile.TemporaryDirectory.return_value.__enter__.return_value = "/tmp"

                result = cluster_pack.upload_zip(
                    "http://myserver/mypex.pex",
                    f"{home_fs_path}/blah.pex"
                )

                mock_request.urlretrieve.assert_called_once_with(
                    "http://myserver/mypex.pex",
                    "/tmp/mypex.pex")
                mock_fs.put.assert_any_call("/tmp/mypex.pex", f"{home_fs_path}/blah.pex")

                assert "/user/j.doe/blah.pex" == result


def test_upload_env_in_a_pex():
    home_path = '/home/j.doe'
    home_fs_path = '/user/j.doe'
    with contextlib.ExitStack() as stack:
        mock_running_from_pex = stack.enter_context(
            mock.patch(f"{MODULE_TO_TEST}.packaging._running_from_pex"))
        mock_running_from_pex.return_value = True
        mock_pex_filepath = stack.enter_context(
            mock.patch(f"{MODULE_TO_TEST}.packaging.get_current_pex_filepath"))
        mock_pex_filepath.return_value = f"{home_path}/myapp.pex"

        mock_resolve_fs = stack.enter_context(
            mock.patch(f"{MODULE_TO_TEST}.filesystem.resolve_filesystem_and_path"))
        mock_fs = mock.MagicMock()
        mock_resolve_fs.return_value = mock_fs, ""

        mock__get_archive_metadata_path = stack.enter_context(
            mock.patch(f"{MODULE_TO_TEST}._get_archive_metadata_path")
        )
        mock__get_archive_metadata_path.return_value = f"{home_fs_path}/blah.json"

        # metadata & pex already exists on fs
        mock_fs.exists.return_value = True

        mock_pex_info = stack.enter_context(
            mock.patch(f"{MODULE_TO_TEST}.PexInfo")
        )

        def _from_pex(arg):
            if arg == f'{home_path}/myapp.pex':
                return PexInfo({"code_hash": 1})
            else:
                return PexInfo({"code_hash": 2})

        mock_pex_info.from_pex.side_effect = _from_pex

        result = cluster_pack.upload_env(f'{home_fs_path}/blah.pex')

        # Check copy pex to remote
        mock_fs.put.assert_any_call(
            f'{home_path}/myapp.pex',
            f'{home_fs_path}/blah.pex')
        # Check metadata has been cleaned
        mock_fs.rm.assert_called_once_with(f'{home_fs_path}/blah.json')
        # check envname
        assert 'myapp' == result[1]


def test__handle_packages_use_local_wheel():
    current_packages = {"horovod": "0.18.2"}
    uploader._handle_packages(
        current_packages,
        additional_packages={"horovod-0.19.0-cp36-cp36m-linux_x86_64.whl": ""}
    )

    assert len(current_packages) == 1
    assert next(iter(current_packages.keys())) == "horovod-0.19.0-cp36-cp36m-linux_x86_64.whl"
    assert next(iter(current_packages.values())) == ""


def test__handle_packages_use_other_package():
    current_packages = {"tensorflow": "0.15.2"}
    uploader._handle_packages(
        current_packages,
        additional_packages={"tensorflow_gpu": "0.15.3"},
        ignored_packages="tensorflow"
    )

    print(current_packages)
    assert len(current_packages) == 1
    assert next(iter(current_packages.keys())) == "tensorflow_gpu"
    assert next(iter(current_packages.values())) == "0.15.3"
