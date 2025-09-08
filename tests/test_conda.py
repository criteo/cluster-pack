import os
import uuid
import pytest
import subprocess
import tarfile
import tempfile

from unittest import mock

from cluster_pack import conda

pytestmark = pytest.mark.conda


@mock.patch('conda_pack.pack')
@mock.patch('cluster_pack.conda.create_and_pack_conda_env')
def test_pack_venv_in_conda(mock_conda_create, mock_conda_pack):
    conda.pack_venv_in_conda(
        name="test-env",
        reqs=["a==1.0.0", "b==2.0.0"],
        changed_reqs=False,
        output="testpath")
    mock_conda_pack.assert_called_once_with(name="test-env", output="testpath")
    mock_conda_create.assert_not_called()


@mock.patch('conda_pack.pack')
@mock.patch('cluster_pack.conda.create_and_pack_conda_env')
def test_pack_venv_in_conda_changed_reqs(mock_conda_create, mock_conda_pack):
    conda.pack_venv_in_conda(
        name="test-env",
        reqs=["a==1.0.0", "b==2.0.0"],
        changed_reqs=True,
        output="testpath")
    mock_conda_pack.assert_not_called()
    mock_conda_create.assert_called_once_with(
        reqs=["a==1.0.0", "b==2.0.0"], output="testpath",
        additional_indexes=None, additional_repo=None)


def test_conda_env_from_reqs():
    with tempfile.TemporaryDirectory() as tempdir:
        env_zip_path = conda.create_and_pack_conda_env(
            reqs=["pycodestyle==2.5.0"]
        )
        assert os.path.isfile(env_zip_path)

        _check_package(
            tempdir, env_zip_path,
            "pycodestyle", "2.5.0"
        )


def test_conda_env_from_reqs_with_external_repo():
    with tempfile.TemporaryDirectory() as tempdir:
        env_zip_path = conda.create_and_pack_conda_env(
            reqs=["torch==1.10.1"],
            additional_repo='https://download.pytorch.org/whl/cu113'
        )
        assert os.path.isfile(env_zip_path)

        _check_package(
            tempdir, env_zip_path,
            "torch", "1.10.1+cu113"
        )


def test_conda_env_from_reqs_with_external_index():
    with tempfile.TemporaryDirectory() as tempdir:
        env_zip_path = conda.create_and_pack_conda_env(
            reqs=["detectron2==0.6", "torch==1.10.1"],
            additional_indexes=[
                "https://dl.fbaipublicfiles.com/detectron2/wheels/cu102/torch1.10/index.html"
            ]
        )
        assert os.path.isfile(env_zip_path)

        _check_package(
            tempdir, env_zip_path,
            "detectron2", "0.6"
        )


def test_conda_env_from_spec():
    spec_file = os.path.join(os.path.dirname(__file__), "resources", "conda.yaml")
    with tempfile.TemporaryDirectory() as tempdir:
        env_zip_path = conda.create_and_pack_conda_env(
            spec_file=spec_file
        )
        assert os.path.isfile(env_zip_path)

        _check_package(
            tempdir, env_zip_path,
            "botocore", "1.17.12"
        )


def _check_package(tempdir, env_zip_path, name, version):
    env_unzipped_path = os.path.join(tempdir, str(uuid.uuid4()))
    with tarfile.open(env_zip_path) as zf:
        zf.extractall(env_unzipped_path)

    env_python_bin = os.path.join(env_unzipped_path, "bin", "python")
    os.chmod(env_python_bin, 0o755)
    subprocess.check_output([
        env_python_bin, "-c",
        (f"print('Start importing {name}..');import {name};"
         f"assert {name}.__version__ == '{version}'")]
    )
