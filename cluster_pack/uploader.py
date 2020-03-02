import getpass
import imp
import json
import logging
import os
import pathlib
import shutil
import sys
import tempfile
from typing import (
    Optional,
    Tuple,
    Dict,
    NamedTuple,
    Callable,
    Collection,
    List
)
from urllib import parse, request
import uuid
import zipfile
import pyarrow

from pex.pex_info import PexInfo

from cluster_pack import filesystem, packaging

EDITABLE_PACKAGES_INDEX = 'editable_packages_index'

_logger = logging.getLogger(__name__)


def _get_archive_metadata_path(package_path: str) -> str:
    url = parse.urlparse(package_path)
    return url._replace(path=str(pathlib.Path(url.path).with_suffix('.json'))).geturl()


def _is_archive_up_to_date(package_path: str,
                           current_packages_list: Dict[str, str],
                           resolved_fs=None
                           ) -> bool:
    if not resolved_fs.exists(package_path):
        return False
    archive_meta_data = _get_archive_metadata_path(package_path)
    if not resolved_fs.exists(archive_meta_data):
        _logger.debug(f'metadata for archive {package_path} does not exist')
        return False
    with resolved_fs.open(archive_meta_data, "rb") as fd:
        packages_installed = json.loads(fd.read())
        return sorted(packages_installed.items()) == sorted(current_packages_list.items())


def _dump_archive_metadata(package_path: str,
                           current_packages_list: Dict[str, str],
                           resolved_fs=None
                           ):
    archive_meta_data = _get_archive_metadata_path(package_path)
    with tempfile.TemporaryDirectory() as tempdir:
        tempfile_path = os.path.join(tempdir, "metadata.json")
        with open(tempfile_path, "w") as fd:
            fd.write(json.dumps(current_packages_list, sort_keys=True, indent=4))
        if resolved_fs.exists(archive_meta_data):
            resolved_fs.rm(archive_meta_data)
        resolved_fs.put(tempfile_path, archive_meta_data)


def upload_zip(
    zip_file: str,
    package_path: str = None,
    force_upload: bool = False,
):
    packer = packaging.detect_packer_from_file(zip_file)
    package_path, _, _ = packaging.detect_archive_names(packer, package_path)

    resolved_fs, path = filesystem.resolve_filesystem_and_path(package_path)

    with tempfile.TemporaryDirectory() as tempdir:
        parsed_url = parse.urlparse(zip_file)
        if parsed_url.scheme == "http":
            tmp_zip_file = os.path.join(tempdir, os.path.basename(parsed_url.path))
            request.urlretrieve(zip_file, tmp_zip_file)
            zip_file = tmp_zip_file

        _upload_zip(zip_file, package_path, resolved_fs, force_upload)

        return package_path


def upload_env(
        package_path: str = None,
        packer=None,
        additional_packages: Dict[str, str] = {},
        ignored_packages: Collection[str] = [],
        force_upload: bool = False,
) -> Tuple[str, str]:
    if packer is None:
        packer = packaging.detect_packer_from_env()
    package_path, env_name, pex_file = packaging.detect_archive_names(packer, package_path)

    resolved_fs, path = filesystem.resolve_filesystem_and_path(package_path)

    if not packaging._running_from_pex():
        _upload_env_from_venv(
            package_path, packer,
            additional_packages, ignored_packages,
            resolved_fs,
            force_upload
        )
    else:
        _upload_zip(pex_file, package_path, resolved_fs, force_upload)

    return (package_path,
            env_name)


def _upload_zip(zip_file: str, package_path: str, resolved_fs=None, force_upload: bool = False):
    packer = packaging.detect_packer_from_file(zip_file)
    if packer == packaging.PEX_PACKER and resolved_fs.exists(package_path):
        with tempfile.TemporaryDirectory() as tempdir:
            local_copy_path = os.path.join(tempdir, os.path.basename(package_path))
            resolved_fs.get(package_path, local_copy_path)
            info_from_storage = PexInfo.from_pex(local_copy_path)
            into_to_upload = PexInfo.from_pex(zip_file)
            if not force_upload and info_from_storage.code_hash == into_to_upload.code_hash:
                _logger.info(f"skip upload of current {zip_file}"
                             f" as it is already uploaded on {package_path}")
                return

    _logger.info(f"upload current {zip_file} to {package_path}")

    dir = os.path.dirname(package_path)
    if not resolved_fs.exists(dir):
        resolved_fs.mkdir(dir)
    resolved_fs.put(zip_file, package_path)
    # Remove previous metadata
    archive_meta_data = _get_archive_metadata_path(package_path)
    if resolved_fs.exists(archive_meta_data):
        resolved_fs.rm(archive_meta_data)


def _handle_packages(
    current_packages: Dict[str, str],
    additional_packages: Dict[str, str] = {},
    ignored_packages: Collection[str] = []
):
    if len(additional_packages) > 0:
        additional_package_names = list(additional_packages.keys())
        current_packages_names = list(current_packages.keys())

        for name in current_packages_names:
            for additional_package_name in additional_package_names:
                if name in additional_package_name:
                    _logger.debug(f"Replace existing package {name} by {additional_package_name}")
                    current_packages.pop(name)
        current_packages.update(additional_packages)

    if len(ignored_packages) > 0:
        for name in ignored_packages:
            if name in current_packages:
                _logger.debug(f"Remove package {name}")
                current_packages.pop(name)


def _upload_env_from_venv(
        package_path: str,
        packer=packaging.PEX_PACKER,
        additional_packages: Dict[str, str] = {},
        ignored_packages: Collection[str] = [],
        resolved_fs=None,
        force_upload: bool = False,
):
    current_packages = {package["name"]: package["version"]
                        for package in packaging.get_non_editable_requirements()}

    _handle_packages(
        current_packages,
        additional_packages,
        ignored_packages
    )

    _logger.debug(f"Packaging current_packages={current_packages}")

    if force_upload or not _is_archive_up_to_date(package_path, current_packages, resolved_fs):
        _logger.info(
            f"Zipping and uploading your env to {package_path}"
        )

        with tempfile.TemporaryDirectory() as tempdir:
            archive_local = packer.pack(
                output=f"{tempdir}/{packer.env_name}.{packer.extension}",
                reqs=current_packages,
                additional_packages=additional_packages,
                ignored_packages=ignored_packages
            )
            dir = os.path.dirname(package_path)
            if not resolved_fs.exists(dir):
                resolved_fs.mkdir(dir)
            resolved_fs.put(archive_local, package_path)

            _dump_archive_metadata(package_path, current_packages, resolved_fs)
    else:
        _logger.info(f"{package_path} already exists")
