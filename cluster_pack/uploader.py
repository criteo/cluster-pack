import getpass
import hashlib
import json
import logging
import os
import sys
import pathlib
import platform
import tempfile
from typing import (
    Tuple,
    Dict,
    Collection,
    List,
    Any,
    Optional,
    Union
)
from urllib import parse, request

from pex.pex_info import PexInfo
from wheel_filename import parse_wheel_filename

from cluster_pack import filesystem, packaging

_logger = logging.getLogger(__name__)

PACKAGE_INSTALLED_KEY = "package_installed"
PLATFORM_KEY = "platform"
PYTHON_VERSION_KEY = "python_version"


def _get_archive_metadata_path(package_path: str) -> str:
    url = parse.urlparse(package_path)
    return url._replace(path=str(pathlib.Path(url.path).with_suffix('.json'))).geturl()


def _is_archive_up_to_date(package_path: str,
                           current_packages_list: List[str],
                           resolved_fs: Any = None
                           ) -> bool:
    if not resolved_fs.exists(package_path):
        return False
    archive_meta_data = _get_archive_metadata_path(package_path)
    if not resolved_fs.exists(archive_meta_data):
        _logger.debug(f'metadata for archive {package_path} does not exist')
        return False
    with resolved_fs.open(archive_meta_data, "rb") as fd:
        metadata_dict = json.loads(fd.read())
        if not isinstance(metadata_dict, dict):
            _logger.debug('metadata exists but was built with old format')
            return False

        current_platform, current_python_version = get_platform_and_python_version()
        packages_installed = metadata_dict.get(PACKAGE_INSTALLED_KEY, [])
        platform = metadata_dict.get(PLATFORM_KEY, "")
        python_version = metadata_dict.get(PYTHON_VERSION_KEY, "")
        return (sorted(packages_installed) == sorted(current_packages_list)
                and platform == current_platform
                and python_version == current_python_version)


def _dump_archive_metadata(package_path: str,
                           current_packages_list: List[str],
                           resolved_fs: Any = None
                           ) -> None:
    archive_meta_data = _get_archive_metadata_path(package_path)
    metadata_dict = build_metadata_dict(current_packages_list)
    with tempfile.TemporaryDirectory() as tempdir:
        tempfile_path = os.path.join(tempdir, "metadata.json")
        with open(tempfile_path, "w") as fd:
            fd.write(json.dumps(metadata_dict, indent=4))
        if resolved_fs.exists(archive_meta_data):
            resolved_fs.rm(archive_meta_data)
        resolved_fs.put(tempfile_path, archive_meta_data)


def build_metadata_dict(current_packages_list: List[str]) -> Dict:
    cur_platform, python_version = get_platform_and_python_version()
    metadata_dict = {
        PACKAGE_INSTALLED_KEY: current_packages_list,
        PLATFORM_KEY: cur_platform,
        PYTHON_VERSION_KEY: python_version
    }
    return metadata_dict


def get_platform_and_python_version() -> Tuple[str, str]:
    system, _node, release, _version, _machine, _processor = platform.uname()
    current_platform_str = f"{system}-{release}"
    python_version = sys.version_info
    python_version_str = f"{python_version.major}.{python_version.minor}.{python_version.micro}"
    return current_platform_str, python_version_str


def upload_zip(
        zip_file: str,
        package_path: str = None,
        force_upload: bool = False,
        fs_args: Dict[str, Any] = {}
) -> str:
    packer = packaging.detect_packer_from_file(zip_file)
    package_path, _, _ = packaging.detect_archive_names(packer, package_path)

    resolved_fs, _ = filesystem.resolve_filesystem_and_path(package_path, **fs_args)

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
        packer: packaging.Packer = None,
        additional_packages: Dict[str, str] = {},
        ignored_packages: Collection[str] = [],
        force_upload: bool = False,
        include_editable: bool = False,
        fs_args: Dict[str, Any] = {},
        allow_large_pex: bool = False,
        additional_repo: Optional[Union[str, List[str]]] = None,
        additional_indexes: Optional[List[str]] = None
) -> Tuple[str, str]:
    """Upload current python env.

    :param package_path: path where to upload current python env
    :param packer: packer to use to package the current python env
    :param additional_packages: additional packages, absent from current env,
                            to add to the uploaded env
    :param ignored_packages: specific arguments for special file systems (like S3)
    :param force_upload: force the packaging and upload of current env
    :param include_editable: whether to include or not packages installed in editable mode
    :param fs_args: filesystem args
    :param allow_large_pex: whether to allow or not building a large pex
    :param additional_repo: additional repositories compliant with PEP 503 or local directories
                            laid out in the same format.
                            ex: https://download.pytorch.org/whl/cu113
    :param additional_indexes: URLs or paths to an html files/indexes listing packages
                            ex: https://dl.fbaipublicfiles.com/detectron2/wheels/cu102/
                                torch1.10/index.html
    :return: package_path
    """
    if packer is None:
        packer = packaging.detect_packer_from_env()
    package_path, env_name, pex_file = \
        packaging.detect_archive_names(packer, package_path, allow_large_pex)

    resolved_fs, _ = filesystem.resolve_filesystem_and_path(package_path, **fs_args)

    if pex_file == "":
        _upload_env_from_venv(
            package_path, packer,
            additional_packages, ignored_packages,
            resolved_fs,
            force_upload,
            include_editable,
            allow_large_pex=allow_large_pex,
            additional_repo=additional_repo,
            additional_indexes=additional_indexes
        )
    else:
        _upload_zip(pex_file, package_path, resolved_fs, force_upload)

    return (package_path,
            env_name)


def upload_spec(
        spec_file: str,
        package_path: str = None,
        force_upload: bool = False,
        fs_args: Dict[str, Any] = {},
        allow_large_pex: bool = False) -> str:
    """Upload an environment from a spec file

    :param spec_file: the spec file, must be requirements.txt or conda.yaml
    :param package_path: the path where to upload the package
    :param force_upload: whether the cache should be cleared
    :param fs_args: specific arguments for special file systems (like S3)
    :param allow_large_pex: Creates a non-executable pex that will need to be unzipped to circumvent
                            python's limitation with zips > 2Gb. The file will need to be unzipped
                            and the entry point will be <output>/__main__.py
    :return: package_path
    """
    packer = packaging.detect_packer_from_spec(spec_file)
    if not package_path:
        package_path = (f"{packaging.get_default_fs()}/user/{getpass.getuser()}"
                        f"/envs/{_unique_filename(spec_file, packer)}")
    elif not package_path.endswith(packer.extension()):
        package_path = os.path.join(package_path, _unique_filename(spec_file, packer))

    if (packer.extension() == packaging.PEX_PACKER.extension()
            and allow_large_pex
            and not package_path.endswith('.zip')):
        package_path += '.zip'

    resolved_fs, path = filesystem.resolve_filesystem_and_path(package_path, **fs_args)

    hash = _get_hash(spec_file)
    _logger.info(f"Packaging from {spec_file} with hash={hash}")
    reqs = [hash]

    up_to_date = _is_archive_up_to_date(package_path, reqs, resolved_fs)
    if force_upload or not up_to_date:
        _logger.info(
            f"Zipping and uploading your env to {package_path}"
        )

        with tempfile.TemporaryDirectory() as tempdir:
            archive_local = packer.pack_from_spec(
                spec_file=spec_file,
                output=f"{tempdir}/{packer.env_name()}.{packer.extension()}",
                allow_large_pex=allow_large_pex)

            dir = os.path.dirname(package_path)
            if not resolved_fs.exists(dir):
                resolved_fs.mkdir(dir)
            resolved_fs.put(archive_local, package_path)

            _dump_archive_metadata(package_path, reqs, resolved_fs)
    else:
        _logger.info(f"{package_path} already exists")

    return package_path


def _unique_filename(spec_file: str, packer: packaging.Packer) -> str:
    repo = os.path.basename(os.path.dirname(spec_file))
    if repo:
        repo = "_" + repo
    return f"cluster_pack{repo}.{packer.extension()}"


def _get_hash(spec_file: str) -> str:
    with open(spec_file) as f:
        return hashlib.sha1(f.read().encode()).hexdigest()


def _upload_zip(
        zip_file: str, package_path: str,
        resolved_fs: Any = None, force_upload: bool = False
) -> None:
    packer = packaging.detect_packer_from_file(zip_file)
    if packer == packaging.PEX_PACKER and resolved_fs.exists(package_path):
        with tempfile.TemporaryDirectory() as tempdir:
            local_copy_path = os.path.join(tempdir, os.path.basename(package_path))
            resolved_fs.get(package_path, local_copy_path)
            info_from_storage = PexInfo.from_pex(local_copy_path)
            info_to_upload = PexInfo.from_pex(zip_file)
            if not force_upload and info_from_storage.code_hash == info_to_upload.code_hash:
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
) -> None:
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
        packer: packaging.Packer = packaging.PEX_PACKER,
        additional_packages: Dict[str, str] = {},
        ignored_packages: Collection[str] = [],
        resolved_fs: Any = None,
        force_upload: bool = False,
        include_editable: bool = False,
        allow_large_pex: bool = False,
        additional_repo: Optional[Union[str, List[str]]] = None,
        additional_indexes: Optional[List[str]] = None
) -> None:
    executable = packaging.get_current_pex_filepath() \
        if packaging._running_from_pex() else sys.executable
    current_packages = packaging.get_non_editable_requirements(executable)

    reqs = _build_reqs_from_venv(additional_packages, current_packages, ignored_packages)

    _logger.debug(f"Packaging current_packages={reqs}")

    if not force_upload and _is_archive_up_to_date(package_path, reqs, resolved_fs):
        _logger.info(f"{package_path} already exists")
        return

    with tempfile.TemporaryDirectory() as tempdir:
        local_package_path = _pack_from_venv(executable, reqs, tempdir, packer, additional_packages,
                                             ignored_packages, force_upload, include_editable,
                                             allow_large_pex, additional_repo, additional_indexes)

        dir = os.path.dirname(package_path)
        if not resolved_fs.exists(dir):
            resolved_fs.mkdir(dir)
        _logger.info(f'Uploading env at {local_package_path} to {package_path}')
        resolved_fs.put(local_package_path, package_path)

        _dump_archive_metadata(package_path, reqs, resolved_fs)


def _build_reqs_from_venv(
        additional_packages: Dict[str, str],
        current_packages: Dict[str, str],
        ignored_packages: Collection[str]) -> List[str]:
    _handle_packages(
        current_packages,
        additional_packages,
        ignored_packages
    )
    return packaging.format_requirements(current_packages)


def _pack_from_venv(executable: str,
                    reqs: List[str],
                    tempdir: str,
                    packer: packaging.Packer = packaging.PEX_PACKER,
                    additional_packages: Dict[str, str] = {},
                    ignored_packages: Collection[str] = [],
                    force_upload: bool = False,
                    include_editable: bool = False,
                    allow_large_pex: bool = False,
                    additional_repo: Optional[Union[str, List[str]]] = None,
                    additional_indexes: Optional[List[str]] = None) -> str:
    env_copied_from_fallback_location = False
    local_package_path = f'{tempdir}/{packer.env_name()}.{packer.extension()}'
    local_fs, local_package_path = filesystem.resolve_filesystem_and_path(local_package_path)
    fallback_path = os.environ.get('C_PACK_ENV_FALLBACK_PATH')
    if not force_upload and fallback_path and packer.extension() == 'pex':
        _logger.info(f"Copying pre-built env from {fallback_path} to {local_package_path}")
        if fallback_path.startswith("http://") or fallback_path.startswith("https://"):
            request.urlretrieve(fallback_path, local_package_path)
        else:
            fallback_fs, fallback_path = filesystem.resolve_filesystem_and_path(fallback_path)
            fallback_fs.get(fallback_path, local_package_path)

        _logger.info(f'Checking requirements in {local_package_path}')

        pex_info = PexInfo.from_pex(local_package_path)

        req_from_pex = _filter_out_requirements(
            _sort_requirements(
                _normalize_requirements(
                    _format_pex_requirements(pex_info)
                )
            )
        )
        req_from_venv = _filter_out_requirements(
            _sort_requirements(
                _normalize_requirements(reqs)
            )
        )

        if (req_from_pex == req_from_venv):
            env_copied_from_fallback_location = True
            _dump_archive_metadata(local_package_path, reqs, local_fs)
            _logger.info('Env copied from fallback location')
        else:
            _logger.warning(f'Requirements not met for pre-built {local_package_path}')
            _logger.info(f'Requirements from pex {req_from_pex}')
            _logger.info(f'Requirements from venv {req_from_venv}')
    if not env_copied_from_fallback_location:
        if include_editable:
            editable_requirements = packaging.get_editable_requirements(executable)
        else:
            editable_requirements = {}

        _logger.info(f"Generating and zipping your env to {local_package_path}")
        local_package_path = packer.pack(
            output=local_package_path,
            reqs=reqs,
            additional_packages=additional_packages,
            ignored_packages=ignored_packages,
            editable_requirements=editable_requirements,
            allow_large_pex=allow_large_pex,
            additional_repo=additional_repo,
            additional_indexes=additional_indexes
        )
    return local_package_path


def _sort_requirements(a: List[str]) -> List[str]:
    return sorted([item.lower() for item in a])


def _format_pex_requirements(pex_info: PexInfo) -> List[str]:
    reqs = [parse_wheel_filename(req) for req in pex_info.distributions.keys()]
    return [f"{req.project}=={req.version}" for req in reqs]


def _normalize_requirements(reqs: List[str]) -> List[str]:
    return [req.replace('_', '-') for req in reqs]


def _filter_out_requirements(reqs: List[str]) -> List[str]:
    def _keep(req: str) -> bool:
        return all([d not in req for d in ["wheel", "pip", "setuptools"]])

    return [req for req in reqs if _keep(req)]
