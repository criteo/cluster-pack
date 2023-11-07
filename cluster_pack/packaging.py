import getpass
import imp
import json
import logging
import os
import pathlib
import shutil
import subprocess
from subprocess import CalledProcessError
import sys
import tempfile
from typing import (
    Tuple, Dict,
    Collection, List, Any, Optional, NamedTuple, Union
)
import uuid
import zipfile
import setuptools

from cluster_pack import conda

CRITEO_PYPI_URL = "https://filer-build-pypi.prod.crto.in/repository/criteo.moab.pypi-read/simple"

CONDA_DEFAULT_ENV = 'CONDA_DEFAULT_ENV'

EDITABLE_PACKAGES_INDEX = 'editable_packages_index'

_logger = logging.getLogger(__name__)

JsonDictType = Dict[str, Any]


class PexTooLargeError(RuntimeError):
    pass


class PexCreationError(RuntimeError):
    pass


class PythonEnvDescription(NamedTuple):
    path_to_archive: str
    interpreter_cmd: str
    dest_path: str
    must_unpack: bool


UNPACKED_ENV_NAME = "pyenv"
CONDA_CMD = f"{UNPACKED_ENV_NAME}/bin/python"
LARGE_PEX_CMD = f"{UNPACKED_ENV_NAME}/__main__.py"


def _get_tmp_dir() -> str:
    tmp_dir = f"/tmp/{uuid.uuid1()}"
    _logger.debug(f"local tmp_dir {tmp_dir}")
    os.makedirs(tmp_dir, exist_ok=True)
    return tmp_dir


def zip_path(py_dir: str, include_base_name: bool = True, tmp_dir: str = _get_tmp_dir()) -> str:
    """
    Zip current directory

    :param py_dir: directory to zip
    :param include_base_name: include the basename of py_dir into the archive (
        for skein zip files it should be False,
        for pyspark zip files it should be True)
    :return: destination of the archive
    """
    py_archive = os.path.join(
        tmp_dir,
        os.path.basename(py_dir) + '.zip'
    )

    with zipfile.ZipFile(py_archive, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(py_dir):
            for file in files:
                # do not include .pyc files, it makes the import
                # fail for no obvious reason
                if not file.endswith(".pyc"):
                    zipf.write(
                        os.path.join(root, file),
                        os.path.join(
                            os.path.basename(py_dir) if include_base_name else "",
                            os.path.relpath(root, py_dir),
                            file
                        )
                        if root != py_dir
                        else os.path.join(
                            os.path.basename(root) if include_base_name else "",
                            file
                        ))
    return py_archive


def format_requirements(requirements: Dict[str, str]) -> List[str]:
    if requirements is None:
        return list()
    else:
        return [name + "==" + version
                if version else name
                for name, version in requirements.items()]


def pack_spec_in_pex(spec_file: str,
                     output: str,
                     pex_inherit_path: str = "fallback",
                     allow_large_pex: bool = False,
                     additional_repo: Optional[Union[List[str], str]] = None,
                     additional_indexes: Optional[List[str]] = None) -> str:
    with open(spec_file, "r") as f:
        lines = [line for line in f.read().splitlines()
                 if line and not line.startswith("#")]
        _logger.debug(f"used requirements: {lines}")
        return pack_in_pex(lines, output, pex_inherit_path=pex_inherit_path,
                           allow_large_pex=allow_large_pex,
                           additional_repo=additional_repo,
                           additional_indexes=additional_indexes)


def pack_in_pex(requirements: List[str],
                output: str,
                ignored_packages: Collection[str] = [],
                pex_inherit_path: str = "fallback",
                editable_requirements: Dict[str, str] = {},
                allow_large_pex: bool = False,
                additional_repo: Optional[Union[str, List[str]]] = None,
                additional_indexes: Optional[List[str]] = None
                ) -> str:
    """Pack current environment using a pex.

    :param additional_repo: an additional pypi repo if one was used env creation
    :param requirements: list of requirements (ex {'tensorflow': '1.15.0'})
    :param output: location of the pex
    :param ignored_packages: packages to be exluded from pex
    :param pex_inherit_path: see https://github.com/pantsbuild/pex/blob/master/pex/bin/pex.py#L264,
                             possible values ['false', 'fallback', 'prefer']
    :param allow_large_pex: Creates a non-executable pex that will need to be unzipped to circumvent
                            python's limitation with zips > 2Gb. The file will need to be unzipped
                            and the entry point will be <output>/__main__.py
    :return: destination of the archive, name of the pex
    """

    with tempfile.TemporaryDirectory() as tempdir:
        cmd = ["pex", f"--inherit-path={pex_inherit_path}"]

        if allow_large_pex:
            cmd.extend(["--layout", "packed"])
            tmp_ext = ".tmp"
        else:
            tmp_ext = ""

        if editable_requirements and len(editable_requirements) > 0:
            for current_package in editable_requirements.values():
                _logger.debug("Add current path as source", current_package)
                shutil.copytree(
                    current_package, os.path.join(tempdir, os.path.basename(current_package))
                )
            cmd.append(f"--sources-directory={tempdir}")

        for req in requirements:
            pkg_name = req.split("=")[0]
            if pkg_name in ignored_packages:
                _logger.debug(f"Ignore requirement {req}")
            else:
                _logger.debug(f"Add requirement {req}")
                cmd.append(req)
        if _is_criteo():
            cmd.append(f"--index-url={CRITEO_PYPI_URL}")

        if additional_repo is not None:
            additional_repo = additional_repo if isinstance(additional_repo, list) \
                else [additional_repo]
            for repo in additional_repo:
                cmd.append(f"--index-url={repo}")

        if additional_indexes:
            for index in additional_indexes:
                cmd.extend(["-f", index])

        cmd.extend(["-o", output + tmp_ext])

        try:
            print(f"Running command: {' '.join(cmd)}")
            call = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            call.check_returncode()

        except CalledProcessError as err:
            _logger.exception('Cannot create pex')
            _logger.exception(err.stderr.decode("ascii"))
            raise PexCreationError(err.stderr.decode("ascii"))

        if not allow_large_pex and os.path.getsize(output + tmp_ext) > 2 * 1024 * 1024 * 1024:
            raise PexTooLargeError("The generate pex is larger than 2Gb and won't be executable"
                                   " by python; Please set the 'allow_large_pex' "
                                   "flag in upload_env")

        if allow_large_pex:
            shutil.make_archive(output, 'zip', output + tmp_ext)

    return output + '.zip' if allow_large_pex else output


def _get_packages(editable: bool, executable: str = sys.executable) -> List[JsonDictType]:
    editable_mode = "-e" if editable else "--exclude-editable"
    # We only keep the first line because pip warnings on subsequent lines can cause
    # JSONDecodeError below
    results = subprocess.check_output(
        [f"{executable}", "-m", "pip", "list", "-l",
         f"{editable_mode}", "--format", "json", "-v"]).decode().split("\n")[0]

    _logger.debug(f"'pip list' with editable={editable} results:" + results)

    try:
        return json.loads(results)
    except json.JSONDecodeError as e:
        _logger.error(f"Caught below exception while parsing output of pip list: {results}")
        raise e


class Packer(object):
    def env_name(self) -> str:
        raise NotImplementedError

    def extension(self) -> str:
        raise NotImplementedError

    def pack(self,
             output: str,
             reqs: List[str],
             additional_packages: Dict[str, str],
             ignored_packages: Collection[str],
             editable_requirements: Dict[str, str],
             allow_large_pex: bool = False,
             additional_repo: Optional[Union[str, List[str]]] = None,
             additional_indexes: Optional[List[str]] = None) -> str:
        raise NotImplementedError

    def pack_from_spec(self,
                       spec_file: str,
                       output: str,
                       allow_large_pex: bool = False) -> str:
        raise NotImplementedError


def get_env_name(env_var_name: str) -> str:
    """
    Return default virtual env
    """
    virtual_env_path = os.environ.get(env_var_name)
    if not virtual_env_path:
        return 'default'
    else:
        return os.path.basename(virtual_env_path)


class CondaPacker(Packer):
    def env_name(self) -> str:
        return pathlib.Path(sys.executable).parents[1].name

    def extension(self) -> str:
        return 'tar.gz'

    def pack(self,
             output: str,
             reqs: List[str],
             additional_packages: Dict[str, str],
             ignored_packages: Collection[str],
             editable_requirements: Dict[str, str],
             allow_large_pex: bool = False,
             additional_repo: Optional[Union[str, List[str]]] = None,
             additional_indexes: Optional[List[str]] = None) -> str:
        return conda.pack_venv_in_conda(
            self.env_name(),
            reqs,
            len(additional_packages) > 0 or len(ignored_packages) > 0,
            output,
            additional_repo,
            additional_indexes)

    def pack_from_spec(self,
                       spec_file: str,
                       output: str,
                       allow_large_pex: bool = False) -> str:
        return conda.create_and_pack_conda_env(
            spec_file=spec_file,
            reqs=None,
            output=output)


class PexPacker(Packer):
    def env_name(self) -> str:
        return get_env_name('VIRTUAL_ENV')

    def extension(self) -> str:
        return 'pex'

    def pack(self,
             output: str,
             reqs: List[str],
             additional_packages: Dict[str, str],
             ignored_packages: Collection[str],
             editable_requirements: Dict[str, str],
             allow_large_pex: bool = False,
             additional_repo: Optional[Union[str, List[str]]] = None,
             additional_indexes: Optional[List[str]] = None) -> str:
        return pack_in_pex(reqs,
                           output,
                           ignored_packages,
                           editable_requirements=editable_requirements,
                           allow_large_pex=allow_large_pex,
                           additional_repo=additional_repo,
                           additional_indexes=additional_indexes)

    def pack_from_spec(self,
                       spec_file: str,
                       output: str,
                       allow_large_pex: bool = False) -> str:
        return pack_spec_in_pex(spec_file=spec_file, output=output, allow_large_pex=allow_large_pex)


CONDA_PACKER = CondaPacker()
PEX_PACKER = PexPacker()


def _get_editable_requirements(executable: str = sys.executable) -> List[str]:
    top_level_pkgs = []
    for pkg in _get_packages(True, executable):
        location = pkg.get("editable_project_location", pkg.get("location", ""))
        packages_found = set(
            setuptools.find_packages(location) + setuptools.find_packages(f"{location}/src")
            )
        for _pkg in packages_found:
            if "." in _pkg:
                continue
            imported = __import__(_pkg)
            top_level_pkgs.append(os.path.dirname(imported.__file__))
    return top_level_pkgs


def get_non_editable_requirements(executable: str = sys.executable) -> Dict[str, str]:
    return {package["name"]: package["version"]
            for package in _get_packages(False, executable)}


def detect_archive_names(
        packer: Packer,
        package_path: str = None,
        allow_large_pex: bool = None) -> Tuple[str, str, str]:
    if _running_from_pex():
        pex_file = get_current_pex_filepath()
        env_name = os.path.splitext(os.path.basename(pex_file))[0]
    else:
        pex_file = ""
        env_name = packer.env_name()

    if not package_path:
        package_path = (f"{get_default_fs()}/user/{getpass.getuser()}"
                        f"/envs/{env_name}.{packer.extension()}")
    else:
        if "".join(os.path.splitext(package_path)[1]) != f".{packer.extension()}":
            raise ValueError(f"{package_path} has the wrong extension"
                             f", .{packer.extension()} is expected")

    if (packer.extension() == PEX_PACKER.extension()
            and allow_large_pex
            and not package_path.endswith('.zip')):
        package_path += '.zip'

    return package_path, env_name, pex_file


def detect_packer_from_spec(spec_file: str) -> Packer:
    if os.path.basename(spec_file) == "requirements.txt":
        return PEX_PACKER
    elif spec_file.endswith(".yaml") or spec_file.endswith(".yml"):
        return CONDA_PACKER
    else:
        raise ValueError(f"Archive format {spec_file} unsupported. "
                         "Must be requirements.txt or conda .yaml")


def detect_packer_from_env() -> Packer:
    if _is_conda_env():
        return CONDA_PACKER
    else:
        return PEX_PACKER


def detect_packer_from_file(zip_file: str) -> Packer:
    if zip_file.endswith('.pex') or zip_file.endswith('.pex.zip'):
        return PEX_PACKER
    elif zip_file.endswith(".zip") or zip_file.endswith(".tar.gz"):
        return CONDA_PACKER
    else:
        raise ValueError(f"Archive format {zip_file} unsupported. "
                         "Must be .pex or conda .zip/.tar.gz")


def get_current_pex_filepath() -> str:
    """
    If we run from a pex, returns the path
    """
    # Env variable PEX has been introduced in pex==2.1.54 and is now the
    # preferred way to detect whether we run from within a pex
    if "PEX" in os.environ:
        return os.environ["PEX"]

    # We still temporarilly support the previous way
    try:
        import _pex
        return os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(_pex.__file__))))
    except ModuleNotFoundError:
        raise RuntimeError("Trying to get current pex file path while not running from PEX")


def get_editable_requirements(
        executable: str = sys.executable,
        editable_packages_dir: str = os.getcwd()
) -> Dict[str, str]:
    editable_requirements: Dict[str, str] = {}
    if _running_from_pex():
        try:
            package_names = open(
                f"{editable_packages_dir}/{EDITABLE_PACKAGES_INDEX}"
            ).read().splitlines()
        except FileNotFoundError:
            editable_requirements = {}
        else:
            for package_name in package_names:
                try:
                    _, path, _ = imp.find_module(package_name)
                    editable_requirements[os.path.basename(path)] = path
                except ImportError:
                    _logger.error(f"Could not import package {package_name}"
                                  f" repo exists={os.path.exists(package_name)}")
    else:
        editable_requirements = {os.path.basename(requirement_dir): requirement_dir
                                 for requirement_dir in _get_editable_requirements(executable)}

    _logger.info(f"found editable requirements {editable_requirements}")
    return editable_requirements


def get_pyenv_usage_from_archive(path_to_archive: str) -> PythonEnvDescription:

    archive_filename = os.path.basename(path_to_archive)

    if archive_filename.endswith('.pex.zip'):
        return PythonEnvDescription(
            path_to_archive,
            LARGE_PEX_CMD,
            UNPACKED_ENV_NAME,
            True)
    elif archive_filename.endswith('.pex'):
        return PythonEnvDescription(
            path_to_archive,
            f"./{archive_filename}",
            archive_filename,
            False)
    elif archive_filename.endswith(".zip") or archive_filename.endswith(".tar.gz"):
        return PythonEnvDescription(
            path_to_archive,
            CONDA_CMD,
            UNPACKED_ENV_NAME,
            True)
    else:
        raise ValueError(f"Archive format {archive_filename} unsupported. "
                         "Must be .pex/pex.zip or conda .zip/.tar.gz")


def get_default_fs() -> str:
    return subprocess.check_output("hdfs getconf -confKey fs.defaultFS".split()).strip().decode()


def _is_conda_env() -> bool:
    return os.environ.get(CONDA_DEFAULT_ENV) is not None


def _running_from_pex() -> bool:
    # Env variable PEX has been introduced in pex==2.1.54 and is now the
    # preferred way to detect whether we run from within a pex
    if "PEX" in os.environ:
        return True

    # We still temporarilly support the previous way
    try:
        import _pex
        return True
    except ModuleNotFoundError:
        return False


def _is_criteo() -> bool:
    return "CRITEO_ENV" in os.environ
