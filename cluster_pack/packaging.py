import getpass
import hashlib
import importlib.util
import json
import logging
import os
import glob
import shutil
import subprocess
from subprocess import CalledProcessError
import sys
import tempfile
from typing import Tuple, Dict, List, Any, Optional, NamedTuple, Union
import uuid
import zipfile
import setuptools
from packaging.version import Version
from importlib.metadata import version as pkg_version

from cluster_pack import dependencies

CRITEO_PYPI_URL = (
    "https://filer-build-pypi.prod.crto.in/repository/criteo.moab.pypi-read/simple"
)

EDITABLE_PACKAGES_INDEX = "editable_packages_index"

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
LARGE_PEX_CMD = f"{UNPACKED_ENV_NAME}/__main__.py"

UV_AVAILABLE: bool = False
USE_ZIPFILE: bool = False

VENV_OPTIMIZATION_LEVEL: int = int(os.environ.get("CLUSTER_PACK_VENV_OPTIMIZATION_LEVEL", "1"))


def set_venv_optimization_level(level: int) -> None:
    """Set the venv optimization level for pex builds.

    :param level: optimization level (0=disabled, 1=default optimizations, 2=aggressive)
    """
    global VENV_OPTIMIZATION_LEVEL
    VENV_OPTIMIZATION_LEVEL = level


def _detect_uv() -> bool:
    """Detect if uv is installed and available in PATH."""
    if shutil.which("uv") is not None:
        return True
    else:
        _logger.warning(f"Cluster-pack can now interact with uv to speed up uploads, it is recommended to install it.") #noqa: E501
        return False


UV_AVAILABLE = _detect_uv()


def set_use_zipfile(use_zipfile: bool) -> None:
    """Set whether to use zipfile module instead of shutil.make_archive for zipping.

    :param use_zipfile: True to use zipfile module, False to use shutil.make_archive
    """
    global USE_ZIPFILE
    USE_ZIPFILE = use_zipfile


def _make_zip_archive_zipfile(output_zip: str, source_dir: str, compresslevel: int = 1) -> None:
    """Create a zip archive using zipfile module with specified compression level.

    :param output_zip: output path (with .zip extension)
    :param source_dir: directory to compress
    :param compresslevel: compression level 0-9 (0=store, 1=fastest, 9=best)
    """
    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED, compresslevel=compresslevel) as zf:
        for root, _, files in os.walk(source_dir):
            for file in files:
                full_path = os.path.join(root, file)
                arc_name = os.path.relpath(full_path, source_dir)
                zf.write(full_path, arc_name)


def _make_zip_archive(output: str, source_dir: str) -> None:
    """Create a zip archive from source_dir.

    Uses zipfile module if USE_ZIPFILE is True, otherwise uses shutil.make_archive.

    :param output: output path without .zip extension
    :param source_dir: directory to compress
    """
    if USE_ZIPFILE:
        _logger.info("Creating zip archive with zipfile (compresslevel=1)")
        _make_zip_archive_zipfile(output + ".zip", source_dir, compresslevel=1)
    else:
        _logger.info("Creating zip archive with shutil.make_archive")
        shutil.make_archive(output, "zip", source_dir)


def _get_tmp_dir() -> str:
    tmp_dir = f"/tmp/{uuid.uuid1()}"
    _logger.debug(f"local tmp_dir {tmp_dir}")
    os.makedirs(tmp_dir, exist_ok=True)
    return tmp_dir


def zip_path(
    py_dir: str, include_base_name: bool = True, tmp_dir: str = _get_tmp_dir()
) -> str:
    """
    Zip current directory

    :param py_dir: directory to zip
    :param include_base_name: include the basename of py_dir into the archive (
        for skein zip files it should be False,
        for pyspark zip files it should be True)
    :return: destination of the archive
    """
    py_archive = os.path.join(tmp_dir, os.path.basename(py_dir) + ".zip")

    with zipfile.ZipFile(py_archive, "w", zipfile.ZIP_DEFLATED) as zipf:
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
                            file,
                        )
                        if root != py_dir
                        else os.path.join(
                            os.path.basename(root) if include_base_name else "", file
                        ),
                    )
    return py_archive


def check_large_pex(allow_large_pex: bool, pex_file: str) -> None:
    if allow_large_pex:
        return

    max_pex_size_gb = 2 if Version(pkg_version('pex')) < Version("2.41.1") else 4
    if os.path.getsize(pex_file) > max_pex_size_gb * 1024 * 1024 * 1024:
        raise PexTooLargeError(
            f"The generate pex is larger than {max_pex_size_gb}Gb and won't be executable"
            " by python; Please set the 'allow_large_pex' flag in upload_env"
        )


def pack_in_pex(
    requirements: List[str],
    output: str,
    pex_inherit_path: str = "fallback",
    editable_requirements: Optional[Dict[str, str]] = None,
    allow_large_pex: bool = False,
    include_pex_tools: bool = False,
    additional_repo: Optional[Union[str, List[str]]] = None,
    additional_indexes: Optional[List[str]] = None,
) -> str:
    """Pack current environment using a pex.
    :param additional_repo: an additional pypi repo if one was used env creation
    :param requirements: list of requirements (ex {'tensorflow': '1.15.0'})
    :param output: location of the pex
    :param pex_inherit_path: see https://github.com/pantsbuild/pex/blob/master/pex/bin/pex.py#L264,
                             possible values ['false', 'fallback', 'prefer']
    :param allow_large_pex: Creates a non-executable pex that will need to be unzipped to circumvent
                            python's limitation with zips > 2Gb. The file will need to be unzipped
                            and the entry point will be <output>/__main__.py
    :return: destination of the archive, name of the pex
    """

    editable_requirements = editable_requirements or {}

    with tempfile.TemporaryDirectory() as tempdir:
        cmd = ["pex", f"--inherit-path={pex_inherit_path}"]

        if allow_large_pex:
            cmd.extend(["--layout", "packed"])
            tmp_ext = ".tmp"
        else:
            tmp_ext = ""

        if include_pex_tools:
            cmd.extend(["--include-tools"])

        if VENV_OPTIMIZATION_LEVEL >= 1:
            if (pex_inherit_path != "false"
                    and dependencies.check_venv_has_requirements(None, requirements)):
                cmd.extend(["--venv-repository"])
            elif UV_AVAILABLE:
                venv_repo_path = os.path.join(tempdir, "venv_repo")
                dependencies.create_uv_venv(
                    venv_repo_path,
                    requirements,
                    additional_repo=additional_repo,
                    additional_indexes=additional_indexes,
                )
                cmd.extend(["--venv-repository", venv_repo_path])
            else:
                _logger.warning("Not running from venv or venv missing some requirements, "
                                "skipping optimization because `uv ` is not available.")

            cmd.extend(["--max-install-jobs", "0"])

        if VENV_OPTIMIZATION_LEVEL >= 2:
            cmd.extend(["--no-pre-install-wheel"])

        sources_dir = os.path.join(tempdir, "sources")
        if editable_requirements and len(editable_requirements) > 0:
            os.makedirs(sources_dir, exist_ok=True)
            for current_package in editable_requirements.values():
                _logger.debug("Add current path as source", current_package)
                shutil.copytree(
                    current_package,
                    os.path.join(sources_dir, os.path.basename(current_package)),
                )
            cmd.append(f"--sources-directory={sources_dir}")

        for req in requirements:
            _logger.debug(f"Add requirement {req}")
            cmd.append(req)

        if _is_criteo():
            cmd.append(f"--index-url={CRITEO_PYPI_URL}")

        if additional_repo is not None:
            repos = additional_repo if isinstance(additional_repo, list) else [additional_repo]
            for repo in repos:
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
            _logger.exception("Cannot create pex")
            _logger.exception(err.stderr.decode("ascii"))
            raise PexCreationError(err.stderr.decode("ascii"))

        check_large_pex(allow_large_pex, output + tmp_ext)

        if allow_large_pex:
            _make_zip_archive(output, output + tmp_ext)

    return output + ".zip" if allow_large_pex else output


def _get_packages(
    editable: bool, executable: str = sys.executable
) -> List[JsonDictType]:
    editable_mode = "-e" if editable else "--exclude-editable"
    # We only keep the first line because pip warnings on subsequent lines can cause
    # JSONDecodeError below
    results = (
        subprocess.check_output(
            [
                f"{executable}",
                "-m",
                "pip",
                "list",
                "-l",
                f"{editable_mode}",
                "--format",
                "json",
                "-v",
            ]
        )
        .decode()
        .split("\n")[0]
    )

    _logger.debug(f"'pip list' with editable={editable} results:" + results)

    try:
        return json.loads(results)
    except json.JSONDecodeError as e:
        _logger.error(
            f"Caught below exception while parsing output of pip list: {results}"
        )
        raise e


class Packer(object):
    def env_name(self) -> str:
        raise NotImplementedError

    def extension(self) -> str:
        raise NotImplementedError

    def pack(
        self,
        output: str,
        reqs: List[str],
        editable_requirements: Dict[str, str],
        allow_large_pex: bool = False,
        include_pex_tools: bool = False,
        additional_repo: Optional[Union[str, List[str]]] = None,
        additional_indexes: Optional[List[str]] = None,
    ) -> str:
        raise NotImplementedError


def get_env_name(env_var_name: str) -> str:
    """
    Return a stable, collision-resistant environment name.

    When the environment variable is set (e.g. VIRTUAL_ENV), we append a short hash
    of its path to the basename. This prevents collisions when multiple venvs share
    the same directory name (e.g. many projects using ".venv" or "venv-linux").
    """
    virtual_env_path = os.environ.get(env_var_name)
    if not virtual_env_path:
        return "default"
    else:
        normalized_path = os.path.normpath(
            os.path.realpath(os.path.abspath(virtual_env_path))
        )
        short_hash = hashlib.sha1(normalized_path.encode("utf-8")).hexdigest()[:7]
        base = os.path.basename(normalized_path)
        return f"{base}_{short_hash}"


class PexPacker(Packer):
    def env_name(self) -> str:
        return get_env_name("VIRTUAL_ENV")

    def extension(self) -> str:
        return "pex"

    def pack(
        self,
        output: str,
        reqs: List[str],
        editable_requirements: Dict[str, str],
        allow_large_pex: bool = False,
        include_pex_tools: bool = False,
        additional_repo: Optional[Union[str, List[str]]] = None,
        additional_indexes: Optional[List[str]] = None,
    ) -> str:
        return pack_in_pex(
            reqs,
            output,
            editable_requirements=editable_requirements,
            allow_large_pex=allow_large_pex,
            include_pex_tools=include_pex_tools,
            additional_repo=additional_repo,
            additional_indexes=additional_indexes,
        )


PEX_PACKER = PexPacker()


def _get_editable_requirements(executable: str = sys.executable) -> List[str]:
    top_level_pkgs = []
    for pkg in _get_packages(True, executable):
        location = pkg.get("editable_project_location", pkg.get("location", ""))
        packages_found = set(
            setuptools.find_packages(location)
            + setuptools.find_packages(f"{location}/src")
        )
        for _pkg in packages_found:
            if "." in _pkg:
                continue
            imported = __import__(_pkg)
            if imported.__file__:
                top_level_pkgs.append(os.path.dirname(imported.__file__))
    return top_level_pkgs


def get_non_editable_requirements(executable: str = sys.executable) -> Dict[str, str]:
    return {
        package["name"]: package["version"]
        for package in _get_packages(False, executable)
    }


def _build_package_path(name: str, extension: Optional[str]) -> str:
    path = f"{get_default_fs()}/user/{getpass.getuser()}/envs/{name}"
    if extension is None:
        return path
    return f"{path}.{extension}"


def detect_archive_names(
    packer: Packer, package_path: str = None, allow_large_pex: bool = None
) -> Tuple[str, str, str]:
    if _running_from_pex():
        pex_file = get_current_pex_filepath()
        env_name = os.path.splitext(os.path.basename(pex_file))[0]
    else:
        pex_file = ""
        env_name = packer.env_name()
    extension = packer.extension()

    if not package_path:
        package_path = _build_package_path(env_name, extension)
    else:
        if "".join(os.path.splitext(package_path)[1]) != f".{extension}":
            raise ValueError(
                f"{package_path} has the wrong extension"
                f", .{packer.extension()} is expected"
            )

    # we are actually building or reusing a large pex and we have the information from the
    # allow_large_pex flag
    if (
        extension == PEX_PACKER.extension()
        and allow_large_pex
        and not package_path.endswith(".zip")
    ):
        package_path += ".zip"

    # We are running from an unzipped large pex and we have the information because `pex_file` is
    # not empty, and it is a directory instead of a zipapp
    if pex_file != "" and os.path.isdir(pex_file) and not package_path.endswith(".zip"):
        zip_pex_file = resolve_zip_from_pex_dir(pex_file)
        package_path = _build_package_path(os.path.basename(zip_pex_file), None)

    return package_path, env_name, pex_file


def resolve_zip_from_pex_dir(pex_dir: str) -> str:
    parent_dir = os.path.dirname(pex_dir)
    pex_files = glob.glob(f"{parent_dir}/*.pex.zip")
    if len(pex_files) == 1:
        return pex_files[0]

    pex_file = next((x for x in pex_files if pex_dir == x.split(".zip")[0]), None)
    if pex_file is None:
        raise ValueError(f"{pex_dir}.zip not found, found {pex_files}")

    return pex_file


def detect_packer_from_env() -> Packer:
    return PEX_PACKER


def detect_packer_from_file(zip_file: str) -> Packer:
    if zip_file.endswith(".pex") or zip_file.endswith(".pex.zip"):
        return PEX_PACKER
    else:
        raise ValueError(f"Archive format {zip_file} unsupported. Must be .pex or .pex.zip")


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

        return os.path.abspath(
            os.path.dirname(os.path.dirname(os.path.dirname(_pex.__file__)))
        )
    except ModuleNotFoundError:
        raise RuntimeError(
            "Trying to get current pex file path while not running from PEX"
        )


def get_editable_requirements(
    executable: str = sys.executable,
    editable_packages_dir: str = os.getcwd(),  # only overridden for tests
) -> Dict[str, str]:
    editable_requirements: Dict[str, str] = {}
    if _running_from_pex():
        try:
            package_names = (
                open(f"{editable_packages_dir}/{EDITABLE_PACKAGES_INDEX}")
                .read()
                .splitlines()
            )
        except FileNotFoundError:
            editable_requirements = {}
        else:
            for package_name in package_names:
                try:
                    spec = importlib.util.find_spec(package_name)
                    if spec is None or spec.origin is None:
                        raise ModuleNotFoundError(f"No module named '{package_name}'")
                    path = os.path.dirname(spec.origin)
                    editable_requirements[os.path.basename(path)] = path
                except ModuleNotFoundError:
                    _logger.error(
                        f"Could not import package {package_name}"
                        f" repo exists={os.path.exists(package_name)}"
                    )
    else:
        editable_requirements = {
            os.path.basename(requirement_dir): requirement_dir
            for requirement_dir in _get_editable_requirements(executable)
        }

    _logger.info(f"found editable requirements {editable_requirements}")
    return editable_requirements


def get_pyenv_usage_from_archive(path_to_archive: str) -> PythonEnvDescription:
    archive_filename = os.path.basename(path_to_archive)

    if archive_filename.endswith(".pex.zip"):
        return PythonEnvDescription(
            path_to_archive, LARGE_PEX_CMD, UNPACKED_ENV_NAME, True
        )
    elif archive_filename.endswith(".pex"):
        return PythonEnvDescription(
            path_to_archive, f"./{archive_filename}", archive_filename, False
        )
    else:
        raise ValueError(
            f"Archive format {archive_filename} unsupported. Must be .pex/pex.zip"
        )


def get_default_fs() -> str:
    return (
        subprocess.check_output("hdfs getconf -confKey fs.defaultFS".split())
        .strip()
        .decode()
    )


def _running_from_pex() -> bool:
    # Env variable PEX has been introduced in pex==2.1.54 and is now the
    # preferred way to detect whether we run from within a pex
    return "PEX" in os.environ


def _is_criteo() -> bool:
    return "CRITEO_ENV" in os.environ
