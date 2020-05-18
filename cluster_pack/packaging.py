import getpass
import imp
import json
import logging
import os
import pathlib
import shutil
import subprocess
from subprocess import Popen, CalledProcessError, PIPE
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

try:
    import conda_pack
except NotImplementedError:
    # conda is not supported on windows
    pass
from pex.pex_builder import PEXBuilder
from pex.resolver import resolve_multi, Unsatisfiable, Untranslateable
from pex.pex_info import PexInfo
from pex.interpreter import PythonInterpreter

from cluster_pack import filesystem

CRITEO_PYPI_URL = "http://build-nexus.prod.crto.in/repository/pypi/simple"

CONDA_DEFAULT_ENV = 'CONDA_DEFAULT_ENV'

EDITABLE_PACKAGES_INDEX = 'editable_packages_index'

_logger = logging.getLogger(__name__)


def _get_tmp_dir():
    tmp_dir = f"/tmp/{uuid.uuid1()}"
    _logger.debug(f"local tmp_dir {tmp_dir}")
    os.makedirs(tmp_dir, exist_ok=True)
    return tmp_dir


def zip_path(py_dir: str, include_base_name=True, tmp_dir: str = _get_tmp_dir()):
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


# from https://github.com/pantsbuild/pex/blob/451977efdf987dd299a1b4798ac2ee298cd6d61b/
# pex/bin/pex.py#L644
def _walk_and_do(fn, src_dir):
    src_dir = os.path.normpath(src_dir)
    for root, dirs, files in os.walk(src_dir):
        for f in files:
            src_file_path = os.path.join(root, f)
            dst_path = os.path.relpath(src_file_path, src_dir)
            fn(src_file_path, dst_path)


def pack_in_pex(requirements: Dict[str, str],
                output: str,
                ignored_packages: Collection[str] = [],
                pex_inherit_path: str = "prefer",
                editable_requirements:  Dict[str, str] = {}
                ) -> str:
    """
    Pack current environment using a pex.

    :param requirements: list of requirements (ex {'tensorflow': '1.15.0'})
    :param output: location of the pex
    :param ignored_packages: packages to be exluded from pex
    :param pex_inherit_path: see https://github.com/pantsbuild/pex/blob/master/pex/bin/pex.py#L264,
                             possible values ['false', 'fallback', 'prefer']
    :return: destination of the archive, name of the pex
    """

    interpreter = PythonInterpreter.get()
    pex_info = PexInfo.default(interpreter)
    pex_info.inherit_path = pex_inherit_path
    pex_builder = PEXBuilder(
        copy=True,
        interpreter=interpreter,
        pex_info=pex_info)

    for current_package in editable_requirements.values():
        _logger.debug("Add current path as source", current_package)
        _walk_and_do(pex_builder.add_source, current_package)

    requirements_to_install = format_requirements(requirements)

    try:
        resolveds = resolve_multi(
            requirements=requirements_to_install,
            indexes=[CRITEO_PYPI_URL] if _is_criteo() else None)

        for resolved in resolveds:
            if resolved.distribution.key in ignored_packages:
                _logger.debug(f"Ignore requirement {resolved.distribution}")
                continue
            else:
                _logger.debug(f"Add requirement {resolved.distribution}")
            pex_builder.add_distribution(resolved.distribution)
            pex_builder.add_requirement(resolved.requirement)
    except (Unsatisfiable, Untranslateable):
        _logger.exception('Cannot create pex')
        raise

    pex_builder.build(output)

    return output


def _get_packages(editable: bool, executable: str = sys.executable):
    editable_mode = "-e" if editable else "--exclude-editable"
    results = subprocess.check_output(
        [f"{executable}", "-m", "pip", "list", "-l",
         f"{editable_mode}", "--format", "json"]).decode()

    _logger.debug(f"'pip list' with editable={editable} results:" + results)

    parsed_results = json.loads(results)

    # https://pip.pypa.io/en/stable/reference/pip_freeze/?highlight=freeze#cmdoption--all
    # freeze hardcodes to ignore those packages: wheel, distribute, pip, setuptools
    # To be iso with freeze we also remove those packages
    return [element for element in parsed_results
            if element["name"] not in
            ["distribute", "wheel", "pip", "setuptools"]]


def pack_current_venv_in_pex(
        output: str,
        reqs: Dict[str, str],
        additional_packages: Dict[str, str],
        ignored_packages: Collection[str],
        editable_requirements:  Dict[str, str]) -> str:
    return pack_in_pex(reqs, output, ignored_packages, editable_requirements=editable_requirements)


def pack_venv_in_conda(
        output: str,
        reqs: Dict[str, str],
        additional_packages: Dict[str, str],
        ignored_packages: Collection[str],
        editable_requirements: Dict[str, str]) -> str:
    if len(additional_packages) == 0 and len(ignored_packages) == 0:
        conda_pack.pack(output=output)
        return output
    else:
        return create_and_pack_conda_env(output, reqs)


def create_and_pack_conda_env(env_path: str, reqs: Dict[str, str]) -> str:
    try:
        _call(["conda"])
    except CalledProcessError:
        raise RuntimeError("conda is not available in $PATH")

    env_path_split = env_path.split('.', 1)
    env_name = env_path_split[0]
    compression_format = env_path_split[1] if len(env_path_split) > 1 else ".zip"
    archive_path = f"{env_name}.{compression_format}"

    if os.path.exists(env_name):
        shutil.rmtree(env_name)

    _logger.info("Creating new env " + env_name)
    python_version = sys.version_info
    _call([
        "conda", "create", "-p", env_name, "-y", "-q", "--copy",
        f"python={python_version.major}.{python_version.minor}.{python_version.micro}"
    ], env=dict(os.environ))

    env_python_bin = os.path.join(env_name, "bin", "python")
    if not os.path.exists(env_python_bin):
        raise RuntimeError(
            "Failed to create Python binary at " + env_python_bin)

    _logger.info("Installing packages into " + env_name)
    _call([env_python_bin, "-m", "pip", "install"] +
          format_requirements(reqs))

    if os.path.exists(archive_path):
        os.remove(archive_path)

    conda_pack.pack(prefix=env_name, output=archive_path)
    return archive_path


def _call(cmd, **kwargs):
    _logger.info(" ".join(cmd))
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE, **kwargs)
    out, err = proc.communicate()
    if proc.returncode:
        _logger.error(out)
        _logger.error(err)
        raise CalledProcessError(proc.returncode, cmd)
    else:
        _logger.debug(out)
        _logger.debug(err)


class Packer(NamedTuple):
    env_name: str
    extension: str
    pack: Callable[[str, Dict[str, str], Dict[str, str], Collection[str], Dict[str, str]], str]


def get_env_name(env_var_name) -> str:
    """
    Return default virtual env
    """
    virtual_env_path = os.environ.get(env_var_name)
    if not virtual_env_path:
        return 'default'
    else:
        return os.path.basename(virtual_env_path)


CONDA_PACKER = Packer(
    get_env_name(CONDA_DEFAULT_ENV),
    'zip',
    pack_venv_in_conda
)
PEX_PACKER = Packer(
    get_env_name('VIRTUAL_ENV'),
    'pex',
    pack_current_venv_in_pex
)


def _get_editable_requirements(executable: str = sys.executable):
    def _get(name):
        pkg = __import__(name.replace("-", "_"))
        return os.path.dirname(pkg.__file__)
    return [_get(package["name"]) for package in _get_packages(True, executable)]


def get_non_editable_requirements(executable: str = sys.executable):
    return {package["name"]: package["version"]
            for package in _get_packages(False, executable)}


def detect_archive_names(
        packer: Packer,
        package_path: str = None
) -> Tuple[str, str, str]:
    if _running_from_pex():
        pex_file = get_current_pex_filepath()
        env_name = os.path.basename(pex_file).split('.')[0]
    else:
        pex_file = ""
        env_name = packer.env_name

    if not package_path:
        package_path = (f"{get_default_fs()}/user/{getpass.getuser()}"
                        f"/envs/{env_name}.{packer.extension}")
    else:
        if pathlib.Path(package_path).suffix != f".{packer.extension}":
            raise ValueError(f"{package_path} has the wrong extension"
                             f", .{packer.extension} is expected")

    return package_path, env_name, pex_file


def detect_packer_from_env() -> Packer:
    if _is_conda_env():
        return CONDA_PACKER
    else:
        return PEX_PACKER


def detect_packer_from_file(zip_file: str) -> Packer:
    if zip_file.endswith('.pex'):
        return PEX_PACKER
    elif zip_file.endswith(".zip"):
        return CONDA_PACKER
    else:
        raise ValueError("Archive format unsupported. Must be .pex or conda .zip")


def get_current_pex_filepath() -> str:
    """
    If we run from a pex, returns the path
    """
    import _pex
    return os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(_pex.__file__))))


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


def get_default_fs():
    return subprocess.check_output("hdfs getconf -confKey fs.defaultFS".split()).strip().decode()


def _is_conda_env():
    return os.environ.get(CONDA_DEFAULT_ENV) is not None


def _running_from_pex() -> bool:
    try:
        import _pex
        return True
    except ModuleNotFoundError:
        return False


def _is_criteo():
    return "CRITEO_ENV" in os.environ
