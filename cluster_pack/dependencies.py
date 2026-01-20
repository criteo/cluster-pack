import logging
import os
import subprocess
import sys
from typing import Dict, List, Optional, Union

from packaging.requirements import Requirement

_logger = logging.getLogger(__name__)


def normalize_package_name(name: str) -> str:
    """Normalize package name for consistent comparison.

    PEP 503: package names are case-insensitive and treat
    underscores, hyphens, and dots as equivalent.
    """
    return name.lower().replace("_", "-").replace(".", "-")


def parse_requirement(req_str: str) -> Requirement:
    return Requirement(req_str)


def format_requirement(name: str, version: Optional[str] = None) -> str:
    if version:
        return f"{name}=={version}"
    return name


def format_requirements(requirements: Dict[str, str]) -> List[str]:
    if requirements is None:
        return []
    return [format_requirement(name, version) for name, version in requirements.items()]


def get_installed_packages(python_executable: Optional[str] = None) -> Dict[str, str]:
    """Get installed packages from a Python environment.

    :param python_executable: Path to python executable, or None for current interpreter.
    :return: Dict mapping normalized package names to versions.
    """
    import json
    executable = python_executable or sys.executable
    try:
        result = subprocess.run(
            [executable, "-m", "pip", "list", "--format", "json"],
            capture_output=True,
            text=True,
            check=True,
        )
        packages = json.loads(result.stdout)
        return {
            normalize_package_name(pkg["name"]): pkg["version"]
            for pkg in packages
        }
    except (subprocess.CalledProcessError, json.JSONDecodeError):
        return {}


def check_requirements_satisfied(
    requirements: List[str],
    python_executable: Optional[str] = None,
) -> bool:
    """Check if all requirements are satisfied in a Python environment.

    :param requirements: List of requirement strings.
    :param python_executable: Path to python executable, or None for current interpreter.
    :return: True if all requirements are satisfied.
    """
    installed = get_installed_packages(python_executable)
    if not installed:
        return False

    for req_str in requirements:
        try:
            req = parse_requirement(req_str)
            pkg_name = normalize_package_name(req.name)

            if pkg_name not in installed:
                _logger.debug(f"Package {req.name} not found")
                return False

            installed_version = installed[pkg_name]
            if req.specifier and not req.specifier.contains(installed_version):
                _logger.debug(
                    f"Package {req.name} version {installed_version} "
                    f"does not satisfy {req.specifier}"
                )
                return False
        except Exception as e:
            _logger.debug(f"Failed to parse requirement {req_str}: {e}")
            return False

    return True


def is_running_in_venv() -> bool:
    """Check if currently running inside a virtual environment."""
    return "VIRTUAL_ENV" in os.environ.keys()


def check_venv_has_requirements(
    venv_path: Optional[str],
    requirements: List[str],
) -> bool:
    """Check if all requirements are installed in a venv.

    :param venv_path: Path to venv, or None to check the currently activated environment.
    :param requirements: List of requirements to check.
    :return: True if all requirements are satisfied, False if not in a venv when venv_path is None.
    """
    if venv_path is None:
        if not is_running_in_venv():
            _logger.debug("Not running in a venv, cannot check requirements")
            return False
        python_executable = sys.executable
    else:
        python_executable = f"{venv_path}/bin/python"

    return check_requirements_satisfied(requirements, python_executable)


def create_uv_venv(
    venv_path: str,
    requirements: List[str],
    editable_requirements: Optional[Dict[str, str]] = None,
    additional_repo: Optional[Union[str, List[str]]] = None,
    additional_indexes: Optional[List[str]] = None,
) -> None:
    """Create a uv venv and install all dependencies.

    :param venv_path: Path where to create the venv.
    :param requirements: List of requirement strings to install.
    :param editable_requirements: Dict mapping package names to paths for editable installs.
    :param additional_repo: Additional PyPI repository URLs.
    :param additional_indexes: Additional package index URLs.
    """
    editable_requirements = editable_requirements or {}

    subprocess.check_call(["uv", "venv", venv_path, "--python", sys.executable, "--seed"])

    pip_cmd = ["uv", "pip", "install", "--python", f"{venv_path}/bin/python"]

    if additional_repo is not None:
        repos = additional_repo if isinstance(additional_repo, list) else [additional_repo]
        for repo in repos:
            pip_cmd.extend(["--index-url", repo])

    if additional_indexes:
        for index in additional_indexes:
            pip_cmd.extend(["-f", index])

    if requirements:
        subprocess.check_call(pip_cmd + requirements)

    for pkg_path in editable_requirements.values():
        subprocess.check_call(pip_cmd + ["-e", pkg_path])


def normalize_requirements(reqs: List[str]) -> List[str]:
    """Normalize requirement strings for comparison."""
    return [req.replace("_", "-") for req in reqs]


def sort_requirements(reqs: List[str]) -> List[str]:
    """Sort requirements alphabetically (case-insensitive)."""
    return sorted([item.lower() for item in reqs])


def filter_build_requirements(reqs: List[str]) -> List[str]:
    """Filter out build-related packages (pip, wheel, setuptools)."""
    build_packages = {"wheel", "pip", "setuptools"}

    def _keep(req: str) -> bool:
        parsed = parse_requirement(req)
        return normalize_package_name(parsed.name) not in build_packages

    return [req for req in reqs if _keep(req)]
