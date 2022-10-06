import hashlib
import json
import logging
import os
try:
    import conda_pack
except NotImplementedError:
    # conda is not supported on windows
    pass

from typing import List, Optional, Union

from cluster_pack import process


_logger = logging.getLogger(__name__)


def get_conda_env_name(spec_file: str = None, reqs: List[str] = None, env_id: str = None) -> str:
    conda_env_contents = open(spec_file).read() if spec_file else ""
    if reqs:
        for req in reqs:
            conda_env_contents += req
    if env_id:
        conda_env_contents += env_id
    return "cluster-pack-%s" % hashlib.sha1(conda_env_contents.encode("utf-8")).hexdigest()


def get_conda_bin_executable(executable_name: str) -> str:
    """
    Return path to the specified executable, assumed to be discoverable within the 'bin'
    subdirectory of a conda installation.
    """
    # Use CONDA_EXE as per https://github.com/conda/conda/issues/7126
    if "CONDA_EXE" in os.environ:
        conda_bin_dir = os.path.dirname(os.environ["CONDA_EXE"])
        return os.path.join(conda_bin_dir, executable_name)
    return executable_name


def get_or_create_conda_env(project_env_name: str = None, spec_file: str = None) -> str:
    conda_path = get_conda_bin_executable("conda")
    try:
        process.call([conda_path, "--help"], throw_on_error=False)
    except EnvironmentError:
        raise RuntimeError(f"Could not find Conda executable at {conda_path}.")

    _logger.info(f"search conda envs for {project_env_name}")

    env_names = [os.path.basename(env) for env in _list_envs(conda_path)]
    if project_env_name not in env_names:
        _logger.info(f"Creating conda environment {project_env_name}")
        if spec_file:
            process.call([conda_path, "env", "create", "-n", project_env_name, "--file",
                          spec_file])
        else:
            process.call(
                [conda_path, "create", "-n", project_env_name, "python=3.6"])

    project_env_path = [env for env in _list_envs(conda_path)
                        if os.path.basename(env) == project_env_name][0]

    _logger.info(f'project env path is {project_env_path}')

    return project_env_path


def _list_envs(conda_path: str) -> List[str]:
    _, stdout, _ = process.call([conda_path, "env", "list", "--json"])
    return [env for env in json.loads(stdout)['envs']]


def pack_venv_in_conda(
        name: str,
        reqs: List[str],
        changed_reqs: bool = False,
        output: str = None,
        additional_repo: Optional[Union[List[str], str]] = None,
        additional_indexes: Optional[List[str]] = None
) -> str:
    """
    Pack the current virtual environment
    :param additional_repo: additional pypi repo(s) to download packages from
    :param additional_indexes: additional indexes to download packages from
    :param reqs: directory to zip
    :param changed_reqs:
       we prefer zipping the current virtual env as much as possible,
       if it has been changed we need to create a new one with 'conda create -n env ..'
       and reinstall the dependencies inside
    :param output: a dedicated output path
    :return: destination of the archive
    """
    if not changed_reqs:
        return conda_pack.pack(name=name, output=output)
    else:
        return create_and_pack_conda_env(
            reqs=reqs, output=output, additional_repo=additional_repo,
            additional_indexes=additional_indexes
        )


def create_and_pack_conda_env(
    spec_file: str = None,
    reqs: List[str] = None,
    output: str = None,
    additional_repo: Optional[Union[List[str], str]] = None,
    additional_indexes: Optional[List[str]] = None
) -> str:
    """
    Create a new conda virtual environment and zip it
    :param additional_repo: additional pypi repo(s) to download packages from
    :param additional_indexes: additional indexes to download packages from
    :param spec_file: conda yaml spec file to use
    :param reqs: dependencies to install
    :param output: a dedicated output path
    :return: destination of the archive
    """
    project_env_name = get_conda_env_name(spec_file=spec_file, reqs=reqs)

    _logger.info(f"Found project env name {project_env_name}")
    env_path = get_or_create_conda_env(project_env_name, spec_file)

    if reqs:
        env_python_bin = os.path.join(env_path, "bin", "python")
        if not os.path.exists(env_python_bin):
            raise RuntimeError(
                "Failed to create Python binary at " + env_python_bin)

        _logger.info("Installing packages into " + env_path)

        cmd = [env_python_bin, "-m", "pip", "install"]
        if additional_repo is not None:
            additional_repo = additional_repo if isinstance(additional_repo, list) \
                else [additional_repo]
            for repo in additional_repo:
                cmd.extend(['--extra-index-url', repo])

        if additional_indexes:
            for index in additional_indexes:
                cmd.extend(["-f", index])

        process.call(cmd + reqs)

    return conda_pack.pack(prefix=env_path, output=output, force=True)
