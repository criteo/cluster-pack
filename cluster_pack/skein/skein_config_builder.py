import cloudpickle
import os
import uuid

from typing import NamedTuple, Callable, Dict, List, Optional, Any

from cluster_pack import packaging, uploader


class SkeinConfig(NamedTuple):
    script: str
    files: Dict[str, str]
    env: Dict[str, str]


def build_with_func(
        func: Callable,
        args: List[Any] = [],
        package_path: Optional[str] = None,
        additional_files: Optional[List[str]] = None,
        tmp_dir: str = packaging._get_tmp_dir(),
        log_level: str = "INFO",
        process_logs: Callable[[str], Any] = None
) -> SkeinConfig:
    """Build the skein config from provided a function

    The function is serialized and shipped to the container

    :param func: the function to execute remotely
    :param args: the function's arguments
    :param package_path: the path on distributed storage where to find the application package
                         (pex, conda zip)
    :param additional_files: additional files to ship to the cluster
    :param tmp_dir: a temp dir for local files
    :param log_level: default remote log level
    :param process_logs: hook with the local log path as a parameter,
                         can be used to uplaod the logs somewhere
    :return: SkeinConfig
    """
    function_name = f"function_{uuid.uuid4()}.dat"
    function_path = f'{tmp_dir}/{function_name}'
    val_to_serialize = {
        "func": func,
        "args": args
    }
    with open(function_path, "wb") as fd:
        cloudpickle.dump(val_to_serialize, fd)

    if additional_files:
        additional_files.append(function_path)
    else:
        additional_files = [function_path]

    return build(
        'cluster_pack.skein._execute_fun',
        [function_name, log_level],
        package_path,
        additional_files,
        tmp_dir,
        process_logs)


def build(
        module_name: str,
        args: List[Any] = [],
        package_path: Optional[str] = None,
        additional_files: Optional[List[str]] = None,
        tmp_dir: str = packaging._get_tmp_dir(),
        process_logs: Callable[[str], Any] = None
) -> SkeinConfig:
    """Build the skein config for a module to execute

    :param module_name: the module to execute remotely
    :param args: the module's cli arguments
    :param package_path: the path on distributed storage where to find the application package
                         (pex, conda zip)
    :param additional_files: additional files to ship to the cluster
    :param tmp_dir: a temp dir for local files
    :param process_logs: hook with the local log path as a parameter,
                         can be used to uplaod the logs somewhere
    :return: SkeinConfig
    """
    if not package_path:
        package_path, _ = uploader.upload_env()

    python_env_descriptor = packaging.get_pyenv_usage_from_archive(package_path)

    script = _get_script(
        python_env_descriptor,
        module_name,
        args
    )

    files = _get_files(python_env_descriptor, additional_files, tmp_dir)

    env = {"SKEIN_CONFIG": "./.skein",
           "GIT_PYTHON_REFRESH": "quiet"}

    if process_logs:
        process_logs_config = build_with_func(
            process_logs,
            ["output.log"],
            package_path,
            additional_files=None,
            tmp_dir=tmp_dir)

        script = ("cat << EOT >> run.sh" + "\n"
                  f"{script}" + "\n"
                  "EOT" + "\n"
                  "chmod +x run.sh" + "\n"
                  "set -o pipefail" + "\n"
                  "./run.sh 2>&1 | tee output.log" + "\n"
                  "CMD_STATUS=$?" + "\n"
                  f"{process_logs_config.script}" + "\n"
                  "LOG_STATUS=$?" + "\n"
                  "exit $(( $CMD_STATUS || $LOG_STATUS ))" + "\n"
                  )
        files.update(process_logs_config.files)

    return SkeinConfig(script, files, env)


def _get_script(
        python_env_descriptor: packaging.PythonEnvDescription,
        module_name: str,
        args: List[Any] = []
) -> str:
    launch_options = "-m" if not module_name.endswith(".py") else ""
    launch_args = " ".join(args)

    cmd = f"{python_env_descriptor.interpreter_cmd} {launch_options} {module_name} {launch_args}"

    script = f'''
                export PEX_ROOT="./.pex"
                export PYTHONPATH="."
                echo "running {cmd}" ..
                {cmd}
              '''

    return script


def _get_files(
        python_env_descriptor: packaging.PythonEnvDescription,
        additional_files: Optional[List[str]] = None,
        tmp_dir: str = packaging._get_tmp_dir()
) -> Dict[str, str]:
    dict_files_to_upload = {
        python_env_descriptor.dest_path: python_env_descriptor.path_to_archive
    }
    if additional_files:
        for additional_file in additional_files:
            dict_files_to_upload[os.path.basename(additional_file)] = additional_file

    editable_requirements = packaging.get_editable_requirements()

    editable_packages = {name: packaging.zip_path(path, False) for name, path in
                         editable_requirements.items()}
    dict_files_to_upload.update(editable_packages)

    editable_packages_index = f"{tmp_dir}/{packaging.EDITABLE_PACKAGES_INDEX}"

    try:
        os.remove(editable_packages_index)
    except OSError:
        pass

    with open(editable_packages_index, "w+") as file:
        for repo in editable_requirements.keys():
            file.write(repo + "\n")
    dict_files_to_upload[
        packaging.EDITABLE_PACKAGES_INDEX
    ] = editable_packages_index

    return dict_files_to_upload
