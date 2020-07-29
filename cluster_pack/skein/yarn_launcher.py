import tempfile

import getpass
import skein
import fire
import os
import time

from typing import Dict, Optional, List, Callable, Any

from cluster_pack.skein import skein_config_builder, skein_helper
from cluster_pack import filesystem


def submit(skein_client: skein.Client,
           module_name: str, args: Optional[List[str]] = None, name: str = "yarn_launcher",
           num_cores: int = 1, memory: str = "1 GiB",
           package_path: Optional[str] = None,
           hadoop_file_systems: Optional[List[str]] = None,
           queue: Optional[str] = None, env_vars: Optional[Dict[str, str]] = None,
           additional_files: Optional[List[str]] = None, node_label: Optional[str] = None,
           num_containers: int = 1, user: Optional[str] = None,
           acquire_map_reduce_delegation_token: bool = False,
           pre_script_hook: Optional[str] = None,
           max_attempts: int = 1, max_restarts: int = 0,
           process_logs: Callable[[str], Any] = None) -> str:
    """Execute a python module in a skein container

    :param skein_client: skein.Client to use
    :param module_name: the module to execute remotely
    :param args: the module's cli arguments
    :param name: skein's application name
    :param num_cores: number of reserved vcore on yarn
    :param memory: memory of yarn container
    :param package_path: path on distributed storage where to find
                         the application package (pex, conda zip)
    :param hadoop_file_systems: hadoop delegation token to aqcuire
    :param queue: yarn queue
    :param env_vars: env varibales for the container
    :param additional_files: additional files to ship to the container
    :param node_label: label of the hadoop node to be scheduled
    :param num_containers: if you want to run the exact same script on more than one container
    :param user: user to run with (for impersonation)
    :param acquire_map_reduce_delegation_token: if you want to ask an additional mapred delegation
                                                token
    :param pre_script_hook: script to be executed before python is invoked
    :param max_attempts: max attemps submission attemps of application master
    :param max_restarts: maximum number of restarts allowed for the service
    :param process_logs: hook with the local log path as a parameter,
                         can be used to uplaod the logs somewhere
    :return: SkeinConfig
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        skein_config = skein_config_builder.build(
            module_name,
            args=args if args else [],
            package_path=package_path,
            additional_files=additional_files,
            tmp_dir=tmp_dir,
            process_logs=process_logs)

        return _submit(
            skein_client, skein_config,
            name=name, num_cores=num_cores, memory=memory,
            hadoop_file_systems=hadoop_file_systems, queue=queue, env_vars=env_vars,
            node_label=node_label, num_containers=num_containers, user=user,
            acquire_map_reduce_delegation_token=acquire_map_reduce_delegation_token,
            pre_script_hook=pre_script_hook, max_attempts=max_attempts, max_restarts=max_restarts)


def submit_func(skein_client: skein.Client,
                func: Callable, args: List[Any] = [], name: str = "yarn_launcher",
                num_cores: int = 1, memory: str = "1 GiB",
                package_path: Optional[str] = None,
                hadoop_file_systems: Optional[List[str]] = None,
                queue: Optional[str] = None, env_vars: Optional[Dict[str, str]] = None,
                additional_files: Optional[List[str]] = None, node_label: Optional[str] = None,
                num_containers: int = 1, user: Optional[str] = None,
                acquire_map_reduce_delegation_token: bool = False,
                pre_script_hook: Optional[str] = None,
                max_attempts: int = 1, max_restarts: int = 0,
                process_logs: Callable[[str], Any] = None) -> str:
    """Submit a function in a skein container

    :param skein_client: skein.Client to use
    :param func: the function to execute remotely
    :param args: the module's cli arguments
    :param name: skein's application name
    :param num_cores: number of reserved vcore on yarn
    :param memory: memory of yarn container
    :param package_path: path on distributed storage where to find
                         the application package (pex, conda zip)
    :param hadoop_file_systems: hadoop delegation token to aqcuire
    :param queue: yarn queue
    :param env_vars: env varibales for the container
    :param additional_files: additional files to ship to the container
    :param node_label: label of the hadoop node to be scheduled
    :param num_containers: if you want to run the exact same script on more than one container
    :param user: user to run with (for impersonation)
    :param acquire_map_reduce_delegation_token: if you want to ask an additional mapred delegation
                                                token
    :param pre_script_hook: script to be executed before python is invoked
    :param max_attempts: max attemps submission attemps of application master
    :param max_restarts: maximum number of restarts allowed for the service
    :param process_logs: hook with the local log path as a parameter,
                         can be used to uplaod the logs somewhere
    :return: SkeinConfig
    """

    with tempfile.TemporaryDirectory() as tmp_dir:
        skein_config = skein_config_builder.build_with_func(
            func,
            args,
            package_path=package_path,
            additional_files=additional_files,
            tmp_dir=tmp_dir,
            process_logs=process_logs)

        return _submit(
            skein_client, skein_config,
            name=name, num_cores=num_cores, memory=memory,
            hadoop_file_systems=hadoop_file_systems, queue=queue, env_vars=env_vars,
            node_label=node_label, num_containers=num_containers, user=user,
            acquire_map_reduce_delegation_token=acquire_map_reduce_delegation_token,
            pre_script_hook=pre_script_hook, max_attempts=max_attempts, max_restarts=max_restarts)


def _submit(
    skein_client: skein.Client,
    skein_config: skein_config_builder.SkeinConfig,
    name: str, num_cores: int, memory: str,
    hadoop_file_systems: Optional[List[str]],
    queue: Optional[str], env_vars: Optional[Dict[str, str]],
    node_label: Optional[str],
    num_containers: int, user: Optional[str],
    acquire_map_reduce_delegation_token: bool,
    pre_script_hook: Optional[str],
    max_attempts: int,
    max_restarts: int
) -> str:
    env = dict(env_vars) if env_vars else dict()
    pre_script_hook = pre_script_hook if pre_script_hook else ""

    service = skein.Service(
        resources=skein.model.Resources(memory, num_cores),
        instances=num_containers,
        files=skein_config.files,
        env=env.update(skein_config.env),
        script=f'''
                    set -x
                    {pre_script_hook}
                    {skein_config.script}
                ''',
        max_restarts=max_restarts
    )

    spec = skein.ApplicationSpec(
        name=name,
        file_systems=hadoop_file_systems,
        services={name: service},
        acls=skein.model.ACLs(
            enable=True,
            ui_users=['*'],
            view_users=['*']
        ),
        # acquire_map_reduce_delegation_token=acquire_map_reduce_delegation_token,
        max_attempts=max_attempts
    )

    # activate Impersonation only if user to run the job is not the current user (yarn issue)
    if user and user != getpass.getuser():
        spec.user = user

    if queue:
        spec.queue = queue

    if node_label:
        service.node_label = node_label

    return skein_client.submit(spec)


def upload_logs_to_hdfs(path_on_hdfs: str, local_log_path: str) -> None:
    fs, _ = filesystem.resolve_filesystem_and_path(path_on_hdfs)
    fs.put(local_log_path, path_on_hdfs)


def get_application_logs(
    client: skein.Client,
    app_id: str,
    wait_for_nb_logs: Optional[int] = None,
    log_tries: int = 15
) -> Optional[skein.model.ApplicationLogs]:
    return skein_helper.get_application_logs(client, app_id, wait_for_nb_logs, log_tries)


def wait_for_finished(client: skein.Client, app_id: str, poll_every_secs: int = 5) -> bool:
    return skein_helper.wait_for_finished(client, app_id, poll_every_secs)


if __name__ == "__main__":
    fire.Fire()
