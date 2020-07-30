import logging
import tempfile

import getpass
import skein
import fire
import time

from typing import Dict, Optional, List, Callable, Any

from cluster_pack.skein import skein_config_builder, skein_helper

EDITABLE_PACKAGES_INDEX = 'editable_packages_index'


logger = logging.getLogger(__name__)


def submit(skein_client: skein.Client,
           module_name: str, args: Optional[List[str]] = None, name: str = "yarn_launcher",
           num_cores: int = 1, memory: str = "1 GiB",
           archive_hdfs: Optional[str] = None,
           hadoop_file_systems: Optional[List[str]] = None,
           queue: Optional[str] = None, env_vars: Optional[Dict[str, str]] = None,
           additional_files: Optional[List[str]] = None, node_label: Optional[str] = None,
           num_containers: int = 1, user: Optional[str] = None,
           acquire_map_reduce_delegation_token: bool = False,
           pre_script_hook: Optional[str] = None,
           max_attempts: int = 1, max_restarts: int = 0) -> str:
    with tempfile.TemporaryDirectory() as tmp_dir:
        skein_config = skein_config_builder.build(
            module_name,
            args=args if args else [],
            package_path=archive_hdfs,
            additional_files=additional_files,
            tmp_dir=tmp_dir)

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
                archive_hdfs: Optional[str] = None,
                hadoop_file_systems: Optional[List[str]] = None,
                queue: Optional[str] = None, env_vars: Optional[Dict[str, str]] = None,
                additional_files: Optional[List[str]] = None, node_label: Optional[str] = None,
                num_containers: int = 1, user: Optional[str] = None,
                acquire_map_reduce_delegation_token: bool = False,
                pre_script_hook: Optional[str] = None,
                max_attempts: int = 1, max_restarts: int = 0) -> str:
    with tempfile.TemporaryDirectory() as tmp_dir:
        skein_config = skein_config_builder.build_with_func(
            func,
            args,
            package_path=archive_hdfs,
            additional_files=additional_files,
            tmp_dir=tmp_dir)

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
    env.update(
        {
            'SKEIN_CONFIG': './.skein',
            "GIT_PYTHON_REFRESH": "quiet"
        }
    )

    service = skein.Service(
        resources=skein.model.Resources(memory, num_cores),
        instances=num_containers,
        files=skein_config.files,
        env=env,
        script=f'''
                    set -x
                    env
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
        max_attempts=max_attempts
    )
    # workaround for https://github.com/jcrist/skein/pull/197
    if hasattr(skein.ApplicationSpec, 'acquire_map_reduce_delegation_token'):
        spec.acquire_map_reduce_delegation_token = acquire_map_reduce_delegation_token

    # activate impersonification only if user to run the job is not the current user (yarn issue)
    if user and user != getpass.getuser():
        spec.user = user

    if queue:
        spec.queue = queue

    if node_label:
        service.node_label = node_label

    return skein_client.submit(spec)


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
