import logging
import os
import skein
import time

from typing import Dict, List, Optional

from cluster_pack import packaging

logger = logging.getLogger(__name__)


def get_script(
        archive_hdfs: str,
        module_name: str,
        args: Optional[str] = None
) -> str:
    python_bin = f"./{os.path.basename(archive_hdfs)}" if archive_hdfs.endswith(
        '.pex') else f"./{os.path.basename(archive_hdfs)}/bin/python"

    launch_options = "-m" if not module_name.endswith(".py") else ""
    launch_args = args if args else ""

    script = f'''
                export PEX_ROOT="./.pex"
                export PYTHONPATH="."
                {python_bin} {launch_options} {module_name} {launch_args}
              '''

    return script


def get_files(
        archive_hdfs: str,
        additional_files: Optional[List[str]] = None,
        tmp_dir: str = packaging._get_tmp_dir()
) -> Dict[str, str]:

    files_to_upload = [archive_hdfs]
    if additional_files:
        files_to_upload = files_to_upload + additional_files

    dict_files_to_upload = {os.path.basename(path): path
                            for path in files_to_upload}

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
