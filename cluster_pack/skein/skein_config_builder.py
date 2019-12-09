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
        args: Optional[str] = None,
        additional_files: Optional[List[str]] = None
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
        additional_files: Optional[List[str]] = None
) -> Dict[str, str]:

    files_to_upload = [archive_hdfs]
    if additional_files:
        files_to_upload = files_to_upload + additional_files

    dict_files_to_upload = {os.path.basename(path): path
                            for path in files_to_upload}

    editable_packages = {name: packaging.zip_path(path, False) for name, path in
                         packaging.get_editable_requirements_from_current_venv().items()}
    dict_files_to_upload.update(editable_packages)

    return dict_files_to_upload
