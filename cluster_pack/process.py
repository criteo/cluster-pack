
import logging
import subprocess

from typing import List, Tuple


_logger = logging.getLogger(__name__)


def call(cmd: List[str], throw_on_error=True, **kwargs) -> Tuple[int, str, str]:
    _logger.info(" ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)
    out, err = proc.communicate()
    if throw_on_error and proc.returncode != 0:
        _logger.error(out.decode())
        _logger.error(err.decode())
        raise subprocess.CalledProcessError(proc.returncode, cmd)

    _logger.debug(out.decode())
    _logger.debug(err.decode())
    return proc.returncode, out, err
