import cloudpickle
import logging
import sys

from typing import Dict


def _execute_fun(path_to_serialized_fun: str) -> None:
    with open(path_to_serialized_fun, "rb") as fd:
        func: Dict = cloudpickle.load(fd)

    func['func'](*func['args'])


if __name__ == "__main__":
    path_to_serialized_fun = sys.argv[1]
    log_level = "INFO"
    if len(sys.argv) > 2:
        if sys.argv[2] in ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'):
            log_level = sys.argv[2]
    logging.basicConfig(level=log_level)
    _execute_fun(path_to_serialized_fun)
