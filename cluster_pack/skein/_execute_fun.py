import cloudpickle
import sys

from typing import Dict


def _execute_fun(path_to_serialized_fun: str) -> None:
    with open(path_to_serialized_fun, "rb") as fd:
        func: Dict = cloudpickle.load(fd)

    func['func'](*func['args'])


if __name__ == "__main__":
    _execute_fun(sys.argv[1])
