import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from rich import traceback

import opteryx

traceback.install()


def test_version():
    assert hasattr(opteryx, "__version__")
    print("__version__", opteryx.__version__)
    assert hasattr(opteryx, "__build__")
    print("__build__", opteryx.__build__)
    assert hasattr(opteryx, "__author__")
    print("__author__", opteryx.__author__)


if __name__ == "__main__":  # pragma: no cover
    test_version()

    print("okay")
