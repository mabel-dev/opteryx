import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import opteryx
from rich import traceback

traceback.install()


def test_version():
    assert hasattr(opteryx, "__version__")


if __name__ == "__main__":  # pragma: no cover
    test_version()

    print("okay")
