import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import waddles
from rich import traceback

traceback.install()


def test_version():
    assert hasattr(waddles, "__version__")


if __name__ == "__main__":  # pragma: no cover
    test_version()

    print("okay")
