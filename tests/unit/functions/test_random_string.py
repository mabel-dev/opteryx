import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))


def test_random_generate_random_string():
    from opteryx.compiled.list_ops import list_random_strings

    count = 1000
    width = 22

    strings = list_random_strings(count, width)

    assert len(set(strings)) == count

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()