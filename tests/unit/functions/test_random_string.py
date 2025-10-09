import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_random_generate_random_string():
    from opteryx.compiled.functions.functions import generate_random_strings

    count = 1000

    strings = generate_random_strings(count, 16)

    assert len(set(strings)) == count

if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()