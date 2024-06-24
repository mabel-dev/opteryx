
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_basic_tarchia():
    import opteryx

    SQL = "SELECT * FROM joocer.planets;"

    results = opteryx.query(SQL)
    assert results.shape == (9, 20)


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
