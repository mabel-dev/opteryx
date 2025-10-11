"""
Test the connection example from the documentation
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_as_polars_no_limit():
    import opteryx

    cur = opteryx.query("SELECT * FROM $planets")
    table = cur.polars()

    assert "name" in table.columns
    assert len(table) == 9
    assert len(table.columns) == 20


def test_as_polars_with_limit():
    import opteryx

    cur = opteryx.query("SELECT * FROM $planets")
    table = cur.polars(size=5)

    assert "name" in table.columns
    assert len(table) == 5
    assert len(table.columns) == 20


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
