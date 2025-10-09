"""
Test the connection example from the documentation
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_as_arrow_no_limit():
    import opteryx

    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM $planets")
    table = cur.arrow()

    assert "name" in table.column_names
    assert table.num_rows == 9
    assert len(table.column_names) == 20


def test_as_arrow_with_limit():
    import opteryx

    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM $planets")
    table = cur.arrow(size=5)

    assert "name" in table.column_names
    assert table.num_rows == 5, table.num_rows
    assert len(table.column_names) == 20


def test_direct_as_arrow_no_limit():
    import opteryx

    conn = opteryx.connect()
    cur = conn.cursor()
    table = cur.execute_to_arrow("SELECT * FROM $planets")

    assert "name" in table.column_names, table.column_names
    assert table.num_rows == 9
    assert len(table.column_names) == 20
    assert cur.stats["rows_read"] == 9, cur.stats


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
