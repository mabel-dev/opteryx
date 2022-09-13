"""
Test the connection example from the documentation
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_to_arrow_no_limit():

    import opteryx

    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM $planets")
    table = cur.to_arrow()

    assert "name" in table.column_names
    assert table.num_rows == 9
    assert len(table.column_names) == 20


def test_to_arrow_with_limit():

    import opteryx

    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM $planets")
    table = cur.to_arrow(limit=5)

    assert "name" in table.column_names
    assert table.num_rows == 5
    assert len(table.column_names) == 20


if __name__ == "__main__":  # pragma: no cover

    test_to_arrow_no_limit()
    test_to_arrow_with_limit()

    print("✅ okay")
