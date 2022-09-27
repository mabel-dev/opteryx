"""
Test the connection example from the documentation
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_connection():

    import opteryx

    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM $planets")

    assert cur.rowcount == 9
    assert cur.shape == (9, 20)


if __name__ == "__main__":  # pragma: no cover

    test_connection()

    print("✅ okay")
