"""
Test the connection example from the documentation
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_documentation_connect_example():

    import opteryx
    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute('SELECT * FROM $planets')
    rows = cur.fetchall()

    # below here is not in the documentation
    rows = list(rows)
    assert len(rows) == 9


if __name__ == "__main__":

    test_documentation_connect_example()