"""
Test show functions works; the number of functions is constantly changing so test it's
more than it was when we last reviewed this test.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_documentation_connect_example():
    import opteryx

    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute("SHOW FUNCTIONS")
    rows = cur.fetchall()

    # below here is not in the documentation
    rows = list(rows)
    assert len(rows) > 85, len(rows)
    conn.close()


if __name__ == "__main__":  # pragma: no cover
    test_documentation_connect_example()

    print("✅ okay")
