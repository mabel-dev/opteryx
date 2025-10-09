"""
Test show functions works; the number of functions is constantly changing so test it's
more than it was when we last reviewed this test.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_hint_hints():
    import opteryx

    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM $planets WITH(NO_PARTITIONS)")
    assert cur.messages == ["All HINTS are currently ignored"], cur.messages


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
