"""
Test show functions works; the number of functions is constantly changing so test it's
more than it was when we last reviewed this test.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_show_functions():
    import opteryx
    from opteryx.exceptions import UnsupportedSyntaxError

    with pytest.raises(UnsupportedSyntaxError):
        conn = opteryx.connect()
        cur = conn.cursor()
        cur.execute("SHOW FUNCTIONS")
        rows = cur.fetchall()

        # below here is not in the documentation
        rows = list(rows)
        assert len(rows) > 85, len(rows)
        conn.close()


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
