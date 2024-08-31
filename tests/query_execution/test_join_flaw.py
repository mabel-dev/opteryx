
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
import pytest

import opteryx


SQL = "SELECT name, surface_pressure FROM $planets AS P1 NATURAL JOIN $planets AS P2"

def test_join_flaw():
    """
    There was a flaw with the join algo that meant that nulls weren't handled correctly, it wasn't
    consistent (about 1 in 5) so we hammer this query to help determine haveb't regressed this bug
    """
    for i in range(100):
        res = opteryx.query(SQL)
        assert res.rowcount == 5
        print(end=".")

if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
