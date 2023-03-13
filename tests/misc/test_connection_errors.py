"""
Test the connection example from the documentation
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest


def test_connection_warnings():
    import opteryx

    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM $planets WITH(_NO_CACHE)")
    cur.fetchone()

    assert len(cur.messages) > 0


def test_connection_parameter_mismatch():
    """test substitution binding errors"""

    import opteryx
    from opteryx.exceptions import ProgrammingError

    conn = opteryx.connect()
    cur = conn.cursor()
    with pytest.raises(ProgrammingError):
        cur.execute("SELECT * FROM $planets WHERE id = ?")
    with pytest.raises(ProgrammingError):
        cur.execute("SELECT * FROM $planets WHERE id = ? AND name = ?", [1])
    with pytest.raises(ProgrammingError):
        cur.execute("SELECT * FROM $planets WHERE id = ? AND name = ?", (1,))
    with pytest.raises(ProgrammingError):
        cur.execute("SELECT * FROM $planets WHERE id = ?", (1, 2))


def test_fetching():
    import opteryx

    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM $planets;")

    assert isinstance(cur.fetchone(), tuple)  # we have 8 records left
    assert len(cur.fetchmany(3)) == 3  # we have 5 records left
    assert len(cur.fetchall()) == 5  # we have zero record left
    assert len(cur.fetchmany(100)) == 0


if __name__ == "__main__":  # pragma: no cover
    test_connection_warnings()
    test_connection_parameter_mismatch()
    test_fetching()

    print("✅ okay")
