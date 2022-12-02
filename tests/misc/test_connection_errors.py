"""
Test the connection example from the documentation
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest


def test_connection_invalid_state():

    import opteryx
    from opteryx.exceptions import CursorInvalidStateError

    conn = opteryx.connect()
    cur = conn.cursor()

    with pytest.raises(CursorInvalidStateError):
        cur.fetchone()

    with pytest.raises(CursorInvalidStateError):
        cur.fetchmany()

    with pytest.raises(CursorInvalidStateError):
        cur.fetchall()

    with pytest.raises(CursorInvalidStateError):
        cur.shape()


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

    assert isinstance(cur.fetchone(), tuple)
    assert isinstance(cur.fetchone(True), dict)
    assert len(list(cur.fetchall())) == 9
    assert len(list(cur.fetchmany(4))) == 4
    assert len(list(cur.fetchmany(100))) == 9


if __name__ == "__main__":  # pragma: no cover

    test_connection_invalid_state()
    test_connection_warnings()
    test_connection_parameter_mismatch()
    test_fetching()

    print("✅ okay")
