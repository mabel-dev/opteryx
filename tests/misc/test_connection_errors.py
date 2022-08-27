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

    assert cur.has_warnings


if __name__ == "__main__":  # pragma: no cover

    test_connection_invalid_state()
    test_connection_warnings()

    print("✅ okay")
