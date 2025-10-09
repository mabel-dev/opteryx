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

    assert len(cur.messages) > 0, cur.messages


def test_connection_parameter_mismatch():
    """test substitution binding errors"""
    import datetime

    import opteryx
    from opteryx.exceptions import ParameterError

    with pytest.raises(ParameterError):
        opteryx.query("SELECT * FROM $planets WHERE id = ?")
    with pytest.raises(ParameterError):
        opteryx.query("SELECT * FROM $planets WHERE id = ? AND name = ?", [1])
    with pytest.raises(ParameterError):
        opteryx.query("SELECT * FROM $planets WHERE id = ? AND name = ?", (1,))
    with pytest.raises(ParameterError):
        opteryx.query("SELECT * FROM $planets WHERE id = ?", (1, 2))

    opteryx.query(
        "SELECT * FROM $astronauts WHERE birth_date < ?", [datetime.datetime.today().date()]
    )
    opteryx.query("SELECT * FROM $astronauts WHERE birth_date < ?", [None])


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
    from tests import run_tests

    run_tests()
