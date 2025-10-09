"""
Test the connection example from the documentation
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest


def test_connection():
    import opteryx

    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM $planets")

    assert cur.rowcount == 9
    assert cur.shape == (9, 20)

    conn.commit()
    conn.close()

    with pytest.raises(AttributeError):
        conn.rollback()

    assert len(cur.id) == 36, len(cur.id)

    cur.close()



def test_execute():
    import pandas

    import opteryx

    cur = opteryx.query("SELECT * FROM $planets")

    assert cur.rowcount == 9
    assert cur.shape == (9, 20)

    planets = opteryx.query("SELECT * FROM $planets").pandas()
    assert isinstance(planets, pandas.DataFrame)


def test_byte_strings():
    import opteryx

    cur = opteryx.query(b"SELECT * FROM $planets")
    assert cur.rowcount == 9
    assert cur.shape == (9, 20)

    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute(b"SELECT * FROM $planets")
    assert cur.rowcount == 9
    assert cur.shape == (9, 20)

    conn = opteryx.connect()
    cur = conn.cursor()
    arrow = cur.execute_to_arrow(b"SELECT * FROM $planets")
    assert arrow.num_rows == 9, cur.rowcount
    assert arrow.shape == (9, 20)

def test_register_errors():
    from opteryx import register_store
    from opteryx.connectors import DiskConnector
    
    with pytest.raises(ValueError):
        register_store(prefix="prefix", connector=DiskConnector(dataset="", statistics=None))

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
