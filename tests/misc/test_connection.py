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


if __name__ == "__main__":  # pragma: no cover

    test_connection()

    print("✅ okay")
