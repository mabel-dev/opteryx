"""
Test we can read from MinIO
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import DiskConnector


def test_parquet_projection_pushdown():
    opteryx.register_store("tests", DiskConnector)

    # with pushdown
    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute(f"SELECT following FROM testdata.flat.formats.parquet;")
    cur.arrow()
    assert cur.stats["columns_read"] == 1, cur.stats

    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute(f"SELECT MAX(following), MAX(followers) FROM testdata.flat.formats.parquet;")
    [a for a in cur.fetchall()]
    assert cur.stats["columns_read"] == 2

    # with pushdown disabled
    #    conn = opteryx.connect()
    #    cur = conn.cursor()
    #    cur.execute(
    #        f"SELECT MAX(following) FROM testdata.flat.formats.parquet WITH(NO_PARTITION, NO_PUSH_PROJECTION);"
    #    )
    #    [a for a in cur.fetchall()]
    #    assert cur.stats["columns_read"] == 13

    # without pushdown
    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM testdata.flat.formats.parquet WITH(NO_PARTITION);")
    [a for a in cur.fetchall()]
    assert cur.stats["columns_read"] == 13


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
