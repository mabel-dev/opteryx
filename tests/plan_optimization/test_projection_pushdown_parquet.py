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
    cur = opteryx.query("SELECT following FROM testdata.flat.formats.parquet;")
    assert cur.stats["columns_read"] == 1, cur.stats

    cur = opteryx.query("SELECT MAX(following), MAX(followers) FROM testdata.flat.formats.parquet;")
    assert cur.stats["columns_read"] == 2

    # without pushdown
    cur = opteryx.query("SELECT * FROM testdata.flat.formats.parquet;")
    assert cur.stats["columns_read"] == 13


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
