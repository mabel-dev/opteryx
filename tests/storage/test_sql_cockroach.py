"""
Test we can read from Sqlite - this is a basic exercise of the SQL Connector
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import SqlConnector
from tests.tools import is_arm, is_mac, is_windows, skip_if

COCKROACH_PASSWORD = os.environ.get("COCKROACH_PASSWORD")
COCKROACH_USER = os.environ.get("COCKROACH_USER")
COCKROACH_CONNECTION = f"cockroachdb://{COCKROACH_USER}:{COCKROACH_PASSWORD}@redleg-hunter-12763.5xj.cockroachlabs.cloud:26257/opteryx?sslmode=require"


# skip to reduce billing
@skip_if(is_arm() or is_windows() or is_mac())
def test_cockroach_storage():
    opteryx.register_store(
        "cockroach",
        SqlConnector,
        remove_prefix=True,
        connection=COCKROACH_CONNECTION,
    )

    results = opteryx.query("SELECT * FROM cockroach.planets")
    assert results.rowcount == 9, results.rowcount
    assert results.columncount == 20

    # PROCESS THE DATA IN SOME WAY
    results = opteryx.query("SELECT COUNT(*) FROM cockroach.planets;")
    assert results.rowcount == 1, results.rowcount
    assert results.columncount == 1

    # PUSH A PROJECTION
    results = opteryx.query("SELECT name FROM cockroach.planets;")
    assert results.rowcount == 9, results.rowcount
    assert results.columncount == 1

    # JOIN ON A NON SQL TABLE
    results = opteryx.query(
        "SELECT * FROM cockroach.planets AS P INNER JOIN $planets ON P.gravity = $planets.gravity;"
    )
    assert results.rowcount == 11, results.rowcount
    assert results.columncount == 40, results.columncount

    # PUSH - CHECK STATS THE PUSHES WORKED
    results = opteryx.query("SELECT name FROM cockroach.planets WHERE name LIKE 'Earth';")
    assert results.rowcount == 1, results.rowcount
    assert results.columncount == 1
    assert results.stats["rows_read"] == 1
    assert results.stats["columns_read"] == 1

    results = opteryx.query("SELECT * FROM cockroach.planets WHERE distancefromsun < lengthofday")
    assert results.rowcount == 2, results.rowcount
    assert results.stats.get("rows_read", 0) == 2, results.stats


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
