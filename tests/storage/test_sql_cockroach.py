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

    # PROCESS THE DATA IN SOME WAY
    results = opteryx.query("SELECT COUNT(*) FROM cockroach.planets;")
    assert results.rowcount == 1, results.rowcount


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
