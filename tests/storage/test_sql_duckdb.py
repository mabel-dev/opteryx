"""
Test we can read from DuckDB - this is a basic exercise of the SQL Connector
"""

import os
import sys

os.environ["OPTERYX_DEBUG"] = "1"
sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import SqlConnector
from tests.tools import create_duck_db


def test_duckdb_storage():
    create_duck_db()

    opteryx.register_store(
        "duckdb",
        SqlConnector,
        remove_prefix=True,
        connection="duckdb:///planets.duckdb",
    )

    results = opteryx.query("SELECT * FROM duckdb.planets")
    assert results.rowcount == 9, results.rowcount
    assert results.columncount == 20

    # PROCESS THE DATA IN SOME WAY
    results = opteryx.query("SELECT COUNT(*) FROM duckdb.planets;")
    assert results.rowcount == 1, results.rowcount
    assert results.columncount == 1

    # PUSH A PROJECTION
    results = opteryx.query("SELECT name FROM duckdb.planets;")
    assert results.rowcount == 9, results.rowcount
    assert results.columncount == 1

    # JOIN ON A NON SQL TABLE
    results = opteryx.query(
        "SELECT * FROM duckdb.planets INNER JOIN $satellites ON duckdb.planets.id = $satellites.planetId;"
    )
    assert results.rowcount == 177, results.rowcount
    assert results.columncount == 28, results.columncount

    # PUSH - CHECK STATS THE PUSHES WORKED
    results = opteryx.query("SELECT name FROM duckdb.planets WHERE name LIKE 'Earth';")
    assert results.rowcount == 1, results.rowcount
    assert results.columncount == 1
    assert results.stats["rows_read"] == 1
    assert results.stats["columns_read"] == 1

    results = opteryx.query("SELECT * FROM duckdb.planets WHERE id > gravity")
    assert results.rowcount == 2, results.rowcount
    assert results.stats.get("rows_read", 0) == 9, results.stats


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
