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

# fmt:off
test_cases = [
    ("SELECT * FROM duckdb.planets", 9, 20),
    ("SELECT * FROM duckdb.satellites", 177, 8),
    ("SELECT COUNT(*) FROM duckdb.planets;", 1, 1),
    ("SELECT COUNT(*) FROM duckdb.satellites;", 1, 1),
    ("SELECT name FROM duckdb.planets;", 9, 1),
    ("SELECT name FROM duckdb.satellites", 177, 1),
    ("SELECT * FROM duckdb.planets, duckdb.satellites", 1593, 28),
    ("SELECT * FROM duckdb.planets INNER JOIN $satellites ON duckdb.planets.id = $satellites.planetId;", 177, 28),
    ("SELECT * FROM duckdb.planets INNER JOIN duckdb.satellites ON duckdb.planets.id = duckdb.satellites.planetId;", 177, 28),
    ("SELECT * FROM duckdb.planets, duckdb.satellites WHERE duckdb.planets.id = duckdb.satellites.planetId;", 177, 28),
    ("SELECT * FROM duckdb.planets, duckdb.satellites WHERE duckdb.planets.id = 5 AND duckdb.satellites.planetId = 5;", 67, 28),
    ("SELECT * FROM duckdb.planets, duckdb.satellites WHERE duckdb.planets.id - duckdb.satellites.planetId = 0;", 177, 28),
    ("SELECT * FROM duckdb.planets, duckdb.satellites WHERE duckdb.planets.id - duckdb.satellites.planetId != 0;", 1416, 28),
]
# fmt:on


def test_duckdb_storage():
    # We have some problems with creating duckdb, particularly in GitHub Actions
    # we're going to brute force.
    for i in range(5):
        if create_duck_db() is None:
            break

    opteryx.register_store(
        "duckdb",
        SqlConnector,
        remove_prefix=True,
        connection="duckdb:///planets.duckdb",
    )
    # PUSH - CHECK STATS THE PUSHES WORKED
    results = opteryx.query("SELECT name FROM duckdb.planets WHERE name LIKE 'Earth';")
    assert results.rowcount == 1, results.rowcount
    assert results.columncount == 1
    assert results.stats["rows_read"] == 1
    assert results.stats["columns_read"] == 1

    results = opteryx.query("SELECT * FROM duckdb.planets WHERE id > gravity")
    assert results.rowcount == 2, results.rowcount
    assert results.stats.get("rows_read", 0) == 9, results.stats


def test_duckdb_battery():
    from opteryx.utils.formatter import format_sql

    # We have some problems with creating duckdb, particularly in GitHub Actions
    # we're going to brute force.
    for i in range(5):
        if create_duck_db() is None:
            break

    opteryx.register_store(
        "duckdb",
        SqlConnector,
        remove_prefix=True,
        connection="duckdb:///planets.duckdb",
    )

    print(f"RUNNING DUCK BATTERY OF {len(test_cases)} TESTS")
    for script, rows, cols in test_cases:
        print(format_sql(script))
        results = opteryx.query(script)
        assert results.rowcount == rows, format_sql(script) + str(results.shape)
        assert results.columncount == cols, format_sql(script) + str(results.shape)

    print()


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
