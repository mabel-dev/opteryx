"""
This module tests the ability to read from DuckDB using the SQLConnector.

DuckDB is used to rigorously test the SQLConnector due to its in-memory nature,
which allows for fast and efficient testing without the overhead of disk I/O.
This enables more intensive testing without the limitations of disk-based databases.

DuckDB appears to not like being tested in GitHub actions, so we run this suite
a little differently. We first create the DuckDB database, then we run the tests as
one function call, rather than it appearing as a test per statement as we do in other
battery test suites.

Note: DuckDB includes additional tests beyond the standard battery. However,
due to DuckDB's unstable file format, it only covers a subset of the required use
cases to save time, as loading it with numerous different tables can be time-consuming.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import SqlConnector
from tests.tools import create_duck_db

# fmt: off
STATEMENTS = [
    ("SELECT * FROM duckdb.planets", 9, 20, None),
    ("SELECT * FROM duckdb.satellites", 177, 8, None),
    ("SELECT COUNT(*) FROM duckdb.planets;", 1, 1, None),
    ("SELECT COUNT(*) FROM duckdb.satellites;", 1, 1, None),
    ("SELECT name FROM duckdb.planets;", 9, 1, None),
    ("SELECT name FROM duckdb.satellites", 177, 1, None),
    ("SELECT * FROM duckdb.planets, duckdb.satellites", 1593, 28, None),
    ("SELECT * FROM duckdb.planets INNER JOIN $satellites ON duckdb.planets.id = $satellites.planetId;", 177, 28, None),
    ("SELECT * FROM duckdb.planets INNER JOIN duckdb.satellites ON duckdb.planets.id = duckdb.satellites.planetId;", 177, 28, None),
    ("SELECT * FROM duckdb.planets, duckdb.satellites WHERE duckdb.planets.id = duckdb.satellites.planetId;", 177, 28, None),
    ("SELECT * FROM duckdb.planets, duckdb.satellites WHERE duckdb.planets.id = 5 AND duckdb.satellites.planetId = 5;", 67, 28, None),
    ("SELECT * FROM duckdb.planets, duckdb.satellites WHERE duckdb.planets.id - duckdb.satellites.planetId = 0;", 177, 28, None),
    ("SELECT * FROM duckdb.planets, duckdb.satellites WHERE duckdb.planets.id - duckdb.satellites.planetId != 0;", 1416, 28, None),
    ("SELECT DISTINCT name FROM duckdb.planets;", 9, 1, None),
    ("SELECT name, COUNT(*) FROM duckdb.satellites GROUP BY name;", 177, 2, None),
    ("SELECT name FROM duckdb.planets WHERE id IN (1, 2, 3);", 3, 1, None),
    ("SELECT name FROM duckdb.satellites WHERE planetId BETWEEN 1 AND 3;", 1, 1, None),
    ("SELECT name FROM duckdb.planets WHERE name LIKE 'E%';", 1, 1, None),
    ("SELECT name FROM duckdb.satellites WHERE name ILIKE '%moon%';", 1, 1, None),
    ("SELECT * FROM duckdb.planets ORDER BY name;", 9, 20, None),
    ("SELECT * FROM duckdb.satellites ORDER BY name DESC;", 177, 8, None),
    ("SELECT * FROM duckdb.planets LIMIT 5;", 5, 20, None),
    ("SELECT * FROM duckdb.satellites LIMIT 10 OFFSET 5;", 10, 8, None),
]
# fmt: on

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

    print(f"RUNNING FLOCK OF {len(STATEMENTS)} DUCK TESTS\n")
    for script, rows, cols, error in STATEMENTS:
        print(format_sql(script))
        results = opteryx.query(script)
        assert results.rowcount == rows, format_sql(script) + str(results.shape)
        assert results.columncount == cols, format_sql(script) + str(results.shape)

    print()


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
