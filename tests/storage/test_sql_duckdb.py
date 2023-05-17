"""
Test we can read from DuckDB - this is a basic exercise of the SQL Connector
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

from opteryx.connectors import SqlConnector


def test_duckdb_storage():
    opteryx.register_store(
        "duckdb",
        SqlConnector,
        remove_prefix=True,
        connection="duckdb:///testdata/duckdb/planets.duckdb",
#        connection="duckdb:///testdata/flat/planets/parquet/planets.parquet",
    )

    results = opteryx.query("SELECT * FROM duckdb.planets")
    assert results.rowcount == 9, results.rowcount

    # PROCESS THE DATA IN SOME WAY
    results = opteryx.query("SELECT COUNT(*) FROM duckdb.planets;")
    assert results.rowcount == 1, results.rowcount


if __name__ == "__main__":  # pragma: no cover
    test_duckdb_storage()
    print("✅ okay")
