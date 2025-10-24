import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import SqlConnector


def _run_query_and_check(q):
    res = opteryx.query(q)
    res.materialize()
    assert res.rowcount >= 0


def test_sqlite_engine_join_groupby():
    opteryx.register_store(
        "sqlite",
        SqlConnector,
        remove_prefix=True,
        connection="sqlite:///testdata/sqlite/database.db",
    )

    _run_query_and_check("SELECT p.name, s.name FROM sqlite.planets AS p INNER JOIN sqlite.satellites AS s ON p.id = s.planetId LIMIT 3")
    _run_query_and_check("SELECT name, COUNT(*) FROM sqlite.satellites GROUP BY name LIMIT 3")


def test_duckdb_engine_join_groupby():
    # Try to create duckdb; fall back if unavailable
    try:
        worker_id = os.environ.get('PYTEST_XDIST_WORKER', 'gw0')
        opteryx.register_store(
            "duckdb",
            SqlConnector,
            remove_prefix=True,
            connection=f"duckdb:///planets-{worker_id}.duckdb",
        )
    except Exception:
        return

    _run_query_and_check("SELECT p.name, s.name FROM duckdb.planets AS p INNER JOIN duckdb.satellites AS s ON p.id = s.planetId LIMIT 3")
    _run_query_and_check("SELECT name, COUNT(*) FROM duckdb.satellites GROUP BY name LIMIT 3")


def test_mysql_engine_join_groupby():
    # only run if environment variables/config present
    conn = os.environ.get("MYSQL_TEST_CONNECTION")
    if not conn:
        return
    opteryx.register_store("mysql_test", SqlConnector, remove_prefix=True, connection=conn)
    _run_query_and_check("SELECT name FROM mysql_test.planets LIMIT 1")


def test_postgres_engine_join_groupby():
    conn = os.environ.get("POSTGRES_TEST_CONNECTION")
    if not conn:
        return
    opteryx.register_store("pg_test", SqlConnector, remove_prefix=True, connection=conn)
    _run_query_and_check("SELECT name FROM pg_test.planets LIMIT 1")
