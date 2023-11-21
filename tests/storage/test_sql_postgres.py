"""
Test we can read from Sqlite - this is a basic exercise of the SQL Connector
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import SqlConnector
from tests.tools import is_arm, is_mac, is_windows, is_version, skip_if

POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_USER = os.environ.get("POSTGRES_USER")


@skip_if(is_arm() or is_mac() or is_windows() or not is_version("3.9"))
def test_postgres_storage():
    opteryx.register_store(
        "pg",
        SqlConnector,
        remove_prefix=True,
        connection=f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@trumpet.db.elephantsql.com/{POSTGRES_USER}",
    )

    results = opteryx.query("SELECT * FROM pg.planets")
    assert results.rowcount == 9, results.rowcount
    assert results.columncount == 20

    # PROCESS THE DATA IN SOME WAY
    results = opteryx.query("SELECT COUNT(*) FROM pg.planets;")
    assert results.rowcount == 1, results.rowcount
    assert results.columncount == 1

    # PUSH A PROJECTION
    results = opteryx.query("SELECT name FROM pg.planets;")
    assert results.rowcount == 9, results.rowcount
    assert results.columncount == 1


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
