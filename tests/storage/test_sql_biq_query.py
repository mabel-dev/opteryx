"""
Test we can read from Sqlite - this is a basic exercise of the SQL Connector
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import SqlConnector
from tests.tools import is_arm, is_mac, is_windows, skip_if

BIG_QUERY_PROJECT: str = "mabeldev"


# skip to reduce billing
@skip_if(is_arm() or is_windows() or is_mac())
def test_bigquery_storage():
    from sqlalchemy.engine import create_engine

    engine = create_engine(f"bigquery://{BIG_QUERY_PROJECT}")

    opteryx.register_store("bq", SqlConnector, remove_prefix=True, engine=engine)

    results = opteryx.query("SELECT * FROM bq.public.planets")
    assert results.rowcount == 9, results.rowcount
    assert results.columncount == 20

    # PROCESS THE DATA IN SOME WAY
    results = opteryx.query("SELECT COUNT(*) FROM bq.public.planets;")
    assert results.rowcount == 1, results.rowcount
    assert results.columncount == 1

    # PUSH A PROJECTION
    results = opteryx.query("SELECT name FROM bq.public.planets;")
    assert results.rowcount == 9, results.rowcount
    assert results.columncount == 1

    # JOIN ON A NON SQL TABLE
    results = opteryx.query(
        "SELECT * FROM bq.public.planets INNER JOIN $satellites ON bq.public.planets.id = $satellites.planetId;"
    )
    assert results.rowcount == 177, results.rowcount
    assert results.columncount == 28, results.columncount

    # PUSH - CHECK STATS THE PUSHES WORKED
    results = opteryx.query("SELECT name FROM bq.public.planets WHERE name LIKE 'Earth';")
    assert results.rowcount == 1, results.rowcount
    assert results.columncount == 1
    assert results.stats["rows_read"] == 1
    assert results.stats["columns_read"] == 1


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
