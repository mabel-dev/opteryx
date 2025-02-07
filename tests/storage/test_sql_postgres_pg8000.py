import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import SqlConnector
from tests.tools import is_arm, is_mac, is_version, is_windows, skip_if

POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST")

CONNECTION = f"postgresql+pg8000://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/defaultdb"


@skip_if(is_arm() or is_mac() or is_windows() or not is_version("3.10"))
def test_postgres_basic_query():
    opteryx.register_store("pg", SqlConnector, remove_prefix=True, connection=CONNECTION)
    results = opteryx.query("SELECT * FROM pg.planets")
    assert results.rowcount == 9, results.rowcount
    assert results.columncount == 20


@skip_if(is_arm() or is_mac() or is_windows() or not is_version("3.10"))
def test_postgres_count_query():
    opteryx.register_store("pg", SqlConnector, remove_prefix=True, connection=CONNECTION)
    results = opteryx.query("SELECT COUNT(*) FROM pg.planets;")
    assert results.rowcount == 1, results.rowcount
    assert results.columncount == 1


@skip_if(is_arm() or is_mac() or is_windows() or not is_version("3.10"))
def test_postgres_projection():
    opteryx.register_store("pg", SqlConnector, remove_prefix=True, connection=CONNECTION)
    results = opteryx.query("SELECT name FROM pg.planets;")
    assert results.rowcount == 9, results.rowcount
    assert results.columncount == 1


@skip_if(is_arm() or is_mac() or is_windows() or not is_version("3.10"))
def test_postgres_join_non_sql_table():
    opteryx.register_store("pg", SqlConnector, remove_prefix=True, connection=CONNECTION)
    results = opteryx.query(
        "SELECT * FROM pg.planets INNER JOIN $satellites ON pg.planets.id = $satellites.planetId;"
    )
    assert results.rowcount == 177, results.rowcount
    assert results.columncount == 28, results.columncount


@skip_if(is_arm() or is_mac() or is_windows() or not is_version("3.10"))
def test_postgres_filtered_query():
    opteryx.register_store("pg", SqlConnector, remove_prefix=True, connection=CONNECTION)
    results = opteryx.query("SELECT name FROM pg.planets WHERE name LIKE 'Earth';")
    assert results.rowcount == 1, results.rowcount
    assert results.columncount == 1
    assert results.stats["rows_read"] == 1
    assert results.stats["columns_read"] == 1


@skip_if(is_arm() or is_mac() or is_windows() or not is_version("3.10"))
def test_postgres_comparison_query():
    opteryx.register_store("pg", SqlConnector, remove_prefix=True, connection=CONNECTION)
    results = opteryx.query("SELECT * FROM pg.planets WHERE id > gravity")
    assert results.rowcount == 2, results.rowcount
    assert results.stats.get("rows_read", 0) == 9, results.stats

@skip_if(is_arm() or is_mac() or is_windows() or not is_version("3.10"))
def test_postgres_non_existant():

    import pytest
    from opteryx.exceptions import DatasetReadError

    with pytest.raises(DatasetReadError):
        opteryx.register_store("pg", SqlConnector, remove_prefix=True, connection=CONNECTION)
        opteryx.query("SELECT * FROM pg.roman_gods")


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
