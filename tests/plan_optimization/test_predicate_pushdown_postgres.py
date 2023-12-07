"""
Test predicate pushdown using the sql connector
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tests.tools import is_arm, is_mac, is_windows, is_version, skip_if

import opteryx
from opteryx.connectors import SqlConnector

POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_USER = os.environ.get("POSTGRES_USER")


opteryx.register_store(
    "pg",
    SqlConnector,
    remove_prefix=True,
    connection=f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@trumpet.db.elephantsql.com/{POSTGRES_USER}",
)


# skip to reduce contention
@skip_if(is_arm() or is_windows() or is_mac() or not is_version("3.9"))
def test_predicate_pushdown_postgres_other():
    res = opteryx.query("SELECT * FROM pg.planets WHERE gravity <= 3.7")
    assert res.rowcount == 3, res.rowcount
    assert res.stats.get("rows_read", 0) == 3, res.stats

    res = opteryx.query("SELECT * FROM pg.planets WHERE name != 'Earth'")
    assert res.rowcount == 8, res.rowcount
    assert res.stats.get("rows_read", 0) == 8, res.stats

    res = opteryx.query("SELECT * FROM pg.planets WHERE name != 'E\"arth'")
    assert res.rowcount == 9, res.rowcount
    assert res.stats.get("rows_read", 0) == 9, res.stats

    res = opteryx.query("SELECT * FROM pg.planets WHERE gravity != 3.7")
    assert res.rowcount == 7, res.rowcount
    assert res.stats.get("rows_read", 0) == 7, res.stats

    res = opteryx.query("SELECT * FROM pg.planets WHERE gravity < 3.7")
    assert res.rowcount == 1, res.rowcount
    assert res.stats.get("rows_read", 0) == 1, res.stats

    res = opteryx.query("SELECT * FROM pg.planets WHERE gravity > 3.7")
    assert res.rowcount == 6, res.rowcount
    assert res.stats.get("rows_read", 0) == 6, res.stats

    res = opteryx.query("SELECT * FROM pg.planets WHERE gravity >= 3.7")
    assert res.rowcount == 8, res.rowcount
    assert res.stats.get("rows_read", 0) == 8, res.stats

    res = opteryx.query("SELECT * FROM pg.planets WHERE name LIKE '%a%'")
    assert res.rowcount == 4, res.rowcount
    assert res.stats.get("rows_read", 0) == 4, res.stats

    res = opteryx.query("SELECT * FROM pg.planets WHERE id > gravity")
    assert res.rowcount == 2, res.rowcount
    assert res.stats.get("rows_read", 0) == 2, res.stats


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
