"""
Test predicate pushdown using the sql connector
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tests.tools import is_arm, is_mac, is_windows, skip_if

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
@skip_if(is_arm() or is_windows() or is_mac())
def test_predicate_pushdowns_postgres_eq():
    """
    This is the same test as the collection pushdown - but on a different dataset
    """

    conn = opteryx.connect()

    # TEST PREDICATE PUSHDOWN
    cur = conn.cursor()
    cur.execute("SET disable_optimizer = true; SELECT * FROM pg.planets WHERE name = 'Mercury';")
    # if we disable pushdown, we read all the rows from the source and we do the filter
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 9, cur.stats

    cur = conn.cursor()
    cur.execute("SELECT * FROM pg.planets WITH(NO_PUSH_SELECTION) WHERE name = 'Mercury';")
    # if we disable pushdown, we read all the rows from the source and we do the filter
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 9, cur.stats

    cur = conn.cursor()
    cur.execute("SELECT * FROM pg.planets WHERE name = 'Mercury';")
    # when pushdown is enabled, we only read the matching rows from the source
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 1, cur.stats

    cur = conn.cursor()
    cur.execute("SELECT * FROM pg.planets WHERE name = 'Mercury' AND gravity = 3.7;")
    # test with a two part filter
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 1, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM pg.planets WHERE name = 'Mercury' AND gravity = 3.7 AND escape_velocity = 5.0;"
    )
    # test with A three part filter
    assert cur.rowcount == 0, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 0, cur.stats

    cur = conn.cursor()
    cur.execute("SELECT * FROM pg.planets WHERE gravity = 3.7 AND name IN ('Mercury', 'Venus');")
    # we don't push all predicates down,
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 2, cur.stats

    # identifier = identifier isn't pushed to SQL engines
    cur = conn.cursor()
    cur.execute("SELECT * FROM pg.planets WHERE rotation_period = length_of_day AND id > 5;")
    assert cur.rowcount == 2, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 4, cur.stats

    conn.close()


# skip to reduce contention
@skip_if(is_arm() or is_windows() or is_mac())
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


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
