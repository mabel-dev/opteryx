"""
Test predicate pushdown using the sql connector
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import SqlConnector

opteryx.register_store(
    "sqlite",
    SqlConnector,
    remove_prefix=True,
    connection="sqlite:///testdata/sqlite/database.db",
)


def test_predicate_pushdowns_sqlite_eq():
    """
    This is the same test as the collection pushdown - but on a different dataset
    """

    conn = opteryx.connect()

    cur = conn.cursor()
    cur.execute("SELECT * FROM sqlite.planets WHERE name = 'Mercury';")
    # when pushdown is enabled, we only read the matching rows from the source
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 1, cur.stats

    cur = conn.cursor()
    cur.execute("SELECT * FROM sqlite.planets WHERE name = 'Mercury' AND gravity = 3.7;")
    # test with a two part filter
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 1, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM sqlite.planets WHERE name = 'Mercury' AND gravity = 3.7 AND escapeVelocity = 5.0;"
    )
    # test with A three part filter
    assert cur.rowcount == 0, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 0, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM sqlite.planets WHERE gravity = 3.7 AND name IN ('Mercury', 'Venus');"
    )
    # we don't push all predicates down,
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 2, cur.stats

    conn.close()


def test_predicate_pushdown_sqlite_other():
    res = opteryx.query("SELECT * FROM sqlite.planets WHERE gravity <= 3.7")
    assert res.rowcount == 3, res.rowcount
    assert res.stats.get("rows_read", 0) == 3, res.stats

    res = opteryx.query("SELECT * FROM sqlite.planets WHERE name != 'Earth'")
    assert res.rowcount == 8, res.rowcount
    assert res.stats.get("rows_read", 0) == 8, res.stats

    res = opteryx.query("SELECT * FROM sqlite.planets WHERE name != 'E\"arth'")
    assert res.rowcount == 9, res.rowcount
    assert res.stats.get("rows_read", 0) == 9, res.stats

    res = opteryx.query("SELECT * FROM sqlite.planets WHERE gravity != 3.7")
    assert res.rowcount == 7, res.rowcount
    assert res.stats.get("rows_read", 0) == 7, res.stats

    res = opteryx.query("SELECT * FROM sqlite.planets WHERE gravity < 3.7")
    assert res.rowcount == 1, res.rowcount
    assert res.stats.get("rows_read", 0) == 1, res.stats

    res = opteryx.query("SELECT * FROM sqlite.planets WHERE gravity > 3.7")
    assert res.rowcount == 6, res.rowcount
    assert res.stats.get("rows_read", 0) == 6, res.stats

    res = opteryx.query("SELECT * FROM sqlite.planets WHERE gravity >= 3.7")
    assert res.rowcount == 8, res.rowcount
    assert res.stats.get("rows_read", 0) == 8, res.stats

    res = opteryx.query("SELECT * FROM sqlite.planets WHERE name LIKE '%a%'")
    assert res.rowcount == 4, res.rowcount
    assert res.stats.get("rows_read", 0) == 4, res.stats

    res = opteryx.query("SELECT * FROM sqlite.planets WHERE id > gravity")
    assert res.rowcount == 2, res.rowcount
    assert res.stats.get("rows_read", 0) == 2, res.stats


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
