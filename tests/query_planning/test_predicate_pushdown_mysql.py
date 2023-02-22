"""
Test predicate pushdown using the sql connector
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

from opteryx.connectors import SqlConnector

MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD")
MYSQL_USER = os.environ.get("MYSQL_USER")


opteryx.register_store(
    "mysql",
    SqlConnector,
    remove_prefix=True,
    connection=f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@sql8.freemysqlhosting.net/{MYSQL_USER}?charset=utf8mb4",
)


def test_predicate_pushdowns_mysql_eq():
    """
    This is the same test as the collection pushdown - but on a different dataset
    """

    conn = opteryx.connect()

    # TEST PREDICATE PUSHDOWN
    cur = conn.cursor()
    cur.execute("SET enable_optimizer = false; SELECT * FROM mysql.planets WHERE name = 'Mercury';")
    # if we disable pushdown, we read all the rows from the source and we do the filter
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats["rows_read"] == 9, cur.stats

    cur = conn.cursor()
    cur.execute("SELECT * FROM mysql.planets WITH(NO_PUSH_SELECTION) WHERE name = 'Mercury';")
    # if we disable pushdown, we read all the rows from the source and we do the filter
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats["rows_read"] == 9, cur.stats

    cur = conn.cursor()
    cur.execute("SELECT * FROM mysql.planets WHERE name = 'Mercury';")
    # when pushdown is enabled, we only read the matching rows from the source
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats["rows_read"] == 1, cur.stats

    cur = conn.cursor()
    cur.execute("SELECT * FROM mysql.planets WHERE name = 'Mercury' AND gravity = 3.7;")
    # test with a two part filter
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats["rows_read"] == 1, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM mysql.planets WHERE name = 'Mercury' AND gravity = 3.7 AND escapeVelocity = 5.0;"
    )
    # test with A three part filter
    assert cur.rowcount == 0, cur.rowcount
    assert cur.stats["rows_read"] == 0, cur.stats

    cur = conn.cursor()
    cur.execute("SELECT * FROM mysql.planets WHERE gravity = 3.7 AND name IN ('Mercury', 'Venus');")
    # we don't push all predicates down,
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats["rows_read"] == 2, cur.stats

    conn.close()


def test_predicate_pushdown_mysql_other():
    res = opteryx.query("SELECT * FROM mysql.planets WHERE gravity <= 3.7")
    assert res.rowcount == 3, res.rowcount
    assert res.stats["rows_read"] == 3, res.stats

    res = opteryx.query("SELECT * FROM mysql.planets WHERE name != 'Earth'")
    assert res.rowcount == 8, res.rowcount
    assert res.stats["rows_read"] == 8, res.stats

    res = opteryx.query("SELECT * FROM mysql.planets WHERE name != 'E\"arth'")
    assert res.rowcount == 9, res.rowcount
    assert res.stats["rows_read"] == 9, res.stats

    res = opteryx.query("SELECT * FROM mysql.planets WHERE gravity != 3.7")
    assert res.rowcount == 7, res.rowcount
    assert res.stats["rows_read"] == 7, res.stats

    res = opteryx.query("SELECT * FROM mysql.planets WHERE gravity < 3.7")
    assert res.rowcount == 1, res.rowcount
    assert res.stats["rows_read"] == 1, res.stats

    res = opteryx.query("SELECT * FROM mysql.planets WHERE gravity > 3.7")
    assert res.rowcount == 6, res.rowcount
    assert res.stats["rows_read"] == 6, res.stats

    res = opteryx.query("SELECT * FROM mysql.planets WHERE gravity >= 3.7")
    assert res.rowcount == 8, res.rowcount
    assert res.stats["rows_read"] == 8, res.stats

    res = opteryx.query("SELECT * FROM mysql.planets WHERE name LIKE '%a%'")
    assert res.rowcount == 4, res.rowcount
    assert res.stats["rows_read"] == 4, res.stats


if __name__ == "__main__":  # pragma: no cover
    test_predicate_pushdowns_mysql_eq()
    test_predicate_pushdown_mysql_other()
    print("✅ okay")
