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


def test_predicate_pushdowns_sqlite():
    """
    This is the same test as the collection pushdown - but on a different dataset
    """

    conn = opteryx.connect()

    # TEST PREDICATE PUSHDOWN
    cur = conn.cursor()
    cur.execute(
        "SET enable_optimizer = false; SELECT * FROM sqlite.planets WHERE name = 'Mercury';"
    )
    # if we disable pushdown, we read all the rows from the source and we do the filter
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats["rows_read"] == 9, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM sqlite.planets WITH(NO_PUSH_SELECTION) WHERE name = 'Mercury';"
    )
    # if we disable pushdown, we read all the rows from the source and we do the filter
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats["rows_read"] == 9, cur.stats

    cur = conn.cursor()
    cur.execute("SELECT * FROM sqlite.planets WHERE name = 'Mercury';")
    # when pushdown is enabled, we only read the matching rows from the source
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats["rows_read"] == 1, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM sqlite.planets WHERE name = 'Mercury' AND gravity = 3.7;"
    )
    # test with a two part filter
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats["rows_read"] == 1, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM sqlite.planets WHERE name = 'Mercury' AND gravity = 3.7 AND escapeVelocity = 5.0;"
    )
    # test with A three part filter
    assert cur.rowcount == 0, cur.rowcount
    assert cur.stats["rows_read"] == 0, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM sqlite.planets WHERE gravity = 3.7 AND name IN ('Mercury', 'Venus');"
    )
    # we don't push all predicates down,
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats["rows_read"] == 2, cur.stats

    conn.close()


if __name__ == "__main__":  # pragma: no cover
    test_predicate_pushdowns_sqlite()
    print("✅ okay")
