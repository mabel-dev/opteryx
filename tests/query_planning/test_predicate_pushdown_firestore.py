"""
Test predicate pushdown using the document collections
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

from opteryx.connectors import GcpFireStoreConnector


def test_predicate_pushdowns_firestore():

    opteryx.register_store("dwarves", GcpFireStoreConnector)
    os.environ["GCP_PROJECT_ID"] = "mabeldev"

    conn = opteryx.connect()

    # TEST PREDICATE PUSHDOWN
    cur = conn.cursor()
    cur.execute(
        "SET enable_optimizer = false; SELECT * FROM dwarves WHERE actor = 'Pinto Colvig';"
    )
    # if we disable pushdown, we read all the rows from the source and we do the filter
    assert cur.rowcount == 2, cur.rowcount
    assert cur.stats["rows_read"] == 7, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM dwarves WITH(NO_PUSH_SELECTION) WHERE actor = 'Pinto Colvig';"
    )
    # if we disable pushdown, we read all the rows from the source and we do the filter
    assert cur.rowcount == 2, cur.rowcount
    assert cur.stats["rows_read"] == 7, cur.stats

    cur = conn.cursor()
    cur.execute("SELECT * FROM dwarves WHERE actor = 'Pinto Colvig';")
    # when pushdown is enabled, we only read the matching rows from the source
    assert cur.rowcount == 2, cur.rowcount
    assert cur.stats["rows_read"] == 2, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM dwarves WHERE actor = 'Pinto Colvig' AND name = 'Sleepy';"
    )
    # test with a more complex filter
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats["rows_read"] == 1, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM dwarves WHERE actor = 'Pinto Colvig' AND name LIKE 'Sleepy';"
    )
    # we don't push all predicates down,
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats["rows_read"] == 2, cur.stats

    conn.close()


if __name__ == "__main__":  # pragma: no cover
    test_predicate_pushdowns_firestore()
    print("✅ okay")
