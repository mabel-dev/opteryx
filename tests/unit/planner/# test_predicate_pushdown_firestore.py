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
        "SET disable_optimizer = false; SELECT * FROM dwarves WHERE actor = 'Pinto Colvig';"
    )
    # if we disable pushdown, we read all the rows from the source and we do the filter
    assert cur.rowcount == 2, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 7, cur.stats

    cur = conn.cursor()
    cur.execute("SELECT * FROM dwarves WITH(NO_PUSH_SELECTION) WHERE actor = 'Pinto Colvig';")
    # if we disable pushdown, we read all the rows from the source and we do the filter
    assert cur.rowcount == 2, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 7, cur.stats

    cur = conn.cursor()
    cur.execute("SELECT * FROM dwarves WHERE actor = 'Pinto Colvig';")
    # when pushdown is enabled, we only read the matching rows from the source
    assert cur.rowcount == 2, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 2, cur.stats

    cur = conn.cursor()
    cur.execute("SELECT * FROM dwarves WHERE actor = 'Pinto Colvig' AND name = 'Sleepy';")
    # test with a two part filter
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 1, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM dwarves WHERE actor = 'Pinto Colvig' AND name = 'Sleepy' AND name = 'Brian';"
    )
    # test with A three part filter
    assert cur.rowcount == 0, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 0, cur.stats

    cur = conn.cursor()
    cur.execute("SELECT * FROM dwarves WHERE actor = 'Pinto Colvig' AND name LIKE 'Sleepy';")
    # we don't push all predicates down,
    assert cur.rowcount == 1, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 2, cur.stats

    conn.close()


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
