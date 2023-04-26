"""
Test predicate pushdown using the blob reader
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx import config


def test_predicate_pushdowns_blobs_orc():
    os.environ["GCP_PROJECT_ID"] = "mabeldev"

    conn = opteryx.connect()

    # TEST PREDICATE PUSHDOWN
    cur = conn.cursor()
    cur.execute(
        "SET enable_optimizer = false; SELECT user_name FROM testdata.flat.formats.orc WITH(NO_PARTITION) WHERE user_verified = TRUE;"
    )
    # if we disable pushdown, we read all the rows from the source and we do the filter
    assert cur.rowcount == 711, cur.rowcount
    assert cur.stats["rows_read"] == 100000, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT user_name FROM testdata.flat.formats.orc WITH(NO_PARTITION, NO_PUSH_SELECTION) WHERE user_verified = TRUE;"
    )
    # if we disable pushdown, we read all the rows from the source and we do the filter
    assert cur.rowcount == 711, cur.rowcount
    assert cur.stats["rows_read"] == 100000, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT user_name FROM testdata.flat.formats.orc WITH(NO_PARTITION) WHERE user_verified = TRUE;"
    )
    # when pushdown is enabled, we only read the matching rows from the source
    assert cur.rowcount == 711, cur.rowcount
    assert cur.stats["rows_read"] == 711, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT user_name FROM testdata.flat.formats.orc WITH(NO_PARTITION) WHERE user_verified = TRUE and following < 1000;"
    )
    # test with a more complex filter
    assert cur.rowcount == 266, cur.rowcount
    assert cur.stats["rows_read"] == 266, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT user_name FROM testdata.flat.formats.orc WITH(NO_PARTITION) WHERE user_verified = TRUE and user_name LIKE '%b%';"
    )
    # we don't push all predicates down,
    assert cur.rowcount == 86, cur.rowcount
    assert cur.stats["rows_read"] == 711, cur.stats

    config.ONLY_PUSH_EQUALS_PREDICATES = True
    cur = conn.cursor()
    cur.execute(
        "SELECT user_name FROM testdata.flat.formats.orc WITH(NO_PARTITION) WHERE user_verified = TRUE and following < 1000;"
    )
    # test only push equals
    assert cur.rowcount == 266, cur.rowcount
    assert cur.stats["rows_read"] == 711, cur.stats
    config.ONLY_PUSH_EQUALS_PREDICATES = False

    conn.close()


if __name__ == "__main__":  # pragma: no cover
    test_predicate_pushdowns_blobs_orc()
    print("✅ okay")
