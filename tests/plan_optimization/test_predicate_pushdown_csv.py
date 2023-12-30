"""
Test predicate pushdown using the blob reader
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from tests.tools import is_arm, is_mac, is_windows, skip_if


@skip_if(is_arm() or is_windows() or is_mac())
def test_predicate_pushdowns_blobs_csv():
    os.environ["GCP_PROJECT_ID"] = "mabeldev"

    conn = opteryx.connect()

    cur = conn.cursor()
    cur.execute(
        "SELECT username FROM testdata.flat.formats.csv WITH(NO_PARTITION) WHERE user_verified = TRUE;"
    )
    # when pushdown is enabled, we only read the matching rows from the source
    assert cur.rowcount == 134, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 134, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT username FROM testdata.flat.formats.csv WITH(NO_PARTITION) WHERE user_verified = TRUE and followers < 1000;"
    )
    # test with a more complex filter
    assert cur.rowcount == 2, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 2, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT username FROM testdata.flat.formats.csv WITH(NO_PARTITION) WHERE user_verified = TRUE and username LIKE '%b%';"
    )
    # we don't push all predicates down,
    assert cur.rowcount == 22, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 134, cur.stats

    conn.close()


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
