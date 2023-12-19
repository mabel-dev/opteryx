import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_count_star():
    """
    The COUNT(*) optimization is brittle, it was being missed if 'COUNT' was in
    lowercase, which is why this additional testcase was written.

    This optimization relies quite heavily on the AST being exactly the same as it is
    when the optimization was written.
    """
    import opteryx

    cur = opteryx.query("SELECT count(*) FROM testdata.flat.formats.parquet")
    stats = cur.stats
    assert stats["columns_read"] == 1, stats["columns_read"]
    assert stats["rows_read"] == 100000, stats["rows_read"]

    cur = opteryx.query("SELECT COUNT(*) FROM testdata.flat.formats.parquet;")
    stats = cur.stats
    assert stats["columns_read"] == 1, stats["columns_read"]
    assert stats["rows_read"] == 100000, stats["rows_read"]

    cur = opteryx.query(
        "SELECT COUNT(*), tweet_id FROM testdata.flat.formats.parquet GROUP BY tweet_id;"
    )
    stats = cur.stats
    assert stats["columns_read"] == 1, stats["columns_read"]
    assert stats["rows_read"] == 100000, stats["rows_read"]


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    test_count_star()
    run_tests()
