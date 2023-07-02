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

    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM testdata.flat.formats.parquet WITH(NO_PARTITION);")
    cur.arrow()
    stats = cur.stats
    assert stats["columns_read"] == 1, stats["columns_read"]
    assert stats["rows_read"] == 100000
    conn.close()

    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM testdata.flat.formats.parquet WITH(NO_PARTITION);")
    cur.arrow()
    stats = cur.stats
    assert stats["columns_read"] == 1
    assert stats["rows_read"] == 100000
    conn.close()


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
