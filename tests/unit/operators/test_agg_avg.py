import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

def test_avg_parquet():
    cur = opteryx.query("SELECT AVG(followers) FROM testdata.flat.formats.parquet")
    stats = cur.stats
    assert stats["columns_read"] == 1, stats["columns_read"]
    assert stats["rows_read"] == 100000, stats["rows_read"]
    assert stats["rows_seen"] == 100000, stats["rows_seen"]
    first = cur.fetchone()[0]
    assert first == 3081.258, first

def test_avg_non_parquet():
    cur = opteryx.query("SELECT AVG(followers) FROM testdata.flat.ten_files;")
    stats = cur.stats
    assert stats["columns_read"] == 1, stats["columns_read"]
    assert stats["rows_read"] == 250, stats["rows_read"]
    assert stats["rows_seen"] == 250, stats["rows_seen"]
    first = cur.fetchone()[0]
    assert first == 7500362.668, first

def test_avg_group_by():
    """ we're reading data from the file, even though it starts SELECT COUNT(*) FROM """
    cur = opteryx.query(
        "SELECT AVG(followers) FROM testdata.flat.formats.parquet GROUP BY tweet_id ORDER BY tweet_id;"
    )
    stats = cur.stats
    assert stats["columns_read"] == 2, stats["columns_read"]
    assert stats["rows_read"] == 100000, stats["rows_read"]
    assert stats["rows_seen"] == 100000, stats["rows_seen"]
    first = cur.fetchone()[0]
    assert first == 6.0, first

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests
    
    run_tests()
