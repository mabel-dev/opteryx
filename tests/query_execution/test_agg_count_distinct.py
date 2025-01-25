"""
COUNT(DISTINCT) tests
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

def test_count_distinct_parquet():
    cur = opteryx.query("SELECT COUNT(DISTINCT user_name) FROM testdata.flat.formats.parquet;")
    stats = cur.stats
    assert stats["columns_read"] == 1, stats["columns_read"]
    assert stats["rows_read"] == 100000, stats["rows_read"]
    assert stats["rows_seen"] == 100000, stats["rows_seen"]
    first = cur.fetchone()[0]
    assert first == 83605, first

def test_count_distinct_identifier_group_by():
    """ we're reading data from the file, even though it starts SELECT COUNT(*) FROM """
    cur = opteryx.query(
        "SELECT COUNT(DISTINCT user_name) AS un FROM testdata.flat.formats.parquet GROUP BY following ORDER BY un DESC;"
    )
    stats = cur.stats
    assert stats["columns_read"] == 2, stats["columns_read"]
    assert stats["rows_read"] == 100000, stats["rows_read"]
    assert stats["rows_seen"] == 100000, stats["rows_seen"]
    first = cur.fetchone()[0]
    assert first == 481, first

if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests
    
    run_tests()
