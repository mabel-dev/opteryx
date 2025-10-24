"""
The COUNT(*) optimization is brittle, it was being missed if 'COUNT' was in
lowercase, which is why this additional testcase was written.

This optimization relies quite heavily on the AST being exactly the same as it is
when the optimization was written.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx

def test_count_star_parquet():
    """ if is just SELECT COUNT(*) for parquet files, we don't read the rows"""
    cur = opteryx.query("SELECT count(*) FROM testdata.flat.formats.parquet")
    stats = cur.stats
    assert stats["columns_read"] <= 1, stats["columns_read"]
    assert stats["rows_read"] == 1, stats["rows_read"]
    assert stats["rows_seen"] == 100000, stats["rows_seen"]
    first = cur.fetchone()[0]
    assert first == 100000, first

def test_count_star_non_parquet():
    """ if is just SELECT COUNT(*) for non-parquet files, we read the rows"""
    cur = opteryx.query("SELECT COUNT(*) FROM testdata.flat.ten_files;")
    stats = cur.stats
    assert stats["columns_read"] <= 1, stats["columns_read"]
    assert stats["rows_read"] == 10, stats["rows_read"]
    assert stats["rows_seen"] == 250, stats["rows_seen"]
    assert cur.fetchone()[0] == 250

def test_count_identifier_parquest_read_the_rows():
    """ we're counting an identifier, so we need to read the rows """
    cur = opteryx.query("SELECT COUNT(user_name) FROM testdata.flat.formats.parquet;")
    stats = cur.stats
    assert stats["columns_read"] <= 1, stats["columns_read"]
    assert stats["rows_read"] == 100000, stats["rows_read"]
    assert stats["rows_seen"] == 100000, stats["rows_seen"]
    first = cur.fetchone()[0]
    assert first == 100000, first

def test_count_star_group_by():
    """ we're reading data from the file, even though it starts SELECT COUNT(*) FROM """
    cur = opteryx.query(
        "SELECT COUNT(*) FROM testdata.flat.formats.parquet GROUP BY tweet_id;"
    )
    stats = cur.stats
    assert stats["columns_read"] <= 1, stats["columns_read"]
    assert stats["rows_read"] == 100000, stats["rows_read"]
    assert stats["rows_seen"] == 100000, stats["rows_seen"]

def test_incorrect_pushdown():
    """ 
    This is a regression test for a pushdown bug relating to COUNT(*)
    subqueries and DISTINCT - its not how I would have written this
    query (count_distinct) so went undetected as a bug
    """
    cur = opteryx.query(
        "SELECT COUNT(*) FROM (SELECT DISTINCT name FROM $planets) AS S"
    )
    stats = cur.stats
    assert stats["columns_read"] == 1, stats["columns_read"]
    assert stats["rows_read"] == 9, stats["rows_read"]
    first = cur.fetchone()[0]
    assert first == 9, first

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests
    
    run_tests()
