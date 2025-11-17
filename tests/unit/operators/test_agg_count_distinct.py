"""
COUNT(DISTINCT) tests
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pyarrow as pa

import opteryx
import opteryx.compiled.aggregations.count_distinct as count_distinct_module  # type: ignore[attr-defined]
import opteryx.third_party.abseil.containers as absl_containers  # type: ignore[attr-defined]

python_count_distinct = count_distinct_module.count_distinct
count_distinct_draken = count_distinct_module.count_distinct_draken
FlatHashSet = absl_containers.FlatHashSet


def _distinct_size(func, column):
    return func(column, FlatHashSet()).items()

def test_count_distinct_parquet():
    cur = opteryx.query("SELECT COUNT(DISTINCT user_name) FROM testdata.flat.formats.parquet;")
    stats = cur.stats
    assert stats["columns_read"] == 1, stats["columns_read"]
    assert stats["rows_read"] == 100000, stats["rows_read"]
    assert stats["rows_seen"] == 100000, stats["rows_seen"]
    first = cur.fetchone()[0]
    assert first == 83606, first

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


def test_draken_hash_matches_python_for_int64():
    column = pa.array([1, 2, 2, None, -5, None, 42], type=pa.int64())
    assert _distinct_size(python_count_distinct, column) == _distinct_size(
        count_distinct_draken, column
    )


def test_draken_hash_matches_python_for_chunked_arrays():
    column = pa.chunked_array(
        [
            pa.array(list(range(1000)) + [None], type=pa.int64()),
            pa.array(list(range(500)) + [None], type=pa.int64()),
        ]
    )
    assert _distinct_size(python_count_distinct, column) == _distinct_size(
        count_distinct_draken, column
    )

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests
    
    run_tests()
