"""
Tests for distinct_with_draken function.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pyarrow as pa

import opteryx.draken as draken
from opteryx.compiled.table_ops.distinct import distinct


def test_distinct_with_draken_basic():
    """Test basic distinct operation with draken morsel."""
    table = pa.table({
        'a': [1, 2, 1, 3, 2],
        'b': [10, 20, 10, 30, 20],
    })
    morsel = draken.Morsel.from_arrow(table)
    
    indices, _hash_set = distinct(morsel)
    
    # Should keep rows 0, 1, 3 (unique combinations)
    assert len(indices) == 3
    assert list(indices) == [0, 1, 3]


def test_distinct_with_draken_single_column_bytes():
    """Test distinct with single column specified as bytes."""
    table = pa.table({
        'a': [1, 2, 1, 3, 2],
        'b': [10, 20, 30, 40, 50],
    })
    morsel = draken.Morsel.from_arrow(table)
    
    # Only distinct on column 'a' (as bytes)
    indices, _hash_set = distinct(morsel, columns=[b'a'])
    
    # Should keep rows 0, 1, 3 (first occurrence of 1, 2, 3)
    assert len(indices) == 3
    assert list(indices) == [0, 1, 3]


def test_distinct_with_draken_multiple_columns_bytes():
    """Test distinct with multiple columns specified as bytes."""
    table = pa.table({
        'a': [1, 1, 2, 2],
        'b': [10, 20, 10, 20],
        'c': [100, 200, 300, 400],
    })
    morsel = draken.Morsel.from_arrow(table)
    
    # Distinct on columns 'a' and 'b' (as bytes)
    indices, _hash_set = distinct(morsel, columns=[b'a', b'b'])
    
    # All combinations are unique
    assert len(indices) == 4
    assert list(indices) == [0, 1, 2, 3]


def test_distinct_with_draken_streaming():
    """Test streaming distinct with seen_hashes."""
    table1 = pa.table({'a': [1, 2, 3]})
    table2 = pa.table({'a': [2, 3, 4]})
    table3 = pa.table({'a': [4, 5, 6]})
    
    morsel1 = draken.Morsel.from_arrow(table1)
    morsel2 = draken.Morsel.from_arrow(table2)
    morsel3 = draken.Morsel.from_arrow(table3)
    
    # First batch
    indices1, hash_set = distinct(morsel1)
    assert len(indices1) == 3
    assert list(indices1) == [0, 1, 2]
    
    # Second batch - 2 and 3 already seen
    indices2, hash_set = distinct(morsel2, seen_hashes=hash_set)
    assert len(indices2) == 1
    assert list(indices2) == [2]  # Only 4 is new
    
    # Third batch - 4 already seen
    indices3, hash_set = distinct(morsel3, seen_hashes=hash_set)
    assert len(indices3) == 2
    assert list(indices3) == [1, 2]  # 5 and 6 are new


def test_distinct_with_draken_all_duplicates():
    """Test distinct when all rows are duplicates."""
    table = pa.table({'a': [1, 1, 1, 1]})
    morsel = draken.Morsel.from_arrow(table)
    
    indices, _hash_set = distinct(morsel)
    
    assert len(indices) == 1
    assert list(indices) == [0]


def test_distinct_with_draken_all_unique():
    """Test distinct when all rows are unique."""
    table = pa.table({'a': [1, 2, 3, 4, 5]})
    morsel = draken.Morsel.from_arrow(table)
    
    indices, _hash_set = distinct(morsel)
    
    assert len(indices) == 5
    assert list(indices) == [0, 1, 2, 3, 4]


def test_distinct_with_draken_empty_morsel():
    """Test distinct with empty morsel."""
    table = pa.table({'a': pa.array([], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    
    indices, _hash_set = distinct(morsel)
    
    assert len(indices) == 0


def test_distinct_with_draken_with_nulls():
    """Test distinct with null values."""
    table = pa.table({
        'a': [1, None, 1, None, 2],
        'b': [10, 20, 10, 20, 30],
    })
    morsel = draken.Morsel.from_arrow(table)
    
    indices, _hash_set = distinct(morsel)
    
    # Should have 3 unique combinations: (1,10), (None,20), (2,30)
    assert len(indices) == 3


def test_distinct_with_draken_column_names_as_strings():
    """Test that column names as strings also work (morsel.hash accepts both)."""
    table = pa.table({
        'a': [1, 2, 1, 3, 2],
        'b': [10, 20, 30, 40, 50],
    })
    morsel = draken.Morsel.from_arrow(table)
    
    # Column names as strings should also work
    indices, _hash_set = distinct(morsel, columns=['a'])
    
    assert len(indices) == 3
    assert list(indices) == [0, 1, 3]


def test_distinct_with_draken_mixed_types():
    """Test distinct with mixed column types."""
    table = pa.table({
        'int_col': [1, 2, 1, 3],
        'str_col': ['a', 'b', 'a', 'c'],
        'float_col': [1.1, 2.2, 1.1, 3.3],
        'bool_col': [True, False, True, False],
    })
    morsel = draken.Morsel.from_arrow(table)
    
    indices, _hash_set = distinct(morsel)
    
    # Rows 0 and 2 are identical
    assert len(indices) == 3
    assert list(indices) == [0, 1, 3]


def test_distinct_with_draken_large_dataset():
    """Test distinct with larger dataset."""
    n = 10000
    # Create dataset with 50% duplicates
    data = list(range(n // 2)) * 2
    table = pa.table({'a': data})
    morsel = draken.Morsel.from_arrow(table)
    
    indices, _hash_set = distinct(morsel)
    
    # Should keep first n/2 unique values
    assert len(indices) == n // 2


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
