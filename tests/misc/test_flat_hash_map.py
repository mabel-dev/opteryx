import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pyarrow

from opteryx.compiled.joins.inner_join import abs_hash_join_map

def test_hash_join_map_with_null_values():
    # Create a pyarrow Table with null values in the join column
    data = {
        'a': [1, 2, None, 4],
        'b': [None, 'x', 'y', 'z']
    }
    table = pyarrow.table(data)
    
    # Run the hash join map function
    hash_table = abs_hash_join_map(table, ['a'])

    # Check that rows with null values are handled correctly
    assert hash_table.get(hash(1)) == [0], hash_table.get(hash(1))
    assert hash_table.get(hash(2)) == [1]
    assert hash_table.get(hash(4)) == [3]
    # Ensure no entry for the row with a null value
    assert hash_table.get(hash(None)) == []

def test_hash_join_map_empty_input():
    # Create an empty pyarrow table
    table = pyarrow.table({'a': [], 'b': []})
    
    # Run the hash join map function
    hash_table = abs_hash_join_map(table, ['a', 'b'])
    
    # Ensure the hash table is empty
    assert hash_table.size() == 0, hash_table.size()

def test_hash_join_map_multicolumn():
    # Create a pyarrow Table with multi-column data
    data = {
        'a': [1, 2, 3, 4],
        'b': ['x', 'y', 'z', 'w']
    }
    table = pyarrow.table(data)
    
    # Run the hash join map function
    hash_table = abs_hash_join_map(table, ['a', 'b'])
    
    # Check for correct hash mappings
    assert hash_table.get(hash(1) * 31 + hash('x')) == [0]
    assert hash_table.get(hash(2) * 31 + hash('y')) == [1]
    assert hash_table.get(hash(3) * 31 + hash('z')) == [2]
    assert hash_table.get(hash(4) * 31 + hash('w')) == [3]

def test_hash_join_map_large_dataset():
    # Create a large dataset to test performance and availability
    data = {
        'a': list(range(100000)),
        'b': ['x'] * 100000
    }
    table = pyarrow.table(data)
    
    # Run the hash join map function
    hash_table = abs_hash_join_map(table, ['a', 'b'])
    
    # Verify it doesn’t crash and handles the large data set
    assert hash_table.get(hash(99999) * 31 + hash('x')) == [99999]

def test_hash_join_map_duplicate_keys():
    # Create a pyarrow Table with duplicate keys
    data = {
        'a': [1, 2, 2, 4],
        'b': ['x', 'y', 'y', 'z']
    }
    table = pyarrow.table(data)
    
    # Run the hash join map function
    hash_table = abs_hash_join_map(table, ['a', 'b'])
    
    # Check for correct hash mappings with duplicates
    assert hash_table.get(hash(1) * 31 + hash('x')) == [0]
    assert hash_table.get(hash(2) * 31 + hash('y')) == [1, 2]
    assert hash_table.get(hash(4) * 31 + hash('z')) == [3]


def test_hash_join_map_large_null_values():
    # Create a large dataset with null values
    data = {
        'a': [None] * 50000 + list(range(50000)),
        'b': ['x'] * 100000
    }
    table = pyarrow.table(data)
    
    # Run the hash join map function
    hash_table = abs_hash_join_map(table, ['a', 'b'])
    
    # Verify it handles the large data set with null values
    assert hash_table.get(hash(49999) * 31 + hash('x')) == [99999]
    assert hash_table.get(hash(None)) == []

if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
