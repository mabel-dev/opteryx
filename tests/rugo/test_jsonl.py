"""
Tests for JSON lines reader functionality.
"""
import sys
from pathlib import Path
import os
sys.path.insert(0, str(Path(__file__).parent.parent))

# Add draken path if available
try:
    draken_path = os.environ.get('DRAKEN_PATH', str(Path(__file__).parent.parent / 'draken'))
    sys.path.insert(0, draken_path)
except Exception:
    pass

import pytest

import opteryx.rugo.jsonl as rj

# Helper function to convert draken vectors to lists for comparison
def vector_to_list(vec):
    """Convert a draken vector or Python list to a Python list."""
    if hasattr(vec, 'to_arrow'):
        # It's a draken vector, convert to pyarrow then to list
        arrow_array = vec.to_arrow()
        return arrow_array.to_pylist()
    else:
        # It's already a Python list
        return vec


def test_get_schema_basic():
    """Test schema extraction from basic JSON lines data."""
    data = b'''{"id": 1, "name": "Alice", "age": 30}
{"id": 2, "name": "Bob", "age": 25}
{"id": 3, "name": "Charlie", "age": 35}'''
    
    schema = rj.get_jsonl_schema(data)
    
    assert isinstance(schema, list)
    assert len(schema) == 3
    
    # Check column names
    names = [col['name'] for col in schema]
    assert 'id' in names
    assert 'name' in names
    assert 'age' in names


def test_read_all_columns():
    """Test reading all columns from JSON lines data."""
    data = b'''{"id": 1, "name": "Alice", "age": 30}
{"id": 2, "name": "Bob", "age": 25}
{"id": 3, "name": "Charlie", "age": 35}'''
    
    result = rj.read_jsonl(data)
    
    assert result['success']
    assert result['num_rows'] == 3
    assert len(result['column_names']) == 3
    assert len(result['columns']) == 3


def test_read_with_projection():
    """Test reading specific columns (projection pushdown)."""
    data = b'''{"id": 1, "name": "Alice", "age": 30, "salary": 50000.0}
{"id": 2, "name": "Bob", "age": 25, "salary": 45000.0}
{"id": 3, "name": "Charlie", "age": 35, "salary": 55000.0}'''
    
    result = rj.read_jsonl(data, columns=['name', 'salary'])
    
    assert result['success']
    assert result['num_rows'] == 3
    assert result['column_names'] == ['name', 'salary']
    assert len(result['columns']) == 2
    
    # Check the data - convert vectors to lists
    names = vector_to_list(result['columns'][0])
    salaries = vector_to_list(result['columns'][1])
    
    assert names == [b'Alice', b'Bob', b'Charlie']
    assert salaries == [50000.0, 45000.0, 55000.0]


def test_read_int64_column():
    """Test reading integer columns."""
    data = b'''{"id": 1, "count": 100}
{"id": 2, "count": 200}
{"id": 3, "count": 300}'''
    
    result = rj.read_jsonl(data, columns=['id', 'count'])
    
    assert result['success']
    ids = vector_to_list(result['columns'][0])
    counts = vector_to_list(result['columns'][1])
    
    assert ids == [1, 2, 3]
    assert counts == [100, 200, 300]


def test_read_string_column():
    """Test reading string columns."""
    data = b'''{"name": "Alice", "city": "NYC"}
{"name": "Bob", "city": "LA"}
{"name": "Charlie", "city": "SF"}'''
    
    result = rj.read_jsonl(data, columns=['name', 'city'])
    
    assert result['success']
    names = vector_to_list(result['columns'][0])
    cities = vector_to_list(result['columns'][1])

    assert names == [b'Alice', b'Bob', b'Charlie']
    assert cities == [b'NYC', b'LA', b'SF']


def test_read_double_column():
    """Test reading double/float columns."""
    data = b'''{"price": 19.99, "tax": 1.5}
{"price": 29.99, "tax": 2.25}
{"price": 39.99, "tax": 3.0}'''
    
    result = rj.read_jsonl(data, columns=['price', 'tax'])
    
    assert result['success']
    prices = vector_to_list(result['columns'][0])
    taxes = vector_to_list(result['columns'][1])
    
    assert prices == [19.99, 29.99, 39.99]
    assert taxes == [1.5, 2.25, 3.0]


def test_read_boolean_column():
    """Test reading boolean columns."""
    data = b'''{"active": true, "verified": false}
{"active": false, "verified": true}
{"active": true, "verified": true}'''
    
    result = rj.read_jsonl(data, columns=['active', 'verified'])
    
    assert result['success']
    active = vector_to_list(result['columns'][0])
    verified = vector_to_list(result['columns'][1])
    
    assert active == [True, False, True]
    assert verified == [False, True, True]


def test_read_with_nulls():
    """Test reading data with null values."""
    data = b'''{"id": 1, "name": "Alice", "age": 30}
{"id": 2, "name": null, "age": 25}
{"id": 3, "name": "Charlie", "age": null}'''
    
    result = rj.read_jsonl(data, columns=['id', 'name', 'age'])
    
    assert result['success']
    ids = vector_to_list(result['columns'][0])
    names = vector_to_list(result['columns'][1])
    ages = vector_to_list(result['columns'][2])
    
    assert ids == [1, 2, 3]
    assert names == [b'Alice', None, b'Charlie']
    assert ages == [30, 25, None]


def test_empty_data():
    """Test handling empty data."""
    data = b''
    
    result = rj.read_jsonl(data)
    
    # Empty data should return failure or empty result
    assert not result['success'] or result['num_rows'] == 0


def test_malformed_json():
    """Test handling malformed JSON."""
    data = b'''{"id": 1, "name": "Alice"
{"id": 2, "name": "Bob"}'''  # Missing closing brace on first line
    
    result = rj.read_jsonl(data)
    
    # Should handle gracefully - might skip malformed lines
    # At minimum shouldn't crash
    assert isinstance(result, dict)


def test_fast_integer_parsing():
    """Test fast integer parsing with various edge cases."""
    data = b'''{"pos": 123, "neg": -456, "zero": 0, "large": 9223372036854775807}
{"pos": 1, "neg": -1, "zero": 0, "large": 1000000000000}'''
    
    result = rj.read_jsonl(data, columns=['pos', 'neg', 'zero', 'large'])
    
    assert result['success']
    assert result['num_rows'] == 2
    
    pos = vector_to_list(result['columns'][0])
    neg = vector_to_list(result['columns'][1])
    zero = vector_to_list(result['columns'][2])
    large = vector_to_list(result['columns'][3])
    
    assert pos == [123, 1]
    assert neg == [-456, -1]
    assert zero == [0, 0]
    assert large == [9223372036854775807, 1000000000000]


def test_fast_float_parsing():
    """Test fast float parsing with various formats."""
    data = b'''{"simple": 1.5, "scientific": 1.23e10, "negative": -3.14159, "zero": 0.0}
{"simple": 2.5, "scientific": 4.56e-5, "negative": -2.71828, "zero": 0.0}'''
    
    result = rj.read_jsonl(data, columns=['simple', 'scientific', 'negative', 'zero'])
    
    assert result['success']
    assert result['num_rows'] == 2
    
    simple = vector_to_list(result['columns'][0])
    scientific = vector_to_list(result['columns'][1])
    negative = vector_to_list(result['columns'][2])
    zero = vector_to_list(result['columns'][3])
    
    assert simple == [1.5, 2.5]
    assert abs(scientific[0] - 1.23e10) < 1e5
    assert abs(scientific[1] - 4.56e-5) < 1e-10
    assert abs(negative[0] - (-3.14159)) < 1e-5
    assert abs(negative[1] - (-2.71828)) < 1e-5
    assert zero == [0.0, 0.0]


def test_large_dataset_preallocation():
    """Test memory pre-allocation with larger datasets."""
    # Create a dataset with 1000 rows
    lines = []
    for i in range(1000):
        lines.append(f'{{"id": {i}, "value": {i * 1.5}, "name": "item_{i}"}}'.encode())
    data = b'\n'.join(lines)
    
    result = rj.read_jsonl(data, columns=['id', 'value'])
    
    assert result['success']
    assert result['num_rows'] == 1000
    
    ids = vector_to_list(result['columns'][0])
    values = vector_to_list(result['columns'][1])
    
    assert len(ids) == 1000
    assert len(values) == 1000
    assert ids[0] == 0
    assert ids[999] == 999
    assert abs(values[0] - 0.0) < 1e-10
    assert abs(values[999] - 1498.5) < 1e-10

def test_consistency():
    """Test schema extraction from basic JSON lines data."""
    data = b'{"id": 1, "name": "Alice", "age": 30}\n' * 100
    data += b'{"id": 2, "name": "[Bob]", "age": 25}\n'
    data += b'{"id": 3, "name": "Charlie", "age": 35.0}'

    table = rj.read_jsonl(data)

    # Convert vectors to lists for consistency checking
    for col in table['columns']:
        col_list = vector_to_list(col)
        if col_list:  # Only check non-empty columns
            first_non_null = next((val for val in col_list if val is not None), None)
            if first_non_null is not None:
                assert all(isinstance(val, type(first_non_null)) or val is None for val in col_list), \
                    f"Inconsistent types in column with first value {first_non_null}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
