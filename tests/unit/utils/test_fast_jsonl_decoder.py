"""
Test fast JSONL decoder
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest


def test_fast_jsonl_decoder_basic():
    """Test basic functionality of fast JSONL decoder"""
    import pyarrow
    from opteryx.utils.file_decoders import fast_jsonl_decoder
    
    # Create simple JSONL data
    data = b'''\
{"id": 1, "name": "Alice", "active": true, "score": 95.5}
{"id": 2, "name": "Bob", "active": false, "score": 87.3}
{"id": 3, "name": "Charlie", "active": true, "score": 92.1}
'''
    
    num_rows, num_cols, table = fast_jsonl_decoder(data)
    
    assert num_rows == 3
    assert num_cols == 4
    assert table.num_rows == 3
    assert set(table.column_names) == {"id", "name", "active", "score"}
    
    # Check data types and values
    assert table.column("id").to_pylist() == [1, 2, 3]
    assert table.column("name").to_pylist() == ["Alice", "Bob", "Charlie"]
    assert table.column("active").to_pylist() == [True, False, True]
    # Floats might have minor precision differences
    scores = table.column("score").to_pylist()
    assert abs(scores[0] - 95.5) < 0.01
    assert abs(scores[1] - 87.3) < 0.01
    assert abs(scores[2] - 92.1) < 0.01


def test_fast_jsonl_decoder_with_nulls():
    """Test fast JSONL decoder with null values"""
    import pyarrow
    from opteryx.utils.file_decoders import fast_jsonl_decoder
    
    data = b'''\
{"id": 1, "name": "Alice", "city": "NYC"}
{"id": 2, "name": "Bob", "city": null}
{"id": 3, "name": null, "city": "LA"}
'''
    
    num_rows, num_cols, table = fast_jsonl_decoder(data)
    
    assert num_rows == 3
    assert table.num_rows == 3
    
    names = table.column("name").to_pylist()
    assert names[0] == "Alice"
    assert names[1] == "Bob"
    assert names[2] is None
    
    cities = table.column("city").to_pylist()
    assert cities[0] == "NYC"
    assert cities[1] is None
    assert cities[2] == "LA"


def test_fast_jsonl_decoder_with_projection():
    """Test fast JSONL decoder with column projection"""
    import pyarrow
    from opteryx.utils.file_decoders import fast_jsonl_decoder
    from opteryx.models import Node
    from opteryx.managers.expression import NodeType
    
    data = b'''\
{"id": 1, "name": "Alice", "age": 30, "city": "NYC"}
{"id": 2, "name": "Bob", "age": 25, "city": "LA"}
'''
    
    # Create mock projection nodes for just "id" and "name"
    class MockColumn:
        def __init__(self, value):
            self.value = value
    
    projection = [MockColumn("id"), MockColumn("name")]
    
    num_rows, num_cols, table = fast_jsonl_decoder(data, projection=projection)
    
    assert num_rows == 2
    assert set(table.column_names) == {"id", "name"}
    assert "age" not in table.column_names
    assert "city" not in table.column_names


def test_fast_jsonl_decoder_with_arrays():
    """Test fast JSONL decoder with array values"""
    import pyarrow
    from opteryx.utils.file_decoders import fast_jsonl_decoder
    
    data = b'''\
{"id": 1, "tags": ["python", "data"], "counts": [1, 2, 3]}
{"id": 2, "tags": ["java"], "counts": [4, 5]}
{"id": 3, "tags": [], "counts": []}
'''
    
    num_rows, num_cols, table = fast_jsonl_decoder(data)
    
    assert num_rows == 3
    assert table.num_rows == 3
    
    # Check that arrays are parsed correctly
    tags = table.column("tags").to_pylist()
    assert tags[0] == ["python", "data"]
    assert tags[1] == ["java"]
    assert tags[2] == []


def test_fast_jsonl_decoder_with_escaped_strings():
    """Test fast JSONL decoder with escaped characters in strings"""
    import pyarrow
    from opteryx.utils.file_decoders import fast_jsonl_decoder
    
    data = b'''\
{"id": 1, "message": "Hello\\nWorld"}
{"id": 2, "message": "Tab\\there"}
{"id": 3, "message": "Quote: \\"test\\""}
'''
    
    num_rows, num_cols, table = fast_jsonl_decoder(data)
    
    assert num_rows == 3
    messages = table.column("message").to_pylist()
    assert "\\n" in messages[0] or "\n" in messages[0]
    assert "\\t" in messages[1] or "\t" in messages[1]
    assert '"' in messages[2]


def test_fast_jsonl_decoder_empty_data():
    """Test fast JSONL decoder with empty data"""
    import pyarrow
    from opteryx.utils.file_decoders import fast_jsonl_decoder
    
    data = b''
    
    num_rows, num_cols, table = fast_jsonl_decoder(data)
    
    assert num_rows == 0
    assert num_cols == 0
    assert table.num_rows == 0


def test_fast_jsonl_decoder_with_negative_numbers():
    """Test fast JSONL decoder with negative numbers"""
    import pyarrow
    from opteryx.utils.file_decoders import fast_jsonl_decoder
    
    data = b'''\
{"id": -1, "balance": -123.45}
{"id": -2, "balance": -0.99}
{"id": 3, "balance": 100.00}
'''
    
    num_rows, num_cols, table = fast_jsonl_decoder(data)
    
    assert num_rows == 3
    ids = table.column("id").to_pylist()
    assert ids == [-1, -2, 3]
    
    balances = table.column("balance").to_pylist()
    assert abs(balances[0] - (-123.45)) < 0.01
    assert abs(balances[1] - (-0.99)) < 0.01
    assert abs(balances[2] - 100.00) < 0.01


def test_jsonl_decoder_integration():
    """Test integration of fast decoder with main jsonl_decoder function"""
    import pyarrow
    from opteryx.utils.file_decoders import jsonl_decoder
    
    data = b'''\
{"id": 1, "name": "Alice", "active": true}
{"id": 2, "name": "Bob", "active": false}
{"id": 3, "name": "Charlie", "active": true}
'''
    
    # Test with fast decoder enabled (default)
    num_rows, num_cols, _, table = jsonl_decoder(data, use_fast_decoder=True)
    
    assert num_rows == 3
    assert table.num_rows == 3
    assert set(table.column_names) == {"id", "name", "active"}
    
    # Test with fast decoder disabled (fallback to original)
    num_rows2, num_cols2, _, table2 = jsonl_decoder(data, use_fast_decoder=False)
    
    assert num_rows2 == 3
    assert table2.num_rows == 3
    assert set(table2.column_names) == {"id", "name", "active"}


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests
    
    run_tests()
