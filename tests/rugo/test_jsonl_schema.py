import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
import opteryx.rugo.jsonl as rj

def test_get_schema_basic():
    """Test schema extraction from basic JSON lines data."""
    data = b'''{"id": 1, "name": "Alice", "age": 30}'''
    schema = rj.get_jsonl_schema(data)

    for col in schema:
        if col['name'] == 'id':
            assert col['type'] == 'int64', f"Expected 'int64' for 'id', got {col['type']}"
        elif col['name'] == 'name':
            assert col['type'] == 'string', f"Expected 'string' for 'name', got {col['type']}"  
        elif col['name'] == 'age':
            assert col['type'] == 'int64', f"Expected 'int64' for 'age', got {col['type']}"
        else:
            pytest.fail(f"Unexpected column name: {col['name']}")

def test_get_schema_with_complex_types():
    """Test schema extraction with varied JSON lines data types."""
    data = b'''{ "id": 0, "values": [""] }
{ "id": 1, "values": [] }
{ "id": 2, "values": [null] }
{ "id": 3, "values": null }
{ "id": 4, "values": [] }
{ "id": 5, "values": ["value", null] }
{ "id": 6, "values": [null, "value"] }
{ "id": 7, "values": ["value1", "value2", "value3"] }
{ "id": 8, "values": [null, null] }
{ "id": 9 }'''
    schema = rj.get_jsonl_schema(data)
    
    expected_types = {
        'id': 'int64',
        'values': 'array<bytes>'
    }
    
    for col in schema:
        expected_type = expected_types.get(col['name'])
        assert expected_type is not None, f"Unexpected column name: {col['name']}"
        assert col['type'] == expected_type, f"Expected '{expected_type}' for '{col['name']}', got {col['type']}"

def test_how_nonexistent_values_are_handled():
    """Test how nonexistent values are handled in schema extraction."""
    data = b'''{"id": 1, "dict": {"list": [1, 2, 3], "key": "value"}, "nested": {"level1": {"key": "val"}}}
{"id": 2, "dict": {"list": [4, 5]}, "nested": {"level1": {"key": null}}}
{"id": 3, "dict": {"other_list": [6, 7, 8]}, "nested": {"level1": {}}}
{"id": 4, "dict": {"list": [], "key": "another_value"}, "nested": {}}
{"id": 5, "dict": {}, "nested": {"level1": {"key": "val"}}}
{"id": 6, "dict": {"list": [9], "nested_list": [{"key": "a"}, {"key": "b"}]}, "nested": {"level1": {"key": "val"}}}
'''
    schema = rj.get_jsonl_schema(data)

    print(schema)

    for col in schema:
        if col['name'] == 'id':
            assert col['type'] == 'int64', f"Expected 'int64' for 'id', got {col['type']}"
        elif col['name'] == 'dict':
            assert col['type'] == 'object', f"Expected 'object' for 'dict', got {col['type']}"
        elif col['name'] == 'nested':
            assert col['type'] == 'object', f"Expected 'object' for 'nested', got {col['type']}"
        else:
            pytest.fail(f"Unexpected column name: {col['name']}")

if __name__ == "__main__":    
    pytest.main([__file__, "-v"])
