"""
Test Cython-based fast JSONL decoder

This test can only run after the Cython extension is built.
Run with: python -m pytest tests/unit/utils/test_cython_jsonl_decoder.py
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest


def test_cython_jsonl_decoder_import():
    """Test that the Cython decoder can be imported"""
    try:
        from opteryx.compiled.structures import jsonl_decoder
        assert hasattr(jsonl_decoder, 'fast_jsonl_decode_columnar')
    except ImportError:
        pytest.skip("Cython extension not built")


def test_cython_jsonl_decoder_basic():
    """Test basic functionality of Cython JSONL decoder"""
    try:
        from opteryx.compiled.structures import jsonl_decoder
    except ImportError:
        pytest.skip("Cython extension not built")
    
    # Create simple JSONL data
    data = b'''\
{"id": 1, "name": "Alice", "active": true, "score": 95.5}
{"id": 2, "name": "Bob", "active": false, "score": 87.3}
{"id": 3, "name": "Charlie", "active": true, "score": 92.1}
'''
    
    column_names = ['id', 'name', 'active', 'score']
    column_types = {
        'id': 'int',
        'name': 'str',
        'active': 'bool',
        'score': 'float'
    }
    
    num_rows, num_cols, column_data = jsonl_decoder.fast_jsonl_decode_columnar(
        data, column_names, column_types
    )
    
    assert num_rows == 3
    assert num_cols == 4
    assert column_data['id'] == [1, 2, 3]
    assert column_data['name'] == ['Alice', 'Bob', 'Charlie']
    assert column_data['active'] == [True, False, True]
    # Floats might have minor precision differences
    assert abs(column_data['score'][0] - 95.5) < 0.01, column_data['score'][0]
    assert abs(column_data['score'][1] - 87.3) < 0.01, column_data['score'][1]
    assert abs(column_data['score'][2] - 92.1) < 0.01, column_data['score'][2]


def test_cython_jsonl_decoder_with_nulls():
    """Test Cython JSONL decoder with null values"""
    try:
        from opteryx.compiled.structures import jsonl_decoder
    except ImportError:
        pytest.skip("Cython extension not built")
    
    data = b'''\
{"id": 1, "name": "Alice", "city": "NYC"}
{"id": 2, "name": "Bob", "city": null}
{"id": 3, "name": null, "city": "LA"}
'''
    
    column_names = ['id', 'name', 'city']
    column_types = {
        'id': 'int',
        'name': 'str',
        'city': 'str'
    }
    
    num_rows, num_cols, column_data = jsonl_decoder.fast_jsonl_decode_columnar(
        data, column_names, column_types
    )
    
    assert num_rows == 3
    assert column_data['name'][0] == "Alice"
    assert column_data['name'][1] == "Bob"
    assert column_data['name'][2] is None
    assert column_data['city'][0] == "NYC"
    assert column_data['city'][1] is None
    assert column_data['city'][2] == "LA"


def test_cython_jsonl_decoder_negative_numbers():
    """Test Cython JSONL decoder with negative numbers"""
    try:
        from opteryx.compiled.structures import jsonl_decoder
    except ImportError:
        pytest.skip("Cython extension not built")
    
    data = b'''\
{"id": -1, "balance": -123.45}
{"id": -2, "balance": -0.99}
{"id": 3, "balance": 100.00}
'''
    
    column_names = ['id', 'balance']
    column_types = {
        'id': 'int',
        'balance': 'float'
    }
    
    num_rows, num_cols, column_data = jsonl_decoder.fast_jsonl_decode_columnar(
        data, column_names, column_types
    )
    
    assert num_rows == 3
    assert column_data['id'] == [-1, -2, 3]
    assert abs(column_data['balance'][0] - (-123.45)) < 0.01


def test_jsonl_decoder_integration():
    """Test integration with file_decoders.jsonl_decoder"""
    from opteryx.utils.file_decoders import jsonl_decoder
    
    data = b'''\
{"id": 1, "name": "Alice", "active": true}
{"id": 2, "name": "Bob", "active": false}
{"id": 3, "name": "Charlie", "active": true}
'''
    
    # This should work with or without the Cython extension
    num_rows, num_cols, _, table = jsonl_decoder(data, use_fast_decoder=True)
    
    assert num_rows == 3
    assert table.num_rows == 3
    assert set(table.column_names) == {"id", "name", "active"}


def test_jsonl_decoder_fallback():
    """Test that decoder falls back gracefully when Cython unavailable"""
    from opteryx.utils.file_decoders import jsonl_decoder
    
    # Small data that won't trigger fast decoder
    data = b'{"id": 1, "name": "Alice"}\n'
    
    num_rows, num_cols, _, table = jsonl_decoder(data)
    
    assert num_rows == 1
    assert table.num_rows == 1


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests
    
    run_tests()
