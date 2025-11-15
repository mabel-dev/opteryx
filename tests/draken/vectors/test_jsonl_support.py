"""Tests for JSONL data type support in Draken.

This module validates that Draken can handle all common data types
found in JSONL (JSON Lines) files, including:
- Integers (various sizes)
- Floats (various sizes)
- Booleans
- Strings (as binary)
- Arrays/Lists
- Nullable columns

These tests ensure that Draken morsels can be used as the storage
format when reading JSONL files.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pytest
import pyarrow as pa

from opteryx.draken import Vector, Morsel


class TestJSONLBasicTypes:
    """Test support for basic JSONL data types."""

    def test_integer_support(self):
        """Test that all integer types are supported with nulls."""
        test_cases = [
            (pa.int8(), [1, -2, None, 127, -128]),
            (pa.int16(), [1000, -2000, None, 32767]),
            (pa.int32(), [100000, -200000, None, 2147483647]),
            (pa.int64(), [1000000000, -2000000000, None, 9223372036854775807]),
        ]
        
        for dtype, values in test_cases:
            arr = pa.array(values, type=dtype)
            vec = Vector.from_arrow(arr)
            
            # Check basic properties
            assert vec.length == len(values)
            assert vec.null_count == 1
            
            # Round-trip should preserve data
            roundtrip = vec.to_arrow()
            assert arr.equals(roundtrip)

    def test_float_support(self):
        """Test that float types are supported with nulls."""
        test_cases = [
            (pa.float32(), [1.5, -2.5, None, 0.0, float('inf')]),
            (pa.float64(), [1.5e10, -2.5e-10, None, 0.0, float('inf')]),
        ]
        
        for dtype, values in test_cases:
            arr = pa.array(values, type=dtype)
            vec = Vector.from_arrow(arr)
            
            assert vec.length == len(values)
            assert vec.null_count == 1
            
            roundtrip = vec.to_arrow()
            assert arr.equals(roundtrip)

    def test_boolean_support(self):
        """Test that boolean type is supported with nulls."""
        arr = pa.array([True, False, None, True, False, None], type=pa.bool_())
        vec = Vector.from_arrow(arr)
        
        assert vec.length == 6
        assert vec.null_count == 2
        
        roundtrip = vec.to_arrow()
        assert arr.equals(roundtrip)

    def test_string_support(self):
        """Test that string type is supported with nulls."""
        arr = pa.array(['hello', 'world', None, '', 'foo bar'], type=pa.string())
        vec = Vector.from_arrow(arr)
        
        assert vec.length == 5
        assert vec.null_count == 1
        
        # Note: StringVector converts to binary, which is acceptable for JSONL
        roundtrip = vec.to_arrow()
        assert roundtrip.type == pa.binary()
        # Data should still match (just as bytes)
        assert roundtrip[0].as_py() == b'hello'
        assert roundtrip[2].as_py() is None

    def test_binary_support(self):
        """Test that binary type is supported with nulls."""
        arr = pa.array([b'data1', b'data2', None, b'', b'data3'], type=pa.binary())
        vec = Vector.from_arrow(arr)
        
        assert vec.length == 5
        assert vec.null_count == 1
        
        roundtrip = vec.to_arrow()
        assert arr.equals(roundtrip)

    def test_array_support(self):
        """Test that array/list types are supported with nulls."""
        test_cases = [
            pa.array([[1, 2, 3], None, [4], [], [5, 6]], type=pa.list_(pa.int64())),
            pa.array([['a', 'b'], None, ['c']], type=pa.list_(pa.string())),
            pa.array([[1.5, 2.5], None, []], type=pa.list_(pa.float64())),
        ]
        
        for arr in test_cases:
            vec = Vector.from_arrow(arr)
            
            assert vec.length == len(arr)
            assert vec.null_count > 0  # All test cases have at least one null
            
            roundtrip = vec.to_arrow()
            assert arr.equals(roundtrip)


class TestJSONLNullableSupport:
    """Test that all types properly handle nullable columns."""

    def test_all_nulls(self):
        """Test vectors with all null values."""
        test_cases = [
            pa.array([None, None, None], type=pa.int64()),
            pa.array([None, None], type=pa.float64()),
            pa.array([None], type=pa.bool_()),
            pa.array([None, None, None], type=pa.string()),
            pa.array([None, None], type=pa.list_(pa.int64())),
        ]
        
        for arr in test_cases:
            vec = Vector.from_arrow(arr)
            assert vec.null_count == vec.length

    def test_no_nulls(self):
        """Test vectors with no null values."""
        test_cases = [
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array([1.5, 2.5], type=pa.float64()),
            pa.array([True, False], type=pa.bool_()),
            pa.array(['a', 'b'], type=pa.string()),
            pa.array([[1, 2], [3]], type=pa.list_(pa.int64())),
        ]
        
        for arr in test_cases:
            vec = Vector.from_arrow(arr)
            assert vec.null_count == 0


class TestJSONLMorselIntegration:
    """Test that Morsels can handle JSONL-like data."""

    def test_create_morsel_from_jsonl_data(self):
        """Test creating a Morsel from JSONL-like structured data."""
        # Simulate data that might come from JSONL:
        # {"id": 1, "name": "Alice", "score": 95.5, "active": true, "tags": ["a", "b"]}
        # {"id": 2, "name": "Bob", "score": 87.2, "active": false, "tags": ["c"]}
        # {"id": null, "name": null, "score": null, "active": null, "tags": null}
        
        table = pa.Table.from_pydict({
            'id': [1, 2, None, 4, 5],
            'name': ['Alice', 'Bob', None, 'David', 'Eve'],
            'score': [95.5, 87.2, None, 92.1, 88.9],
            'active': [True, False, None, True, False],
            'tags': [['a', 'b'], ['c'], None, ['d', 'e'], []],
        })
        
        morsel = Morsel.from_arrow(table)
        
        assert morsel.num_rows == 5
        assert morsel.num_columns == 5
        assert morsel.column_names == [b'id', b'name', b'score', b'active', b'tags']

    def test_morsel_roundtrip_with_nulls(self):
        """Test that Morsel can round-trip JSONL data with nulls."""
        table = pa.Table.from_pydict({
            'int_col': [1, None, 3],
            'float_col': [1.5, None, 3.5],
            'bool_col': [True, None, False],
            'str_col': ['a', None, 'c'],
            'binary_col': [b'x', None, b'z'],
        })
        
        morsel = Morsel.from_arrow(table)
        roundtrip = morsel.to_arrow()
        
        assert roundtrip.num_rows == 3
        assert roundtrip.num_columns == 5
        
        # Check that nulls are preserved in each column
        for col_name in table.column_names:
            original_col = table.column(col_name)
            roundtrip_col = roundtrip.column(col_name)
            
            # Check null counts match
            assert original_col.null_count == roundtrip_col.null_count

    def test_large_jsonl_batch(self):
        """Test handling a larger batch of JSONL-like data."""
        n = 10000
        
        table = pa.Table.from_pydict({
            'id': list(range(n)),
            'value': [i * 1.5 for i in range(n)],
            'flag': [i % 2 == 0 for i in range(n)],
            'label': [f'item_{i}' for i in range(n)],
        })
        
        morsel = Morsel.from_arrow(table)
        
        assert morsel.num_rows == n
        assert morsel.num_columns == 4


class TestJSONLMixedData:
    """Test mixed data scenarios common in JSONL."""

    def test_mixed_numeric_types(self):
        """Test that different numeric types can coexist."""
        table = pa.Table.from_pydict({
            'int8_col': pa.array([1, 2, 3], type=pa.int8()),
            'int32_col': pa.array([100, 200, 300], type=pa.int32()),
            'int64_col': pa.array([10000, 20000, 30000], type=pa.int64()),
            'float32_col': pa.array([1.5, 2.5, 3.5], type=pa.float32()),
            'float64_col': pa.array([1.5e10, 2.5e10, 3.5e10], type=pa.float64()),
        })
        
        morsel = Morsel.from_arrow(table)
        assert morsel.num_rows == 3
        assert morsel.num_columns == 5

    def test_empty_arrays(self):
        """Test handling of empty arrays in list columns."""
        arr = pa.array([[], [1], [], [2, 3], []], type=pa.list_(pa.int64()))
        vec = Vector.from_arrow(arr)
        
        assert vec.length == 5
        assert vec.null_count == 0  # Empty arrays are not null

    def test_nested_nulls_in_arrays(self):
        """Test that nulls within arrays are preserved."""
        arr = pa.array([[1, None, 3], [None], None], type=pa.list_(pa.int64()))
        vec = Vector.from_arrow(arr)
        
        assert vec.length == 3
        assert vec.null_count == 1  # One null array
        
        roundtrip = vec.to_arrow()
        assert arr.equals(roundtrip)


if __name__ == "__main__":  # pragma: no cover
    # Running in the IDE
    import shutil
    import time

    # Get all test classes
    test_classes = [
        TestJSONLBasicTypes,
        TestJSONLNullableSupport,
        TestJSONLMorselIntegration,
        TestJSONLMixedData,
    ]

    start_suite = time.monotonic_ns()
    width = shutil.get_terminal_size((80, 20))[0] - 15
    passed = 0
    failed = 0
    
    print(f"RUNNING JSONL SUPPORT TESTS")
    print("=" * 70)
    
    for test_class in test_classes:
        print(f"\n{test_class.__name__}")
        print("-" * 70)
        test_instance = test_class()
        
        # Get all test methods
        test_methods = [m for m in dir(test_instance) if m.startswith('test_')]
        
        for test_method in test_methods:
            method = getattr(test_instance, test_method)
            print(f"  {test_method:50} ", end="", flush=True)
            
            try:
                start = time.monotonic_ns()
                method()
                elapsed = int((time.monotonic_ns() - start) / 1e6)
                print(f"\033[38;2;26;185;67m{elapsed:4}ms ✅\033[0m")
                passed += 1
            except Exception as err:
                elapsed = int((time.monotonic_ns() - start) / 1e6)
                print(f"\033[0;31m{elapsed:4}ms ❌\033[0m")
                print(f"    Error: {err}")
                failed += 1

    print("\n" + "=" * 70)
    print(f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m "
          f"({(time.monotonic_ns() - start_suite) / 1e9:.2f} seconds)")
    print(f"  \033[38;2;26;185;67m{passed} passed "
          f"({passed * 100 // (passed + failed) if (passed + failed) > 0 else 0}%)\033[0m")
    print(f"  \033[38;2;255;121;198m{failed} failed\033[0m")
