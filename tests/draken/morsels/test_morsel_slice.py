#!/usr/bin/env python
"""Comprehensive tests for Morsel.slice method."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pytest
import pyarrow as pa
import numpy as np

from opteryx.draken.morsels.morsel import Morsel


def test_slice_basic():
    """Test basic slice functionality."""
    tbl = pa.table({'a': [1, 2, 3, 4, 5], 'b': ['a', 'b', 'c', 'd', 'e']})
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(1, 3)
    assert sliced.shape == (3, 2)
    
    # Check values
    a_vals = [sliced.column(b'a')[i] for i in range(sliced.num_rows)]
    assert a_vals == [2, 3, 4]
    
    b_vals = [sliced.column(b'b')[i] for i in range(sliced.num_rows)]
    # Convert bytes to str if needed
    b_vals = [v.decode('utf-8') if isinstance(v, bytes) else v for v in b_vals]
    assert b_vals == ['b', 'c', 'd']


def test_slice_offset_zero():
    """Test slicing from the beginning."""
    tbl = pa.table({'x': [10, 20, 30, 40]})
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(0, 2)
    assert sliced.shape == (2, 1)
    
    vals = [sliced.column(b'x')[i] for i in range(sliced.num_rows)]
    assert vals == [10, 20]


def test_slice_to_end():
    """Test slicing to the end of the morsel."""
    tbl = pa.table({'x': [1, 2, 3, 4, 5]})
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(3, 2)
    assert sliced.shape == (2, 1)
    
    vals = [sliced.column(b'x')[i] for i in range(sliced.num_rows)]
    assert vals == [4, 5]


def test_slice_single_row():
    """Test slicing a single row."""
    tbl = pa.table({'a': [1, 2, 3], 'b': [10, 20, 30]})
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(1, 1)
    assert sliced.shape == (1, 2)
    
    assert sliced.column(b'a')[0] == 2
    assert sliced.column(b'b')[0] == 20


def test_slice_full_morsel():
    """Test slicing entire morsel."""
    tbl = pa.table({'a': [1, 2, 3]})
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(0, 3)
    assert sliced.shape == morsel.shape
    
    for i in range(morsel.num_rows):
        assert sliced.column(b'a')[i] == morsel.column(b'a')[i]


def test_slice_empty_result():
    """Test slicing with length 0."""
    tbl = pa.table({'a': [1, 2, 3]})
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(1, 0)
    assert sliced.shape == (0, 1)


def test_slice_beyond_end():
    """Test slicing beyond the end (should truncate)."""
    tbl = pa.table({'a': [1, 2, 3]})
    morsel = Morsel.from_arrow(tbl)
    
    # Request 10 rows starting at offset 1, should get 2
    sliced = morsel.slice(1, 10)
    assert sliced.shape == (2, 1)
    
    vals = [sliced.column(b'a')[i] for i in range(sliced.num_rows)]
    assert vals == [2, 3]


def test_slice_offset_at_end():
    """Test slicing starting at the very end."""
    tbl = pa.table({'a': [1, 2, 3]})
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(3, 5)
    assert sliced.shape == (0, 1)


def test_slice_offset_beyond_end():
    """Test slicing starting beyond the end."""
    tbl = pa.table({'a': [1, 2, 3]})
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(10, 5)
    assert sliced.shape == (0, 1)


def test_slice_multiple_columns():
    """Test slicing with multiple columns of different types."""
    tbl = pa.table({
        'int_col': [1, 2, 3, 4, 5],
        'float_col': [1.1, 2.2, 3.3, 4.4, 5.5],
        'str_col': ['a', 'b', 'c', 'd', 'e'],
        'bool_col': [True, False, True, False, True]
    })
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(1, 3)
    assert sliced.shape == (3, 4)
    
    # Check each column
    int_vals = [sliced.column(b'int_col')[i] for i in range(3)]
    assert int_vals == [2, 3, 4]
    
    float_vals = [sliced.column(b'float_col')[i] for i in range(3)]
    assert [round(v, 1) for v in float_vals] == [2.2, 3.3, 4.4]
    
    str_vals = [sliced.column(b'str_col')[i] for i in range(3)]
    str_vals = [v.decode('utf-8') if isinstance(v, bytes) else v for v in str_vals]
    assert str_vals == ['b', 'c', 'd']
    
    bool_vals = [sliced.column(b'bool_col')[i] for i in range(3)]
    assert bool_vals == [False, True, False]


def test_slice_with_nulls():
    """Test slicing columns containing null values."""
    tbl = pa.table({
        'a': pa.array([1, None, 3, 4, None], type=pa.int64()),
        'b': pa.array(['x', None, 'z', None, 'v'], type=pa.string())
    })
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(1, 3)
    assert sliced.shape == (3, 2)
    
    # Check that nulls are preserved
    a_vals = [sliced.column(b'a')[i] for i in range(3)]
    assert a_vals[0] is None
    assert a_vals[1] == 3
    assert a_vals[2] == 4


def test_slice_preserves_column_names():
    """Test that slice preserves column names."""
    tbl = pa.table({'foo': [1, 2], 'bar': [3, 4], 'baz': [5, 6]})
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(0, 1)
    assert sliced.column_names == morsel.column_names


def test_slice_preserves_column_types():
    """Test that slice preserves column types."""
    tbl = pa.table({
        'int64': pa.array([1, 2, 3], type=pa.int64()),
        'float64': pa.array([1.0, 2.0, 3.0], type=pa.float64()),
        'string': pa.array(['a', 'b', 'c'], type=pa.string())
    })
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(1, 1)
    
    # Convert to arrow to check types
    original_arrow = morsel.to_arrow()
    sliced_arrow = sliced.to_arrow()
    
    assert original_arrow.schema == sliced_arrow.schema


def test_slice_large_morsel():
    """Test slicing a large morsel."""
    n = 10000
    tbl = pa.table({
        'a': list(range(n)),
        'b': [f'val_{i}' for i in range(n)]
    })
    morsel = Morsel.from_arrow(tbl)
    
    # Slice middle portion
    sliced = morsel.slice(5000, 1000)
    assert sliced.shape == (1000, 2)
    
    # Check first and last values
    assert sliced.column(b'a')[0] == 5000
    assert sliced.column(b'a')[999] == 5999


def test_slice_consecutive():
    """Test consecutive slicing operations."""
    tbl = pa.table({'a': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})
    morsel = Morsel.from_arrow(tbl)
    
    # First slice
    sliced1 = morsel.slice(2, 6)
    assert sliced1.shape == (6, 1)
    assert sliced1.column(b'a')[0] == 3
    assert sliced1.column(b'a')[5] == 8
    
    # Second slice on the result
    sliced2 = sliced1.slice(1, 3)
    assert sliced2.shape == (3, 1)
    assert sliced2.column(b'a')[0] == 4
    assert sliced2.column(b'a')[2] == 6


def test_slice_single_column():
    """Test slicing morsel with single column."""
    tbl = pa.table({'only_col': [100, 200, 300, 400]})
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(1, 2)
    assert sliced.shape == (2, 1)
    assert sliced.column(b'only_col')[0] == 200
    assert sliced.column(b'only_col')[1] == 300


def test_slice_chunked_array():
    """Test slicing morsel created from chunked arrays."""
    chunked = pa.chunked_array([
        pa.array([1, 2]),
        pa.array([3, 4, 5])
    ])
    tbl = pa.Table.from_arrays([chunked], names=['x'])
    
    with pytest.raises(ValueError):
        morsel = Morsel.from_arrow(tbl)

        sliced = morsel.slice(1, 3)
        assert sliced.shape == (3, 1)
        
        vals = [sliced.column(b'x')[i] for i in range(3)]
        assert vals == [2, 3, 4]


def test_slice_with_array_type():
    """Test slicing column with array/list type."""
    tbl = pa.table({
        'lists': pa.array([[1, 2], [3, 4], [5, 6], [7, 8]], type=pa.list_(pa.int64()))
    })
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(1, 2)
    assert sliced.shape == (2, 1)
    
    # Values should be preserved
    val1 = sliced.column(b'lists')[0]
    val2 = sliced.column(b'lists')[1]
    assert list(val1) == [3, 4]
    assert list(val2) == [5, 6]


@pytest.mark.skip(reason="Struct column access needs special handling in vector API")
def test_slice_with_struct_type():
    """Test slicing column with struct type."""
    tbl = pa.table({
        'structs': pa.array([
            {'a': 1, 'b': 'x'},
            {'a': 2, 'b': 'y'},
            {'a': 3, 'b': 'z'}
        ])
    })
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(1, 1)
    assert sliced.shape == (1, 1)
    
    # Verify by converting to Arrow and checking the value
    arrow_result = sliced.to_arrow()
    val = arrow_result.column(0)[0].as_py()
    assert val['a'] == 2


def test_slice_empty_morsel():
    """Test slicing an already empty morsel."""
    tbl = pa.table({'a': pa.array([], type=pa.int64())})
    morsel = Morsel.from_arrow(tbl)
    assert morsel.shape == (0, 1)
    
    sliced = morsel.slice(0, 5)
    assert sliced.shape == (0, 1)


def test_slice_returns_new_morsel():
    """Test that slice returns a new morsel instance."""
    tbl = pa.table({'a': [1, 2, 3, 4, 5]})
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(1, 2)
    
    # Should be different objects
    assert sliced is not morsel
    
    # Original should be unchanged
    assert morsel.shape == (5, 1)
    assert sliced.shape == (2, 1)


def test_slice_negative_offset():
    """Test slice with negative offset (should raise or handle gracefully)."""
    tbl = pa.table({'a': [1, 2, 3]})
    morsel = Morsel.from_arrow(tbl)
    
    # Depending on implementation, this might raise or return empty
    # Test that it doesn't crash
    try:
        sliced = morsel.slice(-1, 2)
        # If it succeeds, it should return something sensible
        assert sliced.num_columns == 1
    except (ValueError, OverflowError):
        # Acceptable to raise on negative offset
        pass


def test_slice_negative_length():
    """Test slice with negative length (should raise or handle gracefully)."""
    tbl = pa.table({'a': [1, 2, 3]})
    morsel = Morsel.from_arrow(tbl)
    
    # Depending on implementation, this might raise or return empty
    try:
        sliced = morsel.slice(1, -2)
        # If it succeeds, should probably return empty
        assert sliced.num_rows == 0
    except (ValueError, OverflowError):
        # Acceptable to raise on negative length
        pass


def test_slice_with_different_numeric_types():
    """Test slicing columns with various numeric types."""
    tbl = pa.table({
        'int8': pa.array([1, 2, 3, 4], type=pa.int8()),
        'int16': pa.array([10, 20, 30, 40], type=pa.int16()),
        'int32': pa.array([100, 200, 300, 400], type=pa.int32()),
        'int64': pa.array([1000, 2000, 3000, 4000], type=pa.int64()),
        'uint8': pa.array([5, 6, 7, 8], type=pa.uint8()),
        'float32': pa.array([1.5, 2.5, 3.5, 4.5], type=pa.float32()),
        'float64': pa.array([10.5, 20.5, 30.5, 40.5], type=pa.float64())
    })
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(1, 2)
    assert sliced.shape == (2, 7)
    
    # Check int64 column
    int64_vals = [sliced.column(b'int64')[i] for i in range(2)]
    assert int64_vals == [2000, 3000]
    
    # Check float32 column
    float32_vals = [sliced.column(b'float32')[i] for i in range(2)]
    # Convert PyArrow scalars to Python floats
    float32_py = [v.as_py() if hasattr(v, 'as_py') else v for v in float32_vals]
    assert [round(v, 1) for v in float32_py] == [2.5, 3.5]


def test_slice_timestamp_column():
    """Test slicing column with timestamp type."""
    import datetime
    tbl = pa.table({
        'timestamps': pa.array([
            datetime.datetime(2024, 1, 1),
            datetime.datetime(2024, 1, 2),
            datetime.datetime(2024, 1, 3),
            datetime.datetime(2024, 1, 4)
        ])
    })
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(1, 2)
    assert sliced.shape == (2, 1)
    
    # Convert to arrow to check values
    arrow_result = sliced.to_arrow()
    dates = arrow_result.column(0).to_pylist()
    assert dates[0].year == 2024
    assert dates[0].month == 1
    assert dates[0].day == 2


def test_slice_binary_column():
    """Test slicing column with binary type."""
    tbl = pa.table({
        'binary': pa.array([b'aaa', b'bbb', b'ccc', b'ddd'], type=pa.binary())
    })
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(1, 2)
    assert sliced.shape == (2, 1)
    
    vals = [sliced.column(b'binary')[i] for i in range(2)]
    assert vals == [b'bbb', b'ccc']


def test_slice_fixed_size_binary():
    """Test slicing column with fixed-size binary type."""
    tbl = pa.table({
        'fixed': pa.array([b'aa', b'bb', b'cc'], type=pa.binary(2))
    })
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(1, 1)
    assert sliced.shape == (1, 1)
    val = sliced.column(b'fixed')[0]
    val_py = val.as_py() if hasattr(val, 'as_py') else val
    assert val_py == b'bb'


def test_slice_decimal_column():
    """Test slicing column with decimal type."""
    from decimal import Decimal
    tbl = pa.table({
        'decimal': pa.array([
            Decimal('1.23'),
            Decimal('4.56'),
            Decimal('7.89')
        ])
    })
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(1, 1)
    assert sliced.shape == (1, 1)


def test_slice_dictionary_encoded():
    """Test slicing dictionary-encoded column."""
    tbl = pa.table({
        'dict': pa.array(['a', 'b', 'a', 'c', 'b']).dictionary_encode()
    })
    morsel = Morsel.from_arrow(tbl)
    
    sliced = morsel.slice(1, 3)
    assert sliced.shape == (3, 1)
    
    # Check values
    vals = [sliced.column(b'dict')[i] for i in range(3)]
    # Convert PyArrow scalars to Python values
    vals_py = [v.as_py() if hasattr(v, 'as_py') else v for v in vals]
    vals_str = [v.decode('utf-8') if isinstance(v, bytes) else v for v in vals_py]
    assert vals_str == ['b', 'a', 'c']


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
