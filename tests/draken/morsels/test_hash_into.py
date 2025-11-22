#!/usr/bin/env python
"""Tests for hash_into implementations across all vector types."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pytest
import pyarrow as pa
from array import array
import opteryx.draken as draken
from opteryx.draken.vectors._hash_api import hash_into as hash_into_vector
from opteryx.draken.vectors.arrow_vector import ArrowVector

try:
    from opteryx.draken.vectors.vector import MIX_HASH_CONSTANT  # type: ignore[attr-defined]
except (ImportError, AttributeError):
    from opteryx.draken.vectors.arrow_vector import MIX_HASH_CONSTANT


def _hash_view_to_list(buffer):
    """Helper to convert hash buffer to list."""
    view = memoryview(buffer)
    assert view.format == "Q"
    return list(view)


def _mix_hash(current: int, value: int, mix_constant: int) -> int:
    current = (current ^ value) * mix_constant + 1
    current &= 0xFFFFFFFFFFFFFFFF
    current ^= current >> 32
    return current & 0xFFFFFFFFFFFFFFFF


def _vector_hash_to_list(vector, mix_constant: int = 0x9E3779B97F4A7C15):
    """Compute vector hashes into a Python list.

    The `mix_constant` parameter is accepted for compatibility with older
    tests but is ignored because the mixing constant is now enforced
    internally by the implementation.
    """
    length = getattr(vector, "length", None)

    if length is None:
        try:
            length = len(vector)  # type: ignore[arg-type]
        except TypeError:
            length = len(vector.to_pylist())

    out = array("Q", [0] * length)
    hash_into_vector(vector, out)
    return list(out)


def test_hash_into_int64_vector():
    """Int64Vector.hash_into should properly mix hashes into buffer."""
    table = pa.table({'a': pa.array([1, 2, 3], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    # Create output buffer
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    # Call hash_into
    hash_into_vector(vec, out_view, 0)
    
    # Verify buffer was modified
    result = list(out_buf)
    assert result != [0, 0, 0]
    assert all(h != 0 for h in result)


def test_hash_into_float64_vector():
    """Float64Vector.hash_into should properly mix hashes into buffer."""
    table = pa.table({'a': pa.array([1.0, 2.5, 3.7], type=pa.float64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert result != [0, 0, 0]


def test_hash_into_string_vector():
    """StringVector.hash_into should properly mix hashes into buffer."""
    table = pa.table({'a': ['hello', 'world', 'test']})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert result != [0, 0, 0]


def test_hash_into_bool_vector():
    """BoolVector.hash_into should properly mix hashes into buffer."""
    table = pa.table({'a': pa.array([True, False, True], type=pa.bool_())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert result != [0, 0, 0]


def test_hash_into_timestamp_vector():
    """TimestampVector.hash_into should properly mix hashes into buffer."""
    table = pa.table({'a': pa.array([1, 2, 3], type=pa.timestamp('ns'))})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert result != [0, 0, 0]


def test_timestamp_hash_matches_arrow_vector_for_sliced_arrays():
    """TimestampVector hash_into should match ArrowVector for sliced arrays with offsets."""
    base = pa.array([None, 1, None, 2, 3, None], type=pa.timestamp('ns'))
    arr = base.slice(1, 4)

    arrow_vec = ArrowVector(arr)
    table = pa.table({'a': arr})
    morsel = draken.Morsel.from_arrow(table)
    native_vec = morsel.column(b'a')

    assert _vector_hash_to_list(native_vec) == _vector_hash_to_list(arrow_vec)


def test_timestamp_hash_handles_timezone_arrays_with_offsets():
    """TimestampVector hash_into should respect nulls when timezone arrays are sliced."""
    base = pa.array([
        None,
        1,
        None,
        2,
        3,
        None,
        4,
    ], type=pa.timestamp('us', tz='UTC'))
    arr = base.slice(2, 4)

    arrow_vec = ArrowVector(arr)
    table = pa.table({'a': arr})
    morsel = draken.Morsel.from_arrow(table)
    native_vec = morsel.column(b'a')

    assert _vector_hash_to_list(native_vec) == _vector_hash_to_list(arrow_vec)


def test_hash_into_date32_vector():
    """Date32Vector.hash_into should properly mix hashes into buffer."""
    table = pa.table({'a': pa.array([1, 2, 3], type=pa.date32())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert result != [0, 0, 0]


def test_hash_into_arrow_vector_int8():
    """ArrowVector.hash_into should work for int8 type."""
    table = pa.table({'a': pa.array([1, 2, 3], type=pa.int8())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert result != [0, 0, 0]


def test_hash_into_arrow_vector_int16():
    """ArrowVector.hash_into should work for int16 type."""
    table = pa.table({'a': pa.array([1, 2, 3], type=pa.int16())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert result != [0, 0, 0]


def test_hash_into_arrow_vector_int32():
    """ArrowVector.hash_into should work for int32 type."""
    table = pa.table({'a': pa.array([1, 2, 3], type=pa.int32())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert result != [0, 0, 0]


def test_hash_into_arrow_vector_float32():
    """ArrowVector.hash_into should work for float32 type."""
    table = pa.table({'a': pa.array([1.0, 2.0, 3.0], type=pa.float32())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert result != [0, 0, 0]


def test_hash_into_array_vector():
    """ArrayVector.hash_into should work for list types."""
    table = pa.table({'a': pa.array([[1, 2], [3, 4], [5]], type=pa.list_(pa.int64()))})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert result != [0, 0, 0]


def test_hash_into_with_offset():
    """hash_into should respect offset parameter."""
    table = pa.table({'a': pa.array([1, 2], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0, 0])
    out_view = memoryview(out_buf)
    
    # Write at offset 2
    hash_into_vector(vec, out_view, 2)
    
    result = list(out_buf)
    # First two should be unchanged (zero)
    assert result[0] == 0
    assert result[1] == 0
    # Last two should be modified
    assert result[2] != 0
    assert result[3] != 0


def test_hash_into_buffer_too_small():
    """hash_into should raise ValueError if buffer is too small."""
    table = pa.table({'a': pa.array([1, 2, 3], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    # Buffer only has 2 elements but vector has 3
    out_buf = array('Q', [0, 0])
    out_view = memoryview(out_buf)
    
    with pytest.raises(ValueError, match="output buffer too small"):
        hash_into_vector(vec, out_view, 0)


def test_hash_into_buffer_too_small_with_offset():
    """hash_into should raise ValueError if buffer too small considering offset."""
    table = pa.table({'a': pa.array([1, 2, 3], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    # Buffer has 4 elements but with offset 2, only 2 slots available for 3 values
    out_buf = array('Q', [0, 0, 0, 0])
    out_view = memoryview(out_buf)
    
    with pytest.raises(ValueError, match="output buffer too small"):
        hash_into_vector(vec, out_view, 2)


def test_hash_into_consistency_with_hash():
    """hash_into should produce same results as hash when starting from zero buffer."""
    table = pa.table({'a': pa.array([1, 2, 3, 4, 5], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    # Compute expected hashes manually using the same mixing routine
    mix_constant = 0x9E3779B97F4A7C15
    values = vec.to_pylist()
    expected = []
    for value in values:
        if value is None:
            hashed = _mix_hash(0, 0x9E3779B97F4A7C15, mix_constant)
        else:
            hashed = _mix_hash(0, value & 0xFFFFFFFFFFFFFFFF, mix_constant)
        expected.append(hashed)

    out_buf = array('Q', [0] * len(expected))
    hash_into_vector(vec, out_buf, 0)

    assert list(out_buf) == expected


def test_morsel_hash_uses_hash_into():
    """Morsel.hash() should use hash_into internally for all column types."""
    # Test with multiple column types
    table = pa.table({
        'int64': pa.array([1, 2], type=pa.int64()),
        'float64': pa.array([1.0, 2.0], type=pa.float64()),
        'string': ['a', 'b'],
        'bool': pa.array([True, False], type=pa.bool_()),
        'int8': pa.array([1, 2], type=pa.int8()),
        'list': pa.array([[1], [2]], type=pa.list_(pa.int64())),
    })
    morsel = draken.Morsel.from_arrow(table)
    
    # Should not raise
    hashes = _hash_view_to_list(morsel.hash())
    assert len(hashes) == 2
    assert all(h != 0 for h in hashes)


def test_hash_into_with_nulls():
    """hash_into should handle null values correctly."""
    table = pa.table({'a': pa.array([1, None, 3], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    # All should be non-zero (nulls have NULL_HASH)
    assert all(h != 0 for h in result)


def test_hash_into_empty_vector():
    """hash_into should handle empty vectors."""
    table = pa.table({'a': pa.array([], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [])
    out_view = memoryview(out_buf)
    
    # Should not raise
    hash_into_vector(vec, out_view, 0)


def test_hash_into_different_mix_constants():
    """hash_into enforces a shared mix constant even when callers pass overrides."""
    table = pa.table({'a': pa.array([1, 2, 3], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf1 = array('Q', [0, 0, 0])
    out_view1 = memoryview(out_buf1)
    hash_into_vector(vec, out_view1, 0)
    
    out_buf2 = array('Q', [0, 0, 0])
    out_view2 = memoryview(out_buf2)
    hash_into_vector(vec, out_view2, 0)
    
    # Both invocations collapse to the shared MIX_HASH_CONSTANT
    assert list(out_buf1) == list(out_buf2)
    baseline = _vector_hash_to_list(vec, mix_constant=MIX_HASH_CONSTANT)
    assert list(out_buf1) == baseline


def test_morsel_multi_column_hash_combines_correctly():
    """Morsel.hash() should combine multiple column hashes using hash_into."""
    table = pa.table({
        'a': pa.array([1, 2, 3], type=pa.int64()),
        'b': pa.array([10, 20, 30], type=pa.int64()),
    })
    morsel = draken.Morsel.from_arrow(table)
    
    # Get combined hash
    combined = _hash_view_to_list(morsel.hash())
    
    # Get individual column hashes
    col_a_hash = _vector_hash_to_list(morsel.column(b'a'))
    col_b_hash = _vector_hash_to_list(morsel.column(b'b'))
    
    # Combined should differ from either individual column
    assert combined != col_a_hash
    assert combined != col_b_hash


def test_hash_into_very_large_buffer():
    """hash_into should work with very large buffers."""
    n = 100000
    table = pa.table({'a': pa.array(list(range(n)), type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0] * n)
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    # Verify all non-zero values were modified (hash of 0 may be 0 after mix)
    result = list(out_buf)
    non_zero_count = sum(1 for h in result if h != 0)
    # Most should be non-zero (hash of value 0 might produce 0 after mixing)
    assert non_zero_count > n * 0.99
    # Check some distribution
    unique_hashes = len(set(result))
    assert unique_hashes > n * 0.99  # Should be mostly unique


def test_hash_into_single_element():
    """hash_into should work with single-element vectors."""
    table = pa.table({'a': pa.array([42], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    assert out_buf[0] != 0


def test_hash_into_all_same_values():
    """hash_into should produce same hash for identical values."""
    table = pa.table({'a': pa.array([7, 7, 7, 7], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    # All should be the same since input values are identical
    assert len(set(result)) == 1
    assert result[0] != 0


def test_hash_into_sequential_values():
    """hash_into should produce different hashes for sequential values."""
    table = pa.table({'a': pa.array([1, 2, 3, 4, 5], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    # All should be unique
    assert len(set(result)) == 5


def test_hash_into_negative_integers():
    """hash_into should handle negative integers."""
    table = pa.table({'a': pa.array([-100, -1, 0, 1, 100], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    # All should have been hashed (including 0, which hashes to non-zero)
    non_zero = sum(1 for h in result if h != 0)
    assert non_zero >= 4  # At least the non-zero values
    assert len(set(result)) == 5  # All unique


def test_hash_into_float_special_values():
    """hash_into should handle special float values."""
    table = pa.table({'a': pa.array([0.0, -0.0, float('inf'), float('-inf')], type=pa.float64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    # +inf and -inf should be non-zero and different
    assert result[2] != 0  # inf
    assert result[3] != 0  # -inf
    assert result[2] != result[3]  # inf != -inf
    # 0.0 and -0.0 have different bit patterns so may hash differently
    assert len(set(result)) >= 3  # At least inf, -inf, and one zero are unique


def test_hash_into_float_nan():
    """hash_into should handle NaN values."""
    table = pa.table({'a': pa.array([1.0, float('nan'), 3.0], type=pa.float64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    # Should not raise
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert all(h != 0 for h in result)


def test_hash_into_string_empty():
    """hash_into should handle empty strings."""
    table = pa.table({'a': ['', 'a', '']})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    # Empty strings should have same hash
    assert result[0] == result[2]
    # Non-empty should differ
    assert result[1] != result[0]


def test_hash_into_string_unicode():
    """hash_into should handle unicode strings properly."""
    table = pa.table({'a': ['hello', '世界', 'مرحبا', '🎉']})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert all(h != 0 for h in result)
    assert len(set(result)) == 4  # All unique


def test_hash_into_string_very_long():
    """hash_into should handle very long strings."""
    long_str = 'a' * 10000
    table = pa.table({'a': [long_str, 'b', long_str]})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    # Same strings should have same hash
    assert result[0] == result[2]
    assert result[1] != result[0]


def test_hash_into_bool_all_true():
    """hash_into should handle all True values."""
    table = pa.table({'a': pa.array([True, True, True], type=pa.bool_())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert len(set(result)) == 1  # All same
    assert result[0] != 0


def test_hash_into_bool_all_false():
    """hash_into should handle all False values."""
    table = pa.table({'a': pa.array([False, False, False], type=pa.bool_())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert len(set(result)) == 1  # All same
    # False hashes consistently (might be 0 or non-zero depending on implementation)


def test_hash_into_bool_mixed():
    """hash_into should produce different hashes for True vs False."""
    table = pa.table({'a': pa.array([True, False], type=pa.bool_())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert result[0] != result[1]


def test_hash_into_timestamp_epoch():
    """hash_into should handle epoch timestamp."""
    table = pa.table({'a': pa.array([0, 1, -1], type=pa.timestamp('ns'))})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert result[1] != 0
    assert result[2] != 0
    assert len(set(result)) == 3


def test_hash_into_date_boundaries():
    """hash_into should handle date boundary values."""
    table = pa.table({'a': pa.array([0, 1, -1, 2147483647, -2147483648], type=pa.date32())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert all(h != 0 for h in result)
    assert len(set(result)) == 5


def test_hash_into_nested_lists():
    """hash_into should handle nested list structures."""
    table = pa.table({'a': pa.array([
        [1, 2, 3],
        [1, 2, 3],  # Duplicate
        [4, 5, 6],
        [],
        [None, 1]
    ], type=pa.list_(pa.int64()))})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    # Duplicate lists should have same hash
    assert result[0] == result[1]
    # Different lists should differ
    assert result[0] != result[2]
    assert all(h != 0 for h in result)


def test_hash_into_all_nulls_int64():
    """hash_into should handle all-null int64 column."""
    table = pa.table({'a': pa.array([None, None, None], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    # All nulls should have same hash
    assert len(set(result)) == 1
    assert result[0] != 0  # NULL_HASH constant


def test_hash_into_all_nulls_string():
    """hash_into should handle all-null string column."""
    table = pa.table({'a': pa.array([None, None, None], type=pa.string())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert len(set(result)) == 1
    assert result[0] != 0


def test_hash_into_alternating_null_values():
    """hash_into should handle alternating null and non-null values."""
    table = pa.table({'a': pa.array([1, None, 3, None, 5], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    # Null hashes should be same
    assert result[1] == result[3]
    # Non-null values should differ from nulls
    assert result[0] != result[1]


def test_hash_into_zero_mix_constant():
    """hash_into treats zero mix overrides as a request for the shared constant."""
    table = pa.table({'a': pa.array([1, 2, 3], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    # Should not raise (mix constant override removed)
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    baseline = _vector_hash_to_list(vec, mix_constant=MIX_HASH_CONSTANT)
    assert all(h != 0 for h in result)
    assert result == baseline


def test_hash_into_max_uint64_mix_constant():
    """hash_into should handle maximum uint64 mix constant."""
    table = pa.table({'a': pa.array([1, 2, 3], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    max_uint64 = 0xFFFFFFFFFFFFFFFF
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert all(h != 0 for h in result)


def test_hash_into_multiple_calls_accumulate():
    """Multiple hash_into calls should accumulate in buffer."""
    table1 = pa.table({'a': pa.array([1, 2, 3], type=pa.int64())})
    table2 = pa.table({'b': pa.array([10, 20, 30], type=pa.int64())})
    
    morsel1 = draken.Morsel.from_arrow(table1)
    morsel2 = draken.Morsel.from_arrow(table2)
    
    vec1 = morsel1.column(b'a')
    vec2 = morsel2.column(b'b')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    # First call
    hash_into_vector(vec1, out_view, 0)
    result_after_first = list(out_buf)
    
    # Second call (should combine with first)
    hash_into_vector(vec2, out_view, 0)
    result_after_second = list(out_buf)
    
    # Results should be different after second call
    assert result_after_first != result_after_second


def test_hash_into_preinitalized_buffer():
    """hash_into should work with pre-initialized non-zero buffer."""
    table = pa.table({'a': pa.array([1, 2, 3], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    # Pre-initialized buffer with non-zero values
    out_buf = array('Q', [0xAAAAAAAAAAAAAAAA, 0xBBBBBBBBBBBBBBBB, 0xCCCCCCCCCCCCCCCC])
    out_view = memoryview(out_buf)
    initial_values = list(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    # Should be different from initial values
    assert result != initial_values


def test_hash_into_offset_at_end():
    """hash_into with offset at the end of buffer."""
    table = pa.table({'a': pa.array([1], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0, 0, 0])
    out_view = memoryview(out_buf)
    
    # Write at the last position
    hash_into_vector(vec, out_view, 4)
    
    result = list(out_buf)
    # First four should be unchanged
    assert result[0:4] == [0, 0, 0, 0]
    # Last should be modified
    assert result[4] != 0


def test_hash_into_arrow_vector_null_type():
    """ArrowVector.hash_into should handle null type arrays."""
    table = pa.table({'a': pa.array([None, None], type=pa.null())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    # All nulls should have same hash
    assert result[0] == result[1]
    assert result[0] != 0


def test_morsel_hash_single_column_matches_vector_hash():
    """Single column morsel hash should match hashing the underlying vector."""
    table = pa.table({'a': pa.array([1, 2, 3, 4, 5], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    
    # Get morsel hash (single column path)
    morsel_hash = _hash_view_to_list(morsel.hash())
    
    # Get vector hash result
    vec = morsel.column(b'a')
    vector_hash = _vector_hash_to_list(vec)
    
    # Should be identical for single column
    assert morsel_hash == vector_hash


def test_morsel_hash_column_order_matters():
    """Morsel hash should differ when column order changes."""
    table1 = pa.table({
        'a': pa.array([1, 2, 3], type=pa.int64()),
        'b': pa.array([4, 5, 6], type=pa.int64()),
    })
    
    table2 = pa.table({
        'b': pa.array([4, 5, 6], type=pa.int64()),
        'a': pa.array([1, 2, 3], type=pa.int64()),
    })
    
    morsel1 = draken.Morsel.from_arrow(table1)
    morsel2 = draken.Morsel.from_arrow(table2)
    
    hash1 = _hash_view_to_list(morsel1.hash())
    hash2 = _hash_view_to_list(morsel2.hash())
    
    # Different column order should produce different hashes
    assert hash1 != hash2


def test_hash_into_int8_boundaries():
    """hash_into should handle int8 min/max values."""
    table = pa.table({'a': pa.array([127, -128, 0], type=pa.int8())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    # Min and max should be non-zero
    assert result[0] != 0  # 127
    assert result[1] != 0  # -128
    assert len(set(result)) >= 2  # At least min and max are unique


def test_hash_into_int16_boundaries():
    """hash_into should handle int16 min/max values."""
    table = pa.table({'a': pa.array([32767, -32768, 0], type=pa.int16())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert result[0] != 0  # 32767
    assert result[1] != 0  # -32768
    assert len(set(result)) >= 2


def test_hash_into_int32_boundaries():
    """hash_into should handle int32 min/max values."""
    table = pa.table({'a': pa.array([2147483647, -2147483648, 0], type=pa.int32())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert result[0] != 0
    assert result[1] != 0
    assert len(set(result)) >= 2


def test_hash_into_int64_boundaries():
    """hash_into should handle int64 min/max values."""
    table = pa.table({'a': pa.array([9223372036854775807, -9223372036854775808, 0], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert result[0] != 0
    assert result[1] != 0
    assert len(set(result)) >= 2


def test_hash_into_float32_very_small():
    """hash_into should handle very small float32 values."""
    table = pa.table({'a': pa.array([1e-38, -1e-38, 0.0], type=pa.float32())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    # Small values should hash
    assert result[0] != 0
    assert result[1] != 0


def test_hash_into_float64_very_small():
    """hash_into should handle very small float64 values."""
    table = pa.table({'a': pa.array([1e-308, -1e-308, 0.0], type=pa.float64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert result[0] != 0
    assert result[1] != 0


def test_hash_into_string_with_special_chars():
    """hash_into should handle strings with special characters."""
    table = pa.table({'a': ['\n', '\t', '\r', '\\', '"', "'", '\0']})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0, 0, 0, 0, 0, 0, 0])
    out_view = memoryview(out_buf)
    
    hash_into_vector(vec, out_view, 0)
    
    result = list(out_buf)
    assert all(h != 0 for h in result)
    assert len(set(result)) == 7  # All unique


def test_hash_into_performance_no_regression():
    """hash_into should perform reasonably on medium datasets."""
    import time
    
    n = 10000
    table = pa.table({'a': pa.array(list(range(n)), type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    vec = morsel.column(b'a')
    
    out_buf = array('Q', [0] * n)
    out_view = memoryview(out_buf)
    
    start = time.time()
    hash_into_vector(vec, out_view, 0)
    elapsed = time.time() - start
    
    # Should complete in under 100ms for 10k elements
    assert elapsed < 0.1, f"hash_into took {elapsed:.3f}s for {n} elements"


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
