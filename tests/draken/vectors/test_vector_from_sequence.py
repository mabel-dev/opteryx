#!/usr/bin/env python
"""Comprehensive tests for vector_from_sequence and Vector.from_sequence methods."""

import sys
from pathlib import Path
from array import array

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pytest
import pyarrow as pa

from opteryx.draken.interop.arrow import vector_from_sequence
from opteryx.draken.vectors.int64_vector import Int64Vector
from opteryx.draken.vectors.float64_vector import Float64Vector
from opteryx.draken.vectors.bool_vector import BoolVector


class TestVectorFromSequenceInt64:
    """Tests for vector_from_sequence with int64 data."""
    
    def test_int64_memoryview_basic(self):
        """Test creating Int64Vector from int64 memoryview."""
        arr = array('q', [1, 2, 3, 4, 5])  # 'q' = signed long long (int64)
        vec = vector_from_sequence(arr)
        
        assert isinstance(vec, Int64Vector)
        arrow_result = vec.to_arrow()
        assert arrow_result.to_pylist() == [1, 2, 3, 4, 5]
    
    def test_int64_memoryview_empty(self):
        """Test creating Int64Vector from empty array."""
        arr = array('q', [])
        vec = vector_from_sequence(arr)
        
        assert isinstance(vec, Int64Vector)
        arrow_result = vec.to_arrow()
        assert len(arrow_result) == 0
    
    def test_int64_memoryview_single(self):
        """Test creating Int64Vector from single element."""
        arr = array('q', [42])
        vec = vector_from_sequence(arr)
        
        assert isinstance(vec, Int64Vector)
        arrow_result = vec.to_arrow()
        assert arrow_result.to_pylist() == [42]
    
    def test_int64_memoryview_negative(self):
        """Test creating Int64Vector with negative values."""
        arr = array('q', [-100, -50, 0, 50, 100])
        vec = vector_from_sequence(arr)
        
        assert isinstance(vec, Int64Vector)
        arrow_result = vec.to_arrow()
        assert arrow_result.to_pylist() == [-100, -50, 0, 50, 100]
    
    def test_int64_memoryview_large(self):
        """Test creating Int64Vector from large array."""
        n = 100000
        arr = array('q', range(n))
        vec = vector_from_sequence(arr)
        
        assert isinstance(vec, Int64Vector)
        arrow_result = vec.to_arrow()
        assert len(arrow_result) == n
        assert arrow_result[0].as_py() == 0
        assert arrow_result[n-1].as_py() == n - 1
    
    def test_int64_memoryview_extremes(self):
        """Test Int64Vector with min/max int64 values."""
        # int64 range: -2^63 to 2^63-1
        min_val = -9223372036854775808
        max_val = 9223372036854775807
        arr = array('q', [min_val, 0, max_val])
        vec = vector_from_sequence(arr)
        
        arrow_result = vec.to_arrow()
        result = arrow_result.to_pylist()
        assert result[0] == min_val
        assert result[1] == 0
        assert result[2] == max_val
    
    def test_int64_zero_copy(self):
        """Test that int64 memoryview wrapping is zero-copy."""
        arr = array('q', [1, 2, 3])
        vec = vector_from_sequence(arr)
        
        # Vector should reference the same memory
        # (We can't easily test this directly, but we verify no crash/corruption)
        arrow_result = vec.to_arrow()
        assert arrow_result.to_pylist() == [1, 2, 3]
    
    def test_int64_memoryview_directly(self):
        """Test creating Int64Vector from explicit memoryview."""
        arr = array('q', [10, 20, 30])
        mv = memoryview(arr)
        vec = vector_from_sequence(mv)
        
        assert isinstance(vec, Int64Vector)
        arrow_result = vec.to_arrow()
        assert arrow_result.to_pylist() == [10, 20, 30]


class TestVectorFromSequenceFloat64:
    """Tests for vector_from_sequence with float64 data."""
    
    def test_float64_memoryview_basic(self):
        """Test creating Float64Vector from float64 memoryview."""
        arr = array('d', [1.1, 2.2, 3.3, 4.4, 5.5])  # 'd' = double (float64)
        vec = vector_from_sequence(arr)
        
        assert isinstance(vec, Float64Vector)
        arrow_result = vec.to_arrow()
        result = arrow_result.to_pylist()
        assert all(abs(a - b) < 1e-10 for a, b in zip(result, [1.1, 2.2, 3.3, 4.4, 5.5]))
    
    def test_float64_memoryview_empty(self):
        """Test creating Float64Vector from empty array."""
        arr = array('d', [])
        vec = vector_from_sequence(arr)
        
        assert isinstance(vec, Float64Vector)
        arrow_result = vec.to_arrow()
        assert len(arrow_result) == 0
    
    def test_float64_memoryview_single(self):
        """Test creating Float64Vector from single element."""
        arr = array('d', [3.14159])
        vec = vector_from_sequence(arr)
        
        assert isinstance(vec, Float64Vector)
        arrow_result = vec.to_arrow()
        assert abs(arrow_result[0].as_py() - 3.14159) < 1e-10
    
    def test_float64_memoryview_negative(self):
        """Test creating Float64Vector with negative values."""
        arr = array('d', [-100.5, -50.25, 0.0, 50.75, 100.125])
        vec = vector_from_sequence(arr)
        
        arrow_result = vec.to_arrow()
        result = arrow_result.to_pylist()
        expected = [-100.5, -50.25, 0.0, 50.75, 100.125]
        assert all(abs(a - b) < 1e-10 for a, b in zip(result, expected))
    
    def test_float64_memoryview_special_values(self):
        """Test Float64Vector with inf, -inf, and nan."""
        arr = array('d', [float('inf'), float('-inf'), float('nan'), 1.0])
        vec = vector_from_sequence(arr)
        
        arrow_result = vec.to_arrow()
        result = arrow_result.to_pylist()
        # Check inf
        assert result[0] > 1e308  # Effectively infinity
        assert result[1] < -1e308  # Effectively -infinity
        # Check nan
        assert result[2] != result[2]  # NaN != NaN
        assert result[3] == 1.0
    
    def test_float64_memoryview_large(self):
        """Test creating Float64Vector from large array."""
        n = 50000
        # Create array with linearly spaced values
        arr = array('d', [i * 1000.0 / n for i in range(n)])
        vec = vector_from_sequence(arr)
        
        assert isinstance(vec, Float64Vector)
        arrow_result = vec.to_arrow()
        assert len(arrow_result) == n
    
    def test_float64_memoryview_scientific(self):
        """Test Float64Vector with scientific notation values."""
        arr = array('d', [1e-10, 1e10, 1.23e-5, 9.87e15])
        vec = vector_from_sequence(arr)
        
        arrow_result = vec.to_arrow()
        result = arrow_result.to_pylist()
        expected = [1e-10, 1e10, 1.23e-5, 9.87e15]
        assert all(abs(a - b) < abs(a) * 1e-10 if a != 0 else abs(b) < 1e-10 for a, b in zip(result, expected))
    
    def test_float64_memoryview_directly(self):
        """Test creating Float64Vector from explicit memoryview."""
        arr = array('d', [1.5, 2.5, 3.5])
        mv = memoryview(arr)
        vec = vector_from_sequence(mv)
        
        assert isinstance(vec, Float64Vector)
        arrow_result = vec.to_arrow()
        result = arrow_result.to_pylist()
        assert all(abs(a - b) < 1e-10 for a, b in zip(result, [1.5, 2.5, 3.5]))


class TestVectorFromSequenceBool:
    """Tests for vector_from_sequence with boolean data."""
    
    def test_bool_memoryview_basic(self):
        """Test creating BoolVector from uint8 memoryview (bit-packed)."""
        # 1 byte = 8 bools: 0b10110010
        arr = array('B', [0b10110010])  # 'B' = unsigned char (uint8)
        vec = vector_from_sequence(arr)
        
        assert isinstance(vec, BoolVector)
        arrow_result = vec.to_arrow()
        # Note: bit packing is LSB first
        assert len(arrow_result) == 8
    
    def test_bool_memoryview_multiple_bytes(self):
        """Test BoolVector with multiple bytes."""
        # 2 bytes = 16 bools
        arr = array('B', [0b11111111, 0b00000000])
        vec = vector_from_sequence(arr)
        
        assert isinstance(vec, BoolVector)
        arrow_result = vec.to_arrow()
        assert len(arrow_result) == 16
    
    def test_bool_memoryview_empty(self):
        """Test creating BoolVector from empty array."""
        arr = array('B', [])
        vec = vector_from_sequence(arr)
        
        assert isinstance(vec, BoolVector)
        arrow_result = vec.to_arrow()
        assert len(arrow_result) == 0
    
    def test_bool_memoryview_single_byte(self):
        """Test BoolVector with single byte."""
        arr = array('B', [0b01010101])
        vec = vector_from_sequence(arr)
        
        arrow_result = vec.to_arrow()
        assert len(arrow_result) == 8
    
    def test_bool_memoryview_large(self):
        """Test BoolVector with large array."""
        n_bytes = 1000
        # Create pattern of alternating bytes
        arr = array('B', [i % 256 for i in range(n_bytes)])
        vec = vector_from_sequence(arr)
        
        assert isinstance(vec, BoolVector)
        arrow_result = vec.to_arrow()
        assert len(arrow_result) == n_bytes * 8
    
    def test_bool_memoryview_directly(self):
        """Test creating BoolVector from explicit memoryview."""
        arr = array('B', [0xFF, 0x00])
        mv = memoryview(arr)
        vec = vector_from_sequence(mv)
        
        assert isinstance(vec, BoolVector)
        arrow_result = vec.to_arrow()
        assert len(arrow_result) == 16


class TestVectorFromSequenceFallback:
    """Tests for vector_from_sequence fallback to Arrow."""
    
    def test_string_fallback(self):
        """Test that string data falls back to Arrow conversion."""
        from opteryx.draken.vectors.string_vector import StringVector
        
        arr = ['hello', 'world', 'test']
        vec = vector_from_sequence(arr)
        
        assert isinstance(vec, StringVector)
        arrow_result = vec.to_arrow()
        result = [v.decode('utf-8') if isinstance(v, bytes) else v for v in arrow_result.to_pylist()]
        assert result == ['hello', 'world', 'test']
    
    def test_bytes_fallback(self):
        """Test that bytes data falls back to Arrow conversion."""
        arr = [b'aaa', b'bbb', b'ccc']
        vec = vector_from_sequence(arr)
        
        arrow_result = vec.to_arrow()
        assert len(arrow_result) == 3
    
    def test_nested_list_fallback(self):
        """Test that nested lists fall back to Arrow."""
        arr = [[1, 2], [3, 4], [5, 6]]
        vec = vector_from_sequence(arr)
        
        arrow_result = vec.to_arrow()
        assert len(arrow_result) == 3
    
    def test_dict_fallback(self):
        """Test that dict data falls back to Arrow struct."""
        arr = [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]
        vec = vector_from_sequence(arr)
        
        arrow_result = vec.to_arrow()
        assert len(arrow_result) == 2
    
    def test_none_values_fallback(self):
        """Test that None values in list fall back to Arrow."""
        arr = [1, None, 3, None, 5]
        vec = vector_from_sequence(arr)
        
        arrow_result = vec.to_arrow()
        result = arrow_result.to_pylist()
        assert result == [1, None, 3, None, 5]
    
    def test_array_int32_fallback(self):
        """Test that int32 arrays fall back to Arrow."""
        arr = array('i', [1, 2, 3])  # 'i' = signed int (int32)
        vec = vector_from_sequence(arr)
        
        # Should create a vector via Arrow (not Int64Vector)
        arrow_result = vec.to_arrow()
        assert arrow_result.to_pylist() == [1, 2, 3]
    
    def test_array_float32_fallback(self):
        """Test that float32 arrays fall back to Arrow."""
        arr = array('f', [1.0, 2.0, 3.0])  # 'f' = float (float32)
        vec = vector_from_sequence(arr)
        
        arrow_result = vec.to_arrow()
        assert len(arrow_result) == 3


class TestVectorFromSequenceWithDtype:
    """Tests for vector_from_sequence with explicit dtype parameter."""
    
    def test_dtype_int64(self):
        """Test explicit int64 dtype."""
        arr = array('q', [1, 2, 3])
        vec = vector_from_sequence(arr, dtype=pa.int64())
        
        assert isinstance(vec, Int64Vector)
    
    def test_dtype_float64(self):
        """Test explicit float64 dtype."""
        arr = array('d', [1.0, 2.0, 3.0])
        vec = vector_from_sequence(arr, dtype=pa.float64())
        
        assert isinstance(vec, Float64Vector)
    
    def test_dtype_passed_to_arrow(self):
        """Test that dtype parameter is passed to pa.array()."""
        # The dtype parameter is passed to pa.array() for type specification
        arr = [1.0, 2.0, 3.0]
        vec = vector_from_sequence(arr, dtype=pa.float64())
        
        arrow_result = vec.to_arrow()
        assert arrow_result.type == pa.float64()


class TestVectorFromSequenceEdgeCases:
    """Edge case tests for vector_from_sequence."""
    
    def test_pyarrow_array_input(self):
        """Test that PyArrow arrays work as input."""
        arr = pa.array([1, 2, 3, 4], type=pa.int64())
        vec = vector_from_sequence(arr)
        
        arrow_result = vec.to_arrow()
        assert arrow_result.to_pylist() == [1, 2, 3, 4]
    
    def test_python_list_int(self):
        """Test Python list of ints."""
        arr = [10, 20, 30, 40]
        vec = vector_from_sequence(arr)
        
        arrow_result = vec.to_arrow()
        assert arrow_result.to_pylist() == [10, 20, 30, 40]
    
    def test_python_list_float(self):
        """Test Python list of floats."""
        arr = [1.5, 2.5, 3.5]
        vec = vector_from_sequence(arr)
        
        arrow_result = vec.to_arrow()
        result = arrow_result.to_pylist()
        assert all(abs(a - b) < 1e-10 for a, b in zip(result, [1.5, 2.5, 3.5]))
    
    def test_array_unsigned_types(self):
        """Test that unsigned integer arrays work."""
        # 'H' = unsigned short (uint16)
        arr = array('H', [1, 2, 3, 100, 65000])
        vec = vector_from_sequence(arr)
        
        arrow_result = vec.to_arrow()
        assert arrow_result.to_pylist() == [1, 2, 3, 100, 65000]
    
    def test_array_signed_types(self):
        """Test that other signed integer arrays work."""
        # 'h' = signed short (int16)
        arr = array('h', [-100, -50, 0, 50, 100])
        vec = vector_from_sequence(arr)
        
        arrow_result = vec.to_arrow()
        assert arrow_result.to_pylist() == [-100, -50, 0, 50, 100]


class TestVectorFromSequenceMemoryManagement:
    """Tests for memory management in vector_from_sequence."""
    
    def test_keeps_reference_to_memoryview_base(self):
        """Test that vector keeps reference to prevent GC."""
        arr = array('q', [1, 2, 3, 4, 5])
        vec = vector_from_sequence(arr)
        
        # Delete original array
        del arr
        
        # Vector should still be valid
        arrow_result = vec.to_arrow()
        assert arrow_result.to_pylist() == [1, 2, 3, 4, 5]
    
    def test_multiple_vectors_same_source(self):
        """Test creating multiple vectors from same source."""
        arr = array('q', [1, 2, 3])
        vec1 = vector_from_sequence(arr)
        vec2 = vector_from_sequence(arr)
        
        # Both should be valid
        assert vec1.to_arrow().to_pylist() == [1, 2, 3]
        assert vec2.to_arrow().to_pylist() == [1, 2, 3]
    
    def test_large_memoryview_no_leak(self):
        """Test that large memoryviews don't leak."""
        n = 1000000
        arr = array('q', range(n))
        vec = vector_from_sequence(arr)
        
        # Should be able to convert to Arrow without crash
        arrow_result = vec.to_arrow()
        assert len(arrow_result) == n
        
        # Cleanup
        del arr
        del vec


class TestVectorFromSequenceIntegration:
    """Integration tests with Morsel and other components."""
    
    def test_with_morsel_creation(self):
        """Test using vector_from_sequence with Morsel.from_arrow."""
        from opteryx.draken.morsels.morsel import Morsel
        
        int_arr = array('q', [1, 2, 3, 4, 5])
        float_arr = array('d', [1.1, 2.2, 3.3, 4.4, 5.5])
        
        int_vec = vector_from_sequence(int_arr)
        float_vec = vector_from_sequence(float_arr)
        
        table = pa.table({
            'integers': int_vec.to_arrow(),
            'floats': float_vec.to_arrow()
        })
        
        morsel = Morsel.from_arrow(table)
        assert morsel.shape == (5, 2)
    
    def test_roundtrip_array_to_vector_to_arrow_to_array(self):
        """Test full roundtrip conversion."""
        original = array('q', [10, 20, 30, 40, 50])
        
        vec = vector_from_sequence(original)
        arrow_arr = vec.to_arrow()
        result_list = arrow_arr.to_pylist()
        result = array('q', result_list)
        
        assert list(original) == list(result)
    
    def test_multiple_dtypes_in_same_call_sequence(self):
        """Test creating vectors of different types in sequence."""
        int_vec = vector_from_sequence(array('q', [1, 2, 3]))
        float_vec = vector_from_sequence(array('d', [1.5, 2.5]))
        bool_vec = vector_from_sequence(array('B', [0xFF]))
        str_vec = vector_from_sequence(['a', 'b'])
        
        assert isinstance(int_vec, Int64Vector)
        assert isinstance(float_vec, Float64Vector)
        assert isinstance(bool_vec, BoolVector)
        # str_vec will be StringVector via fallback


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
