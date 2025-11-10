"""
Tests for error conditions and edge cases across vector operations.

This module tests:
- Type mismatches
- Invalid indices
- Empty vectors
- Boundary conditions
- Invalid operations
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pytest
import pyarrow as pa

from opteryx.draken import Vector


class TestVectorLengthMismatch:
    """Test error handling for vector length mismatches."""
    
    def test_int64_vector_comparison_length_mismatch(self):
        """Test vector-vector comparison with different lengths."""
        arr1 = pa.array([1, 2, 3], type=pa.int64())
        arr2 = pa.array([1, 2], type=pa.int64())
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        with pytest.raises(ValueError, match="same length"):
            vec1.equals_vector(vec2)
    
    def test_float64_vector_comparison_length_mismatch(self):
        """Test float vector-vector comparison with different lengths."""
        arr1 = pa.array([1.0, 2.0, 3.0], type=pa.float64())
        arr2 = pa.array([1.0, 2.0], type=pa.float64())
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        with pytest.raises(ValueError, match="same length"):
            vec1.equals_vector(vec2)
    
    def test_bool_vector_and_length_mismatch(self):
        """Test boolean AND operation with different lengths."""
        arr1 = pa.array([True, False, True], type=pa.bool_())
        arr2 = pa.array([True, False], type=pa.bool_())
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        with pytest.raises(ValueError, match="same length"):
            vec1.and_vector(vec2)
    
    def test_bool_vector_or_length_mismatch(self):
        """Test boolean OR operation with different lengths."""
        arr1 = pa.array([True, False], type=pa.bool_())
        arr2 = pa.array([True, False, True], type=pa.bool_())
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        with pytest.raises(ValueError, match="same length"):
            vec1.or_vector(vec2)
    
    def test_bool_vector_xor_length_mismatch(self):
        """Test boolean XOR operation with different lengths."""
        arr1 = pa.array([True], type=pa.bool_())
        arr2 = pa.array([True, False], type=pa.bool_())
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        with pytest.raises(ValueError, match="same length"):
            vec1.xor_vector(vec2)


class TestEmptyVectorOperations:
    """Test operations on empty vectors."""
    
    def test_empty_int64_min_raises(self):
        """Test that min on empty Int64Vector raises error."""
        arr = pa.array([], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        with pytest.raises(ValueError, match="empty"):
            vec.min()
    
    def test_empty_int64_max_raises(self):
        """Test that max on empty Int64Vector raises error."""
        arr = pa.array([], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        with pytest.raises(ValueError, match="empty"):
            vec.max()
    
    def test_empty_float64_min_raises(self):
        """Test that min on empty Float64Vector raises error."""
        arr = pa.array([], type=pa.float64())
        vec = Vector.from_arrow(arr)
        
        with pytest.raises(ValueError, match="empty"):
            vec.min()
    
    def test_empty_float64_max_raises(self):
        """Test that max on empty Float64Vector raises error."""
        arr = pa.array([], type=pa.float64())
        vec = Vector.from_arrow(arr)
        
        with pytest.raises(ValueError, match="empty"):
            vec.max()
    
    def test_empty_vector_sum(self):
        """Test sum on empty vector returns 0."""
        arr = pa.array([], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec.sum() == 0
    
    def test_empty_vector_comparisons(self):
        """Test comparisons on empty vector."""
        arr = pa.array([], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        result = vec.equals(5)
        assert list(result) == []
    
    def test_empty_vector_take(self):
        """Test take on empty vector with empty indices."""
        import numpy as np
        arr = pa.array([], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        result = vec.take(np.array([], dtype=np.int32))
        assert result.length == 0
    
    def test_empty_bool_vector_any(self):
        """Test any() on empty BoolVector."""
        arr = pa.array([], type=pa.bool_())
        vec = Vector.from_arrow(arr)
        
        assert vec.any() == 0
    
    def test_empty_bool_vector_all(self):
        """Test all() on empty BoolVector."""
        arr = pa.array([], type=pa.bool_())
        vec = Vector.from_arrow(arr)
        
        # Empty set: all() should be 1 (vacuous truth)
        assert vec.all() == 1


class TestInvalidIndices:
    """Test error handling for invalid indices in take operations."""
    
    def test_take_duplicate_indices(self):
        """Test take with duplicate indices."""
        import numpy as np
        arr = pa.array([1, 2, 3], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        result = vec.take(np.array([0, 0, 1, 1], dtype=np.int32))
        # Should return duplicated values
        assert result.length == 4


class TestTypeSpecificErrors:
    """Test type-specific error conditions."""
    
    def test_string_equals_wrong_type(self):
        """Test string comparison with wrong type."""
        arr = pa.array(['hello', 'world'])
        vec = Vector.from_arrow(arr)
        
        # Should work with bytes
        result = vec.equals(b'hello')
        assert list(result) == [1, 0]
    
    def test_all_nulls_min_max(self):
        """Test min/max on vector with all nulls."""
        arr = pa.array([None, None, None], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        # Current implementation: all nulls returns 0
        assert vec.min() == 0
        assert vec.max() == 0


class TestBoundaryConditions:
    """Test boundary conditions for various operations."""
    
    def test_single_element_operations(self):
        """Test operations on single-element vector."""
        arr = pa.array([42], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec.sum() == 42
        assert vec.min() == 42
        assert vec.max() == 42
        assert list(vec.equals(42)) == [1]
        assert list(vec.is_null()) == [0]
    
    def test_two_element_comparisons(self):
        """Test comparisons on two-element vector."""
        arr = pa.array([1, 2], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert list(vec.less_than(2)) == [1, 0]
        assert list(vec.greater_than(1)) == [0, 1]
    
    def test_vector_vector_empty_comparison(self):
        """Test vector-vector comparison with empty vectors."""
        arr1 = pa.array([], type=pa.int64())
        arr2 = pa.array([], type=pa.int64())
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        # Should not raise, just return empty
        result = vec1.equals_vector(vec2)
        assert result.length == 0
    
    def test_large_vector_operations(self):
        """Test operations on large vectors."""
        # Create a moderately large vector
        size = 10000
        arr = pa.array(list(range(size)), type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec.length == size
        assert vec.sum() == sum(range(size))
        assert vec.min() == 0
        assert vec.max() == size - 1
    
    def test_max_int64_value(self):
        """Test with maximum int64 values."""
        max_val = 2**63 - 1
        arr = pa.array([max_val, max_val - 1, max_val - 2], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec.max() == max_val
        assert vec.min() == max_val - 2
    
    def test_min_int64_value(self):
        """Test with minimum int64 values."""
        min_val = -(2**63)
        arr = pa.array([min_val, min_val + 1, min_val + 2], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec.min() == min_val
        assert vec.max() == min_val + 2


class TestComparisonEdgeCases:
    """Test edge cases in comparison operations."""
    
    def test_equals_all_match(self):
        """Test equals when all values match."""
        arr = pa.array([5, 5, 5, 5], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        result = vec.equals(5)
        assert list(result) == [1, 1, 1, 1]
    
    def test_equals_none_match(self):
        """Test equals when no values match."""
        arr = pa.array([1, 2, 3, 4], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        result = vec.equals(5)
        assert list(result) == [0, 0, 0, 0]
    
    def test_vector_vector_identical(self):
        """Test vector-vector comparison with identical vectors."""
        arr = pa.array([1, 2, 3, 4, 5], type=pa.int64())
        
        vec1 = Vector.from_arrow(arr)
        vec2 = Vector.from_arrow(arr)
        
        result = vec1.equals_vector(vec2)
        assert list(result) == [1, 1, 1, 1, 1]
    
    def test_vector_vector_all_different(self):
        """Test vector-vector comparison with completely different values."""
        arr1 = pa.array([1, 2, 3, 4, 5], type=pa.int64())
        arr2 = pa.array([6, 7, 8, 9, 10], type=pa.int64())
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        result = vec1.equals_vector(vec2)
        assert list(result) == [0, 0, 0, 0, 0]


class TestFloatSpecialValues:
    """Test handling of special float values."""
    
    def test_float_infinity(self):
        """Test operations with infinity values."""
        arr = pa.array([float('inf'), 1.0, -float('inf'), 2.0], type=pa.float64())
        vec = Vector.from_arrow(arr)
        
        assert vec.max() == float('inf')
        assert vec.min() == -float('inf')
    
    def test_float_nan_handling(self):
        """Test operations with NaN values."""
        import math
        arr = pa.array([1.0, float('nan'), 3.0], type=pa.float64())
        vec = Vector.from_arrow(arr)
        
        # NaN behavior in comparisons can vary
        result = vec.to_pylist()
        assert result[0] == 1.0
        assert math.isnan(result[1])
        assert result[2] == 3.0
    
    def test_float_very_small_values(self):
        """Test operations with very small float values."""
        arr = pa.array([1e-100, 1e-200, 1e-300], type=pa.float64())
        vec = Vector.from_arrow(arr)
        
        assert vec.length == 3
        # Just verify operations don't crash
        vec.sum()
        vec.min()
        vec.max()
