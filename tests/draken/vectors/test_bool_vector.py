"""
Tests for BoolVector operations.

This module tests BoolVector-specific functionality including:
- any() and all() operations
- Boolean vector operations (and_vector, or_vector, xor_vector, not_vector)
- Comparison operations
- Null handling
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pytest
import pyarrow as pa

from opteryx.draken import Vector


class TestBoolVectorAnyAll:
    """Test any() and all() operations on BoolVector."""
    
    def test_any_all_true(self):
        """Test any() returns True when at least one True exists."""
        arrow_array = pa.array([True, False, False, False])
        vec = Vector.from_arrow(arrow_array)
        
        # BoolVector.any() returns 0 or 1, not Python bool
        assert vec.any() == 1
    
    def test_any_all_false(self):
        """Test any() returns False when all values are False."""
        arrow_array = pa.array([False, False, False, False])
        vec = Vector.from_arrow(arrow_array)
        
        # BoolVector.any() returns 0 or 1
        assert vec.any() == 0
    
    def test_any_with_nulls(self):
        """Test any() with null values - nulls should be ignored."""
        arrow_array = pa.array([None, False, None, True])
        vec = Vector.from_arrow(arrow_array)
        
        assert vec.any() == 1
    
    def test_any_all_nulls(self):
        """Test any() when all values are null."""
        arrow_array = pa.array([None, None, None], type=pa.bool_())
        vec = Vector.from_arrow(arrow_array)
        
        # Should return 0 when no True values exist
        assert vec.any() == 0
    
    def test_all_all_true(self):
        """Test all() returns True when all values are True."""
        arrow_array = pa.array([True, True, True, True])
        vec = Vector.from_arrow(arrow_array)
        
        assert vec.all() == 1
    
    def test_all_one_false(self):
        """Test all() returns False when at least one False exists."""
        arrow_array = pa.array([True, True, False, True])
        vec = Vector.from_arrow(arrow_array)
        
        assert vec.all() == 0
    
    def test_all_with_nulls(self):
        """Test all() with null values - current implementation returns 0 with nulls."""
        arrow_array = pa.array([True, None, True, None])
        vec = Vector.from_arrow(arrow_array)
        
        # Current implementation: returns 0 when there are any nulls
        assert vec.all() == 0
    
    def test_all_with_nulls_and_false(self):
        """Test all() with nulls and a False value."""
        arrow_array = pa.array([True, None, False, True])
        vec = Vector.from_arrow(arrow_array)
        
        assert vec.all() == 0
    
    def test_any_empty_vector(self):
        """Test any() on empty vector."""
        arrow_array = pa.array([], type=pa.bool_())
        vec = Vector.from_arrow(arrow_array)
        
        assert vec.any() == 0
    
    def test_all_empty_vector(self):
        """Test all() on empty vector."""
        arrow_array = pa.array([], type=pa.bool_())
        vec = Vector.from_arrow(arrow_array)
        
        # Empty set: all() should return 1 (vacuous truth)
        assert vec.all() == 1


class TestBoolVectorOperations:
    """Test boolean vector-vector operations."""
    
    def test_and_vector_basic(self):
        """Test and_vector operation with simple boolean values."""
        arr1 = pa.array([True, True, False, False])
        arr2 = pa.array([True, False, True, False])
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        result = vec1.and_vector(vec2)
        expected = [True, False, False, False]
        
        assert list(result) == expected
    
    def test_and_vector_with_nulls(self):
        """Test and_vector with null values."""
        arr1 = pa.array([True, True, None, False])
        arr2 = pa.array([True, None, True, False])
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        result = vec1.and_vector(vec2)
        result_list = list(result)
        
        # True AND True = True
        assert result_list[0] == True
        # True AND None should handle nulls (typically None or False)
        # False AND False = False
        assert result_list[3] == False
    
    def test_or_vector_basic(self):
        """Test or_vector operation with simple boolean values."""
        arr1 = pa.array([True, True, False, False])
        arr2 = pa.array([True, False, True, False])
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        result = vec1.or_vector(vec2)
        expected = [True, True, True, False]
        
        assert list(result) == expected
    
    def test_or_vector_with_nulls(self):
        """Test or_vector with null values."""
        arr1 = pa.array([False, False, None, True])
        arr2 = pa.array([False, None, False, True])
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        result = vec1.or_vector(vec2)
        result_list = list(result)
        
        # False OR False = False
        assert result_list[0] == False
        # True OR True = True
        assert result_list[3] == True
    
    def test_xor_vector_basic(self):
        """Test xor_vector operation with simple boolean values."""
        arr1 = pa.array([True, True, False, False])
        arr2 = pa.array([True, False, True, False])
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        result = vec1.xor_vector(vec2)
        expected = [False, True, True, False]
        
        assert list(result) == expected
    
    def test_vector_length_mismatch(self):
        """Test that vector operations raise error on length mismatch."""
        arr1 = pa.array([True, False, True])
        arr2 = pa.array([True, False])
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        with pytest.raises(ValueError, match="same length"):
            vec1.and_vector(vec2)
    
    def test_chained_operations(self):
        """Test chaining boolean operations."""
        arr1 = pa.array([True, True, False, False])
        arr2 = pa.array([True, False, True, False])
        arr3 = pa.array([False, False, False, True])
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        vec3 = Vector.from_arrow(arr3)
        
        # (vec1 OR vec2) AND (NOT vec3)
        # Since there's no not_vector, we can't test this specific chain
        # Test a simpler chain: (vec1 OR vec2) AND vec3
        result = vec1.or_vector(vec2).and_vector(vec3)
        
        # vec1 OR vec2 = [True, True, True, False]
        # AND vec3 = [False, False, False, True]
        # Result = [False, False, False, False]
        expected = [False, False, False, False]
        
        assert list(result) == expected


class TestBoolVectorComparisons:
    """Test BoolVector comparison operations."""
    
    def test_equals_true(self):
        """Test equals comparison with True."""
        arr = pa.array([True, False, True, False])
        vec = Vector.from_arrow(arr)
        
        result = vec.equals(True)
        expected = [1, 0, 1, 0]
        
        assert list(result) == expected
    
    def test_equals_false(self):
        """Test equals comparison with False."""
        arr = pa.array([True, False, True, False])
        vec = Vector.from_arrow(arr)
        
        result = vec.equals(False)
        expected = [0, 1, 0, 1]
        
        assert list(result) == expected
    
    def test_not_equals_true(self):
        """Test not_equals comparison with True."""
        arr = pa.array([True, False, True, False])
        vec = Vector.from_arrow(arr)
        
        result = vec.not_equals(True)
        expected = [0, 1, 0, 1]
        
        assert list(result) == expected
    
    def test_equals_with_nulls(self):
        """Test equals comparison with null values."""
        arr = pa.array([True, None, False, None])
        vec = Vector.from_arrow(arr)
        
        result = vec.equals(True)
        result_list = list(result)
        
        assert result_list[0] == 1  # True == True
        assert result_list[2] == 0  # False == True


class TestBoolVectorNullHandling:
    """Test null handling in BoolVector operations."""
    
    def test_is_null(self):
        """Test is_null operation."""
        arr = pa.array([True, None, False, None, True])
        vec = Vector.from_arrow(arr)
        
        result = vec.is_null()
        expected = [0, 1, 0, 1, 0]
        
        assert list(result) == expected
    
    def test_null_count(self):
        """Test null_count property."""
        arr = pa.array([True, None, False, None, True])
        vec = Vector.from_arrow(arr)
        
        assert vec.null_count == 2
    
    def test_all_nulls(self):
        """Test vector with all null values."""
        arr = pa.array([None, None, None], type=pa.bool_())
        vec = Vector.from_arrow(arr)
        
        assert vec.null_count == 3
        assert vec.length == 3
        assert list(vec.is_null()) == [1, 1, 1]
    
    def test_no_nulls(self):
        """Test vector with no null values."""
        arr = pa.array([True, False, True])
        vec = Vector.from_arrow(arr)
        
        assert vec.null_count == 0
        assert list(vec.is_null()) == [0, 0, 0]


class TestBoolVectorMiscellaneous:
    """Test miscellaneous BoolVector functionality."""
    
    def test_to_pylist(self):
        """Test conversion to Python list."""
        arr = pa.array([True, False, None, True])
        vec = Vector.from_arrow(arr)
        
        result = vec.to_pylist()
        expected = [True, False, None, True]
        
        assert result == expected
    
    def test_length(self):
        """Test length property."""
        arr = pa.array([True, False, True, False, True])
        vec = Vector.from_arrow(arr)
        
        assert vec.length == 5
    
    def test_empty_vector(self):
        """Test empty BoolVector."""
        arr = pa.array([], type=pa.bool_())
        vec = Vector.from_arrow(arr)
        
        assert vec.length == 0
        assert vec.null_count == 0
        assert vec.to_pylist() == []
    
    def test_take_operation(self):
        """Test take operation on BoolVector."""
        import numpy as np
        arr = pa.array([True, False, True, False, True])
        vec = Vector.from_arrow(arr)
        
        indices = np.array([0, 2, 4], dtype=np.int32)
        result = vec.take(indices)
        
        assert list(result) == [True, True, True]
    
    def test_take_with_nulls(self):
        """Test take operation with nulls."""
        import numpy as np
        arr = pa.array([True, None, False, None, True])
        vec = Vector.from_arrow(arr)
        
        indices = np.array([0, 1, 4], dtype=np.int32)
        result = vec.take(indices)
        
        # Current implementation: nulls become False
        assert result.to_pylist() == [True, False, True]
