"""Tests for vector-vector comparison operations.

This module tests the vector-vector comparison operations for Int64Vector and Float64Vector,
including equals, not_equals, greater_than, greater_than_or_equals, less_than, and 
less_than_or_equals operations.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pyarrow as pa
import pytest

from opteryx.draken import Vector


class TestInt64VectorComparisons:
    """Test Int64Vector vector-vector and vector-scalar comparison operations."""
    
    def test_equals_vector(self):
        """Test Int64Vector equals_vector operation."""
        arr1 = pa.array([1, 2, 3, 4, 5], type=pa.int64())
        arr2 = pa.array([1, 3, 3, 2, 6], type=pa.int64())
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        result = vec1.equals_vector(vec2)
        expected = [1, 0, 1, 0, 0]
        
        assert list(result) == expected
    
    def test_not_equals_vector(self):
        """Test Int64Vector not_equals_vector operation."""
        arr1 = pa.array([1, 2, 3, 4, 5], type=pa.int64())
        arr2 = pa.array([1, 3, 3, 2, 6], type=pa.int64())
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        result = vec1.not_equals_vector(vec2)
        expected = [0, 1, 0, 1, 1]
        
        assert list(result) == expected
    
    def test_greater_than_vector(self):
        """Test Int64Vector greater_than_vector operation."""
        arr1 = pa.array([1, 2, 3, 4, 5], type=pa.int64())
        arr2 = pa.array([1, 3, 3, 2, 6], type=pa.int64())
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        result = vec1.greater_than_vector(vec2)
        expected = [0, 0, 0, 1, 0]
        
        assert list(result) == expected
    
    def test_greater_than_or_equals_vector(self):
        """Test Int64Vector greater_than_or_equals_vector operation."""
        arr1 = pa.array([1, 2, 3, 4, 5], type=pa.int64())
        arr2 = pa.array([1, 3, 3, 2, 6], type=pa.int64())
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        result = vec1.greater_than_or_equals_vector(vec2)
        expected = [1, 0, 1, 1, 0]
        
        assert list(result) == expected
    
    def test_less_than_vector(self):
        """Test Int64Vector less_than_vector operation."""
        arr1 = pa.array([1, 2, 3, 4, 5], type=pa.int64())
        arr2 = pa.array([1, 3, 3, 2, 6], type=pa.int64())
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        result = vec1.less_than_vector(vec2)
        expected = [0, 1, 0, 0, 1]
        
        assert list(result) == expected
    
    def test_less_than_or_equals_vector(self):
        """Test Int64Vector less_than_or_equals_vector operation."""
        arr1 = pa.array([1, 2, 3, 4, 5], type=pa.int64())
        arr2 = pa.array([1, 3, 3, 2, 6], type=pa.int64())
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        result = vec1.less_than_or_equals_vector(vec2)
        expected = [1, 1, 1, 0, 1]
        
        assert list(result) == expected
    
    def test_vector_length_mismatch(self):
        """Test that vector-vector comparisons raise error on length mismatch."""
        arr1 = pa.array([1, 2, 3], type=pa.int64())
        arr2 = pa.array([1, 2], type=pa.int64())
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        with pytest.raises(ValueError, match="Vectors must have the same length"):
            vec1.equals_vector(vec2)
    
    def test_scalar_comparisons_still_work(self):
        """Test that scalar comparisons still work after adding vector comparisons."""
        arr = pa.array([1, 2, 3, 4, 5], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert list(vec.equals(3)) == [0, 0, 1, 0, 0]
        assert list(vec.not_equals(3)) == [1, 1, 0, 1, 1]
        assert list(vec.greater_than(3)) == [0, 0, 0, 1, 1]
        assert list(vec.less_than(3)) == [1, 1, 0, 0, 0]


class TestFloat64VectorComparisons:
    """Test Float64Vector vector-vector and vector-scalar comparison operations."""
    
    def test_equals_vector(self):
        """Test Float64Vector equals_vector operation."""
        arr1 = pa.array([1.5, 2.7, 3.3, 4.1, 5.9], type=pa.float64())
        arr2 = pa.array([1.5, 3.0, 3.3, 2.0, 6.0], type=pa.float64())
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        result = vec1.equals_vector(vec2)
        expected = [1, 0, 1, 0, 0]
        
        assert list(result) == expected
    
    def test_not_equals_vector(self):
        """Test Float64Vector not_equals_vector operation."""
        arr1 = pa.array([1.5, 2.7, 3.3, 4.1, 5.9], type=pa.float64())
        arr2 = pa.array([1.5, 3.0, 3.3, 2.0, 6.0], type=pa.float64())
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        result = vec1.not_equals_vector(vec2)
        expected = [0, 1, 0, 1, 1]
        
        assert list(result) == expected
    
    def test_greater_than_vector(self):
        """Test Float64Vector greater_than_vector operation."""
        arr1 = pa.array([1.5, 2.7, 3.3, 4.1, 5.9], type=pa.float64())
        arr2 = pa.array([1.5, 3.0, 3.3, 2.0, 6.0], type=pa.float64())
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        result = vec1.greater_than_vector(vec2)
        expected = [0, 0, 0, 1, 0]
        
        assert list(result) == expected
    
    def test_greater_than_or_equals_vector(self):
        """Test Float64Vector greater_than_or_equals_vector operation."""
        arr1 = pa.array([1.5, 2.7, 3.3, 4.1, 5.9], type=pa.float64())
        arr2 = pa.array([1.5, 3.0, 3.3, 2.0, 6.0], type=pa.float64())
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        result = vec1.greater_than_or_equals_vector(vec2)
        expected = [1, 0, 1, 1, 0]
        
        assert list(result) == expected
    
    def test_less_than_vector(self):
        """Test Float64Vector less_than_vector operation."""
        arr1 = pa.array([1.5, 2.7, 3.3, 4.1, 5.9], type=pa.float64())
        arr2 = pa.array([1.5, 3.0, 3.3, 2.0, 6.0], type=pa.float64())
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        result = vec1.less_than_vector(vec2)
        expected = [0, 1, 0, 0, 1]
        
        assert list(result) == expected
    
    def test_less_than_or_equals_vector(self):
        """Test Float64Vector less_than_or_equals_vector operation."""
        arr1 = pa.array([1.5, 2.7, 3.3, 4.1, 5.9], type=pa.float64())
        arr2 = pa.array([1.5, 3.0, 3.3, 2.0, 6.0], type=pa.float64())
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        result = vec1.less_than_or_equals_vector(vec2)
        expected = [1, 1, 1, 0, 1]
        
        assert list(result) == expected
    
    def test_vector_length_mismatch(self):
        """Test that vector-vector comparisons raise error on length mismatch."""
        arr1 = pa.array([1.5, 2.7, 3.3], type=pa.float64())
        arr2 = pa.array([1.5, 2.7], type=pa.float64())
        
        vec1 = Vector.from_arrow(arr1)
        vec2 = Vector.from_arrow(arr2)
        
        with pytest.raises(ValueError, match="Vectors must have the same length"):
            vec1.equals_vector(vec2)
    
    def test_scalar_comparisons_still_work(self):
        """Test that scalar comparisons still work after adding vector comparisons."""
        arr = pa.array([1.5, 2.7, 3.3, 4.1, 5.9], type=pa.float64())
        vec = Vector.from_arrow(arr)
        
        assert list(vec.equals(3.3)) == [0, 0, 1, 0, 0]
        assert list(vec.not_equals(3.3)) == [1, 1, 0, 1, 1]
        assert list(vec.greater_than(3.3)) == [0, 0, 0, 1, 1]
        assert list(vec.less_than(3.3)) == [1, 1, 0, 0, 0]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests
    
    run_tests()
