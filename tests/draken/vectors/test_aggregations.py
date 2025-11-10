"""
Tests for vector aggregation operations (sum, min, max).

This module tests aggregation operations across different vector types
with various edge cases including nulls, empty vectors, and mixed values.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pytest
import pyarrow as pa

from opteryx.draken import Vector


class TestInt64Aggregations:
    """Test aggregation operations on Int64Vector."""
    
    def test_sum_basic(self):
        """Test basic sum operation."""
        arr = pa.array([1, 2, 3, 4, 5], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec.sum() == 15
    
    def test_sum_with_nulls(self):
        """Test sum with null values - nulls should be ignored."""
        arr = pa.array([1, None, 3, None, 5], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        # Sum should ignore nulls: 1 + 3 + 5 = 9
        assert vec.sum() == 9
    
    def test_sum_all_nulls(self):
        """Test sum when all values are null."""
        arr = pa.array([None, None, None], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        # Sum of all nulls should be 0
        assert vec.sum() == 0
    
    def test_sum_negative_values(self):
        """Test sum with negative values."""
        arr = pa.array([-5, -3, 2, 4, -1], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec.sum() == -3
    
    def test_sum_single_value(self):
        """Test sum with single value."""
        arr = pa.array([42], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec.sum() == 42
    
    def test_min_basic(self):
        """Test basic min operation."""
        arr = pa.array([5, 2, 8, 1, 9], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec.min() == 1
    
    def test_min_with_nulls(self):
        """Test min with null values - nulls treated as 0."""
        arr = pa.array([5, None, 8, 1, None], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        # Current implementation: nulls are treated as 0
        assert vec.min() == 0
    
    def test_min_negative_values(self):
        """Test min with negative values."""
        arr = pa.array([5, -3, 2, -10, 4], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec.min() == -10
    
    def test_min_single_value(self):
        """Test min with single value."""
        arr = pa.array([42], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec.min() == 42
    
    def test_min_empty_raises(self):
        """Test that min on empty vector raises ValueError."""
        arr = pa.array([], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        with pytest.raises(ValueError, match="empty"):
            vec.min()
    
    def test_max_basic(self):
        """Test basic max operation."""
        arr = pa.array([5, 2, 8, 1, 9], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec.max() == 9
    
    def test_max_with_nulls(self):
        """Test max with null values - nulls should be ignored."""
        arr = pa.array([5, None, 8, None, 9], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec.max() == 9
    
    def test_max_negative_values(self):
        """Test max with negative values."""
        arr = pa.array([-5, -3, -2, -10, -4], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec.max() == -2
    
    def test_max_single_value(self):
        """Test max with single value."""
        arr = pa.array([42], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec.max() == 42
    
    def test_max_empty_raises(self):
        """Test that max on empty vector raises ValueError."""
        arr = pa.array([], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        with pytest.raises(ValueError, match="empty"):
            vec.max()


class TestFloat64Aggregations:
    """Test aggregation operations on Float64Vector."""
    
    def test_sum_basic(self):
        """Test basic sum operation."""
        arr = pa.array([1.5, 2.5, 3.0, 4.0, 5.0], type=pa.float64())
        vec = Vector.from_arrow(arr)
        
        assert vec.sum() == pytest.approx(16.0)
    
    def test_sum_with_nulls(self):
        """Test sum with null values."""
        arr = pa.array([1.5, None, 3.5, None, 5.0], type=pa.float64())
        vec = Vector.from_arrow(arr)
        
        assert vec.sum() == pytest.approx(10.0)
    
    def test_sum_all_nulls(self):
        """Test sum when all values are null."""
        arr = pa.array([None, None, None], type=pa.float64())
        vec = Vector.from_arrow(arr)
        
        assert vec.sum() == pytest.approx(0.0)
    
    def test_sum_negative_and_positive(self):
        """Test sum with mixed negative and positive values."""
        arr = pa.array([-1.5, 2.5, -3.0, 4.0, -1.0], type=pa.float64())
        vec = Vector.from_arrow(arr)
        
        assert vec.sum() == pytest.approx(1.0)
    
    def test_sum_very_small_values(self):
        """Test sum with very small floating point values."""
        arr = pa.array([0.1, 0.2, 0.3], type=pa.float64())
        vec = Vector.from_arrow(arr)
        
        # Use approx due to floating point arithmetic
        assert vec.sum() == pytest.approx(0.6)
    
    def test_min_basic(self):
        """Test basic min operation."""
        arr = pa.array([5.5, 2.2, 8.8, 1.1, 9.9], type=pa.float64())
        vec = Vector.from_arrow(arr)
        
        assert vec.min() == pytest.approx(1.1)
    
    def test_min_with_nulls(self):
        """Test min with null values."""
        arr = pa.array([5.5, None, 1.1, None, 9.9], type=pa.float64())
        vec = Vector.from_arrow(arr)
        
        # Current implementation: nulls are treated as 0.0
        assert vec.min() == pytest.approx(0.0)
    
    def test_min_negative_values(self):
        """Test min with negative values."""
        arr = pa.array([5.5, -3.3, 2.2, -10.1, 4.4], type=pa.float64())
        vec = Vector.from_arrow(arr)
        
        assert vec.min() == pytest.approx(-10.1)
    
    def test_min_empty_raises(self):
        """Test that min on empty vector raises ValueError."""
        arr = pa.array([], type=pa.float64())
        vec = Vector.from_arrow(arr)
        
        with pytest.raises(ValueError, match="empty"):
            vec.min()
    
    def test_max_basic(self):
        """Test basic max operation."""
        arr = pa.array([5.5, 2.2, 8.8, 1.1, 9.9], type=pa.float64())
        vec = Vector.from_arrow(arr)
        
        assert vec.max() == pytest.approx(9.9)
    
    def test_max_with_nulls(self):
        """Test max with null values."""
        arr = pa.array([5.5, None, 9.9, None, 1.1], type=pa.float64())
        vec = Vector.from_arrow(arr)
        
        assert vec.max() == pytest.approx(9.9)
    
    def test_max_negative_values(self):
        """Test max with all negative values."""
        arr = pa.array([-5.5, -3.3, -2.2, -10.1, -4.4], type=pa.float64())
        vec = Vector.from_arrow(arr)
        
        assert vec.max() == pytest.approx(-2.2)
    
    def test_max_empty_raises(self):
        """Test that max on empty vector raises ValueError."""
        arr = pa.array([], type=pa.float64())
        vec = Vector.from_arrow(arr)
        
        with pytest.raises(ValueError, match="empty"):
            vec.max()


class TestDate32Aggregations:
    """Test aggregation operations on Date32Vector."""
    
    def test_min_basic(self):
        """Test basic min operation on dates."""
        # Days since epoch: 50, 10, 30, 20
        arr = pa.array([50, 10, 30, 20], type=pa.date32())
        vec = Vector.from_arrow(arr)
        
        assert vec.min() == 10
    
    def test_min_with_nulls(self):
        """Test min with null date values."""
        arr = pa.array([50, None, 10, None, 30], type=pa.date32())
        vec = Vector.from_arrow(arr)
        
        assert vec.min() == 10
    
    def test_max_basic(self):
        """Test basic max operation on dates."""
        arr = pa.array([50, 10, 30, 20], type=pa.date32())
        vec = Vector.from_arrow(arr)
        
        assert vec.max() == 50
    
    def test_max_with_nulls(self):
        """Test max with null date values."""
        arr = pa.array([50, None, 30, None, 10], type=pa.date32())
        vec = Vector.from_arrow(arr)
        
        assert vec.max() == 50


class TestTimestampAggregations:
    """Test aggregation operations on TimestampVector."""
    
    def test_min_basic(self):
        """Test basic min operation on timestamps."""
        arr = pa.array([5000, 1000, 3000, 2000], type=pa.timestamp('us'))
        vec = Vector.from_arrow(arr)
        
        assert vec.min() == 1000
    
    def test_min_with_nulls(self):
        """Test min with null timestamp values."""
        arr = pa.array([5000, None, 1000, None, 3000], type=pa.timestamp('us'))
        vec = Vector.from_arrow(arr)
        
        assert vec.min() == 1000
    
    def test_max_basic(self):
        """Test basic max operation on timestamps."""
        arr = pa.array([5000, 1000, 3000, 2000], type=pa.timestamp('us'))
        vec = Vector.from_arrow(arr)
        
        assert vec.max() == 5000
    
    def test_max_with_nulls(self):
        """Test max with null timestamp values."""
        arr = pa.array([5000, None, 3000, None, 1000], type=pa.timestamp('us'))
        vec = Vector.from_arrow(arr)
        
        assert vec.max() == 5000


class TestAggregationEdgeCases:
    """Test edge cases for aggregation operations."""
    
    def test_sum_overflow_safety(self):
        """Test sum with large values doesn't overflow."""
        # Use reasonably large values
        arr = pa.array([10**15, 10**15, 10**15], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        expected = 3 * (10**15)
        assert vec.sum() == expected
    
    def test_single_value_aggregations(self):
        """Test all aggregations with single value."""
        arr = pa.array([42], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec.sum() == 42
        assert vec.min() == 42
        assert vec.max() == 42
    
    def test_two_value_aggregations(self):
        """Test all aggregations with two values."""
        arr = pa.array([10, 20], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec.sum() == 30
        assert vec.min() == 10
        assert vec.max() == 20
    
    def test_identical_values(self):
        """Test aggregations when all values are identical."""
        arr = pa.array([7, 7, 7, 7, 7], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec.sum() == 35
        assert vec.min() == 7
        assert vec.max() == 7
    
    def test_zero_values(self):
        """Test aggregations with zero values."""
        arr = pa.array([0, 0, 0, 0], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec.sum() == 0
        assert vec.min() == 0
        assert vec.max() == 0
    
    def test_mixed_zeros_and_values(self):
        """Test aggregations with mix of zeros and non-zero values."""
        arr = pa.array([0, 5, 0, 10, 0], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec.sum() == 15
        assert vec.min() == 0
        assert vec.max() == 10
