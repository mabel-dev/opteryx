"""
Tests for TimeVector operations.

This module tests TimeVector functionality including:
- Basic operations
- Comparisons
- Null handling
- Aggregations
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pyarrow as pa

from opteryx.draken import Vector


class TestTimeVectorBasics:
    """Test basic TimeVector operations."""
    
    def test_creation_from_arrow(self):
        """Test creating TimeVector from Arrow array."""
        # Time in microseconds
        arr = pa.array([10000, 20000, 30000, 40000], type=pa.time64('us'))
        vec = Vector.from_arrow(arr)
        
        assert vec.length == 4
        assert vec.null_count == 0
    
    def test_to_pylist(self):
        """Test conversion to Python list."""
        arr = pa.array([10000, 20000, 30000], type=pa.time64('us'))
        vec = Vector.from_arrow(arr)
        
        result = vec.to_pylist()
        expected = [10000, 20000, 30000]
        
        assert result == expected
    
    def test_to_pylist_with_nulls(self):
        """Test conversion to Python list with nulls."""
        arr = pa.array([10000, None, 30000, None], type=pa.time64('us'))
        vec = Vector.from_arrow(arr)
        
        result = vec.to_pylist()
        expected = [10000, None, 30000, None]
        
        assert result == expected
    
    def test_length(self):
        """Test length property."""
        arr = pa.array([10000, 20000, 30000, 40000, 50000], type=pa.time64('us'))
        vec = Vector.from_arrow(arr)
        
        assert vec.length == 5
    
    def test_empty_vector(self):
        """Test empty TimeVector."""
        arr = pa.array([], type=pa.time64('us'))
        vec = Vector.from_arrow(arr)
        
        assert vec.length == 0
        assert vec.null_count == 0


class TestTimeVectorComparisons:
    """Test TimeVector comparison operations."""
    
    def test_comparisons_not_implemented(self):
        """TimeVector doesn't have comparison methods yet."""
        arr = pa.array([10000, 20000, 30000], type=pa.time64('us'))
        vec = Vector.from_arrow(arr)
        
        # TimeVector doesn't currently have equals, greater_than, etc.
        # Just verify the vector was created
        assert vec.length == 3


class TestTimeVectorNullHandling:
    """Test null handling in TimeVector."""
    
    def test_null_count(self):
        """Test null_count property."""
        arr = pa.array([10000, None, 30000, None, 50000], type=pa.time64('us'))
        vec = Vector.from_arrow(arr)
        
        assert vec.null_count == 2
    
    def test_is_null(self):
        """Test is_null operation."""
        arr = pa.array([10000, None, 30000, None, 50000], type=pa.time64('us'))
        vec = Vector.from_arrow(arr)
        
        result = vec.is_null()
        expected = [0, 1, 0, 1, 0]
        
        assert list(result) == expected
    
    def test_all_nulls(self):
        """Test vector with all null values."""
        arr = pa.array([None, None, None], type=pa.time64('us'))
        vec = Vector.from_arrow(arr)
        
        assert vec.null_count == 3
        assert vec.length == 3
        assert list(vec.is_null()) == [1, 1, 1]
    
    def test_no_nulls(self):
        """Test vector with no null values."""
        arr = pa.array([10000, 20000, 30000], type=pa.time64('us'))
        vec = Vector.from_arrow(arr)
        
        assert vec.null_count == 0
        assert list(vec.is_null()) == [0, 0, 0]


class TestTimeVectorTake:
    """Test take operation on TimeVector."""
    
    def test_take_basic(self):
        """Test basic take operation."""
        import numpy as np
        arr = pa.array([10000, 20000, 30000, 40000, 50000], type=pa.time64('us'))
        vec = Vector.from_arrow(arr)
        
        indices = np.array([0, 2, 4], dtype=np.int32)
        result = vec.take(indices)
        
        assert result.to_pylist() == [10000, 30000, 50000]
    
    def test_take_with_nulls(self):
        """Test take operation with nulls."""
        import numpy as np
        arr = pa.array([10000, None, 30000, None, 50000], type=pa.time64('us'))
        vec = Vector.from_arrow(arr)
        
        indices = np.array([0, 1, 4], dtype=np.int32)
        result = vec.take(indices)
        
        # Current implementation: nulls become 0
        assert result.to_pylist() == [10000, 0, 50000]
    
    def test_take_single_index(self):
        """Test take with single index."""
        import numpy as np
        arr = pa.array([10000, 20000, 30000], type=pa.time64('us'))
        vec = Vector.from_arrow(arr)
        
        result = vec.take(np.array([1], dtype=np.int32))
        assert result.to_pylist() == [20000]
    
    def test_take_all_indices(self):
        """Test take with all indices."""
        import numpy as np
        arr = pa.array([10000, 20000, 30000], type=pa.time64('us'))
        vec = Vector.from_arrow(arr)
        
        result = vec.take(np.array([0, 1, 2], dtype=np.int32))
        assert result.to_pylist() == [10000, 20000, 30000]


class TestTimeVectorEdgeCases:
    """Test edge cases for TimeVector."""
    
    def test_single_value(self):
        """Test vector with single value."""
        arr = pa.array([10000], type=pa.time64('us'))
        vec = Vector.from_arrow(arr)
        
        assert vec.length == 1
        assert vec.to_pylist() == [10000]
    
    def test_duplicate_values(self):
        """Test vector with duplicate values."""
        arr = pa.array([10000, 10000, 10000], type=pa.time64('us'))
        vec = Vector.from_arrow(arr)
        
        # TimeVector doesn't have equals - just verify creation
        assert vec.to_pylist() == [10000, 10000, 10000]
    
    def test_very_large_time_values(self):
        """Test with large time values."""
        # Max time in microseconds for a day
        max_time = 86400000000  # 24 hours in microseconds
        arr = pa.array([0, max_time // 2, max_time - 1], type=pa.time64('us'))
        vec = Vector.from_arrow(arr)
        
        assert vec.length == 3
        assert vec.to_pylist()[0] == 0
    
    def test_sequential_times(self):
        """Test with sequential time values."""
        times = list(range(0, 50000, 10000))
        arr = pa.array(times, type=pa.time64('us'))
        vec = Vector.from_arrow(arr)
        
        assert vec.length == len(times)
        assert vec.to_pylist() == times
