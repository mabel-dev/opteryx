"""Test for subscript access returning None for null values (GitHub Issue)."""
import pyarrow as pa
from opteryx.draken import Vector
from opteryx.draken.vectors import string_vector as string_vector_module

StringVectorBuilder = string_vector_module.StringVectorBuilder  # type: ignore[attr-defined]


class TestSubscriptNullHandling:
    """Test that subscript access (__getitem__) returns None for null values."""
    
    def test_string_vector_subscript_null(self):
        """StringVector should return None for null values, not empty bytes."""
        arr = pa.array(['abc123', 'xyz789', None])
        vec = Vector.from_arrow(arr)
        
        assert vec[0] == b'abc123'
        assert vec[1] == b'xyz789'
        assert vec[2] is None
        
    def test_string_vector_builder_subscript_null(self):
        """StringVector built directly should also return None for null values."""
        builder = StringVectorBuilder.with_counts(3, 12)
        builder.append(b'abc123')
        builder.append(b'xyz789')
        builder.append_null()
        vec = builder.finish()
        
        assert vec[0] == b'abc123'
        assert vec[1] == b'xyz789'
        assert vec[2] is None
        
    def test_int64_vector_subscript_null(self):
        """Int64Vector should return None for null values, not raise ValueError."""
        arr = pa.array([1, 2, None], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec[0] == 1
        assert vec[1] == 2
        assert vec[2] is None
        
    def test_float64_vector_subscript_null(self):
        """Float64Vector should return None for null values, not raise ValueError."""
        arr = pa.array([1.5, 2.5, None], type=pa.float64())
        vec = Vector.from_arrow(arr)
        
        assert vec[0] == 1.5
        assert vec[1] == 2.5
        assert vec[2] is None
        
    def test_bool_vector_subscript_null(self):
        """BoolVector should return None for null values, not raise ValueError."""
        arr = pa.array([True, False, None], type=pa.bool_())
        vec = Vector.from_arrow(arr)
        
        assert vec[0] is True
        assert vec[1] is False
        assert vec[2] is None
        
    def test_date32_vector_subscript_null(self):
        """Date32Vector should return None for null values."""
        arr = pa.array([0, 1, None], type=pa.date32())
        vec = Vector.from_arrow(arr)
        
        assert vec[0] == 0
        assert vec[1] == 1
        assert vec[2] is None
        
    def test_timestamp_vector_subscript_null(self):
        """TimestampVector should return None for null values."""
        arr = pa.array([0, 1000000, None], type=pa.timestamp('us'))
        vec = Vector.from_arrow(arr)
        
        assert vec[0] == 0
        assert vec[1] == 1000000
        assert vec[2] is None
        
    def test_time_vector_subscript_null(self):
        """TimeVector should return None for null values."""
        arr = pa.array([0, 1000, None], type=pa.time64('us'))
        vec = Vector.from_arrow(arr)
        
        assert vec[0] == 0
        assert vec[1] == 1000
        assert vec[2] is None
        
    def test_multiple_nulls(self):
        """Test vector with multiple nulls."""
        arr = pa.array([None, 'hello', None, 'world', None])
        vec = Vector.from_arrow(arr)
        
        assert vec[0] is None
        assert vec[1] == b'hello'
        assert vec[2] is None
        assert vec[3] == b'world'
        assert vec[4] is None
        
    def test_all_nulls(self):
        """Test vector with all nulls."""
        arr = pa.array([None, None, None], type=pa.int64())
        vec = Vector.from_arrow(arr)
        
        assert vec[0] is None
        assert vec[1] is None
        assert vec[2] is None
        
    def test_to_pylist_consistency_with_subscript(self):
        """Ensure to_pylist() and subscript access return consistent null representation."""
        arr = pa.array(['hello', None, 'world'])
        vec = Vector.from_arrow(arr)
        
        # Subscript access should return None for nulls
        assert vec[0] == b'hello'
        assert vec[1] is None
        assert vec[2] == b'world'
        
        # to_pylist() should also return None for nulls
        pylist = vec.to_pylist()
        assert pylist[0] == b'hello'
        assert pylist[1] is None
        assert pylist[2] == b'world'
        
    def test_builder_to_pylist_consistency(self):
        """Ensure builder-created vectors have consistent null handling."""
        builder = StringVectorBuilder.with_counts(3, 10)
        builder.append(b'hello')
        builder.append_null()
        builder.append(b'world')
        vec = builder.finish()
        
        # Subscript should return None
        assert vec[0] == b'hello'
        assert vec[1] is None
        assert vec[2] == b'world'
        
        # to_pylist should also return None
        pylist = vec.to_pylist()
        assert pylist[0] == b'hello'
        assert pylist[1] is None
        assert pylist[2] == b'world'
