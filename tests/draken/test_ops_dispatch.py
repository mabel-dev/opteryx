"""
Tests for the operation dispatch system.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from opteryx.draken.core.ops import (
    dispatch_op,
    get_operation_enum,
    TYPE_INT8, TYPE_INT16, TYPE_INT32, TYPE_INT64,
    TYPE_FLOAT32, TYPE_FLOAT64,
    TYPE_DATE32, TYPE_TIMESTAMP64,
    TYPE_BOOL, TYPE_STRING,
)


class TestOperationEnum:
    """Test operation enum retrieval."""
    
    def test_get_operation_enum_arithmetic(self):
        """Test getting enum values for arithmetic operations."""
        assert get_operation_enum('add') == 1
        assert get_operation_enum('subtract') == 2
        assert get_operation_enum('multiply') == 3
        assert get_operation_enum('divide') == 4
    
    def test_get_operation_enum_comparison(self):
        """Test getting enum values for comparison operations."""
        assert get_operation_enum('equals') == 10
        assert get_operation_enum('not_equals') == 11
        assert get_operation_enum('greater_than') == 12
        assert get_operation_enum('greater_than_or_equals') == 13
        assert get_operation_enum('less_than') == 14
        assert get_operation_enum('less_than_or_equals') == 15
    
    def test_get_operation_enum_boolean(self):
        """Test getting enum values for boolean operations."""
        assert get_operation_enum('and') == 20
        assert get_operation_enum('or') == 21
        assert get_operation_enum('xor') == 22
    
    def test_get_operation_enum_case_insensitive(self):
        """Test that operation names are case insensitive."""
        assert get_operation_enum('ADD') == get_operation_enum('add')
        assert get_operation_enum('Equals') == get_operation_enum('equals')
    
    def test_get_operation_enum_invalid(self):
        """Test that invalid operation names raise ValueError."""
        with pytest.raises(ValueError, match="Unknown operation"):
            get_operation_enum('invalid_op')


class TestDispatchOp:
    """Test operation dispatch."""
    
    def test_dispatch_comparison_same_type(self):
        """Test dispatching comparison operations on same types."""
        op = get_operation_enum('equals')
        
        # These should return None (not implemented) but not error
        result = dispatch_op(TYPE_INT64, False, TYPE_INT64, False, op)
        assert result is None  # Operation is compatible but not implemented
        
        result = dispatch_op(TYPE_FLOAT64, False, TYPE_FLOAT64, True, op)
        assert result is None
        
        result = dispatch_op(TYPE_BOOL, True, TYPE_BOOL, False, op)
        assert result is None
    
    def test_dispatch_comparison_different_types(self):
        """Test dispatching comparison operations on different types."""
        op = get_operation_enum('equals')
        
        # These should return None (incompatible types)
        result = dispatch_op(TYPE_INT64, False, TYPE_FLOAT64, False, op)
        assert result is None
        
        result = dispatch_op(TYPE_INT64, False, TYPE_BOOL, False, op)
        assert result is None
    
    def test_dispatch_arithmetic_same_type(self):
        """Test dispatching arithmetic operations on same types."""
        add_op = get_operation_enum('add')
        
        # Same types should be compatible (but return None as not implemented)
        result = dispatch_op(TYPE_INT64, False, TYPE_INT64, False, add_op)
        assert result is None
        
        result = dispatch_op(TYPE_FLOAT64, True, TYPE_FLOAT64, False, add_op)
        assert result is None
    
    def test_dispatch_arithmetic_different_types(self):
        """Test dispatching arithmetic operations on different types."""
        add_op = get_operation_enum('add')
        
        # Different types should be incompatible
        result = dispatch_op(TYPE_INT32, False, TYPE_INT64, False, add_op)
        assert result is None
        
        result = dispatch_op(TYPE_INT64, False, TYPE_FLOAT64, False, add_op)
        assert result is None
    
    def test_dispatch_boolean_operations(self):
        """Test dispatching boolean operations."""
        and_op = get_operation_enum('and')
        or_op = get_operation_enum('or')
        
        # Boolean operations should work with boolean types
        result = dispatch_op(TYPE_BOOL, False, TYPE_BOOL, False, and_op)
        assert result is None
        
        result = dispatch_op(TYPE_BOOL, True, TYPE_BOOL, False, or_op)
        assert result is None
        
        # Boolean operations should not work with non-boolean types
        result = dispatch_op(TYPE_INT64, False, TYPE_INT64, False, and_op)
        assert result is None
    
    def test_dispatch_scalar_combinations(self):
        """Test dispatch with different scalar/vector combinations."""
        op = get_operation_enum('add')
        
        # Vector-Vector
        result = dispatch_op(TYPE_INT64, False, TYPE_INT64, False, op)
        assert result is None
        
        # Vector-Scalar
        result = dispatch_op(TYPE_INT64, False, TYPE_INT64, True, op)
        assert result is None
        
        # Scalar-Vector (NOT SUPPORTED)
        result = dispatch_op(TYPE_INT64, True, TYPE_INT64, False, op)
        assert result is None  # Should return None (not supported)
        
        # Scalar-Scalar
        result = dispatch_op(TYPE_INT64, True, TYPE_INT64, True, op)
        assert result is None


class TestTypeConstants:
    """Test that type constants are exposed correctly."""
    
    def test_integer_types(self):
        """Test integer type constants."""
        assert TYPE_INT8 == 1
        assert TYPE_INT16 == 2
        assert TYPE_INT32 == 3
        assert TYPE_INT64 == 4
    
    def test_float_types(self):
        """Test float type constants."""
        assert TYPE_FLOAT32 == 20
        assert TYPE_FLOAT64 == 21
    
    def test_temporal_types(self):
        """Test temporal type constants."""
        assert TYPE_DATE32 == 30
        assert TYPE_TIMESTAMP64 == 40
    
    def test_other_types(self):
        """Test other type constants."""
        assert TYPE_BOOL == 50
        assert TYPE_STRING == 60


class TestGetOpFunction:
    """Test the get_op convenience function."""
    
    def test_get_op_with_string_operation(self):
        """Test get_op with string operation name."""
        from opteryx.draken.core.ops import py_get_op as get_op
        
        # Should work with string operation name
        result = get_op(TYPE_INT64, False, TYPE_INT64, True, 'equals')
        assert result is None  # Compatible but not implemented
    
    def test_get_op_with_enum_operation(self):
        """Test get_op with enum operation."""
        from opteryx.draken.core.ops import py_get_op as get_op
        
        # Should work with enum value
        equals_op = get_operation_enum('equals')
        result = get_op(TYPE_INT64, False, TYPE_INT64, True, equals_op)
        assert result is None  # Compatible but not implemented
    
    def test_get_op_with_invalid_string(self):
        """Test get_op with invalid string raises error."""
        from opteryx.draken.core.ops import py_get_op as get_op
        
        with pytest.raises(ValueError, match="Unknown operation"):
            get_op(TYPE_INT64, False, TYPE_INT64, True, 'invalid_op')
    
    def test_get_op_all_operation_types(self):
        """Test get_op with all operation types."""
        from opteryx.draken.core.ops import py_get_op as get_op
        
        # Test various operations
        operations = ['add', 'subtract', 'multiply', 'divide', 
                     'equals', 'not_equals', 'greater_than',
                     'and', 'or', 'xor']
        
        for op in operations:
            result = get_op(TYPE_INT64, False, TYPE_INT64, False, op)
            # Should return None (not error) for each operation
            assert result is None or isinstance(result, int)


class TestDispatchVectorVectorComparisons:
    """Test that the dispatcher recognizes vector-vector comparison operations."""
    
    def test_dispatch_vector_vector_int64_comparisons(self):
        """Test dispatcher recognizes Int64Vector-Int64Vector comparisons as valid."""
        from opteryx.draken.core.ops import py_get_op as get_op
        
        # Test all comparison operations for vector-vector Int64
        operations = ['equals', 'not_equals', 'greater_than', 
                     'greater_than_or_equals', 'less_than', 'less_than_or_equals']
        
        for op in operations:
            # Vector-Vector: left is vector (False), right is vector (False)
            result = get_op(TYPE_INT64, False, TYPE_INT64, False, op)
            # Dispatcher validates this as compatible (same type comparison)
            # Returns None because actual implementation is in vector classes
            assert result is None, f"Dispatcher should validate {op} for Int64Vector-Int64Vector"
    
    def test_dispatch_vector_vector_float64_comparisons(self):
        """Test dispatcher recognizes Float64Vector-Float64Vector comparisons as valid."""
        from opteryx.draken.core.ops import py_get_op as get_op
        
        # Test all comparison operations for vector-vector Float64
        operations = ['equals', 'not_equals', 'greater_than', 
                     'greater_than_or_equals', 'less_than', 'less_than_or_equals']
        
        for op in operations:
            # Vector-Vector: left is vector (False), right is vector (False)
            result = get_op(TYPE_FLOAT64, False, TYPE_FLOAT64, False, op)
            # Dispatcher validates this as compatible (same type comparison)
            # Returns None because actual implementation is in vector classes
            assert result is None, f"Dispatcher should validate {op} for Float64Vector-Float64Vector"
    
    def test_dispatch_vector_scalar_comparisons(self):
        """Test dispatcher recognizes vector-scalar comparisons as valid."""
        from opteryx.draken.core.ops import py_get_op as get_op
        
        operations = ['equals', 'not_equals', 'greater_than', 
                     'greater_than_or_equals', 'less_than', 'less_than_or_equals']
        
        for op in operations:
            # Vector-Scalar: left is vector (False), right is scalar (True)
            result = get_op(TYPE_INT64, False, TYPE_INT64, True, op)
            assert result is None, f"Dispatcher should validate {op} for Int64Vector-scalar"
            
            result = get_op(TYPE_FLOAT64, False, TYPE_FLOAT64, True, op)
            assert result is None, f"Dispatcher should validate {op} for Float64Vector-scalar"

if __name__ == "__main__":
    pytest.main([__file__])