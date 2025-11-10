#!/usr/bin/env python
"""Tests for new Morsel methods: take, select, rename, to_arrow."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pytest
import pyarrow as pa
import opteryx.draken as draken


def test_methods_exist():
    """Test that all required methods exist on the Morsel class."""
    table = pa.table({'a': [1, 2, 3]})
    morsel = draken.Morsel.from_arrow(table)
    
    # Check that all methods exist
    assert hasattr(morsel, 'take')
    assert hasattr(morsel, 'select')
    assert hasattr(morsel, 'rename')
    assert hasattr(morsel, 'to_arrow')
    
    # Check that methods are callable
    assert callable(morsel.take)
    assert callable(morsel.select)
    assert callable(morsel.rename)
    assert callable(morsel.to_arrow)

def test_take_method_signature():
    """Test that take method modifies morsel in-place and returns self."""
    table = pa.table({'a': [1, 2, 3, 4, 5], 'b': ['x', 'y', 'z', 'w', 'v']})
    morsel = draken.Morsel.from_arrow(table)
    
    # Test with list of indices (in-place)
    result = morsel.take([0, 2, 4])
    assert result is morsel  # Should return self
    assert morsel.shape[0] == 3  # Should have 3 rows
    assert morsel.shape[1] == 2  # Should have same number of columns
    
    # Test with copy(mask=) for non-mutating operation
    morsel2 = draken.Morsel.from_arrow(table)
    result_copy = morsel2.copy(mask=[1])
    assert result_copy is not morsel2  # Should be a new object
    assert result_copy.shape[0] == 1
    
    # Test with pyarrow array
    morsel3 = draken.Morsel.from_arrow(table)
    indices_array = pa.array([0, 1], type=pa.int32())
    result_array = morsel3.take(indices_array)
    assert result_array is morsel3  # Should return self
    assert morsel3.shape[0] == 2

def test_select_method_signature():
    """Test that select method modifies morsel in-place and returns self."""
    table = pa.table({'a': [1, 2, 3], 'b': ['x', 'y', 'z'], 'c': [1.1, 2.2, 3.3]})
    
    # Test with list of column names
    morsel = draken.Morsel.from_arrow(table)
    result = morsel.select(['a', 'c'])
    assert result is morsel  # Should return self
    assert morsel.shape[0] == 3  # Same number of rows
    assert morsel.shape[1] == 2  # Selected 2 columns
    
    # Test with single column name
    morsel2 = draken.Morsel.from_arrow(table)
    result_single = morsel2.select(['b'])
    assert result_single is morsel2  # Should return self
    assert morsel2.shape[1] == 1
    
    # Test with string (single column)
    morsel3 = draken.Morsel.from_arrow(table)
    result_str = morsel3.select('a')
    assert result_str is morsel3  # Should return self
    assert morsel3.shape[1] == 1
    
    # Test with copy(columns=) for non-mutating operation
    morsel4 = draken.Morsel.from_arrow(table)
    result_copy = morsel4.copy(columns=['a', 'b'])
    assert result_copy is not morsel4  # Should be a new object
    assert result_copy.shape[1] == 2
    assert morsel4.shape[1] == 3  # Original unchanged

def test_select_nonexistent_column():
    """Test that selecting a non-existent column raises KeyError."""
    table = pa.table({'a': [1, 2, 3]})
    morsel = draken.Morsel.from_arrow(table)
    
    with pytest.raises(KeyError):
        morsel.select(['nonexistent'])

def test_rename_method_signature():
    """Test that rename method modifies morsel in-place and returns self."""
    table = pa.table({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
    
    # Test with list of new names
    morsel = draken.Morsel.from_arrow(table)
    result = morsel.rename(['col1', 'col2'])
    assert result is morsel  # Should return self
    assert morsel.shape == (3, 2)  # Same dimensions
    
    # Check that column names are updated
    result_arrow = morsel.to_arrow()
    assert result_arrow.column_names == ['col1', 'col2']
    
    # Test with dict mapping
    morsel2 = draken.Morsel.from_arrow(table)
    result_dict = morsel2.rename({'a': 'alpha', 'b': 'beta'})
    assert result_dict is morsel2  # Should return self
    result_dict_arrow = morsel2.to_arrow()
    assert result_dict_arrow.column_names == ['alpha', 'beta']
    
    # Test with partial dict mapping
    morsel3 = draken.Morsel.from_arrow(table)
    result_partial = morsel3.rename({'a': 'alpha'})
    result_partial_arrow = morsel3.to_arrow()
    assert 'alpha' in result_partial_arrow.column_names
    assert 'b' in result_partial_arrow.column_names

def test_rename_wrong_number_names():
    """Test that providing wrong number of names raises ValueError."""
    table = pa.table({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
    morsel = draken.Morsel.from_arrow(table)
    
    # Too few names
    with pytest.raises(ValueError):
        morsel.rename(['only_one'])
    
    # Too many names
    with pytest.raises(ValueError):
        morsel.rename(['one', 'two', 'three'])

def test_to_arrow_method():
    """Test that to_arrow method returns a pyarrow.Table."""
    table = pa.table({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
    morsel = draken.Morsel.from_arrow(table)
    
    result = morsel.to_arrow()
    assert isinstance(result, pa.Table)
    assert result.num_columns == morsel.num_columns
    assert result.num_rows == morsel.num_rows
    
    # Column names should match (though may be decoded differently)
    original_names = set(table.column_names)
    result_names = set(result.column_names)
    assert original_names == result_names

def test_method_chaining():
    """Test that methods can be chained together (in-place operations)."""
    table = pa.table({'a': [1, 2, 3, 4], 'b': ['w', 'x', 'y', 'z'], 'c': [1.1, 2.2, 3.3, 4.4]})
    morsel = draken.Morsel.from_arrow(table)
    
    # Chain: select -> take -> rename (all in-place)
    result = (morsel
                .select(['a', 'b'])
                .take([0, 2])
                .rename(['first', 'second']))
    
    assert result is morsel  # Should be same object (in-place)
    assert morsel.shape == (2, 2)  # 2 rows, 2 columns
    
    result_arrow = morsel.to_arrow()
    assert result_arrow.column_names == ['first', 'second']

def test_api_compatibility_with_pyarrow():
    """Test that the method signatures work correctly with similar semantics to pyarrow.Table.
    
    Note: PyArrow methods return new objects, while Draken methods are in-place.
    Use copy(mask=) and copy(columns=) for non-mutating operations in Draken.
    """
    table = pa.table({'a': [1, 2, 3, 4, 5], 'b': ['v', 'w', 'x', 'y', 'z']})
    
    # Test indices parameter types accepted by both
    indices = [0, 2, 4]
    
    # PyArrow returns new object, Draken modifies in-place
    pa_result = table.take(indices)
    morsel = draken.Morsel.from_arrow(table)
    morsel_result = morsel.take(indices)
    assert morsel_result is morsel  # In-place
    assert morsel.shape[0] == pa_result.num_rows
    
    # For non-mutating, use copy(mask=)
    morsel2 = draken.Morsel.from_arrow(table)
    morsel_copy = morsel2.copy(mask=indices)
    assert morsel_copy is not morsel2  # New object
    assert morsel_copy.shape[0] == pa_result.num_rows
    
    # Both should accept column name lists for select
    columns = ['a']
    pa_select = table.select(columns)
    morsel3 = draken.Morsel.from_arrow(table)
    morsel_select = morsel3.select(columns)
    assert morsel_select is morsel3  # In-place
    assert morsel3.shape[1] == pa_select.num_columns
    
    # For non-mutating select, use copy(columns=)
    morsel4 = draken.Morsel.from_arrow(table)
    morsel_select_copy = morsel4.copy(columns=columns)
    assert morsel_select_copy is not morsel4  # New object
    assert morsel_select_copy.shape[1] == pa_select.num_columns
    
    # Both should accept list of names for rename
    new_names = ['col1', 'col2']
    pa_rename = table.rename_columns(new_names)
    morsel5 = draken.Morsel.from_arrow(table)
    morsel_rename = morsel5.rename(new_names)
    assert morsel_rename is morsel5  # In-place
    assert morsel5.to_arrow().column_names == pa_rename.column_names

def test_empty_operations():
    """Test operations on empty or minimal data."""
    # Test with single row
    table = pa.table({'a': [42]})
    morsel = draken.Morsel.from_arrow(table)
    
    # Take the only row
    result = morsel.take([0])
    assert result.shape == (1, 1)
    
    # Select the only column
    result = morsel.select(['a'])
    assert result.shape == (1, 1)
    
    # Rename the only column
    result = morsel.rename(['new_name'])
    assert result.to_arrow().column_names == ['new_name']


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()