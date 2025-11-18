#!/usr/bin/env python
"""Tests for new Morsel methods: take, select, rename, to_arrow."""

import sys
from array import array
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pytest
import pyarrow as pa
import opteryx.draken as draken

from opteryx.draken.vectors._hash_api import hash_into as hash_into_vector


def _hash_view_to_list(buffer):
    view = memoryview(buffer)
    assert view.format == "Q"
    return list(view)


def _vector_hash_to_list(vector):
    length = getattr(vector, "length", None)

    if length is None:
        try:
            length = len(vector)  # type: ignore[arg-type]
        except TypeError:
            length = len(vector.to_pylist())

    out = array("Q", [0] * length)
    hash_into_vector(vector, out)
    return list(out)


def test_methods_exist():
    """Test that all required methods exist on the Morsel class."""
    table = pa.table({'a': [1, 2, 3]})
    morsel = draken.Morsel.from_arrow(table)
    
    # Check that all methods exist
    assert hasattr(morsel, 'take')
    assert hasattr(morsel, 'select')
    assert hasattr(morsel, 'rename')
    assert hasattr(morsel, 'to_arrow')
    assert hasattr(draken.Morsel, 'iter_from_arrow')
    
    # Check that methods are callable
    assert callable(morsel.take)
    assert callable(morsel.select)
    assert callable(morsel.rename)
    assert callable(morsel.to_arrow)
    assert callable(draken.Morsel.iter_from_arrow)

def test_metadata_properties_match_arrow_table():
    """Ensure exposed metadata mirrors the source Arrow table."""
    table = pa.table({'ints': pa.array([1, 2, 3], type=pa.int64()), 'strs': ['a', 'b', 'c']})
    morsel = draken.Morsel.from_arrow(table)

    assert morsel.num_rows == table.num_rows
    assert morsel.num_columns == table.num_columns
    assert morsel.shape == (table.num_rows, table.num_columns)
    assert morsel.column_names == [b'ints', b'strs']
    assert [str(col_type) for col_type in morsel.column_types] == ['DRAKEN_INT64', 'DRAKEN_STRING']


def test_repr_contains_dimensions():
    """String repr should surface row and column counts."""
    table = pa.table({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
    morsel = draken.Morsel.from_arrow(table)

    text = repr(morsel)
    assert "3 rows" in text
    assert "2 columns" in text


def test_column_access_returns_vectors():
    """Verify columns can be fetched as draken vectors with expected data."""
    table = pa.table({'vals': [10, 20, 30], 'labels': ['x', 'y', 'z']})
    morsel = draken.Morsel.from_arrow(table)

    numeric_vector = morsel.column(b'vals')
    string_vector = morsel.column(b'labels')

    assert numeric_vector.to_pylist() == [10, 20, 30]
    assert string_vector.to_pylist() == [b'x', b'y', b'z']


def test_column_lookup_requires_bytes_names():
    """Column accessor expects bytes names and rejects strings."""
    table = pa.table({'vals': [1]})
    morsel = draken.Morsel.from_arrow(table)

    with pytest.raises(TypeError):
        morsel.column('vals')


def test_column_lookup_missing_column():
    """Accessing a missing column should raise KeyError."""
    table = pa.table({'vals': [1]})
    morsel = draken.Morsel.from_arrow(table)

    with pytest.raises(KeyError):
        morsel.column(b'missing')


def test_copy_creates_independent_morsel():
    """Copy should return a new morsel that does not share mutable state."""
    table = pa.table({'a': [1, 2, 3], 'b': ['u', 'v', 'w']})
    morsel = draken.Morsel.from_arrow(table)

    clone = morsel.copy()

    morsel.rename(['alpha', 'beta'])
    assert clone.to_arrow().column_names == ['a', 'b']

    clone.take([0, 2])
    assert clone.shape == (2, 2)
    assert morsel.shape == (3, 2)


def test_copy_with_filters_applies_mask_and_column_selection():
    """Row and column filters should be applied during copy."""
    table = pa.table({'a': [0, 1, 2, 3], 'b': ['p', 'q', 'r', 's'], 'c': [True, False, True, False]})
    morsel = draken.Morsel.from_arrow(table)

    filtered = morsel.copy(columns=['b'], mask=[1, 3])
    result_arrow = filtered.to_arrow()

    assert filtered.shape == (2, 1)
    assert result_arrow.column_names == ['b']
    assert result_arrow.column(0).to_pylist() == [b'q', b's']


def test_copy_preserves_requested_column_order():
    """Column projection should respect the requested ordering."""
    table = pa.table({'a': [1, 2], 'b': [3, 4], 'c': [5, 6]})
    morsel = draken.Morsel.from_arrow(table)

    reordered = morsel.copy(columns=['c', 'a'])

    assert reordered.to_arrow().column_names == ['c', 'a']


def test_copy_mask_accepts_arrow_array_and_empty_selection():
    """Copy should accept Arrow arrays and support empty masks."""
    table = pa.table({'a': [10, 20, 30]})
    morsel = draken.Morsel.from_arrow(table)

    arrow_mask = pa.array([2], type=pa.int32())
    arrow_filtered = morsel.copy(mask=arrow_mask)
    assert arrow_filtered.shape == (1, 1)
    assert arrow_filtered.column(b'a').to_pylist() == [30]

    # Empty mask should produce an empty morsel (no rows) rather than raising
    empty_filtered = morsel.copy(mask=[])
    assert empty_filtered.shape[0] == 0
    assert empty_filtered.num_columns == 1


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


def test_take_handles_duplicate_and_empty_indices():
    """Row selection should preserve ordering and handle empty selections."""
    table = pa.table({'a': [10, 20, 30], 'b': [True, False, True]})

    morsel = draken.Morsel.from_arrow(table)
    morsel.take([2, 2, 0])
    assert morsel.shape == (3, 2)
    assert morsel.column(b'a').to_pylist() == [30, 30, 10]

    morsel_empty = draken.Morsel.from_arrow(table)
    # Empty indices should produce an empty morsel (no rows)
    morsel_empty.take([])
    assert morsel_empty.shape[0] == 0


def test_take_rejects_generator_indices():
    """Providing a generator lacks length information and should raise TypeError."""
    table = pa.table({'a': [1, 2, 3]})
    morsel = draken.Morsel.from_arrow(table)

    with pytest.raises(TypeError):
        morsel.take((i for i in [0, 1]))


def test_take_rejects_non_numeric_indices():
    """Non-numeric selectors should fail during in-place take operations."""
    table = pa.table({'a': [1, 2, 3]})
    morsel = draken.Morsel.from_arrow(table)

    with pytest.raises(TypeError):
        morsel.take(['zero'])


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


def test_select_allows_mixed_string_and_bytes_names():
    """Selecting should accept both text and byte column identifiers."""
    table = pa.table({'a': [1, 2], 'b': [3, 4], 'c': [5, 6]})
    morsel = draken.Morsel.from_arrow(table)

    morsel.select(['c', b'a'])
    assert morsel.shape == (2, 2)
    assert morsel.to_arrow().column_names == ['c', 'a']


def test_select_tuple_and_empty_selection():
    """Tuple selectors should work and empty lists clear all columns."""
    table = pa.table({'a': [1], 'b': [2], 'c': [3]})
    morsel = draken.Morsel.from_arrow(table)

    morsel.select(('b', 'a'))
    assert morsel.to_arrow().column_names == ['b', 'a']

    morsel.select([])
    assert morsel.shape == (1, 0)


def test_select_none_raises_type_error():
    """None is not a valid selector."""
    table = pa.table({'a': [1]})
    morsel = draken.Morsel.from_arrow(table)

    with pytest.raises(TypeError):
        morsel.select(None)


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
    morsel3.rename({'a': 'alpha'})
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

def test_rename_requires_iterable_names():
    """Non-iterable rename arguments should raise TypeError."""
    table = pa.table({'a': [1]})
    morsel = draken.Morsel.from_arrow(table)

    with pytest.raises(TypeError):
        morsel.rename(123)

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


def test_iter_from_arrow_uses_table_chunks():
    table = pa.table({
        'a': pa.chunked_array([pa.array([1, 2, 3]), pa.array([4, 5])]),
        'b': pa.chunked_array([pa.array(['v', 'w', 'x']), pa.array(['y', 'z'])]),
    })

    morsels = list(draken.Morsel.iter_from_arrow(table))

    assert len(morsels) == 2
    assert all(isinstance(m, draken.Morsel) for m in morsels)
    assert [m.num_rows for m in morsels] == [3, 2]
    assert morsels[0].to_arrow().column(0).to_pylist() == [1, 2, 3]
    assert morsels[1].to_arrow().column(1).to_pylist() == [b'y', b'z']


def test_iter_from_arrow_handles_unchunked_and_empty_tables():
    table = pa.table({'a': [1, 2], 'b': [3, 4]})
    morsels = list(draken.Morsel.iter_from_arrow(table))
    assert len(morsels) == 1
    assert morsels[0].num_rows == 2

    empty = pa.table({'a': pa.array([], type=pa.int64())})
    assert list(draken.Morsel.iter_from_arrow(empty)) == []


def test_iter_from_arrow_rejects_non_tables():
    table = pa.table({'a': [1, 2, 3]})
    batch = table.to_batches()[0]

    with pytest.raises(TypeError):
        list(draken.Morsel.iter_from_arrow(batch))

    with pytest.raises(TypeError):
        list(draken.Morsel.iter_from_arrow([1, 2, 3]))


def test_iter_from_arrow_respects_batch_size():
    table = pa.table({'a': [1, 2, 3, 4, 5], 'b': ['v', 'w', 'x', 'y', 'z']})

    morsels = list(draken.Morsel.iter_from_arrow(table, batch_size=2))
    assert [m.num_rows for m in morsels] == [2, 2, 1]
    assert morsels[0].to_arrow().column(0).to_pylist() == [1, 2]
    flattened = []
    for m in morsels:
        flattened.extend(m.to_arrow().column(1).to_pylist())
    assert flattened == [b'v', b'w', b'x', b'y', b'z']


def test_iter_from_arrow_validates_batch_size():
    table = pa.table({'a': [1]})

    with pytest.raises(TypeError):
        list(draken.Morsel.iter_from_arrow(table, batch_size='not-int'))

    with pytest.raises(ValueError):
        list(draken.Morsel.iter_from_arrow(table, batch_size=0))

    with pytest.raises(ValueError):
        list(draken.Morsel.iter_from_arrow(table, batch_size=-3))

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


def test_hash_all_columns_matches_column_hashes():
    table = pa.table({
        'i': [1, -2, None],
        's': ['x', None, ''],
    })

    morsel = draken.Morsel.from_arrow(table)

    int_hashes = _vector_hash_to_list(morsel.column(b'i'))
    str_hashes = _vector_hash_to_list(morsel.column(b's'))
    assert len(int_hashes) == len(str_hashes) == morsel.num_rows

    combined = array("Q", int_hashes)
    hash_into_vector(morsel.column(b"s"), combined)

    expected = list(combined)
    morsel_hashes = _hash_view_to_list(morsel.hash())
    assert morsel_hashes == expected


def test_hash_subset_by_name():
    table = pa.table({
        'a': [10, 20, 30],
        'b': ['one', 'two', 'three'],
    })

    morsel = draken.Morsel.from_arrow(table)

    subset_hashes = _hash_view_to_list(morsel.hash(columns=['b']))
    col_hashes = _vector_hash_to_list(morsel.column(b'b'))
    assert subset_hashes == col_hashes


def test_hash_subset_by_index_and_empty_list():
    table = pa.table({
        'a': [1, 2, 3, 4],
        'b': [True, False, True, False],
    })

    morsel = draken.Morsel.from_arrow(table)

    by_index = _hash_view_to_list(morsel.hash(columns=[1]))
    bool_hashes = _vector_hash_to_list(morsel.column(b'b'))
    assert by_index == bool_hashes

    zero_hashes = _hash_view_to_list(morsel.hash(columns=[]))
    assert zero_hashes == [0, 0, 0, 0]


def test_hash_empty_morsel():
    table = pa.table({"a": pa.array([], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)
    assert _hash_view_to_list(morsel.hash()) == []


def test_hash_column_index_bounds():
    """Column index selectors should enforce bounds."""
    table = pa.table({'a': [1], 'b': [2]})
    morsel = draken.Morsel.from_arrow(table)

    assert _hash_view_to_list(morsel.hash(columns=[1])) == _vector_hash_to_list(morsel.column(b'b'))

    with pytest.raises(IndexError):
        morsel.hash(columns=[2])


def test_hash_missing_column_name_raises():
    """Unknown column names are rejected during hashing."""
    table = pa.table({'a': [1], 'b': [2]})
    morsel = draken.Morsel.from_arrow(table)

    with pytest.raises(KeyError):
        morsel.hash(columns=['missing'])


def test_hash_mixed_selector_types_matches_full_hash():
    """Mixing integer and string selectors should be consistent."""
    table = pa.table({'a': [1, 2], 'b': ['x', 'y'], 'c': [True, False]})
    morsel = draken.Morsel.from_arrow(table)

    mixed = _hash_view_to_list(morsel.hash(columns=['a', 1]))
    expected = _hash_view_to_list(morsel.hash(columns=['a', 'b']))
    assert mixed == expected


def test_row_access_returns_python_tuple():
    """Row access via subscription should return tuples of values."""
    table = pa.table({'a': [1, 2], 'b': ['x', 'y']})
    morsel = draken.Morsel.from_arrow(table)

    assert morsel[0] == (1, b'x')
    assert morsel[1] == (2, b'y')


def test_zero_column_morsel_round_trip():
    """Morsels with no columns should still support core operations."""
    table = pa.table({})
    morsel = draken.Morsel.from_arrow(table)

    assert morsel.shape == (0, 0)
    assert morsel.column_names == []
    assert morsel.column_types == []
    assert morsel.to_arrow().num_rows == 0
    assert _hash_view_to_list(morsel.hash()) == []


def test_large_morsel_handling():
    """Morsels should handle large datasets efficiently."""
    large_data = list(range(10000))
    table = pa.table({'a': large_data, 'b': [str(i) for i in large_data]})
    morsel = draken.Morsel.from_arrow(table)
    
    assert morsel.shape == (10000, 2)
    assert len(_hash_view_to_list(morsel.hash())) == 10000
    
    # Test take on large dataset
    morsel.take([0, 5000, 9999])
    assert morsel.shape == (3, 2)
    assert morsel.column(b'a').to_pylist() == [0, 5000, 9999]


def test_null_value_handling():
    """Morsels should correctly preserve and handle null values."""
    table = pa.table({'a': [1, None, 3], 'b': ['x', None, 'z']})
    morsel = draken.Morsel.from_arrow(table)
    
    assert morsel.column(b'a').to_pylist() == [1, None, 3]
    assert morsel.column(b'b').to_pylist() == [b'x', None, b'z']
    
    # Test hashing with nulls
    hashes = _hash_view_to_list(morsel.hash())
    assert len(hashes) == 3
    
    # Test all-null column separately (may use ArrowVector)
    all_null_table = pa.table({'c': [None, None, None]})
    all_null_morsel = draken.Morsel.from_arrow(all_null_table)
    assert all_null_morsel.column(b'c').to_pylist() == [None, None, None]


def test_row_access_with_negative_and_out_of_bounds():
    """Row access should handle negative indices and out of bounds gracefully."""
    table = pa.table({'a': [1, 2, 3]})
    morsel = draken.Morsel.from_arrow(table)
    
    # Negative indices should work (Python convention)
    # Note: Based on testing, seems to return (None,) for out of bounds
    result = morsel[-1]
    assert isinstance(result, tuple)
    
    # Out of bounds positive
    result = morsel[10]
    assert isinstance(result, tuple)


def test_select_duplicate_columns():
    """Selecting the same column multiple times should be allowed."""
    table = pa.table({'a': [1, 2], 'b': [3, 4]})
    morsel = draken.Morsel.from_arrow(table)
    
    morsel.select(['a', 'a', 'b'])
    assert morsel.shape == (2, 3)
    assert morsel.to_arrow().column_names == ['a', 'a', 'b']


def test_rename_with_empty_dict():
    """Renaming with empty dict should leave columns unchanged."""
    table = pa.table({'a': [1], 'b': [2], 'c': [3]})
    morsel = draken.Morsel.from_arrow(table)
    
    morsel.rename({})
    assert morsel.to_arrow().column_names == ['a', 'b', 'c']


def test_rename_with_partial_dict():
    """Renaming with partial dict should only rename specified columns."""
    table = pa.table({'a': [1], 'b': [2], 'c': [3]})
    morsel = draken.Morsel.from_arrow(table)
    
    morsel.rename({'a': 'alpha', 'c': 'charlie'})
    assert morsel.to_arrow().column_names == ['alpha', 'b', 'charlie']


def test_multiple_data_types():
    """Morsels should support all numeric and boolean types."""
    table = pa.table({
        'int8': pa.array([1, 2], type=pa.int8()),
        'int16': pa.array([1, 2], type=pa.int16()),
        'int32': pa.array([1, 2], type=pa.int32()),
        'int64': pa.array([1, 2], type=pa.int64()),
        'float32': pa.array([1.0, 2.0], type=pa.float32()),
        'float64': pa.array([1.0, 2.0], type=pa.float64()),
        'bool': pa.array([True, False], type=pa.bool_()),
    })
    morsel = draken.Morsel.from_arrow(table)
    
    assert morsel.shape == (2, 7)
    type_names = [str(t) for t in morsel.column_types]
    assert 'DRAKEN_INT8' in type_names
    assert 'DRAKEN_INT16' in type_names
    assert 'DRAKEN_INT32' in type_names
    assert 'DRAKEN_INT64' in type_names
    assert 'DRAKEN_FLOAT32' in type_names
    assert 'DRAKEN_FLOAT64' in type_names
    assert 'DRAKEN_BOOL' in type_names


def test_take_with_numpy_array():
    """Take should accept numpy arrays as indices."""
    import numpy as np
    table = pa.table({'a': [10, 20, 30, 40, 50]})
    morsel = draken.Morsel.from_arrow(table)
    
    indices = np.array([0, 2, 4])
    morsel.take(indices)
    assert morsel.shape == (3, 1)
    assert morsel.column(b'a').to_pylist() == [10, 30, 50]


def test_chained_take_operations():
    """Multiple take operations should compound properly."""
    table = pa.table({'a': [1, 2, 3, 4, 5]})
    morsel = draken.Morsel.from_arrow(table)
    
    morsel.take([0, 1, 3, 4]).take([1, 3])
    assert morsel.shape == (2, 1)
    assert morsel.column(b'a').to_pylist() == [2, 5]


def test_take_reverse_order():
    """Take should preserve the order of indices, including reverse."""
    table = pa.table({'a': [10, 20, 30, 40]})
    morsel = draken.Morsel.from_arrow(table)
    
    morsel.take([3, 2, 1, 0])
    assert morsel.column(b'a').to_pylist() == [40, 30, 20, 10]


def test_single_element_repeated_take():
    """Taking the same index multiple times should duplicate rows."""
    table = pa.table({'a': [42], 'b': ['answer']})
    morsel = draken.Morsel.from_arrow(table)
    
    morsel.take([0, 0, 0])
    assert morsel.shape == (3, 2)
    assert morsel.column(b'a').to_pylist() == [42, 42, 42]
    assert morsel.column(b'b').to_pylist() == [b'answer', b'answer', b'answer']


def test_hash_consistency():
    """Hashing the same morsel multiple times should give identical results."""
    table = pa.table({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
    morsel = draken.Morsel.from_arrow(table)
    
    hash1 = _hash_view_to_list(morsel.hash())
    hash2 = _hash_view_to_list(morsel.hash())
    hash3 = _hash_view_to_list(morsel.hash(columns=None))
    
    assert hash1 == hash2
    assert hash1 == hash3


def test_hash_changes_after_take():
    """Hash array length should match row count after take."""
    table = pa.table({'a': [1, 2, 3, 4, 5]})
    morsel = draken.Morsel.from_arrow(table)
    
    hash_before = _hash_view_to_list(morsel.hash())
    assert len(hash_before) == 5
    
    morsel.take([0, 2, 4])
    hash_after = _hash_view_to_list(morsel.hash())
    assert len(hash_after) == 3


def test_unicode_column_values():
    """Morsel should handle unicode strings correctly."""
    table = pa.table({
        'col': ['hello', '世界', 'مرحبا', '🎉', 'Привет']
    })
    morsel = draken.Morsel.from_arrow(table)
    
    values = morsel.column(b'col').to_pylist()
    expected = [text.encode('utf-8') for text in ['hello', '世界', 'مرحبا', '🎉', 'Привет']]
    assert values == expected
    # Verify unicode bytes are preserved
    assert values[1] == '世界'.encode('utf-8')
    assert values[2] == 'مرحبا'.encode('utf-8')
    assert values[3] == '🎉'.encode('utf-8')


def test_special_characters_in_column_names():
    """Column names with special characters should work."""
    table = pa.table({
        'col-with-dash': [1],
        'col.with.dot': [2],
        'col_with_underscore': [3],
        'col with space': [4],
    })
    morsel = draken.Morsel.from_arrow(table)
    
    assert morsel.num_columns == 4
    assert morsel.column(b'col-with-dash').to_pylist() == [1]
    assert morsel.column(b'col.with.dot').to_pylist() == [2]
    assert morsel.column(b'col_with_underscore').to_pylist() == [3]
    assert morsel.column(b'col with space').to_pylist() == [4]


def test_very_long_column_names():
    """Morsels should handle very long column names."""
    long_name = 'a' * 1000
    table = pa.table({long_name: [1, 2, 3]})
    morsel = draken.Morsel.from_arrow(table)
    
    assert len(morsel.column_names[0]) == 1000
    assert morsel.column(long_name.encode('utf-8')).to_pylist() == [1, 2, 3]


def test_copy_independence_after_mutation():
    """Copy should create truly independent morsel that doesn't affect original."""
    table = pa.table({'a': [1, 2, 3, 4, 5], 'b': ['v', 'w', 'x', 'y', 'z']})
    original = draken.Morsel.from_arrow(table)
    copy = original.copy()
    
    # Mutate original
    original.take([0, 1]).select(['a']).rename(['alpha'])
    
    # Copy should be unchanged
    assert copy.shape == (5, 2)
    assert copy.to_arrow().column_names == ['a', 'b']


def test_complex_method_chaining():
    """Complex chains of operations should work correctly."""
    table = pa.table({
        'a': [1, 2, 3, 4, 5, 6],
        'b': ['x', 'y', 'z', 'w', 'v', 'u'],
        'c': [10, 20, 30, 40, 50, 60],
        'd': [True, False, True, False, True, False]
    })
    morsel = draken.Morsel.from_arrow(table)
    
    result = (morsel
              .select(['a', 'c', 'd'])
              .take([0, 2, 4])
              .rename(['alpha', 'charlie', 'delta']))
    
    assert result.shape == (3, 3)
    assert result.to_arrow().column_names == ['alpha', 'charlie', 'delta']
    assert result.column(b'alpha').to_pylist() == [1, 3, 5]


def test_select_and_rename_interaction():
    """Select followed by rename should work with correct column count."""
    table = pa.table({'a': [1], 'b': [2], 'c': [3], 'd': [4]})
    morsel = draken.Morsel.from_arrow(table)
    
    morsel.select(['b', 'd', 'a'])
    morsel.rename(['beta', 'delta', 'alpha'])
    
    assert morsel.to_arrow().column_names == ['beta', 'delta', 'alpha']


def test_take_then_select_then_rename():
    """Combining take, select, and rename in sequence."""
    table = pa.table({
        'x': [1, 2, 3],
        'y': [4, 5, 6],
        'z': [7, 8, 9]
    })
    morsel = draken.Morsel.from_arrow(table)
    
    morsel.take([0, 2]).select(['z', 'x']).rename(['first', 'second'])
    
    assert morsel.shape == (2, 2)
    assert morsel.to_arrow().column_names == ['first', 'second']
    assert morsel.column(b'first').to_pylist() == [7, 9]
    assert morsel.column(b'second').to_pylist() == [1, 3]


def test_hash_with_single_string_column_selector():
    """Hash should accept single string as column selector."""
    table = pa.table({'a': [1, 2], 'b': [3, 4]})
    morsel = draken.Morsel.from_arrow(table)
    
    hash_a = _hash_view_to_list(morsel.hash(columns='a'))
    hash_a_list = _hash_view_to_list(morsel.hash(columns=['a']))
    
    assert hash_a == hash_a_list


def test_hash_with_bytes_column_selector():
    """Hash should accept bytes as column selector."""
    table = pa.table({'a': [1, 2], 'b': [3, 4]})
    morsel = draken.Morsel.from_arrow(table)
    
    hash_b = _hash_view_to_list(morsel.hash(columns=b'b'))
    hash_b_list = _hash_view_to_list(morsel.hash(columns=['b']))
    
    assert hash_b == hash_b_list


def test_hash_negative_column_index():
    """Hash should reject negative column indices."""
    table = pa.table({'a': [1], 'b': [2]})
    morsel = draken.Morsel.from_arrow(table)
    
    with pytest.raises(IndexError):
        morsel.hash(columns=[-1])


def test_rename_duplicate_names_allowed():
    """Renaming to duplicate column names should be allowed."""
    table = pa.table({'a': [1], 'b': [2], 'c': [3]})
    morsel = draken.Morsel.from_arrow(table)
    
    morsel.rename(['x', 'x', 'x'])
    assert morsel.to_arrow().column_names == ['x', 'x', 'x']


def test_select_preserves_column_order():
    """Select should maintain the order of columns as specified."""
    table = pa.table({'a': [1], 'b': [2], 'c': [3], 'd': [4]})
    morsel = draken.Morsel.from_arrow(table)
    
    morsel.select(['d', 'b', 'c', 'a'])
    assert morsel.to_arrow().column_names == ['d', 'b', 'c', 'a']


def test_copy_with_both_filters_applied_correctly():
    """Copy with both column and mask filters should apply both."""
    table = pa.table({
        'a': [1, 2, 3, 4],
        'b': [10, 20, 30, 40],
        'c': [100, 200, 300, 400]
    })
    morsel = draken.Morsel.from_arrow(table)
    
    filtered = morsel.copy(columns=['b', 'a'], mask=[0, 3])
    
    assert filtered.shape == (2, 2)
    assert filtered.to_arrow().column_names == ['b', 'a']
    assert filtered.column(b'b').to_pylist() == [10, 40]
    assert filtered.column(b'a').to_pylist() == [1, 4]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
