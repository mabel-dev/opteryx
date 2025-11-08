"""
Tests for opteryx.utils.arrow module
"""

import pyarrow

from opteryx.utils.arrow import align_tables


class TestAlignTables:
    """Tests for the align_tables function"""

    def test_align_tables_normal_path_both_have_columns(self):
        """Test normal path when both tables have columns"""
        source_data = pyarrow.table({"x": [10, 20, 30]})
        append_data = pyarrow.table({"y": [40, 50, 60]})
        source_indices = [0, 1, 2]
        append_indices = [2, 1, 0]

        result = align_tables(source_data, append_data, source_indices, append_indices)

        assert result.num_rows == 3
        assert result.num_columns == 2
        assert result.schema.names == ["x", "y"]
        assert result.to_pydict() == {"x": [10, 20, 30], "y": [60, 50, 40]}

    def test_align_tables_empty_indices(self):
        """Test when indices arrays are empty"""
        source_data = pyarrow.table({"x": [10, 20, 30]})
        append_data = pyarrow.table({"y": [40, 50, 60]})
        source_indices = []
        append_indices = []

        result = align_tables(source_data, append_data, source_indices, append_indices)

        assert result.num_rows == 0
        assert result.num_columns == 2
        assert result.schema.names == ["x", "y"]

    def test_align_tables_with_pyarrow_arrays(self):
        """Test that function works with PyArrow arrays as indices"""
        source_data = pyarrow.table({"a": [1, 2, 3]})
        append_data = pyarrow.table({"b": [4, 5, 6]})
        source_indices = pyarrow.array([0, 1, 2], type=pyarrow.int64())
        append_indices = pyarrow.array([2, 1, 0], type=pyarrow.int64())

        result = align_tables(source_data, append_data, source_indices, append_indices)

        assert result.num_rows == 3
        assert result.num_columns == 2
        assert result.to_pydict() == {"a": [1, 2, 3], "b": [6, 5, 4]}

    def test_align_tables_with_none_indices_outer_join(self):
        """Test that None indices (outer join case) are handled correctly"""
        # This simulates an outer join where some rows don't match
        source_data = pyarrow.table({"x": [10, 20, 30]})
        append_data = pyarrow.table({"y": [40, 50]})
        source_indices = [0, 1, None]  # Third row has no match in source
        append_indices = [0, 1, 1]

        result = align_tables(source_data, append_data, source_indices, append_indices)

        # Should have 3 rows, with null in x for the third row
        assert result.num_rows == 3
        assert result.num_columns == 2
        assert result.schema.names == ["x", "y"]
        result_dict = result.to_pydict()
        assert result_dict["x"][2] is None  # The None index should produce null
        assert result_dict["y"] == [40, 50, 50]

    def test_align_tables_empty_source_table_with_schema(self):
        """Test when source table has columns but no data rows (outer join unmatched case)"""
        # This simulates a RIGHT OUTER JOIN where the left (source) side has no matches
        source_data = pyarrow.table({"satellite_id": pyarrow.array([], type=pyarrow.int64())})
        append_data = pyarrow.table({"planet_id": [1, 2, 3]})
        source_indices = [None, None, None]  # No matches
        append_indices = [0, 1, 2]

        result = align_tables(source_data, append_data, source_indices, append_indices)

        # Should preserve schema from source even though it has no data
        assert result.num_rows == 3
        assert "satellite_id" in result.schema.names
        assert "planet_id" in result.schema.names
        result_dict = result.to_pydict()
        # All satellite_ids should be null
        assert all(v is None for v in result_dict["satellite_id"])
        assert result_dict["planet_id"] == [1, 2, 3]
