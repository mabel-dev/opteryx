"""
Integration tests for wildcard support in file paths
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

# Skip this if opteryx is not properly installed
try:
    import opteryx
except ImportError:
    pytest.skip("opteryx not installed", allow_module_level=True)


def test_wildcard_asterisk():
    """Test SELECT with * wildcard in path"""
    result = opteryx.query("SELECT COUNT(*) FROM 'testdata/wildcard_test/*.parquet'")
    
    # Should read from all 3 parquet files
    # Each file has 100000 rows, so total should be 300000
    count = result.arrow().column(0)[0].as_py()
    assert count == 300000, f"Expected 300000 rows, got {count}"


def test_wildcard_question_mark_range():
    """Test SELECT with range wildcard [1-2] in path"""
    result = opteryx.query("SELECT COUNT(*) FROM 'testdata/wildcard_test/file[1-2].parquet'")
    
    # Should read from file1 and file2 only (200000 rows total)
    count = result.arrow().column(0)[0].as_py()
    assert count == 200000, f"Expected 200000 rows, got {count}"


def test_wildcard_specific_columns():
    """Test SELECT specific columns with wildcard path"""
    result = opteryx.query("SELECT user_name FROM 'testdata/wildcard_test/*.parquet' LIMIT 5")
    
    # Should return results
    assert result.rowcount == 5
    assert "user_name" in result.column_names


def test_wildcard_with_where_clause():
    """Test SELECT with WHERE clause and wildcard path"""
    result = opteryx.query(
        "SELECT user_name, user_verified FROM 'testdata/wildcard_test/*.parquet' "
        "WHERE user_name ILIKE '%news%'"
    )
    
    # Should read from all files and filter
    # Original single file has 122 matching rows, so 3 files should have 366
    assert result.rowcount == 366, f"Expected 366 rows, got {result.rowcount}"


def test_wildcard_no_matches():
    """Test that wildcard with no matches raises appropriate error"""
    with pytest.raises(Exception):  # Should raise DatasetNotFoundError
        opteryx.query("SELECT * FROM 'testdata/nonexistent/*.parquet'")


def test_wildcard_path_traversal_blocked():
    """Test that path traversal is blocked even with wildcards"""
    with pytest.raises(Exception):  # Should raise DatasetNotFoundError
        opteryx.query("SELECT * FROM '../*.parquet'")


if __name__ == "__main__":  # pragma: no cover
    # Run tests
    pytest.main([__file__, "-v"])
