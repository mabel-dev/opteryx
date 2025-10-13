"""
Test wildcard support in file paths
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.connectors.file_connector import FileConnector
from opteryx.exceptions import DatasetNotFoundError


class MockStatistics:
    """Mock statistics object for testing"""
    def __init__(self):
        self.bytes_read = 0


def test_wildcard_detection():
    """Test that wildcards are correctly detected"""
    stats = MockStatistics()
    
    # These should be detected as wildcards
    connector = FileConnector(dataset="testdata/wildcard_test/*.parquet", statistics=stats)
    assert connector.has_wildcards is True

    connector = FileConnector(dataset="testdata/wildcard_test/file?.parquet", statistics=stats)
    assert connector.has_wildcards is True

    connector = FileConnector(dataset="testdata/wildcard_test/file[0-9].parquet", statistics=stats)
    assert connector.has_wildcards is True


def test_wildcard_no_matches():
    """Test that wildcard with no matches raises DatasetNotFoundError"""
    stats = MockStatistics()
    
    with pytest.raises(DatasetNotFoundError):
        FileConnector(dataset="nonexistent/path/*.parquet", statistics=stats)


def test_path_traversal_protection():
    """Test that path traversal is still blocked with wildcards"""
    stats = MockStatistics()
    
    # These should raise DatasetNotFoundError due to path traversal
    with pytest.raises(DatasetNotFoundError):
        FileConnector(dataset="../*.parquet", statistics=stats)
    
    with pytest.raises(DatasetNotFoundError):
        FileConnector(dataset="path/../../*.parquet", statistics=stats)
    
    with pytest.raises(DatasetNotFoundError):
        FileConnector(dataset="~/*.parquet", statistics=stats)


def test_wildcard_expansion():
    """Test that wildcards are properly expanded to matching files"""
    stats = MockStatistics()
    pattern = "testdata/wildcard_test/*.parquet"
    
    connector = FileConnector(dataset=pattern, statistics=stats)
    
    # Check that all files were found
    assert len(connector.files) == 3
    assert connector.has_wildcards is True
    
    # Check files are sorted
    expected_files = sorted([
        "testdata/wildcard_test/file1.parquet",
        "testdata/wildcard_test/file2.parquet",
        "testdata/wildcard_test/file3.parquet"
    ])
    assert connector.files == expected_files


def test_single_file_no_wildcard():
    """Test that single files still work without wildcards"""
    stats = MockStatistics()
    test_file = "testdata/wildcard_test/file1.parquet"
    
    connector = FileConnector(dataset=test_file, statistics=stats)
    
    assert connector.has_wildcards is False
    assert connector.files == [test_file]


def test_wildcard_range_pattern():
    """Test wildcard with range patterns like [0-9]"""
    stats = MockStatistics()
    pattern = "testdata/wildcard_test/file[1-3].parquet"
    
    connector = FileConnector(dataset=pattern, statistics=stats)
    
    # Should match files 1, 2, 3 (all 3 files)
    assert len(connector.files) == 3
    expected_files = sorted([
        "testdata/wildcard_test/file1.parquet",
        "testdata/wildcard_test/file2.parquet",
        "testdata/wildcard_test/file3.parquet"
    ])
    assert connector.files == expected_files


def test_wildcard_question_mark():
    """Test wildcard with ? (single character match)"""
    stats = MockStatistics()
    # Use ? to match single digit in filename
    pattern = "testdata/wildcard_test/file?.parquet"
    
    connector = FileConnector(dataset=pattern, statistics=stats)
    
    # Should match all 3 files (file1, file2, file3)
    assert len(connector.files) == 3
    expected_files = sorted([
        "testdata/wildcard_test/file1.parquet",
        "testdata/wildcard_test/file2.parquet",
        "testdata/wildcard_test/file3.parquet"
    ])
    assert connector.files == expected_files


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
