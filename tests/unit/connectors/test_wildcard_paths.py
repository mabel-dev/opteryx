"""
Test wildcard support in file paths
"""

import os
import sys
import tempfile

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
    connector = FileConnector(dataset="path/*.parquet", statistics=stats)
    assert connector.has_wildcards is True
    
    connector = FileConnector(dataset="path/file?.parquet", statistics=stats)
    assert connector.has_wildcards is True
    
    connector = FileConnector(dataset="path/file[0-9].parquet", statistics=stats)
    assert connector.has_wildcards is True


def test_wildcard_no_matches():
    """Test that wildcard with no matches raises DatasetNotFoundError"""
    stats = MockStatistics()
    
    with pytest.raises(DatasetNotFoundError):
        FileConnector(dataset="/nonexistent/path/*.parquet", statistics=stats)


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
    # Create temporary test files
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create some test files
        test_files = [
            os.path.join(tmpdir, "file1.txt"),
            os.path.join(tmpdir, "file2.txt"),
            os.path.join(tmpdir, "file3.txt"),
        ]
        for f in test_files:
            with open(f, "w") as fp:
                fp.write("test content")
        
        stats = MockStatistics()
        pattern = os.path.join(tmpdir, "*.txt")
        
        connector = FileConnector(dataset=pattern, statistics=stats)
        
        # Check that all files were found
        assert len(connector.files) == 3
        assert connector.has_wildcards is True
        
        # Check files are sorted
        assert connector.files == sorted(test_files)


def test_single_file_no_wildcard():
    """Test that single files still work without wildcards"""
    with tempfile.TemporaryDirectory() as tmpdir:
        test_file = os.path.join(tmpdir, "test.txt")
        with open(test_file, "w") as fp:
            fp.write("test content")
        
        stats = MockStatistics()
        connector = FileConnector(dataset=test_file, statistics=stats)
        
        assert connector.has_wildcards is False
        assert connector.files == [test_file]


def test_wildcard_range_pattern():
    """Test wildcard with range patterns like [0-9]"""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create files matching a range pattern
        test_files = []
        for i in range(5):
            f = os.path.join(tmpdir, f"file{i}.txt")
            with open(f, "w") as fp:
                fp.write("test")
            test_files.append(f)
        
        # Create a file that shouldn't match
        non_match = os.path.join(tmpdir, "fileX.txt")
        with open(non_match, "w") as fp:
            fp.write("test")
        
        stats = MockStatistics()
        pattern = os.path.join(tmpdir, "file[0-9].txt")
        
        connector = FileConnector(dataset=pattern, statistics=stats)
        
        # Should match only files with digits
        assert len(connector.files) == 5
        assert all("file" in f and any(str(i) in f for i in range(5)) for f in connector.files)
        assert non_match not in connector.files


def test_wildcard_question_mark():
    """Test wildcard with ? (single character match)"""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create files
        file1 = os.path.join(tmpdir, "fileA.txt")
        file2 = os.path.join(tmpdir, "fileB.txt")
        file_no_match = os.path.join(tmpdir, "fileAB.txt")
        
        for f in [file1, file2, file_no_match]:
            with open(f, "w") as fp:
                fp.write("test")
        
        stats = MockStatistics()
        pattern = os.path.join(tmpdir, "file?.txt")
        
        connector = FileConnector(dataset=pattern, statistics=stats)
        
        # Should match only single-character files
        assert len(connector.files) == 2
        assert file1 in connector.files
        assert file2 in connector.files
        assert file_no_match not in connector.files


if __name__ == "__main__":  # pragma: no cover
    import sys
    
    # Run tests
    pytest.main([__file__, "-v"])
