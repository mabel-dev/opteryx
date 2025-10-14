"""
Test wildcard path utilities
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.utils import paths


def test_has_wildcards():
    """Test wildcard detection"""
    assert paths.has_wildcards("bucket/path/*.parquet") is True
    assert paths.has_wildcards("bucket/path/file?.parquet") is True
    assert paths.has_wildcards("bucket/path/file[0-9].parquet") is True
    assert paths.has_wildcards("bucket/path/data.parquet") is False
    assert paths.has_wildcards("bucket/path/") is False


def test_split_wildcard_path():
    """Test splitting wildcard paths into prefix and pattern"""
    # Asterisk wildcard
    prefix, pattern = paths.split_wildcard_path("bucket/path/*.parquet")
    assert prefix == "bucket/path/"
    assert pattern == "bucket/path/*.parquet"
    
    # Question mark wildcard
    prefix, pattern = paths.split_wildcard_path("bucket/path/file?.parquet")
    assert prefix == "bucket/path/"
    assert pattern == "bucket/path/file?.parquet"
    
    # Range wildcard
    prefix, pattern = paths.split_wildcard_path("bucket/path/file[0-9].parquet")
    assert prefix == "bucket/path/"
    assert pattern == "bucket/path/file[0-9].parquet"
    
    # Wildcard in middle of path
    prefix, pattern = paths.split_wildcard_path("bucket/*/data.parquet")
    assert prefix == "bucket/"
    assert pattern == "bucket/*/data.parquet"
    
    # Multiple wildcards
    prefix, pattern = paths.split_wildcard_path("bucket/path*/sub*/*.parquet")
    assert prefix == "bucket/"
    assert pattern == "bucket/path*/sub*/*.parquet"
    
    # No wildcards
    prefix, pattern = paths.split_wildcard_path("bucket/path/data.parquet")
    assert prefix == "bucket/path/data.parquet"
    assert pattern == "bucket/path/data.parquet"


def test_match_wildcard():
    """
    Test wildcard pattern matching with glob-like semantics.
    
    Glob-like semantics means wildcards don't cross directory boundaries:
    - * matches any characters except path separators
    - ? matches single character except path separators  
    - [range] matches character range except path separators
    """
    # Asterisk matches multiple characters (but not path separators)
    assert paths.match_wildcard("bucket/path/*.parquet", "bucket/path/file1.parquet") is True
    assert paths.match_wildcard("bucket/path/*.parquet", "bucket/path/file2.parquet") is True
    assert paths.match_wildcard("bucket/path/*.parquet", "bucket/path/data.csv") is False
    
    # Asterisk does NOT match across directory boundaries (glob-like behavior)
    assert paths.match_wildcard("bucket/path/*.parquet", "bucket/path/subdir/file.parquet") is False
    
    # Question mark matches single character (but not path separators)
    assert paths.match_wildcard("bucket/path/file?.parquet", "bucket/path/file1.parquet") is True
    assert paths.match_wildcard("bucket/path/file?.parquet", "bucket/path/file2.parquet") is True
    assert paths.match_wildcard("bucket/path/file?.parquet", "bucket/path/file10.parquet") is False
    
    # Range matches character range
    assert paths.match_wildcard("bucket/path/file[0-9].parquet", "bucket/path/file1.parquet") is True
    assert paths.match_wildcard("bucket/path/file[0-9].parquet", "bucket/path/file5.parquet") is True
    assert paths.match_wildcard("bucket/path/file[0-9].parquet", "bucket/path/fileA.parquet") is False
    
    # Wildcard in middle of path
    assert paths.match_wildcard("bucket/*/data.parquet", "bucket/subdir/data.parquet") is True
    assert paths.match_wildcard("bucket/*/data.parquet", "bucket/a/b/data.parquet") is False
    
    # No wildcards - exact match
    assert paths.match_wildcard("bucket/path/data.parquet", "bucket/path/data.parquet") is True
    assert paths.match_wildcard("bucket/path/data.parquet", "bucket/path/other.parquet") is False


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
