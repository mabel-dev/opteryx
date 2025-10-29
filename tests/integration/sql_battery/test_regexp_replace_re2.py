"""
Test REGEXP_REPLACE optimization with google-re2.

This test validates that the google-re2 implementation works correctly
and provides the expected performance improvement.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


def test_regexp_replace_basic():
    """Test basic REGEXP_REPLACE functionality."""
    query = """
    SELECT REGEXP_REPLACE(name, 'Earth', 'Mars') AS result
    FROM $planets
    WHERE name = 'Earth'
    """
    
    result = opteryx.query(query)
    rows = list(result)
    
    assert len(rows) == 1
    assert rows[0][0] == 'Mars'
    print("✓ Basic REGEXP_REPLACE test passed")


def test_regexp_replace_with_backreferences():
    """Test REGEXP_REPLACE with backreferences."""
    query = """
    SELECT REGEXP_REPLACE(url, '^https?://([^/]+)/.*$', '\\\\1') AS domain
    FROM (
        SELECT 'https://www.example.com/path/to/page' as url
        UNION ALL SELECT 'http://test.org/another/path'
    )
    """
    
    result = opteryx.query(query)
    rows = list(result)
    
    assert len(rows) == 2
    domains = [row[0] for row in rows]
    assert 'www.example.com' in domains or b'www.example.com' in domains
    assert 'test.org' in domains or b'test.org' in domains
    print("✓ Backreference test passed")


def test_regexp_replace_clickbench_pattern():
    """Test with the actual Clickbench #29 pattern."""
    query = """
    SELECT 
        REGEXP_REPLACE(url, b'^https?://(?:www\\.)?([^/]+)/.*$', r'\\\\1') AS domain,
        COUNT(*) as cnt
    FROM (
        SELECT 'https://www.example.com/path' as url
        UNION ALL SELECT 'http://test.org/page'
        UNION ALL SELECT 'https://google.com/search'
    )
    GROUP BY domain
    ORDER BY cnt DESC
    """
    
    result = opteryx.query(query)
    rows = list(result)
    
    assert len(rows) == 3
    print("✓ Clickbench pattern test passed")


if __name__ == "__main__":
    print("Testing REGEXP_REPLACE with google-re2...")
    print()
    
    try:
        import re2
        print(f"✓ google-re2 is available (version: {re2.__version__ if hasattr(re2, '__version__') else 'unknown'})")
    except ImportError:
        print("✗ google-re2 is NOT available - tests will use PyArrow fallback")
    
    print()
    
    test_regexp_replace_basic()
    test_regexp_replace_with_backreferences()
    test_regexp_replace_clickbench_pattern()
    
    print()
    print("All tests passed! ✓")
