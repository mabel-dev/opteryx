"""
Unit tests for the optimized list_regex_replace Cython function.

These tests verify the correctness of the optimized implementation
independent of the SQL engine.
"""

import numpy as np
import pytest


def test_list_regex_replace_import():
    """Test that the optimized function can be imported."""
    try:
        from opteryx.compiled.list_ops import list_regex_replace
        assert callable(list_regex_replace)
    except ImportError:
        pytest.skip("Compiled list_regex_replace not available (not built yet)")


def test_list_regex_replace_basic():
    """Test basic regex replacement functionality."""
    try:
        from opteryx.compiled.list_ops import list_regex_replace
    except ImportError:
        pytest.skip("Compiled list_regex_replace not available")
    
    # Test data
    data = np.array(['hello world', 'foo bar', 'test 123'], dtype=object)
    pattern = r'([a-z]+)\s([a-z0-9]+)'
    replacement = r'\2_\1'
    
    result = list_regex_replace(data, pattern, replacement)
    
    assert len(result) == 3
    assert result[0] == 'world_hello'
    assert result[1] == 'bar_foo'
    assert result[2] == '123_test'


def test_list_regex_replace_clickbench_pattern():
    """Test with the Clickbench #29 URL extraction pattern."""
    try:
        from opteryx.compiled.list_ops import list_regex_replace
    except ImportError:
        pytest.skip("Compiled list_regex_replace not available")
    
    # URLs like in Clickbench #29
    data = np.array([
        'https://www.example.com/path/to/page',
        'http://test.org/another/path',
        'https://subdomain.example.net/xyz',
        'https://www.google.com/search?q=test',
        ''
    ], dtype=object)
    
    pattern = r'^https?://(?:www\.)?([^/]+)/.*$'
    replacement = r'\1'
    
    result = list_regex_replace(data, pattern, replacement)
    
    assert len(result) == 5
    assert result[0] == 'example.com'
    assert result[1] == 'test.org'
    assert result[2] == 'subdomain.example.net'
    assert result[3] == 'google.com'
    assert result[4] == ''  # No match, returns original


def test_list_regex_replace_bytes_mode():
    """Test regex replacement with bytes pattern and data."""
    try:
        from opteryx.compiled.list_ops import list_regex_replace
    except ImportError:
        pytest.skip("Compiled list_regex_replace not available")
    
    # Bytes data
    data = np.array([
        b'https://www.example.com/path',
        b'http://test.org/page',
        b'https://google.com/search'
    ], dtype=object)
    
    pattern = b'^https?://(?:www\.)?([^/]+)/.*$'
    replacement = b'\\1'
    
    result = list_regex_replace(data, pattern, replacement)
    
    assert len(result) == 3
    assert result[0] == b'example.com'
    assert result[1] == b'test.org'
    assert result[2] == b'google.com'


def test_list_regex_replace_with_none():
    """Test handling of None values."""
    try:
        from opteryx.compiled.list_ops import list_regex_replace
    except ImportError:
        pytest.skip("Compiled list_regex_replace not available")
    
    data = np.array(['test', None, 'hello'], dtype=object)
    pattern = r'test'
    replacement = r'TEST'
    
    result = list_regex_replace(data, pattern, replacement)
    
    assert len(result) == 3
    assert result[0] == 'TEST'
    assert result[1] is None
    assert result[2] == 'hello'


def test_list_regex_replace_no_match():
    """Test behavior when pattern doesn't match."""
    try:
        from opteryx.compiled.list_ops import list_regex_replace
    except ImportError:
        pytest.skip("Compiled list_regex_replace not available")
    
    data = np.array(['abc', 'def', 'ghi'], dtype=object)
    pattern = r'xyz'
    replacement = r'XYZ'
    
    result = list_regex_replace(data, pattern, replacement)
    
    # When there's no match, original strings should be returned
    assert len(result) == 3
    assert result[0] == 'abc'
    assert result[1] == 'def'
    assert result[2] == 'ghi'


def test_list_regex_replace_empty_array():
    """Test with empty input array."""
    try:
        from opteryx.compiled.list_ops import list_regex_replace
    except ImportError:
        pytest.skip("Compiled list_regex_replace not available")
    
    data = np.array([], dtype=object)
    pattern = r'test'
    replacement = r'TEST'
    
    result = list_regex_replace(data, pattern, replacement)
    
    assert len(result) == 0


def test_list_regex_replace_special_chars():
    """Test with special regex characters in replacement."""
    try:
        from opteryx.compiled.list_ops import list_regex_replace
    except ImportError:
        pytest.skip("Compiled list_regex_replace not available")
    
    data = np.array(['email@example.com', 'test@test.org'], dtype=object)
    pattern = r'(.+)@(.+)'
    replacement = r'\1 [at] \2'
    
    result = list_regex_replace(data, pattern, replacement)
    
    assert len(result) == 2
    assert result[0] == 'email [at] example.com'
    assert result[1] == 'test [at] test.org'


if __name__ == "__main__":
    # Run tests manually
    print("Testing list_regex_replace Cython function...")
    
    test_list_regex_replace_import()
    print("✓ Import test passed")
    
    test_list_regex_replace_basic()
    print("✓ Basic functionality test passed")
    
    test_list_regex_replace_clickbench_pattern()
    print("✓ Clickbench pattern test passed")
    
    test_list_regex_replace_bytes_mode()
    print("✓ Bytes mode test passed")
    
    test_list_regex_replace_with_none()
    print("✓ None handling test passed")
    
    test_list_regex_replace_no_match()
    print("✓ No match test passed")
    
    test_list_regex_replace_empty_array()
    print("✓ Empty array test passed")
    
    test_list_regex_replace_special_chars()
    print("✓ Special characters test passed")
    
    print("\nAll tests passed! ✓")
