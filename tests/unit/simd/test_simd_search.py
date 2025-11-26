"""
Tests for SIMD-accelerated search operations.

This module tests the SIMD implementations in src/cpp/simd_search.cpp:
- avx_search() / neon_search() - Find first occurrence
- avx_find_all() / neon_find_all() - Find all occurrences
- avx_count() / neon_count() - Count occurrences
- avx_find_delimiter() / neon_find_delimiter() - Find JSON delimiters

Tests verify correctness across different input sizes to exercise:
- AVX2 path (32-63 bytes)
- NEON path (16-31 bytes on ARM)
- Scalar fallback (<16 bytes)
"""

import pytest


@pytest.fixture
def strings_module():
    """Import the strings module or skip if not available."""
    try:
        from opteryx.compiled.functions import strings
        return strings
    except ImportError:
        pytest.skip("Cython strings module not available")


class TestSIMDSearch:
    """Test SIMD-accelerated search operations."""

    def test_search_basic(self, strings_module):
        """Test basic character search."""
        # Search for 'o' in "hello world"
        data = b"hello world"
        result = strings_module.find_char(data, ord('o'))
        assert result == 4  # First 'o' is at index 4

    def test_search_not_found(self, strings_module):
        """Test search when character is not found."""
        
        data = b"hello world"
        result = strings_module.find_char(data, ord('x'))
        assert result == -1  # Not found

    def test_search_at_start(self, strings_module):
        """Test search when character is at the start."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        data = b"hello world"
        result = strings_module.find_char(data, ord('h'))
        assert result == 0

    def test_search_at_end(self, strings_module):
        """Test search when character is at the end."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        data = b"hello world"
        result = strings_module.find_char(data, ord('d'))
        assert result == 10

    def test_search_long_string(self, strings_module):
        """Test search with long string (64+ bytes) to exercise vectorized path (AVX2/NEON)."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        # Create a 100-byte string with target at position 80
        data = b"a" * 80 + b"x" + b"a" * 19
        result = strings_module.find_char(data, ord('x'))
        assert result == 80

    def test_search_repeated_chars(self, strings_module):
        """Test that search returns first occurrence."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        data = b"aaaxaaaxaaa"
        result = strings_module.find_char(data, ord('x'))
        assert result == 3  # First 'x' is at index 3

    def test_count_basic(self, strings_module):
        """Test basic character counting."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        data = b"hello world"
        result = strings_module.count_char(data, ord('l'))
        assert result == 3  # Three 'l's in "hello world"

    def test_count_none(self, strings_module):
        """Test counting when character doesn't exist."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        data = b"hello world"
        result = strings_module.count_char(data, ord('x'))
        assert result == 0

    def test_count_all(self, strings_module):
        """Test counting when all characters match."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        data = b"aaaa"
        result = strings_module.count_char(data, ord('a'))
        assert result == 4

    def test_count_long_string(self, strings_module):
        """Test counting with long string (64+ bytes) to exercise vectorized path (AVX2/NEON)."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        # Create a 100-byte string with known number of 'x's
        data = b"ax" * 50  # 50 'x's
        result = strings_module.count_char(data, ord('x'))
        assert result == 50

    def test_find_all_basic(self, strings_module):
        """Test finding all occurrences."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        data = b"hello world"
        result = strings_module.find_all_char(data, ord('l'))
        assert result == [2, 3, 9]  # All positions of 'l'

    def test_find_all_none(self, strings_module):
        """Test find_all when character doesn't exist."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        data = b"hello world"
        result = strings_module.find_all_char(data, ord('x'))
        assert result == []

    def test_find_all_single(self, strings_module):
        """Test find_all with single occurrence."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        data = b"hello world"
        result = strings_module.find_all_char(data, ord('h'))
        assert result == [0]

    def test_find_all_long_string(self, strings_module):
        """Test find_all with long string (64+ bytes) to exercise vectorized path (AVX2/NEON)."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        # Create pattern with known positions
        data = b"a" * 10 + b"x" + b"a" * 20 + b"x" + b"a" * 30 + b"x" + b"a" * 10
        result = strings_module.find_all_char(data, ord('x'))
        assert result == [10, 31, 62]

    def test_search_newline(self, strings_module):
        """Test searching for newline character."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        data = b"line1\nline2\nline3"
        result = strings_module.find_char(data, ord('\n'))
        assert result == 5  # First newline

    def test_count_newlines(self, strings_module):
        """Test counting newline characters."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        data = b"line1\nline2\nline3\n"
        result = strings_module.count_char(data, ord('\n'))
        assert result == 3

    def test_find_all_newlines(self, strings_module):
        """Test finding all newline positions."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        data = b"line1\nline2\nline3"
        result = strings_module.find_all_char(data, ord('\n'))
        assert result == [5, 11]

    def test_search_empty_string(self, strings_module):
        """Test search in empty string."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        data = b""
        result = strings_module.find_char(data, ord('a'))
        assert result == -1

    def test_count_empty_string(self, strings_module):
        """Test count in empty string."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        data = b""
        result = strings_module.count_char(data, ord('a'))
        assert result == 0

    def test_find_all_empty_string(self, strings_module):
        """Test find_all in empty string."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        data = b""
        result = strings_module.find_all_char(data, ord('a'))
        assert result == []

    def test_search_with_null_byte(self, strings_module):
        """Test search for null byte."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        data = b"hello\x00world"
        result = strings_module.find_char(data, 0)
        assert result == 5

    def test_search_special_chars(self, strings_module):
        """Test search for special characters."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        data = b"tab\there"
        result = strings_module.find_char(data, ord('\t'))
        assert result == 3

    def test_search_high_byte_value(self, strings_module):
        """Test search for high byte values (>127)."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        data = b"abc\xff\xfexyz"
        result = strings_module.find_char(data, 0xFF)
        assert result == 3