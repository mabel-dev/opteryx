"""
Tests for SIMD-accelerated string operations.

This module tests the SIMD implementations in src/cpp/simd_string_ops.cpp:
- simd_to_upper() - ASCII uppercase conversion
- simd_to_lower() - ASCII lowercase conversion

Tests verify correctness across different input sizes to exercise:
- AVX512 path (64+ bytes)
- AVX2 path (32-63 bytes)
- Scalar fallback (<32 bytes)
"""

import pytest


class TestSIMDStringOps:
    """Test SIMD-accelerated string case conversion operations."""

    def test_to_upper_basic(self):
        """Test basic uppercase conversion."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        # Test basic ASCII lowercase to uppercase
        test_str = b"hello world"
        result = strings.to_upper(test_str)
        assert result == b"HELLO WORLD"

    def test_to_lower_basic(self):
        """Test basic lowercase conversion."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        # Test basic ASCII uppercase to lowercase
        test_str = b"HELLO WORLD"
        result = strings.to_lower(test_str)
        assert result == b"hello world"

    def test_to_upper_mixed_case(self):
        """Test uppercase conversion with mixed case input."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        test_str = b"HeLLo WoRLd 123!@#"
        result = strings.to_upper(test_str)
        assert result == b"HELLO WORLD 123!@#"

    def test_to_lower_mixed_case(self):
        """Test lowercase conversion with mixed case input."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        test_str = b"HeLLo WoRLd 123!@#"
        result = strings.to_lower(test_str)
        assert result == b"hello world 123!@#"

    def test_to_upper_long_string(self):
        """Test with long string (64+ bytes) to exercise AVX512 path."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        # 80 characters - exercises AVX512 path
        test_str = b"this is a very long string designed to test the avx512 simd implementation path"
        result = strings.to_upper(test_str)
        assert result == b"THIS IS A VERY LONG STRING DESIGNED TO TEST THE AVX512 SIMD IMPLEMENTATION PATH"

    def test_to_lower_long_string(self):
        """Test with long string (64+ bytes) to exercise AVX512 path."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        # 80 characters - exercises AVX512 path
        test_str = b"THIS IS A VERY LONG STRING DESIGNED TO TEST THE AVX512 SIMD IMPLEMENTATION PATH"
        result = strings.to_lower(test_str)
        assert result == b"this is a very long string designed to test the avx512 simd implementation path"

    def test_to_upper_empty_string(self):
        """Test with empty string."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        test_str = b""
        result = strings.to_upper(test_str)
        assert result == b""

    def test_to_upper_repeated_pattern(self):
        """Test with repeated pattern to stress SIMD lanes."""
        try:
            from opteryx.compiled.functions import strings
        except ImportError:
            pytest.skip("Cython strings module not available")
        
        # Repeat pattern to fill multiple SIMD registers
        test_str = b"abcdefgh" * 20  # 160 bytes
        expected = b"ABCDEFGH" * 20
        result = strings.to_upper(test_str)
        assert result == expected
