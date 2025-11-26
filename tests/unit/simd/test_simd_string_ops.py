"""
Tests for SIMD-accelerated string operations.

This module tests the SIMD implementations in src/cpp/simd_string_ops.cpp:
- simd_to_upper() - ASCII uppercase conversion
- simd_to_lower() - ASCII lowercase conversion

Tests verify correctness across different input sizes to exercise:
- AVX2 path (32-63 bytes)
- Scalar fallback (<32 bytes)
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


class TestSIMDStringOps:
    """Test SIMD-accelerated string case conversion operations."""

    def test_to_upper_basic(self, strings_module):
        """Test basic uppercase conversion."""
        test_str = b"hello world"
        result = strings_module.to_upper(test_str)
        assert result == b"HELLO WORLD"

    def test_to_lower_basic(self, strings_module):
        """Test basic lowercase conversion."""
        test_str = b"HELLO WORLD"
        result = strings_module.to_lower(test_str)
        assert result == b"hello world"

    def test_to_upper_mixed_case(self, strings_module):
        """Test uppercase conversion with mixed case input."""
        test_str = b"HeLLo WoRLd 123!@#"
        result = strings_module.to_upper(test_str)
        assert result == b"HELLO WORLD 123!@#"

    def test_to_lower_mixed_case(self, strings_module):
        """Test lowercase conversion with mixed case input."""
        test_str = b"HeLLo WoRLd 123!@#"
        result = strings_module.to_lower(test_str)
        assert result == b"hello world 123!@#"

    def test_to_upper_long_string(self, strings_module):
        """Test with long string (64+ bytes) to exercise the vectorized path (AVX2/NEON)."""
        # 80 characters - exercises the vectorized path (AVX2/NEON)
        test_str = b"this is a very long string designed to test the avx512 simd implementation path"
        result = strings_module.to_upper(test_str)
        assert result == b"THIS IS A VERY LONG STRING DESIGNED TO TEST THE AVX512 SIMD IMPLEMENTATION PATH"

    def test_to_lower_long_string(self, strings_module):
        """Test with long string (64+ bytes) to exercise the vectorized path (AVX2/NEON)."""
        # 80 characters - exercises the vectorized path (AVX2/NEON)
        test_str = b"THIS IS A VERY LONG STRING DESIGNED TO TEST THE AVX512 SIMD IMPLEMENTATION PATH"
        result = strings_module.to_lower(test_str)
        assert result == b"this is a very long string designed to test the avx512 simd implementation path"

    def test_to_upper_empty_string(self, strings_module):
        """Test with empty string."""
        test_str = b""
        result = strings_module.to_upper(test_str)
        assert result == b""

    def test_to_upper_repeated_pattern(self, strings_module):
        """Test with repeated pattern to stress SIMD lanes."""
        # Repeat pattern to fill multiple SIMD registers
        test_str = b"abcdefgh" * 20  # 160 bytes
        expected = b"ABCDEFGH" * 20
        result = strings_module.to_upper(test_str)
        assert result == expected