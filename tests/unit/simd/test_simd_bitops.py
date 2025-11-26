"""
Tests for SIMD-accelerated bit mask operations.

This module tests the SIMD implementations in src/cpp/simd_bitops.cpp:
- simd_and_mask() - Bitwise AND
- simd_or_mask() - Bitwise OR
- simd_xor_mask() - Bitwise XOR
- simd_not_mask() - Bitwise NOT
- simd_popcount() - Count set bits

Tests verify correctness across different input sizes to exercise:
- AVX2 path (32-63 bytes)
- NEON path (16-31 bytes on ARM)
- Scalar fallback (<16 bytes)
"""

import pytest


@pytest.fixture
def maskops_module():
    """Import the maskops module or skip if not available."""
    try:
        from opteryx.draken.compiled import maskops
        return maskops
    except ImportError:
        pytest.skip("maskops module not available")


class TestSIMDBitOps:
    """Test SIMD-accelerated bit mask operations."""

    def test_and_mask_basic(self, maskops_module):
        """Test basic AND operation."""
        a = bytes([0xFF, 0xAA, 0x55, 0x00])
        b = bytes([0xF0, 0x0F, 0xFF, 0x00])
        expected = bytes([0xF0, 0x0A, 0x55, 0x00])
        
        result = maskops_module.and_mask(a, b, 4)
        assert result == expected

    def test_or_mask_basic(self, maskops_module):
        """Test basic OR operation."""
        a = bytes([0xF0, 0xAA, 0x55, 0x00])
        b = bytes([0x0F, 0x55, 0xAA, 0x00])
        expected = bytes([0xFF, 0xFF, 0xFF, 0x00])
        
        result = maskops_module.or_mask(a, b, 4)
        assert result == expected

    def test_xor_mask_basic(self, maskops_module):
        """Test basic XOR operation."""
        a = bytes([0xFF, 0xAA, 0x55, 0x00])
        b = bytes([0xF0, 0xAA, 0x55, 0xFF])
        expected = bytes([0x0F, 0x00, 0x00, 0xFF])
        
        result = maskops_module.xor_mask(a, b, 4)
        assert result == expected

    def test_not_mask_basic(self, maskops_module):
        """Test basic NOT operation."""
        a = bytes([0xFF, 0x00, 0xAA, 0x55])
        expected = bytes([0x00, 0xFF, 0x55, 0xAA])
        
        result = maskops_module.not_mask(a, 4)
        assert result == expected

    def test_and_mask_long(self, maskops_module):
        """Test AND with long input (64+ bytes) to exercise the vectorized path (AVX2/NEON)."""
        # Create 128 bytes of data
        a = bytes([0xAA] * 128)
        b = bytes([0x55] * 128)
        expected = bytes([0x00] * 128)
        
        result = maskops_module.and_mask(a, b, 128)
        assert result == expected

    def test_or_mask_long(self, maskops_module):
        """Test OR with long input (64+ bytes) to exercise the vectorized path (AVX2/NEON)."""
        # Create 128 bytes of data
        a = bytes([0xAA] * 128)
        b = bytes([0x55] * 128)
        expected = bytes([0xFF] * 128)
        
        result = maskops_module.or_mask(a, b, 128)
        assert result == expected

    def test_xor_mask_long(self, maskops_module):
        """Test XOR with long input (64+ bytes) to exercise the vectorized path (AVX2/NEON)."""
        # Create 128 bytes of data
        a = bytes([0xAA] * 128)
        b = bytes([0xAA] * 128)
        expected = bytes([0x00] * 128)
        
        result = maskops_module.xor_mask(a, b, 128)
        assert result == expected

    def test_not_mask_long(self, maskops_module):
        """Test NOT with long input (64+ bytes) to exercise the vectorized path (AVX2/NEON)."""
        # Create 128 bytes of data
        a = bytes([0x00] * 64 + [0xFF] * 64)
        expected = bytes([0xFF] * 64 + [0x00] * 64)
        
        result = maskops_module.not_mask(a, 128)
        assert result == expected

    def test_popcount_basic(self, maskops_module):
        """Test basic popcount operation."""
        # 0xFF has 8 bits set, 0xAA has 4 bits set (10101010)
        a = bytes([0xFF, 0xAA, 0x00, 0x01])
        expected = 8 + 4 + 0 + 1  # 13
        
        result = maskops_module.popcount_mask(a, 4)
        assert result == expected

    def test_popcount_all_zeros(self, maskops_module):
        """Test popcount with all zeros."""
        a = bytes([0x00] * 64)
        result = maskops_module.popcount_mask(a, 64)
        assert result == 0

    def test_popcount_all_ones(self, maskops_module):
        """Test popcount with all ones."""
        a = bytes([0xFF] * 64)
        expected = 64 * 8  # 512 bits
        result = maskops_module.popcount_mask(a, 64)
        assert result == expected

    def test_popcount_long(self, maskops_module):
        """Test popcount with long input (64+ bytes) to exercise the vectorized path (AVX2/NEON)."""
        # Alternating pattern: 0xAA = 10101010 (4 bits set)
        a = bytes([0xAA] * 128)
        expected = 128 * 4  # 512 bits
        result = maskops_module.popcount_mask(a, 128)
        assert result == expected

    def test_and_mask_identity(self, maskops_module):
        """Test AND with identity (all ones)."""
        a = bytes([0x12, 0x34, 0x56, 0x78])
        b = bytes([0xFF] * 4)
        
        result = maskops_module.and_mask(a, b, 4)
        assert result == a

    def test_or_mask_identity(self, maskops_module):
        """Test OR with identity (all zeros)."""
        a = bytes([0x12, 0x34, 0x56, 0x78])
        b = bytes([0x00] * 4)
        
        result = maskops_module.or_mask(a, b, 4)
        assert result == a

    def test_xor_mask_self_cancels(self, maskops_module):
        """Test XOR with itself produces zeros."""
        a = bytes([0x12, 0x34, 0x56, 0x78])
        expected = bytes([0x00] * 4)
        
        result = maskops_module.xor_mask(a, a, 4)
        assert result == expected

    def test_not_mask_double_negation(self, maskops_module):
        """Test that NOT applied twice returns original."""
        a = bytes([0x12, 0x34, 0x56, 0x78])
        
        result1 = maskops_module.not_mask(a, 4)
        result2 = maskops_module.not_mask(result1, 4)
        assert result2 == a

    def test_operations_with_medium_size(self, maskops_module):
        """Test operations with medium size (32-63 bytes) to exercise AVX2."""
        # 48 bytes - fits in AVX2 but requires multiple iterations
        a = bytes([0xAA] * 48)
        b = bytes([0x55] * 48)
        
        # AND should produce all zeros
        result_and = maskops_module.and_mask(a, b, 48)
        assert result_and == bytes([0x00] * 48)
        
        # OR should produce all ones
        result_or = maskops_module.or_mask(a, b, 48)
        assert result_or == bytes([0xFF] * 48)

    def test_operations_with_unaligned_size(self, maskops_module):
        """Test operations with size not aligned to SIMD width."""
        # 67 bytes - not aligned to 64, tests tail handling
        a = bytes([0xFF] * 67)
        b = bytes([0x00] * 67)
        
        result = maskops_module.and_mask(a, b, 67)
        assert result == bytes([0x00] * 67)
        
        result = maskops_module.or_mask(a, b, 67)
        assert result == bytes([0xFF] * 67)