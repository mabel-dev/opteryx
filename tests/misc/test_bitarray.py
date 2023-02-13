import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from opteryx.utils.bitarray.bitarray import bitarray


def test_bitarray_init():
    # Test initialization with size
    bits = bitarray(5)
    assert bits.size == 5

    # Test size is zero
    bits = bitarray(0)
    assert bits.size == 0

    # Test size is negative
    with pytest.raises(ValueError):
        bits = bitarray(-1)


def test_bitarray_get_bit():
    bits = bitarray(5)
    bits.array = [0b100100]

    # Test index within range
    assert bits.get(2) == True
    assert bits.get(4) == False

    # Test index out of range
    with pytest.raises(IndexError):
        bits.get(5)


def test_bitarray_set_bit():
    bits = bitarray(5)

    # Test setting bit to 1 within range
    bits.set(2, True)
    assert list(bits.array) == [0b000100], list(bits.array)

    # Test setting bit to 0 within range
    bits.set(2, False)
    assert list(bits.array) == [0b000000]

    # Test index out of range
    with pytest.raises(IndexError):
        bits.set(5, True)


def test_bitarray_functionality():
    bits = bitarray(8)

    # Test setting all bits to 1
    for i in range(8):
        bits.set(i, True)
    assert list(bits.array) == [0b11111111]

    # Test setting all bits to 0
    for i in range(8):
        bits.set(i, False)
    assert list(bits.array) == [0b00000000]

    # Test alternating bits
    for i in range(0, 8, 2):
        bits.set(i, True)
    assert list(bits.array) == [0b01010101]


def test_set_bit_multiple_bits():
    bits = bitarray(16)
    bits.set(0, True)
    bits.set(1, False)
    bits.set(2, True)
    bits.set(3, False)
    bits.set(4, True)
    bits.set(5, False)
    bits.set(6, True)
    bits.set(7, False)

    assert bits.get(0) == True
    assert bits.get(1) == False
    assert bits.get(2) == True
    assert bits.get(3) == False
    assert bits.get(4) == True
    assert bits.get(5) == False
    assert bits.get(6) == True
    assert bits.get(7) == False


if __name__ == "__main__":  # pragma: no cover
    test_bitarray_functionality()
    test_bitarray_set_bit()
    test_bitarray_get_bit()
    test_bitarray_init()
    test_set_bit_multiple_bits()

    print("✅ okay")
