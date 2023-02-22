import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from opteryx.utils.bitarray.bitarray import bitarray


import pytest


def test_init_size():
    with pytest.raises(AssertionError, match="bitarray size must be a positive integer"):
        bitarray(0)


def test_get_index_error():
    b = bitarray(8)
    with pytest.raises(IndexError, match="Index out of range"):
        print(b.get(8))
    with pytest.raises(IndexError, match="Index out of range"):
        print(b.get(-1))


def test_set_index_error():
    b = bitarray(8)
    with pytest.raises(IndexError, match="Index out of range"):
        b.set(8, 1)
    with pytest.raises(IndexError, match="Index out of range"):
        b.set(-1, 1)


def test_get_set():
    b = bitarray(8)
    b.set(2, 1)
    assert b.get(2) is True
    b.set(2, 0)
    assert b.get(2) is False


def test_bits_representation():
    b = bitarray(8)
    b.set(2, 1)
    b.set(4, 1)
    assert b.array == bytearray([20]), b.array


if __name__ == "__main__":  # pragma: no cover
    test_bits_representation()
    test_get_index_error()
    test_get_set()
    test_init_size()
    test_set_index_error()

    print("✅ okay")
