# type:ignore
import pytest

import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from uintset import count_ones, get_bit, set_bit, unset_bit, find_ones


@pytest.mark.parametrize(
    "bigint, want",
    [
        (0, 0),
        (1, 1),
        (0b10, 1),
        (0b11, 2),
        (0b1_0101_0101, 5),
        (2 ** 64, 1),
        (2 ** 64 - 1, 64),
    ],
)
def test_count_ones(bigint, want):
    got = count_ones(bigint)
    assert got == want


@pytest.mark.parametrize(
    "bigint, index, want",
    [
        (0, 0, 0),
        (0, 1, 0),
        (0, 100, 0),
        (1, 0, 1),
        (1, 1, 0),
        (0b10, 0, 0),
        (0b10, 1, 1),
        (0b1_0101_0101, 2, 1),
        (0b1_0101_0101, 3, 0),
        (0b1_0101_0101, 7, 0),
        (0b1_0101_0101, 8, 1),
        (2 ** 64, 0, 0),
        (2 ** 64, 64, 1),
        (2 ** 64 - 1, 0, 1),
        (2 ** 64 - 1, 1, 1),
        (2 ** 64 - 1, 63, 1),
    ],
)
def test_get_bit(bigint, index, want):
    got = get_bit(bigint, index)
    assert got == want


@pytest.mark.parametrize(
    "bigint, index, want",
    [
        (0, 0, 1),
        (1, 0, 1),
        (0, 8, 0b1_0000_0000),
        (1, 8, 0b1_0000_0001),
        (0b10, 0, 0b11),
        (0b11, 1, 0b11),
        (0b1_0101_0101, 1, 0b1_0101_0111),
        (0b1_0101_0101, 2, 0b1_0101_0101),
        (0b1_0101_0101, 7, 0b1_1101_0101),
        (0b1_0101_0101, 9, 0b11_0101_0101),
        (2 ** 64, 0, 2 ** 64 + 1),
        (2 ** 64, 64, 2 ** 64),
        (2 ** 64, 65, 2 ** 65 + 2 ** 64),
    ],
)
def test_set_bit(bigint, index, want):
    got = set_bit(bigint, index)
    assert got == want


@pytest.mark.parametrize(
    "bigint, index, want",
    [
        (0, 0, 0),
        (1, 0, 0),
        (0, 8, 0),
        (0b10, 0, 0b10),
        (0b11, 1, 0b01),
        (0b1_0101_0101, 0, 0b1_0101_0100),
        (0b1_0101_0101, 1, 0b1_0101_0101),
        (0b1_0101_0101, 2, 0b1_0101_0001),
        (0b1_0101_0101, 8, 0b0_0101_0101),
        (0b1_0101_0101, 9, 0b1_0101_0101),
        (2 ** 64, 0, 2 ** 64),
        (2 ** 64 + 1, 0, 2 ** 64),
        (2 ** 64, 64, 0),
    ],
)
def test_unset_bit(bigint, index, want):
    got = unset_bit(bigint, index)
    assert got == want


@pytest.mark.parametrize(
    "bigint, want",
    [
        (0, []),
        (1, [0]),
        (0b10, [1]),
        (0b11, [0, 1]),
        (0b1_0101_0101, [0, 2, 4, 6, 8]),
        (2 ** 64, [64]),
        (2 ** 64 - 1, list(range(0, 64))),
    ],
)
def test_find_ones(bigint, want):
    got = list(find_ones(bigint))
    assert got == want
