import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pyarrow as pa
import pytest
from array import array

from opteryx.draken import Vector
from opteryx.draken.vectors._hash_api import hash_into as hash_into_vector
from opteryx.draken.vectors.arrow_vector import ArrowVector
from opteryx.third_party.cyan4973.xxhash import hash_bytes  # type: ignore[attr-defined]
NULL_HASH = 0x4c3f95a36ab8ecca
MASK = 0xFFFFFFFFFFFFFFFF


MIX_HASH_CONSTANT = 0x9e3779b97f4a7c15

def _mix_hash(current: int, value: int, mix_constant: int = MIX_HASH_CONSTANT) -> int:
    current ^= value & MASK
    current = ((current * mix_constant) + 1) & MASK
    result = current ^ (current >> 32)
    return result & MASK


def _expected_single(values: list[int]) -> list[int]:
    return [_mix_hash(0, value) for value in values]

def _as_uint64_list(buffer) -> list[int]:
    view = memoryview(buffer)
    assert view.format == "Q"
    return list(view)


def _hash_buffer(vector) -> memoryview:
    length = getattr(vector, "length", None)
    if length is None:
        try:
            length = len(vector)
        except TypeError:
            length = len(vector.to_pylist())

    out = array("Q", [0] * length)
    hash_into_vector(vector, out)
    return memoryview(out)


def test_int64_hash_returns_uint64_view():
    arrow_array = pa.array([1, -2, None], type=pa.int64())
    vector = Vector.from_arrow(arrow_array)

    hash_values = _as_uint64_list(_hash_buffer(vector))

    expected = _expected_single([
        1,
        (-2) & MASK,
        NULL_HASH,
    ])
    assert hash_values == expected


def test_string_hash_matches_xxhash3():
    arrow_array = pa.array(["abc", None, ""], type=pa.string())
    vector = Vector.from_arrow(arrow_array)

    hash_values = _as_uint64_list(_hash_buffer(vector))

    expected = _expected_single([
        hash_bytes(b"abc"),
        NULL_HASH,
        hash_bytes(b""),
    ])
    assert hash_values == expected


def test_array_vector_hash_uses_xxhash3_for_lists():
    arrow_array = pa.array([[1, 2], None, []])
    vector = Vector.from_arrow(arrow_array)

    hash_values = _as_uint64_list(_hash_buffer(vector))

    expected = _expected_single([
        hash_bytes(repr([1, 2]).encode("utf-8")),
        NULL_HASH,
        hash_bytes(repr([]).encode("utf-8")),
    ])

    assert hash_values == expected


def test_arrow_vector_hash_delegates_to_native_vector():
    arrow_array = pa.array([5, None], type=pa.int64())
    vector = ArrowVector(arrow_array)

    assert _as_uint64_list(_hash_buffer(vector)) == _expected_single([5, NULL_HASH])

if __name__ == "__main__":
    pytest.main([__file__])
