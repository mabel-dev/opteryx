import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pyarrow as pa
import pytest

from opteryx.draken.vectors.arrow_vector import ArrowVector
from opteryx.draken.vectors.vector import Vector  # type: ignore[attr-defined]
from opteryx.third_party.cyan4973.xxhash import hash_bytes  # type: ignore[attr-defined]

NULL_HASH = 0x9E3779B97F4A7C15

def _as_uint64_list(buffer) -> list[int]:
    view = memoryview(buffer)
    assert view.format == "Q"
    return list(view)


def test_int64_hash_returns_uint64_view():
    array = pa.array([1, -2, None], type=pa.int64())
    vector = Vector.from_arrow(array)

    hash_values = _as_uint64_list(vector.hash())

    assert hash_values == [1, (-2) & 0xFFFFFFFFFFFFFFFF, NULL_HASH]


def test_string_hash_matches_xxhash3():
    array = pa.array(["abc", None, ""], type=pa.string())
    vector = Vector.from_arrow(array)

    hash_values = _as_uint64_list(vector.hash())

    expected = [
        hash_bytes(b"abc"),
        NULL_HASH,
        hash_bytes(b""),
    ]
    assert hash_values == expected


def test_array_vector_hash_uses_xxhash3_for_lists():
    array = pa.array([[1, 2], None, []])
    vector = Vector.from_arrow(array)

    hash_values = _as_uint64_list(vector.hash())

    expected = [
        hash_bytes(repr([1, 2]).encode("utf-8")),
        NULL_HASH,
        hash_bytes(repr([]).encode("utf-8")),
    ]

    assert hash_values == expected


def test_arrow_vector_hash_delegates_to_native_vector():
    array = pa.array([5, None], type=pa.int64())
    vector = ArrowVector(array)

    assert _as_uint64_list(vector.hash()) == [5, NULL_HASH]

if __name__ == "__main__":
    pytest.main([__file__])
