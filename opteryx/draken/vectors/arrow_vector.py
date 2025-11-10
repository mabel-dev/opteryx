"""ArrowVector: Fallback Vector implementation using PyArrow arrays.

This module provides ArrowVector, a Vector implementation that delegates
all operations to PyArrow's compute functions. It serves as a fallback
for data types that don't yet have optimized native Draken implementations.

The ArrowVector class provides:
- Full Vector API compatibility
- Delegation to PyArrow compute functions
- Support for all PyArrow data types
- Seamless integration with Draken's type system

This allows Draken to handle any Arrow-compatible data type while maintaining
a consistent API, even before native implementations are developed.
"""

from __future__ import annotations

import struct
from array import array
from typing import TYPE_CHECKING
from typing import Any

from opteryx.draken.vectors.vector import Vector

if TYPE_CHECKING:
    import pyarrow


NULL_HASH = 0x9E3779B97F4A7C15


class ArrowVector(Vector):
    """Fallback Vector implementation backed by a ``pyarrow.Array``."""

    def __init__(self, arrow_array: "pyarrow.Array"):
        import pyarrow as pa

        if not isinstance(arrow_array, pa.Array):
            raise TypeError("ArrowVector requires a pyarrow.Array")
        self._arr = arrow_array
        self._pa = pa
        self._pc = pa.compute

    # -------- Core metadata --------
    @property
    def length(self) -> int:
        return len(self._arr)

    @property
    def dtype(self):
        from opteryx.draken.interop.arrow import arrow_type_to_draken

        return arrow_type_to_draken(self._arr.type)

    @property
    def itemsize(self):
        try:
            return self._arr.type.bit_width // 8
        except Exception:
            return None

    def __getitem__(self, i: int):
        """Return the value at index i, or None if null."""
        if i < 0 or i >= len(self._arr):
            raise IndexError("Index out of bounds")
        v = self._arr[i]
        # pyarrow returns None for nulls
        return v

    def to_arrow(self):
        return self._arr

    # -------- Generic operations --------
    def take(self, indices) -> "ArrowVector":
        indices_arr = self._pa.array(indices, type=self._pa.int32())
        out = self._pc.take(self._arr, indices_arr)
        return ArrowVector(out)

    def equals(self, value):
        return self._pc.equal(self._arr, value).to_numpy(False).astype("bool")

    def not_equals(self, value):
        return self._pc.not_equal(self._arr, value).to_numpy(False).astype("bool")

    def greater_than(self, value):
        return self._pc.greater(self._arr, value).to_numpy(False).astype("bool")

    def greater_than_or_equals(self, value):
        return self._pc.greater_equal(self._arr, value).to_numpy(False).astype("bool")

    def less_than(self, value):
        return self._pc.less(self._arr, value).to_numpy(False).astype("bool")

    def less_than_or_equals(self, value):
        return self._pc.less_equal(self._arr, value).to_numpy(False).astype("bool")

    def sum(self):
        return self._pc.sum(self._arr).as_py()

    def min(self):
        return self._pc.min(self._arr).as_py()

    def max(self):
        return self._pc.max(self._arr).as_py()

    def is_null(self):
        return self._pc.is_null(self._arr).to_numpy(False).astype("bool")

    @property
    def null_count(self) -> int:
        """Return the number of nulls in the array."""
        return self._arr.null_count

    def to_pylist(self):
        return self._arr.to_pylist()

    def hash(self):
        """Return a ``uint64`` memory view consistent with native Draken vectors."""
        from opteryx.draken.interop.arrow import vector_from_arrow  # type: ignore[attr-defined]
        from opteryx.third_party.cyan4973.xxhash import hash_bytes  # type: ignore[attr-defined]

        # Prefer native Draken implementations when available.
        try:
            candidate = vector_from_arrow(self._arr)
        except (RuntimeError, TypeError, ValueError):  # pragma: no cover - defensive fallback
            candidate = None

        if (
            candidate is not None
            and candidate is not self
            and hasattr(candidate, "hash")
            and not isinstance(candidate, ArrowVector)
        ):
            return candidate.hash()

        values = self._arr.to_pylist()
        result = array("Q", [0] * len(values))

        for i, value in enumerate(values):
            if value is None:
                result[i] = NULL_HASH
                continue

            if isinstance(value, bool):
                result[i] = 1 if value else 0
                continue

            if isinstance(value, int):
                result[i] = value & 0xFFFFFFFFFFFFFFFF
                continue

            if isinstance(value, float):
                packed = struct.pack("<d", value)
                result[i] = int.from_bytes(packed, "little", signed=False)
                continue

            if isinstance(value, str):
                data = value.encode("utf-8")
            elif isinstance(value, (bytes, bytearray, memoryview)):
                data = bytes(value)
            else:
                data = repr(value).encode("utf-8")

            result[i] = hash_bytes(data)

        return memoryview(result)

    def __str__(self):
        return f"<ArrowVector type={self._arr.type} len={len(self._arr)} values={self._arr.to_pylist()[:10]}>"


# convenience
def from_arrow(arrow_array: Any) -> ArrowVector:
    return ArrowVector(arrow_array)
