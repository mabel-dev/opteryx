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

from typing import Any

from draken._optional import require_pyarrow
from draken.vectors.vector import Vector


class ArrowVector(Vector):
    """Fallback Vector implementation backed by a ``pyarrow.Array``."""

    def __init__(self, arr: Any):
        pa = require_pyarrow("ArrowVector")
        if not isinstance(arr, pa.Array):
            raise TypeError("ArrowVector requires a pyarrow.Array")
        self._arr = arr
        self._pa = pa
        self._pc = pa.compute

    # -------- Core metadata --------
    @property
    def length(self) -> int:
        return len(self._arr)

    @property
    def dtype(self):
        from draken.interop.arrow import arrow_type_to_draken

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
        # Arrow has experimental hash kernels; fallback: use Python's hash
        try:
            return self._pc.hash(self._arr).to_numpy(False).astype("uint64")
        except Exception:
            return [hash(v) if v is not None else 0 for v in self._arr.to_pylist()]

    def __str__(self):
        return f"<ArrowVector type={self._arr.type} len={len(self._arr)} values={self._arr.to_pylist()[:10]}>"


# convenience
def from_arrow(array: Any) -> ArrowVector:
    return ArrowVector(array)
