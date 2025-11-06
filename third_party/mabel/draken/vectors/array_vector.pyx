# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
ArrayVector: Cython implementation for array/list column vector for Draken.

This module provides:
- The ArrayVector class for array/list column storage
- Arrow interoperability for zero-copy conversion
- Support for nested types

For now, this is a lightweight wrapper around PyArrow arrays since array types
can contain any element type and require complex handling.
"""

from draken.vectors.vector cimport Vector
from draken.core.buffers cimport DRAKEN_ARRAY
from draken._optional import require_pyarrow


cdef class ArrayVector(Vector):

    def __cinit__(self):
        self._arr = None

    @property
    def length(self):
        return len(self._arr) if self._arr is not None else 0

    @property
    def dtype(self):
        return DRAKEN_ARRAY

    @property
    def itemsize(self):
        # Arrays have variable size, return 0
        return 0

    def __getitem__(self, i: int):
        """Return the value at index i, or None if null."""
        if self._arr is None:
            raise IndexError("Array is not initialized")
        if i < 0 or i >= len(self._arr):
            raise IndexError("Index out of bounds")
        val = self._arr[i]
        return val.as_py() if val.is_valid else None

    def to_arrow(self):
        return self._arr

    # -------- Generic operations --------
    def take(self, indices):
        pa = require_pyarrow("ArrayVector.take()")
        pc = pa.compute
        indices_arr = pa.array(indices, type=pa.int32())
        out = pc.take(self._arr, indices_arr)
        result = ArrayVector()
        result._arr = out
        return result

    def is_null(self):
        pa = require_pyarrow("ArrayVector.is_null()")
        pc = pa.compute
        return pc.is_null(self._arr).to_numpy(False).astype("bool")

    @property
    def null_count(self) -> int:
        """Return the number of nulls in the array."""
        return self._arr.null_count if self._arr is not None else 0

    def to_pylist(self):
        return self._arr.to_pylist() if self._arr is not None else []

    def hash(self):
        # Use Python's hash for list elements (Arrow doesn't hash lists)
        return [hash(tuple(v)) if v is not None else 0 for v in self._arr.to_pylist()]

    def __str__(self):
        if self._arr is None:
            return "<ArrayVector uninitialized>"
        return f"<ArrayVector type={self._arr.type} len={len(self._arr)} values={self._arr.to_pylist()[:10]}>"


cdef ArrayVector from_arrow(object array):
    vec = ArrayVector()
    vec._arr = array
    return vec
