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

from array import array

from opteryx.draken.vectors.vector cimport Vector
from opteryx.draken.core.buffers cimport DRAKEN_ARRAY
from opteryx.third_party.cyan4973.xxhash import hash_bytes


NULL_HASH: int = 0x9E3779B97F4A7C15


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
        import pyarrow as pa
        
        pc = pa.compute
        indices_arr = pa.array(indices, type=pa.int32())
        out = pc.take(self._arr, indices_arr)
        result = ArrayVector()
        result._arr = out
        return result

    def is_null(self):
        import pyarrow as pa
        
        pc = pa.compute
        return pc.is_null(self._arr).to_numpy(False).astype("bool")

    @property
    def null_count(self) -> int:
        """Return the number of nulls in the array."""
        return self._arr.null_count if self._arr is not None else 0

    def to_pylist(self):
        return self._arr.to_pylist() if self._arr is not None else []

    def hash(self):
        """Return a ``uint64`` memory view of xxHash3 digests for each element."""
        values = self._arr.to_pylist() if self._arr is not None else []
        result = array("Q", [0] * len(values))

        for i, value in enumerate(values):
            if value is None:
                result[i] = NULL_HASH
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
        if self._arr is None:
            return "<ArrayVector uninitialized>"
        return f"<ArrayVector type={self._arr.type} len={len(self._arr)} values={self._arr.to_pylist()[:10]}>"


cdef ArrayVector from_arrow(object array):
    vec = ArrayVector()
    vec._arr = array
    return vec
