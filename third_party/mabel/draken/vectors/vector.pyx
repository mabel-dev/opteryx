# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Base Vector class for Draken columnar data structures.

This module provides the abstract base class for all Vector implementations
in Draken. Vectors are columnar data containers that provide:
- Zero-copy interoperability with Apache Arrow
- Efficient memory layout for analytical workloads
- Type-specific optimized implementations

The Vector class defines the common interface that all concrete vector
types (Int64Vector, StringVector, etc.) implement.
"""

from libc.stdint cimport uint64_t

from opteryx.draken.interop.arrow cimport vector_from_arrow

cdef const uint64_t MIX_HASH_CONSTANT = <uint64_t>0x9e3779b97f4a7c15ULL
cdef const uint64_t NULL_HASH = <uint64_t>0x4c3f95a36ab8eccaULL

cdef class Vector:

    @classmethod
    def from_arrow(cls, arrow_array):
        return vector_from_arrow(arrow_array)

    cpdef object null_bitmap(self):
        """Return the null bitmap for this vector, or ``None`` when the vector has no nulls."""
        return None

    def __str__(self):
        return f"<{self.__class__.__name__} len={len(self)}>"

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0,
    ) except *:
        """Default implementation delegates to Python overrides when available."""
        cdef object py_self = <object>self
        cdef object py_hash = getattr(py_self, "hash_into", None)

        if py_hash is None:
            raise NotImplementedError(
                f"{self.__class__.__name__} does not implement hash_into"
            )

        py_hash(out_buf, offset=offset)
