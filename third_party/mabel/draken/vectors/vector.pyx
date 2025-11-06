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

from draken.interop.arrow cimport vector_from_arrow

cdef class Vector:

    @classmethod
    def from_arrow(cls, arrow_array):
        return vector_from_arrow(arrow_array)

    cpdef object null_bitmap(self):
        """Return the null bitmap for this vector, or ``None`` when the vector has no nulls."""
        return None

    def __str__(self):
        return f"<{self.__class__.__name__} len={len(self)}>"
