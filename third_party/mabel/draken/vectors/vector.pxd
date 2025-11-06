# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from draken.interop.arrow import vector_from_arrow

cdef class Vector:
    cdef bint here
    cpdef object null_bitmap(self)
