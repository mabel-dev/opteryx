# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int32_t, int64_t, uint8_t, uint64_t
import numpy
cimport numpy

cdef numpy.ndarray[int64_t, ndim=1] non_null_row_indices(object relation, list column_names)