# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint64_t

from opteryx.third_party.abseil.containers cimport FlatHashSet
from opteryx.compiled.table_ops.distinct cimport process_column

import numpy
cimport numpy
numpy.import_array()

cpdef FlatHashSet count_distinct(column, FlatHashSet seen_hashes=None):

    cdef uint64_t[::1] row_hashes
    cdef Py_ssize_t num_rows, row_idx

    num_rows = len(column)
    row_hashes = numpy.zeros(num_rows, dtype=numpy.uint64)

    process_column(column, row_hashes)

    for row_idx in range(num_rows):
        seen_hashes.just_insert(row_hashes[row_idx])

    return seen_hashes
