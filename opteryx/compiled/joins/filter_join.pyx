# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import numpy
cimport numpy
numpy.import_array()

from libc.stdint cimport int64_t, uint64_t
from opteryx.compiled.table_ops.hash_ops cimport compute_row_hashes
from opteryx.third_party.abseil.containers cimport FlatHashSet


cpdef FlatHashSet filter_join_set(table, list columns=None, FlatHashSet seen_hashes=None):
    cdef:
        Py_ssize_t num_rows = table.num_rows
        uint64_t[::1] row_hashes = numpy.empty(num_rows, dtype=numpy.uint64)
        list columns_of_interest = columns if columns else table.column_names
        Py_ssize_t row_idx

    compute_row_hashes(table, columns_of_interest, row_hashes)

    if seen_hashes is None:
        seen_hashes = FlatHashSet()

    for row_idx in range(num_rows):
        seen_hashes.insert(row_hashes[row_idx])

    return seen_hashes


cpdef semi_join(object relation, list join_columns, FlatHashSet seen_hashes):
    cdef:
        Py_ssize_t num_rows = relation.num_rows
        Py_ssize_t row_idx
        Py_ssize_t count = 0
        uint64_t[::1] row_hashes = numpy.empty(num_rows, dtype=numpy.uint64)
        numpy.ndarray[int64_t, ndim=1] index_buffer = numpy.empty(num_rows, dtype=numpy.int64)

    compute_row_hashes(relation, join_columns, row_hashes)

    for row_idx in range(num_rows):
        if seen_hashes.contains(row_hashes[row_idx]):
            index_buffer[count] = row_idx
            count += 1

    return relation.take(index_buffer[:count]) if count > 0 else relation.slice(0, 0)

cpdef anti_join(object relation, list join_columns, FlatHashSet seen_hashes):
    cdef:
        Py_ssize_t num_rows = relation.num_rows
        Py_ssize_t row_idx
        Py_ssize_t count = 0
        uint64_t[::1] row_hashes = numpy.empty(num_rows, dtype=numpy.uint64)
        numpy.ndarray[int64_t, ndim=1] index_buffer = numpy.empty(num_rows, dtype=numpy.int64)

    compute_row_hashes(relation, join_columns, row_hashes)

    for row_idx in range(num_rows):
        if not seen_hashes.contains(row_hashes[row_idx]):
            index_buffer[count] = row_idx
            count += 1

    return relation.take(index_buffer[:count]) if count > 0 else relation.slice(0, 0)
