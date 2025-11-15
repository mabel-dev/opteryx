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

from opteryx.compiled.structures.buffers cimport IntBuffer
from opteryx.compiled.table_ops.hash_ops cimport compute_row_hashes
from opteryx.compiled.table_ops.null_avoidant_ops cimport non_null_row_indices


cpdef tuple nested_loop_join(left_relation, right_relation, list left_columns, list right_columns):
    """
    Perform a buffer-aware nested loop join using Arrow buffer hashing.

    This implementation is optimized for small relations where building a hash map would be
    more expensive than a quadratic scan.
    """
    cdef int64_t[::1] left_non_null_indices = non_null_row_indices(left_relation, left_columns)
    cdef int64_t[::1] right_non_null_indices = non_null_row_indices(right_relation, right_columns)

    cdef int64_t nl = left_non_null_indices.shape[0]
    cdef int64_t nr = right_non_null_indices.shape[0]

    if nl == 0 or nr == 0:
        return numpy.empty(0, dtype=numpy.int64), numpy.empty(0, dtype=numpy.int64)

    cdef IntBuffer left_indexes = IntBuffer()
    cdef IntBuffer right_indexes = IntBuffer()
    cdef uint64_t[::1] left_hashes = numpy.empty(left_relation.num_rows, dtype=numpy.uint64)
    cdef uint64_t[::1] right_hashes = numpy.empty(right_relation.num_rows, dtype=numpy.uint64)
    cdef int64_t i, j, left_row, right_row
    cdef uint64_t left_hash, right_hash

    compute_row_hashes(left_relation, left_columns, left_hashes)
    compute_row_hashes(right_relation, right_columns, right_hashes)

    if nl <= nr:
        for i in range(nl):
            left_row = left_non_null_indices[i]
            left_hash = left_hashes[left_row]
            for j in range(nr):
                right_row = right_non_null_indices[j]
                if left_hash == right_hashes[right_row]:
                    left_indexes.append(left_row)
                    right_indexes.append(right_row)
    else:
        for j in range(nr):
            right_row = right_non_null_indices[j]
            right_hash = right_hashes[right_row]
            for i in range(nl):
                left_row = left_non_null_indices[i]
                if right_hash == left_hashes[left_row]:
                    left_indexes.append(left_row)
                    right_indexes.append(right_row)

    return left_indexes.to_numpy(), right_indexes.to_numpy()
