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

from opteryx.third_party.abseil.containers cimport FlatHashMap
from opteryx.compiled.structures.buffers cimport IntBuffer
from opteryx.compiled.structures.hash_table cimport HashTable
from opteryx.compiled.table_ops.hash_ops cimport compute_row_hashes
from opteryx.compiled.table_ops.null_avoidant_ops cimport non_null_row_indices


cdef:
    int64_t NULL_HASH = <int64_t>0xBADF00D
    int64_t EMPTY_HASH = <int64_t>0xBADC0FFEE
    uint64_t SEED = <uint64_t>0x9e3779b97f4a7c15


cpdef HashTable probe_side_hash_map(object relation, list join_columns):
    """
    Build a hash table for the join operations (probe-side) using buffer-level hashing.
    """
    cdef HashTable ht = HashTable()
    cdef int64_t num_rows = relation.num_rows
    cdef int64_t[::1] non_null_indices
    cdef uint64_t[::1] row_hashes = numpy.empty(num_rows, dtype=numpy.uint64)
    cdef Py_ssize_t i

    non_null_indices = non_null_row_indices(relation, join_columns)

    # Compute hash of each row on the buffer level
    compute_row_hashes(relation, join_columns, row_hashes)

    # Insert into HashTable using row index + buffer-computed hash
    for i in range(non_null_indices.shape[0]):
        ht.insert(row_hashes[non_null_indices[i]], non_null_indices[i])

    return ht


cpdef FlatHashMap build_side_hash_map(object relation, list join_columns):
    cdef FlatHashMap ht = FlatHashMap()
    cdef int64_t num_rows = relation.num_rows
    cdef int64_t[::1] non_null_indices
    cdef uint64_t[::1] row_hashes = numpy.empty(num_rows, dtype=numpy.uint64)
    cdef Py_ssize_t i

    non_null_indices = non_null_row_indices(relation, join_columns)

    compute_row_hashes(relation, join_columns, row_hashes)

    for i in range(non_null_indices.shape[0]):
        ht.insert(row_hashes[non_null_indices[i]], non_null_indices[i])

    return ht

cpdef tuple nested_loop_join(left_relation, right_relation, list left_columns, list right_columns):
    """
    A buffer-aware nested loop join using direct Arrow buffer access and hash computation.
    Only intended for small relations (<1000 rows), primarily used for correctness testing or fallbacks.
    """
    # determine the rows we're going to try to join on
    cdef int64_t[::1] left_non_null_indices = non_null_row_indices(left_relation, left_columns)
    cdef int64_t[::1] right_non_null_indices = non_null_row_indices(right_relation, right_columns)

    cdef int64_t nl = left_non_null_indices.shape[0]
    cdef int64_t nr = right_non_null_indices.shape[0]
    cdef IntBuffer left_indexes = IntBuffer()
    cdef IntBuffer right_indexes = IntBuffer()
    cdef int64_t left_non_null_idx, right_non_null_idx, left_record_idx, right_record_idx

    cdef uint64_t[::1] left_hashes = numpy.empty(nl, dtype=numpy.uint64)
    cdef uint64_t[::1] right_hashes = numpy.empty(nr, dtype=numpy.uint64)

    # remove the rows from the relations
    left_relation = left_relation.select(sorted(set(left_columns))).drop_null()
    right_relation = right_relation.select(sorted(set(right_columns))).drop_null()

    # build hashes for the columns we're joining on
    compute_row_hashes(left_relation, left_columns, left_hashes)
    compute_row_hashes(right_relation, right_columns, right_hashes)

    # Compare each pair of rows (naive quadratic approach)
    for left_non_null_idx in range(nl):
        for right_non_null_idx in range(nr):
            # if we have a match, look up the offset in the original table
            if left_hashes[left_non_null_idx] == right_hashes[right_non_null_idx]:
                left_record_idx = left_non_null_indices[left_non_null_idx]
                right_record_idx = right_non_null_indices[right_non_null_idx]
                left_indexes.append(left_record_idx)
                right_indexes.append(right_record_idx)

    return (left_indexes.to_numpy(), right_indexes.to_numpy())
