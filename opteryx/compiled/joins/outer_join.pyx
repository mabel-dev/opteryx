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

from opteryx.compiled.structures.hash_table cimport HashTable
from opteryx.compiled.table_ops.hash_ops cimport compute_row_hashes
from opteryx.compiled.table_ops.null_avoidant_ops cimport non_null_row_indices


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
