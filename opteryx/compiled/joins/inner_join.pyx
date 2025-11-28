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
from libc.stddef cimport size_t
from libcpp.vector cimport vector

from time import perf_counter_ns
cimport cython

from opteryx.third_party.abseil.containers cimport (
    FlatHashMap,
    IdentityHash,
    flat_hash_map,
)
from opteryx.compiled.structures.buffers cimport CIntBuffer, IntBuffer
from opteryx.compiled.table_ops.hash_ops cimport compute_row_hashes
from opteryx.compiled.table_ops.null_avoidant_ops cimport non_null_row_indices

cdef extern from "join_kernels.h":
    void inner_join_probe(
        flat_hash_map[uint64_t, vector[int64_t], IdentityHash]* left_map,
        const int64_t* non_null_indices,
        size_t non_null_count,
        const uint64_t* row_hashes,
        size_t row_hash_count,
        CIntBuffer* left_out,
        CIntBuffer* right_out
    ) nogil

cdef public long long last_hash_time_ns = 0
cdef public long long last_probe_time_ns = 0
cdef public long long last_materialize_time_ns = 0
cdef public Py_ssize_t last_rows_hashed = 0
cdef public Py_ssize_t last_candidate_rows = 0
cdef public Py_ssize_t last_result_rows = 0


cpdef tuple inner_join(object right_relation, list join_columns, FlatHashMap left_hash_table):
    """
    Perform an inner join between a right-hand relation and a pre-built left-side hash table.
    This function uses precomputed hashes and avoids null rows for optimal speed.
    """
    global last_hash_time_ns, last_probe_time_ns, last_materialize_time_ns
    global last_rows_hashed, last_candidate_rows, last_result_rows
    cdef IntBuffer left_indexes = IntBuffer()
    cdef IntBuffer right_indexes = IntBuffer()
    cdef int64_t num_rows = right_relation.num_rows
    cdef int64_t[::1] non_null_indices = non_null_row_indices(right_relation, join_columns)
    cdef Py_ssize_t candidate_count = non_null_indices.shape[0]

    if candidate_count == 0 or num_rows == 0:
        last_hash_time_ns = 0
        last_probe_time_ns = 0
        last_rows_hashed = num_rows
        last_candidate_rows = candidate_count
        last_result_rows = 0
        last_materialize_time_ns = 0
        return numpy.empty(0, dtype=numpy.int64), numpy.empty(0, dtype=numpy.int64)

    cdef uint64_t[::1] row_hashes = numpy.empty(num_rows, dtype=numpy.uint64)
    cdef long long t_start = perf_counter_ns()

    # Precompute hashes for right relation
    compute_row_hashes(right_relation, join_columns, row_hashes)
    cdef long long t_after_hash = perf_counter_ns()
    last_hash_time_ns = t_after_hash - t_start

    with nogil:
        with cython.boundscheck(False):
            inner_join_probe(
                &left_hash_table._map,
                &non_null_indices[0],
                <size_t>candidate_count,
                &row_hashes[0],
                <size_t>num_rows,
                left_indexes.c_buffer,
                right_indexes.c_buffer,
            )
    cdef long long t_after_probe = perf_counter_ns()
    last_probe_time_ns = t_after_probe - t_after_hash
    last_rows_hashed = num_rows
    last_candidate_rows = candidate_count

    # Return matched row indices from both sides
    cdef long long t_before_numpy = perf_counter_ns()
    cdef numpy.ndarray[int64_t, ndim=1] left_np = left_indexes.to_numpy()
    cdef numpy.ndarray[int64_t, ndim=1] right_np = right_indexes.to_numpy()
    cdef long long t_after_numpy = perf_counter_ns()
    last_result_rows = left_np.shape[0]
    last_materialize_time_ns = t_after_numpy - t_before_numpy

    return left_np, right_np


cpdef tuple get_last_inner_join_metrics():
    """Return instrumentation captured during the most recent inner join call."""
    return (
        last_hash_time_ns,
        last_probe_time_ns,
        last_rows_hashed,
        last_candidate_rows,
        last_result_rows,
        last_materialize_time_ns,
    )


cpdef FlatHashMap build_side_hash_map(object relation, list join_columns):
    """
    Builds a hash map from non-null rows of the given relation using the specified join columns.
    Used to support hash-based joins.
    """
    cdef FlatHashMap ht = FlatHashMap()
    cdef int64_t num_rows = relation.num_rows
    cdef int64_t[::1] non_null_indices = non_null_row_indices(relation, join_columns)
    cdef uint64_t[::1] row_hashes = numpy.empty(num_rows, dtype=numpy.uint64)
    cdef int64_t i, row_idx

    compute_row_hashes(relation, join_columns, row_hashes)

    for i in range(non_null_indices.shape[0]):
        row_idx = non_null_indices[i]
        ht.insert(row_hashes[row_idx], row_idx)

    return ht
