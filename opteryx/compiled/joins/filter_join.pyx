# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False


import numpy as np

cimport numpy as cnp
from libc.stdint cimport int64_t
from cpython.object cimport PyObject_Hash

from opteryx.third_party.abseil.containers cimport FlatHashSet

cpdef FlatHashSet filter_join_set(relation, list join_columns, FlatHashSet seen_hashes):
    """
    Build the set for the right of a filter join (ANTI/SEMI)
    """

    cdef int64_t num_columns = len(join_columns)

    if seen_hashes is None:
        seen_hashes = FlatHashSet()

    # Memory view for the values array (for the join columns)
    cdef object[:, ::1] values_array = np.array(list(relation.select(join_columns).drop_null().itercolumns()), dtype=object)

    cdef int64_t hash_value, i

    if num_columns == 1:
        col = values_array[0, :]
        for i in range(len(col)):
            hash_value = PyObject_Hash(col[i])
            seen_hashes.insert(hash_value)
    else:
        for i in range(values_array.shape[1]):
            # Combine the hashes of each value in the row
            hash_value = 0
            for value in values_array[:, i]:
                hash_value = <int64_t>(hash_value * 31 + PyObject_Hash(value))
            seen_hashes.insert(hash_value)

    return seen_hashes

cpdef anti_join(relation, list join_columns, FlatHashSet seen_hashes):
    cdef int64_t num_columns = len(join_columns)
    cdef int64_t num_rows = relation.shape[0]
    cdef int64_t hash_value, i
    cdef cnp.ndarray[int64_t, ndim=1] index_buffer = np.empty(num_rows, dtype=np.int64)
    cdef int64_t idx_count = 0

    cdef object[:, ::1] values_array = np.array(list(relation.select(join_columns).drop_null().itercolumns()), dtype=object)

    if num_columns == 1:
        col = values_array[0, :]
        for i in range(len(col)):
            hash_value = PyObject_Hash(col[i])
            if not seen_hashes.contains(hash_value):
                index_buffer[idx_count] = i
                idx_count += 1
    else:
        for i in range(values_array.shape[1]):
            # Combine the hashes of each value in the row
            hash_value = 0
            for value in values_array[:, i]:
                hash_value = <int64_t>(hash_value * 31 + PyObject_Hash(value))
            if not seen_hashes.contains(hash_value):
                index_buffer[idx_count] = i
                idx_count += 1

    if idx_count > 0:
        return relation.take(index_buffer[:idx_count])
    else:
        return relation.slice(0, 0)


cpdef semi_join(relation, list join_columns, FlatHashSet seen_hashes):
    cdef int64_t num_columns = len(join_columns)
    cdef int64_t num_rows = relation.shape[0]
    cdef int64_t hash_value, i
    cdef cnp.ndarray[int64_t, ndim=1] index_buffer = np.empty(num_rows, dtype=np.int64)
    cdef int64_t idx_count = 0

    cdef object[:, ::1] values_array = np.array(list(relation.select(join_columns).drop_null().itercolumns()), dtype=object)

    if num_columns == 1:
        col = values_array[0, :]
        for i in range(len(col)):
            hash_value = PyObject_Hash(col[i])
            if seen_hashes.contains(hash_value):
                index_buffer[idx_count] = i
                idx_count += 1
    else:
        for i in range(values_array.shape[1]):
            # Combine the hashes of each value in the row
            hash_value = 0
            for value in values_array[:, i]:
                hash_value = <int64_t>(hash_value * 31 + PyObject_Hash(value))
            if seen_hashes.contains(hash_value):
                index_buffer[idx_count] = i
                idx_count += 1

    if idx_count > 0:
        return relation.take(index_buffer[:idx_count])
    else:
        return relation.slice(0, 0)
