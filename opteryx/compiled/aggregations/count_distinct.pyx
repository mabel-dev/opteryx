# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: embedsignature=False
# cython: c_string_type=bytes
# cython: c_string_encoding=ascii
# cython: profile=True
# cython: linetrace=True

from libc.stdint cimport uint64_t

from opteryx.third_party.abseil.containers cimport FlatHashSet
cimport cython
from libc.stdlib cimport malloc, free
from libc.string cimport memset

from opteryx.draken.interop.arrow cimport vector_from_arrow
from opteryx.draken.vectors.vector cimport Vector

import pyarrow


cdef inline FlatHashSet _count_distinct(object column, FlatHashSet seen_hashes):
    """Fast distinct counter that hashes via Draken vectors when possible."""

    cdef list chunks
    cdef Vector draken_vector
    cdef Py_ssize_t row_count = 0
    cdef Py_ssize_t num_chunks = 0
    cdef Py_ssize_t i
    cdef uint64_t* data_ptr = NULL
    cdef uint64_t[::1] hash_buffer
    cdef Py_ssize_t max_rows = 0

    if seen_hashes is None:
        seen_hashes = FlatHashSet()

    # Get chunks efficiently
    if isinstance(column, pyarrow.ChunkedArray):
        chunks = column.chunks
        num_chunks = len(chunks)
    else:
        chunks = [column]
        num_chunks = 1

    # Find max chunk size
    for i in range(num_chunks):
        max_rows = max(max_rows, len(chunks[i]))

    if max_rows > 0:
        data_ptr = <uint64_t*>malloc(max_rows * cython.sizeof(uint64_t))
        if data_ptr == NULL:
            raise MemoryError("Failed to allocate hash buffer")

    try:
        for i in range(num_chunks):
            chunk = chunks[i]
            row_count = len(chunk)
            if row_count == 0:
                continue

            memset(data_ptr, 0, row_count * cython.sizeof(uint64_t))
            hash_buffer = <uint64_t[:row_count]>data_ptr
            draken_vector = <Vector>vector_from_arrow(chunk)
            draken_vector.hash_into(hash_buffer)

            seen_hashes.insert_many(data_ptr, row_count)
    finally:
        if data_ptr != NULL:
            free(data_ptr)

    return seen_hashes


cpdef FlatHashSet count_distinct(object column, FlatHashSet seen_hashes):
    return _count_distinct(column, seen_hashes)


cpdef FlatHashSet count_distinct_draken(object column, FlatHashSet seen_hashes):
    return _count_distinct(column, seen_hashes)
