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

from libc.stdint cimport uint64_t

from opteryx.third_party.abseil.containers cimport FlatHashSet
from opteryx.compiled.structures.buffers cimport IntBuffer
from opteryx.compiled.table_ops.hash_ops cimport process_column

cpdef tuple distinct(table, FlatHashSet seen_hashes=None, list columns=None):
    """
    DISTINCT using xxhash and direct Arrow buffer access
    """
    cdef:
        Py_ssize_t num_rows, row_idx
        list columns_of_interest
        uint64_t[::1] row_hashes

    columns_of_interest = columns if columns else table.column_names
    num_rows = table.num_rows
    row_hashes = numpy.zeros(num_rows, dtype=numpy.uint64)

    # Process columns directly from Arrow buffers
    for col_name in columns_of_interest:
        column = table.column(col_name)
        process_column(column, row_hashes)

    # Deduplicate using hash set
    cdef IntBuffer keep = IntBuffer()
    if seen_hashes is None:
        seen_hashes = FlatHashSet()

    for row_idx in range(num_rows):
        if seen_hashes.insert(row_hashes[row_idx]):
            keep.append(row_idx)

    return keep.to_numpy(), seen_hashes
