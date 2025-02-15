# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import pyarrow
import numpy
cimport numpy
numpy.import_array()

from libc.stdint cimport int64_t
from opteryx.third_party.abseil.containers cimport FlatHashSet
from libc.math cimport isnan


cdef inline object recast_column(column):
    cdef column_type = column.type

    if pyarrow.types.is_struct(column_type) or pyarrow.types.is_list(column_type):
        return numpy.array([str(a) for a in column], dtype=numpy.str_)

    # Otherwise, return the column as-is
    return column

cpdef tuple _distinct(relation, FlatHashSet seen_hashes=None, list columns=None):
    """
    This is faster but I haven't workout out how to deal with nulls...
    NULL is a valid value for DISTINCT
    """
    if columns is None:
        columns = relation.column_names

    # Memory view for the values array (for the join columns)
    cdef object[:, ::1] values_array = numpy.array(list(relation.select(columns).itercolumns()), dtype=object)

    cdef int64_t hash_value, i
    cdef list keep = []
    cdef int64_t num_columns = len(columns)

    if num_columns == 1:
        col = values_array[0, :]
        for i in range(len(col)):
            hash_value = <int64_t>hash(col[i])
            if seen_hashes.insert(hash_value):
                keep.append(i)
    else:
        for i in range(values_array.shape[1]):
            # Combine the hashes of each value in the row
            hash_value = 0
            for value in values_array[:, i]:
                hash_value = <int64_t>(hash_value * 31 + hash(value))
            if seen_hashes.insert(hash_value):
                keep.append(i)

    return keep, seen_hashes


cpdef tuple distinct(table, FlatHashSet seen_hashes=None, list columns=None):
    """
    Perform a distinct operation on the given table using an external FlatHashSet.
    """

    cdef:
        int64_t null_hash = hash(None)
        Py_ssize_t num_rows, i
        list columns_of_interest
        list columns_data = []
        list columns_hashes = []
        numpy.ndarray[int64_t] combined_hashes
        list keep = []
        object column_data
        numpy.ndarray data_array
        numpy.ndarray[int64_t] hashes

    if seen_hashes is None:
        seen_hashes = FlatHashSet()

    columns_of_interest = columns if columns is not None else table.column_names
    num_rows = table.num_rows  # Use PyArrow's num_rows attribute

    # Prepare data and compute hashes for each column
    for column_name in columns_of_interest:
        # Get the column from the table
        column = table.column(column_name)
        # Recast column if necessary
        column_data = recast_column(column)
        # Convert PyArrow array to NumPy array without copying
        if isinstance(column_data, pyarrow.ChunkedArray):
            data_array = column_data.combine_chunks().to_numpy(zero_copy_only=False)
        elif isinstance(column_data, pyarrow.Array):
            data_array = column_data.to_numpy(zero_copy_only=False)
        else:
            data_array = column_data  # Already a NumPy array

        columns_data.append(data_array)
        hashes = numpy.empty(num_rows, dtype=numpy.int64)

        # Determine data type and compute hashes accordingly
        if numpy.issubdtype(data_array.dtype, numpy.integer):
            compute_int_hashes(data_array, null_hash, hashes)
        elif numpy.issubdtype(data_array.dtype, numpy.floating):
            compute_float_hashes(data_array, null_hash, hashes)
        elif data_array.dtype == numpy.object_:
            compute_object_hashes(data_array, null_hash, hashes)
        else:
            # For other types (e.g., strings), treat as object
            compute_object_hashes(data_array.astype(numpy.object_), null_hash, hashes)

        columns_hashes.append(hashes)

    # Combine the hashes per row
    combined_hashes = columns_hashes[0]
    for hashes in columns_hashes[1:]:
        combined_hashes = combined_hashes * 31 + hashes

    # Check for duplicates using the HashSet
    for i in range(num_rows):
        if seen_hashes.insert(combined_hashes[i]):
            keep.append(i)

    return keep, seen_hashes

cdef void compute_float_hashes(numpy.ndarray[numpy.float64_t] data, int64_t null_hash, int64_t[:] hashes):
    cdef Py_ssize_t i, n = data.shape[0]
    cdef numpy.float64_t value
    for i in range(n):
        value = data[i]
        if isnan(value):
            hashes[i] = null_hash
        else:
            hashes[i] = hash(value)


cdef void compute_int_hashes(numpy.ndarray[numpy.int64_t] data, int64_t null_hash, int64_t[:] hashes):
    cdef Py_ssize_t i, n = data.shape[0]
    cdef numpy.int64_t value
    for i in range(n):
        value = data[i]
        # Assuming a specific value represents missing data
        # Adjust this condition based on how missing integers are represented
        if value == numpy.iinfo(numpy.int64).min:
            hashes[i] = null_hash
        else:
            hashes[i] = value  # Hash of int is the int itself in Python 3

cdef void compute_object_hashes(numpy.ndarray data, int64_t null_hash, int64_t[:] hashes):
    cdef Py_ssize_t i, n = data.shape[0]
    cdef object value
    for i in range(n):
        value = data[i]
        if value is None:
            hashes[i] = null_hash
        else:
            hashes[i] = hash(value)
